"""
app/services/token_manager.py

Dhan token lifecycle management:

  1. Token stored in DB with expiry timestamp (24h from generation)
  2. On every API call — check expiry FIRST, call Dhan only if valid
  3. On DH-906 — refresh once, with an asyncio.Lock to prevent parallel refresh storms
  4. Scheduled refresh at 8:00 AM IST every day for all active clients
     (so tokens are always fresh before market opens at 9:15 AM)

This eliminates:
  - Redundant mid-session refreshes
  - Parallel "Token can be generated once every 2 minutes" errors
  - Latency on every request from unnecessary refresh attempts
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.encryption import encrypt, decrypt
from app.services.dhan import generate_access_token

# ── IST timezone ──────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))

# ── Per-profile refresh lock — prevents parallel refresh storms ───────────────
# Key: profile_id → asyncio.Lock
_refresh_locks: dict[str, asyncio.Lock] = {}

def _get_lock(profile_id: str) -> asyncio.Lock:
    if profile_id not in _refresh_locks:
        _refresh_locks[profile_id] = asyncio.Lock()
    return _refresh_locks[profile_id]


# ── Token expiry check ────────────────────────────────────────────────────────

def token_is_valid(profile) -> bool:
    """
    Return True if the stored token is present AND not expired.
    Dhan tokens last 24h — we use a 30-min safety buffer.
    """
    cred = profile.dhan_cred
    if not cred or not cred.is_active:
        return False

    # No expiry recorded → assume expired (will refresh once)
    if not cred.token_expires_at:
        return False

    now = datetime.now(timezone.utc)
    expires = cred.token_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)

    # 30-minute buffer before actual expiry
    return now < (expires - timedelta(minutes=30))


def get_stored_token(profile) -> Optional[str]:
    """Return decrypted stored token, or None if not available."""
    cred = profile.dhan_cred
    if not cred:
        return None
    try:
        t = decrypt(cred.access_token)
        return t if t and t.strip() else None
    except Exception:
        return None


# ── Token refresh with lock ───────────────────────────────────────────────────

async def refresh_token(
    profile,
    client_id: str,
    pin: str,
    totp: str,
    db: Session,
    reason: str = "manual",
    **proxy_kw,
) -> Optional[str]:
    """
    Generate a fresh Dhan token via TOTP and store it in DB.
    Uses per-profile lock so only ONE refresh happens even with parallel requests.

    Returns new token string, or None if generation failed.
    """
    lock = _get_lock(profile.id)

    # If another coroutine already holds the lock, wait for it then
    # check if the token was already refreshed — avoid double refresh
    async with lock:
        # Re-check after acquiring lock — another request may have already refreshed
        db.refresh(profile)
        if token_is_valid(profile) and reason != "forced":
            stored = get_stored_token(profile)
            if stored:
                print(f"[token_manager] Token already refreshed by parallel request — reusing")
                return stored

        print(f"[token_manager] Refreshing token for profile {profile.id} (reason: {reason})")
        result = await generate_access_token(client_id, pin, totp, **proxy_kw)

        if not result["success"]:
            print(f"[token_manager] Refresh failed: {result['message']}")
            return None

        new_token = result["access_token"]
        cred = profile.dhan_cred
        if cred:
            cred.access_token    = encrypt(new_token)
            cred.is_active       = True
            cred.last_error      = None
            cred.last_verified   = datetime.now(timezone.utc)
            # Store expiry = now + 24h (Dhan tokens are valid 24h)
            cred.token_expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
            db.commit()
            print(f"[token_manager] Token stored, expires at {cred.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')}")

        return new_token


# ── 8 AM IST scheduled refresh ────────────────────────────────────────────────

async def scheduled_morning_refresh(db_factory):
    """
    Run in background. Every day at 8:00 AM IST refresh tokens for ALL
    clients that have valid credentials — so market opens with fresh tokens.

    Call this from main.py lifespan with:
        asyncio.create_task(scheduled_morning_refresh(SessionLocal))
    """
    from app.models.trading import ClientProfile, DhanCredential
    from app.core.encryption import decrypt

    while True:
        now_ist = datetime.now(IST)
        # Calculate seconds until next 8:00 AM IST
        target = now_ist.replace(hour=8, minute=0, second=0, microsecond=0)
        if now_ist >= target:
            target += timedelta(days=1)
        wait_secs = (target - now_ist).total_seconds()

        print(f"[token_manager] Next scheduled refresh in {wait_secs/3600:.1f}h "
              f"(at {target.strftime('%Y-%m-%d %H:%M IST')})")

        await asyncio.sleep(wait_secs)

        print(f"[token_manager] === 8 AM scheduled token refresh starting ===")
        db: Session = db_factory()
        try:
            profiles = (
                db.query(ClientProfile)
                .join(DhanCredential, DhanCredential.client_profile_id == ClientProfile.id)
                .all()
            )
            print(f"[token_manager] Refreshing tokens for {len(profiles)} client(s)")

            for profile in profiles:
                cred = profile.dhan_cred
                if not cred:
                    continue
                try:
                    client_id = decrypt(cred.dhan_client_id)
                    pin       = decrypt(cred.pin)
                    totp      = decrypt(cred.totp_secret)
                    if not client_id or not pin or not totp:
                        continue

                    # Stagger by 5 seconds between clients to avoid Dhan rate limits
                    await asyncio.sleep(5)

                    result = await generate_access_token(client_id, pin, totp)
                    if result["success"]:
                        cred.access_token     = encrypt(result["access_token"])
                        cred.is_active        = True
                        cred.last_error       = None
                        cred.last_verified    = datetime.now(timezone.utc)
                        cred.token_expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
                        db.commit()
                        print(f"[token_manager] ✓ Refreshed: profile {profile.id}")
                    else:
                        print(f"[token_manager] ✗ Failed: profile {profile.id}: {result['message']}")

                except Exception as e:
                    print(f"[token_manager] Error refreshing profile {profile.id}: {e}")

        except Exception as e:
            print(f"[token_manager] Scheduled refresh error: {e}")
        finally:
            db.close()

        print(f"[token_manager] === Scheduled refresh complete ===")
