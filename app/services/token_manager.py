"""
app/services/token_manager.py

Dhan token lifecycle management:
  1. Token stored in DB with expiry timestamp (24h from generation)
  2. On every API call — check expiry FIRST, no Dhan call if token valid
  3. On DH-906 — refresh once with asyncio.Lock (no parallel refresh storms)
  4. Scheduled 8:00 AM IST refresh for all clients (fresh before 9:15 AM market open)
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.encryption import encrypt, decrypt
from app.services.dhan import generate_access_token

IST = timezone(timedelta(hours=5, minutes=30))

_refresh_locks: dict[str, asyncio.Lock] = {}

def _get_lock(profile_id: str) -> asyncio.Lock:
    if profile_id not in _refresh_locks:
        _refresh_locks[profile_id] = asyncio.Lock()
    return _refresh_locks[profile_id]


def token_is_valid(profile) -> bool:
    """Return True if stored token is present and not expired (30-min buffer)."""
    cred = profile.dhan_cred
    if not cred or not cred.is_active:
        return False
    if not cred.token_expires_at:
        return False
    now = datetime.now(timezone.utc)
    expires = cred.token_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return now < (expires - timedelta(minutes=30))


def get_stored_token(profile) -> Optional[str]:
    """Return decrypted stored token, or None."""
    cred = profile.dhan_cred
    if not cred:
        return None
    try:
        t = decrypt(cred.access_token)
        return t if t and t.strip() else None
    except Exception:
        return None


def _get_proxy_kwargs(profile) -> dict:
    """Extract proxy kwargs from profile.proxy_setting."""
    p = profile.proxy_setting
    if not p or not p.is_active:
        return {}
    try:
        return {
            "proxy_scheme": p.scheme or "https",
            "proxy_host":   p.host,
            "proxy_port":   p.port,
            "proxy_user":   p.username,
            "proxy_pass":   decrypt(p.password) if p.password else None,
        }
    except Exception:
        return {}


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
    Generate fresh Dhan token via TOTP and store in DB.
    Lock prevents parallel refresh storms.
    """
    lock = _get_lock(profile.id)

    async with lock:
        # Re-check after lock — another request may have already refreshed
        db.refresh(profile)
        if token_is_valid(profile) and reason != "forced":
            stored = get_stored_token(profile)
            if stored:
                print("[token_manager] Token already refreshed by parallel request — reusing")
                return stored

        print(f"[token_manager] Refreshing token for profile {profile.id} (reason: {reason})")
        result = await generate_access_token(client_id, pin, totp, **proxy_kw)

        if not result["success"]:
            print(f"[token_manager] Refresh failed: {result['message']}")
            return None

        new_token = result["access_token"]
        cred = profile.dhan_cred
        if cred:
            cred.access_token     = encrypt(new_token)
            cred.is_active        = True
            cred.last_error       = None
            cred.last_verified    = datetime.now(timezone.utc)
            cred.token_expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
            db.commit()
            print(f"[token_manager] Token stored, expires at {cred.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')}")

        return new_token


async def scheduled_morning_refresh(db_factory):
    """
    Background task — refreshes all client tokens at 8:00 AM IST every day.
    Staggered 5s between clients to avoid Dhan rate limits.
    """
    from app.models.trading import ClientProfile, DhanCredential

    while True:
        now_ist = datetime.now(IST)
        target  = now_ist.replace(hour=8, minute=0, second=0, microsecond=0)
        if now_ist >= target:
            target += timedelta(days=1)
        wait_secs = (target - now_ist).total_seconds()

        print(f"[token_manager] Next scheduled refresh in {wait_secs/3600:.1f}h "
              f"(at {target.strftime('%Y-%m-%d %H:%M IST')})")

        await asyncio.sleep(wait_secs)

        print("[token_manager] === 8 AM scheduled token refresh starting ===")
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

                    proxy_kw = _get_proxy_kwargs(profile)

                    await asyncio.sleep(5)  # stagger between clients

                    result = await generate_access_token(client_id, pin, totp, **proxy_kw)
                    if result["success"]:
                        cred.access_token     = encrypt(result["access_token"])
                        cred.is_active        = True
                        cred.last_error       = None
                        cred.last_verified    = datetime.now(timezone.utc)
                        cred.token_expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
                        db.commit()
                        print(f"[token_manager] Refreshed: profile {profile.id}")
                    else:
                        print(f"[token_manager] Failed: profile {profile.id}: {result['message']}")

                except Exception as e:
                    print(f"[token_manager] Error: profile {profile.id}: {e}")

        except Exception as e:
            print(f"[token_manager] Scheduled refresh error: {e}")
        finally:
            db.close()

        print("[token_manager] === Scheduled refresh complete ===")
