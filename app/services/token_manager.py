"""
app/services/token_manager.py

Dhan token lifecycle management:
  1. Token stored in DB with expiry timestamp (24h from generation)
  2. On every API call — check expiry FIRST, no Dhan call if token valid
  3. On DH-906 — refresh once with asyncio.Lock (no parallel refresh storms)
  4. Scheduled 8:00 AM IST refresh for all clients with retry every 5 min
     until success or market opens (9:00 AM)
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.encryption import encrypt, decrypt
from app.services.dhan import generate_access_token

IST = timezone(timedelta(hours=5, minutes=30))

_refresh_locks: dict[str, asyncio.Lock] = {}

# Profiles whose login can never succeed without the client fixing their credentials
# (bad TOTP secret, missing PIN…). Skipped for 6 h so one broken client can't keep the morning job looping.
_permanent_until: dict[str, float] = {}
_PERMANENT_MARKERS = ("non-base32", "incorrect padding", "missing credentials", "invalid client id", "invalid pin")


def _mark_permanent_if_needed(profile_id: str, msg: str) -> bool:
    import time as _t
    if any(m in (msg or "").lower() for m in _PERMANENT_MARKERS):
        _permanent_until[profile_id] = _t.time() + 6 * 3600
        return True
    return False


def _is_skipped(profile_id: str) -> bool:
    import time as _t
    return _permanent_until.get(profile_id, 0) > _t.time()

def _get_lock(profile_id: str) -> asyncio.Lock:
    if profile_id not in _refresh_locks:
        _refresh_locks[profile_id] = asyncio.Lock()
    return _refresh_locks[profile_id]


def token_is_valid(profile) -> bool:
    cred = profile.dhan_cred
    if not cred or not cred.is_active:
        return False
    if not cred.token_expires_at:
        return False
    now     = datetime.now(timezone.utc)
    expires = cred.token_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return now < (expires - timedelta(minutes=30))


def get_stored_token(profile) -> Optional[str]:
    cred = profile.dhan_cred
    if not cred:
        return None
    try:
        t = decrypt(cred.access_token)
        return t if t and t.strip() else None
    except Exception:
        return None


def _get_proxy_kwargs(profile) -> dict:
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
    stale_token: Optional[str] = None,
    **proxy_kw,
) -> Optional[str]:
    """stale_token: a token Dhan just rejected (DH-906). It must never be reused even if the
    stored expiry time says it is still valid."""
    lock = _get_lock(profile.id)

    async with lock:
        db.refresh(profile)
        if token_is_valid(profile) and reason != "forced":
            stored = get_stored_token(profile)
            if stored and not (stale_token and stored == stale_token):
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
            print(f"[token_manager] Token stored, expires at "
                  f"{cred.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')}")

        return new_token


async def _refresh_one_profile(profile, db: Session) -> bool:
    """
    Try to refresh token for a single profile.
    Returns True on success, False on failure.
    """
    cred = profile.dhan_cred
    if not cred:
        return False
    try:
        client_id = decrypt(cred.dhan_client_id)
        pin       = decrypt(cred.pin)
        totp      = decrypt(cred.totp_secret)
        if not client_id or not pin or not totp:
            print(f"[token_manager] Profile {profile.id}: missing credentials")
            cred.last_error = "Dhan credentials incomplete (client id / PIN / TOTP secret)"
            db.commit()
            _mark_permanent_if_needed(profile.id, "missing credentials")
            return False

        proxy_kw = _get_proxy_kwargs(profile)
        result   = await generate_access_token(client_id, pin, totp, **proxy_kw)

        if result["success"]:
            new_token             = result["access_token"]
            cred.access_token     = encrypt(new_token)
            cred.is_active        = True
            cred.last_error       = None
            cred.last_verified    = datetime.now(timezone.utc)
            cred.token_expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
            db.commit()
            _permanent_until.pop(profile.id, None)
            t_fp = f"{new_token[:12]}...{new_token[-6:]}" if len(new_token) > 18 else "SHORT"
            print(f"[token_manager] ✓ Client token refreshed: profile {profile.id} "
                  f"| fp={t_fp} "
                  f"| expires {cred.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')}")
            return True
        else:
            msg = result.get("message", "unknown error")
            print(f"[token_manager] ✗ Failed: profile {profile.id}: {msg}")
            cred.last_error = msg
            db.commit()
            _mark_permanent_if_needed(profile.id, msg)
            return False

    except Exception as e:
        print(f"[token_manager] Error: profile {profile.id}: {e}")
        try:
            cred.last_error = f"{type(e).__name__}: {e}"[:300]
            db.commit()
        except Exception:
            db.rollback()
        if _mark_permanent_if_needed(profile.id, str(e)):
            print(f"[token_manager] → profile {profile.id}: credentials are invalid, skipping retries for 6 h "
                  f"(client must fix their TOTP secret / PIN)")
        return False


async def _refresh_all_with_retry(db_factory, deadline_ist: datetime):
    """
    Refresh tokens for all profiles.
    Retries every 5 minutes until ALL succeed or deadline (9:00 AM) is reached.
    """
    from app.models.trading import ClientProfile, DhanCredential

    RETRY_INTERVAL = 2 * 60   # retry only the profiles that failed, every 2 minutes
    attempt        = 0
    pending: Optional[set[str]] = None   # None = first pass (everyone)

    while datetime.now(IST) < deadline_ist:
        attempt += 1
        db: Session = db_factory()
        try:
            profiles = (
                db.query(ClientProfile)
                .join(DhanCredential, DhanCredential.client_profile_id == ClientProfile.id)
                .all()
            )
            if pending is not None:
                profiles = [p for p in profiles if p.id in pending]
            profiles = [p for p in profiles if not _is_skipped(p.id)]

            if not profiles:
                print("[token_manager] Nothing (left) to refresh")
                return

            print(f"\n[token_manager] === Token refresh attempt {attempt} "
                  f"({datetime.now(IST).strftime('%H:%M:%S IST')}) — {len(profiles)} client(s) ===")

            failed = set()
            for i, profile in enumerate(profiles):
                if i > 0:
                    await asyncio.sleep(2)   # stagger between clients
                if not await _refresh_one_profile(profile, db):
                    if not _is_skipped(profile.id):
                        failed.add(profile.id)

            if not failed:
                print("[token_manager] ✓ All refreshable tokens refreshed")
                return

            pending = failed
            remaining = (deadline_ist - datetime.now(IST)).total_seconds()
            if remaining > RETRY_INTERVAL and attempt < 6:
                print(f"[token_manager] {len(failed)} token(s) failed — retrying only those in 2 min")
                await asyncio.sleep(RETRY_INTERVAL)
            else:
                print(f"[token_manager] {len(failed)} token(s) still failing — giving up for now "
                      f"(the keeper keeps trying every 10 min)")
                return

        except Exception as e:
            print(f"[token_manager] Refresh loop error: {e}")
            await asyncio.sleep(30)
        finally:
            db.close()


async def scheduled_morning_refresh(db_factory):
    """
    Runs inside the morning_scheduler loop.
    Called once at 8:00 AM — retries every 5 min until 9:00 AM if any fail.
    """
    now_ist      = datetime.now(IST)
    # Deadline: 9:00 AM IST — must succeed before algo starts at 8:45 AM
    # but keep retrying until 9 AM in case 8:45 AM fetch also fails
    deadline_ist = now_ist.replace(hour=9, minute=0, second=0, microsecond=0)
    if deadline_ist < now_ist:
        deadline_ist += timedelta(days=1)

    print(f"[token_manager] === 8 AM scheduled token refresh starting ===")
    print(f"[token_manager] Will retry every 5 min until {deadline_ist.strftime('%H:%M IST')} if needed")

    await _refresh_all_with_retry(db_factory, deadline_ist)

    now_ist  = datetime.now(IST)
    next_day = (now_ist + timedelta(days=1)).replace(
        hour=8, minute=0, second=0, microsecond=0
    )
    print(f"[token_manager] Next scheduled refresh at {next_day.strftime('%Y-%m-%d %H:%M IST')}")


# ── Keeper: never leave a client with an expired token (restarts, missed 8 AM slot) ──────────

async def refresh_stale_tokens_once(db_factory) -> int:
    """Refresh every profile whose stored token is missing/expired (per DB expiry). Returns #refreshed."""
    from app.models.trading import ClientProfile, DhanCredential
    db: Session = db_factory()
    done = 0
    try:
        profiles = (db.query(ClientProfile)
                    .join(DhanCredential, DhanCredential.client_profile_id == ClientProfile.id)
                    .all())
        for profile in profiles:
            if token_is_valid(profile) or _is_skipped(profile.id):
                continue
            if await _refresh_one_profile(profile, db):
                done += 1
            await asyncio.sleep(3)
    except Exception as e:
        print(f"[token_manager] keeper error: {type(e).__name__}: {e}")
    finally:
        db.close()
    return done


async def token_keeper(db_factory):
    """Runs forever. 20 s after boot, then every 10 min on weekdays 06:00–16:00 IST."""
    await asyncio.sleep(20)
    while True:
        try:
            now = datetime.now(IST)
            if now.weekday() < 5 and 6 <= now.hour < 16:
                n = await refresh_stale_tokens_once(db_factory)
                if n:
                    print(f"[token_manager] keeper refreshed {n} stale token(s)")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[token_manager] keeper loop error: {e}")
        await asyncio.sleep(600)
