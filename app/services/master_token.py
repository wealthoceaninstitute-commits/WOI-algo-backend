"""
app/services/master_token.py

Angel One master token management.
No in-memory cache — DB is the single source of truth.
Full fingerprint logging so we can trace exactly which token is used.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.encryption import decrypt, encrypt
from app.models.master_account import MasterDataAccount
from app.services.angel_one import angel_generate_token, angel_refresh_token


def _fp(token: str) -> str:
    if not token or len(token) < 18:
        return "EMPTY"
    return f"{token[:12]}...{token[-6:]}"


def _account(db: Session) -> Optional[MasterDataAccount]:
    return db.query(MasterDataAccount).first()


def _token_valid(acc: MasterDataAccount) -> bool:
    if not acc or not acc.is_active:
        return False
    if not acc.angel_jwt_token:
        return False
    try:
        t = decrypt(acc.angel_jwt_token)
        if not t or not t.strip():
            return False
    except Exception:
        return False
    if not acc.token_expires_at:
        return False
    now     = datetime.now(timezone.utc)
    expires = acc.token_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    # 5-min buffer
    return now < (expires - timedelta(minutes=5))


async def get_master_token(db: Session) -> tuple[str, str, str]:
    """
    Return (jwt_token, api_key, client_id) for Angel One master account.
    Reads directly from DB — no in-memory cache.
    Tries refresh token first, then full TOTP login if refresh fails.
    """
    acc = _account(db)
    if not acc:
        raise RuntimeError(
            "No master data account configured. "
            "Go to Master → Settings → Data Account."
        )

    client_id = decrypt(acc.angel_client_id) if acc.angel_client_id else ""
    api_key   = decrypt(acc.angel_api_key)   if acc.angel_api_key   else ""

    if _token_valid(acc):
        jwt   = decrypt(acc.angel_jwt_token)
        expires = acc.token_expires_at
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        remaining = (expires - datetime.now(timezone.utc)).total_seconds() / 3600
        print(f"[master_token] Angel One DB token OK — "
              f"fp={_fp(jwt)} "
              f"expires={acc.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')} "
              f"({remaining:.1f}h remaining)")
        return jwt, api_key, client_id

    # Try refresh token first (avoids full TOTP re-login)
    if acc.angel_refresh_token:
        try:
            ref_token = decrypt(acc.angel_refresh_token)
            print(f"[master_token] Trying refresh token...")
            result = await angel_refresh_token(ref_token, api_key)
            if result["success"]:
                return await _save_and_return(acc, result, db, client_id, api_key)
        except Exception as e:
            print(f"[master_token] Refresh token failed: {e} — falling back to TOTP")

    # Full TOTP login
    return await refresh_master_token_now(db)


async def _save_and_return(
    acc: MasterDataAccount,
    result: dict,
    db: Session,
    client_id: str,
    api_key: str,
) -> tuple[str, str, str]:
    jwt = result["jwt_token"]
    now = datetime.now(timezone.utc)

    acc.angel_jwt_token     = encrypt(jwt)
    acc.angel_refresh_token = encrypt(result.get("refresh_token") or "")
    acc.angel_feed_token    = result.get("feed_token") or ""
    acc.is_active           = True
    acc.last_verified       = now
    acc.last_error          = None
    acc.token_expires_at    = now + timedelta(hours=24)
    db.commit()

    print(f"[master_token] ✓ Angel One token saved — "
          f"fp={_fp(jwt)} "
          f"expires={acc.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')}")
    return jwt, api_key, client_id


async def refresh_master_token_now(db: Session) -> tuple[str, str, str]:
    """Full TOTP login — use at 8AM scheduler or Test button."""
    acc = _account(db)
    if not acc:
        raise RuntimeError("No master data account configured.")

    client_id   = decrypt(acc.angel_client_id)   if acc.angel_client_id   else ""
    pin         = decrypt(acc.angel_pin)          if acc.angel_pin         else ""
    totp_secret = decrypt(acc.angel_totp_secret)  if acc.angel_totp_secret else ""
    api_key     = decrypt(acc.angel_api_key)      if acc.angel_api_key     else ""

    if not all([client_id, pin, totp_secret, api_key]):
        raise RuntimeError(
            "Angel One credentials incomplete. "
            "Go to Settings → Data Account and fill all fields."
        )

    result = await angel_generate_token(client_id, pin, totp_secret, api_key)
    if not result["success"]:
        raise RuntimeError(f"Angel One login failed: {result['message']}")

    return await _save_and_return(acc, result, db, client_id, api_key)


def clear_master_token_cache():
    """No-op — no cache. DB is source of truth."""
    print("[master_token] DB-backed — no cache to clear")
