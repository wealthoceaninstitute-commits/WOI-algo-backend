"""
app/services/master_token.py

Master Data Account token — reads from DB every time.
No in-memory cache — DB is the single source of truth.
Full logging to trace exactly which token is used at each step.
"""

import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.encryption import decrypt, encrypt
from app.models.master_account import MasterDataAccount
from app.services.dhan import generate_access_token


def _fp(token: str) -> str:
    """Token fingerprint — first 12 + last 6 chars for log tracing."""
    if not token or len(token) < 18:
        return "EMPTY/SHORT"
    return f"{token[:12]}...{token[-6:]}"


def _account(db: Session) -> Optional[MasterDataAccount]:
    return db.query(MasterDataAccount).first()


def _is_token_valid(acc: MasterDataAccount) -> bool:
    if not acc or not acc.is_active:
        return False
    try:
        t = decrypt(acc.access_token)
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
    return now < (expires - timedelta(minutes=5))


async def get_master_token(db: Session, force: bool = False) -> tuple[str, str]:
    """
    Return (access_token, client_id) for the master data account.
    Reads from DB every time — no in-memory cache.
    Generates new token only if DB token is expired or force=True.
    """
    acc = _account(db)
    if not acc:
        raise RuntimeError(
            "No master data account configured. "
            "Go to Master → Settings → Data Account."
        )

    client_id = decrypt(acc.dhan_client_id)

    if not force and _is_token_valid(acc):
        # DB token is valid — use it directly
        token = decrypt(acc.access_token)
        expires = acc.token_expires_at
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        remaining = (expires - datetime.now(timezone.utc)).total_seconds() / 3600
        print(f"[master_token] DB token OK — "
              f"fp={_fp(token)} "
              f"expires={acc.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')} "
              f"({remaining:.1f}h remaining)")
        return token, client_id

    # Need fresh token
    reason = "force=True" if force else "DB token expired/invalid"
    print(f"[master_token] Generating new token ({reason})...")
    return await refresh_master_token_now(db)


async def refresh_master_token_now(db: Session) -> tuple[str, str]:
    """
    Force-generate a new token via TOTP and save to DB.
    Use ONLY at 8 AM scheduler or explicit Test button.
    """
    acc = _account(db)
    if not acc:
        raise RuntimeError("No master data account configured.")

    client_id   = decrypt(acc.dhan_client_id)
    pin         = decrypt(acc.pin)
    totp_secret = decrypt(acc.totp_secret)

    print(f"[master_token] Calling Dhan generateAccessToken "
          f"(client_id={client_id})...")
    result = await generate_access_token(client_id, pin, totp_secret)

    if not result["success"]:
        raise RuntimeError(f"Master token refresh failed: {result['message']}")

    new_token = result["access_token"]
    now       = datetime.now(timezone.utc)

    acc.access_token     = encrypt(new_token)
    acc.is_active        = True
    acc.last_verified    = now
    acc.last_error       = None
    acc.token_expires_at = now + timedelta(hours=24)
    db.commit()

    print(f"[master_token] ✓ New token saved to DB — "
          f"fp={_fp(new_token)} "
          f"expires={acc.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')}")
    return new_token, client_id


def clear_master_token_cache():
    """No-op — cache removed. DB is source of truth."""
    print("[master_token] No cache to clear — DB is source of truth")
