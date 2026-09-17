"""
app/services/master_token.py

Master Data Account token management — completely separate from client tokens.

Architecture:
  - MasterDataAccount (master_data_account table) — ONE row in system
    → Used ONLY for market data: LTP, OHLC, candles
    → Configured in Settings → Data Account
    → Paid data subscription (₹499+GST)

  - DhanCredential (dhan_credentials table) — one per client
    → Used ONLY for order placement / portfolio
    → Never used for data fetching

This module manages the master token lifecycle:
  1. Load from DB once per session and cache in memory
  2. Auto-refresh via TOTP when expired (30-min buffer)
  3. Cache for up to 23 hours — avoids repeated Dhan logins
     (each new login invalidates the previous token)
  4. Force-refresh available for explicit refresh calls
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.encryption import decrypt, encrypt
from app.models.master_account import MasterDataAccount
from app.services.dhan import generate_access_token

# ── In-memory cache ───────────────────────────────────────────
# Avoids re-authenticating between steps (8:45 → 9:12 → 9:16 etc.)
# Each re-auth invalidates the previous Dhan token on their side.
_CACHE_TTL_SECONDS = 23 * 3600   # 23 hours — just under Dhan's 24h expiry

_cached_token:     Optional[str]   = None
_cached_client_id: Optional[str]   = None
_cache_fetched_at: float           = 0.0
_cache_lock = asyncio.Lock()
# ─────────────────────────────────────────────────────────────


def _account(db: Session) -> Optional[MasterDataAccount]:
    return db.query(MasterDataAccount).first()


def _is_db_token_valid(acc: MasterDataAccount) -> bool:
    """Check if the DB-stored token is still within its validity window."""
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
    # 30-min buffer before expiry
    return now < (expires - timedelta(minutes=30))


async def _do_refresh(acc: MasterDataAccount, db: Session) -> str:
    """
    Generate a new Dhan token for the master account and persist it.
    WARNING: calling this invalidates any previously issued token.
    Only call when genuinely needed.
    """
    client_id   = decrypt(acc.dhan_client_id)
    pin         = decrypt(acc.pin)
    totp_secret = decrypt(acc.totp_secret)

    print("[master_token] Generating new master data token via Dhan TOTP...")
    result = await generate_access_token(client_id, pin, totp_secret)

    if not result["success"]:
        raise RuntimeError(f"Master token refresh failed: {result['message']}")

    now = datetime.now(timezone.utc)
    acc.access_token     = encrypt(result["access_token"])
    acc.is_active        = True
    acc.last_verified    = now
    acc.last_error       = None
    acc.token_expires_at = now + timedelta(hours=24)
    db.commit()

    new_token = result["access_token"]
    t_fp = f"{new_token[:12]}...{new_token[-6:]}" if new_token and len(new_token) > 18 else "EMPTY"
    print(f"[master_token] ✓ New token generated — "
          f"token={t_fp} "
          f"expires={acc.token_expires_at.strftime('%Y-%m-%d %H:%M UTC')}")
    return new_token


async def get_master_token(db: Session, force: bool = False) -> tuple[str, str]:
    """
    Return (access_token, client_id) for the master data account.

    Token priority:
      1. In-memory cache (< 23h old) — no DB or Dhan call needed
      2. DB token (still valid per token_expires_at) — load to cache
      3. Generate fresh via TOTP — only if cache and DB are both expired

    Use force=True only for explicit "Test" or manual refresh actions.
    NEVER call with force=True in the algo loop — it would invalidate
    a working token mid-session.
    """
    global _cached_token, _cached_client_id, _cache_fetched_at

    async with _cache_lock:
        acc = _account(db)
        if not acc:
            raise RuntimeError(
                "No master data account configured. "
                "Go to Master → Settings → Data Account."
            )

        client_id = decrypt(acc.dhan_client_id)
        cache_age = time.monotonic() - _cache_fetched_at

        # 1. Memory cache hit (not forced, not stale)
        if _cached_token and cache_age < _CACHE_TTL_SECONDS and not force:
            t_fp = f"{_cached_token[:12]}...{_cached_token[-6:]}"
            print(f"[master_token] Cache HIT — age={cache_age:.0f}s "
                  f"token={t_fp}")
            return _cached_token, _cached_client_id

        # 2. DB token still valid — load into cache
        if not force and _is_db_token_valid(acc):
            token = decrypt(acc.access_token)
            t_fp  = f"{token[:12]}...{token[-6:]}" if token and len(token) > 18 else "EMPTY"
            _cached_token     = token
            _cached_client_id = client_id
            _cache_fetched_at = time.monotonic()
            print(f"[master_token] DB HIT — loaded into cache "
                  f"token={t_fp} "
                  f"expires={acc.token_expires_at.strftime('%H:%M UTC')}")
            return token, client_id

        # 3. Need fresh token — generate via TOTP
        token = await _do_refresh(acc, db)
        _cached_token     = token
        _cached_client_id = client_id
        _cache_fetched_at = time.monotonic()
        return token, client_id


async def refresh_master_token_now(db: Session) -> tuple[str, str]:
    """
    Force-refresh the master token. Use ONLY for:
      - Morning 8 AM scheduler refresh
      - Manual "Test" button in Settings
    Never use mid-algo-session.
    """
    global _cached_token, _cached_client_id, _cache_fetched_at

    async with _cache_lock:
        acc = _account(db)
        if not acc:
            raise RuntimeError("No master data account configured.")

        client_id = decrypt(acc.dhan_client_id)
        token     = await _do_refresh(acc, db)

        # Update cache
        _cached_token     = token
        _cached_client_id = client_id
        _cache_fetched_at = time.monotonic()

        return token, client_id


def clear_master_token_cache():
    """Clear in-memory cache — call on server startup to force DB reload."""
    global _cached_token, _cached_client_id, _cache_fetched_at
    _cached_token     = None
    _cached_client_id = None
    _cache_fetched_at = 0.0
    print("[master_token] Cache cleared")
