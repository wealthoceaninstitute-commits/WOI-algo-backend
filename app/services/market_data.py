"""
app/services/market_data.py

Fetches market data from Dhan using the MASTER data account token.
Used exclusively by the algo engine — no client credentials involved.

Covers:
  - Quote fetch (batch, for gap scanning)
  - Intraday candle data (1-min OHLCV)
  - Pre-open price fetch
"""
import httpx
from datetime import datetime, date, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.encryption import encrypt, decrypt
from app.models.master_account import MasterDataAccount
from app.services.dhan import generate_access_token

API_BASE = "https://api.dhan.co/v2"
TIMEOUT  = 20.0


# ── Token management ──────────────────────────────────────────────────────────

def _get_account(db: Session) -> Optional[MasterDataAccount]:
    return db.query(MasterDataAccount).first()


def _token_valid(acc: MasterDataAccount) -> bool:
    if not acc or not acc.is_active or not acc.access_token:
        return False
    if not acc.token_expires_at:
        return False
    now     = datetime.now(timezone.utc)
    expires = acc.token_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return now < (expires - timedelta(minutes=30))


async def get_master_token(db: Session) -> str:
    """
    Return a valid master data account token.
    Auto-refreshes via TOTP if expired.
    """
    acc = _get_account(db)
    if not acc:
        raise RuntimeError("No master data account configured. Set it up in Master → Settings → Data Account.")

    if _token_valid(acc):
        return decrypt(acc.access_token)

    # Refresh via TOTP
    client_id   = decrypt(acc.dhan_client_id)
    pin         = decrypt(acc.pin)
    totp_secret = decrypt(acc.totp_secret)

    print("[market_data] Refreshing master data account token...")
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
    print("[market_data] Master token refreshed")
    return result["access_token"]


def _headers(token: str) -> dict:
    return {
        "access-token": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# ── Quote fetch (batch) ───────────────────────────────────────────────────────

async def fetch_quotes_batch(
    token: str,
    security_ids: list[str],
    exchange_segment: str = "NSE_EQ",
) -> dict:
    """
    Fetch LTP + OHLC for a batch of securities.
    Dhan allows up to 1000 per request.
    Returns: { security_id: { ltp, open, high, low, close, prev_close, ... } }
    """
    url  = f"{API_BASE}/marketfeed/ltp"
    body = {exchange_segment: security_ids}

    async with httpx.AsyncClient(headers=_headers(token), timeout=TIMEOUT) as c:
        resp = await c.post(url, json=body)

    if resp.status_code != 200:
        print(f"[market_data] Quote batch failed {resp.status_code}: {resp.text[:200]}")
        return {}

    data   = resp.json()
    result = {}
    for seg, items in (data.get("data") or {}).items():
        for item in (items or []):
            sid = str(item.get("securityId", ""))
            if sid:
                result[sid] = {
                    "ltp":        item.get("lastTradedPrice", 0),
                    "open":       item.get("openPrice", 0),
                    "high":       item.get("highPrice", 0),
                    "low":        item.get("lowPrice", 0),
                    "close":      item.get("closingPrice", 0),
                    "prev_close": item.get("previousClosePrice", 0),
                    "volume":     item.get("totalTradedVolume", 0),
                    "symbol":     item.get("tradingSymbol", ""),
                }
    return result


async def fetch_ohlc_batch(
    token: str,
    security_ids: list[str],
    exchange_segment: str = "NSE_EQ",
) -> dict:
    """
    Fetch OHLC data for gap calculation.
    Returns: { security_id: { open, high, low, close, prev_close } }
    """
    url  = f"{API_BASE}/marketfeed/ohlc"
    body = {exchange_segment: security_ids}

    async with httpx.AsyncClient(headers=_headers(token), timeout=TIMEOUT) as c:
        resp = await c.post(url, json=body)

    if resp.status_code != 200:
        print(f"[market_data] OHLC batch failed {resp.status_code}: {resp.text[:200]}")
        return {}

    data   = resp.json()
    result = {}
    for seg, items in (data.get("data") or {}).items():
        for item in (items or []):
            sid = str(item.get("securityId", ""))
            if sid:
                result[sid] = {
                    "open":       item.get("openPrice", 0),
                    "high":       item.get("highPrice", 0),
                    "low":        item.get("lowPrice", 0),
                    "close":      item.get("closingPrice", 0),
                    "prev_close": item.get("previousClosePrice", 0),
                    "volume":     item.get("totalTradedVolume", 0),
                    "symbol":     item.get("tradingSymbol", ""),
                }
    return result


async def fetch_ltp(token: str, security_ids: list[str], exchange_segment: str = "NSE_EQ") -> dict:
    """Fetch LTP only — lightweight, used for monitoring open positions."""
    url  = f"{API_BASE}/marketfeed/ltp"
    body = {exchange_segment: security_ids}

    async with httpx.AsyncClient(headers=_headers(token), timeout=TIMEOUT) as c:
        resp = await c.post(url, json=body)

    if resp.status_code != 200:
        return {}

    data   = resp.json()
    result = {}
    for seg, items in (data.get("data") or {}).items():
        for item in (items or []):
            sid = str(item.get("securityId", ""))
            if sid:
                result[sid] = float(item.get("lastTradedPrice", 0))
    return result


# ── Intraday candles ──────────────────────────────────────────────────────────

async def fetch_intraday_candles(
    token: str,
    security_id: str,
    exchange_segment: str = "NSE_EQ",
    instrument_type: str = "EQUITY",
) -> list[dict]:
    """
    Fetch today's 1-min candles for a security.
    Returns list of { timestamp, open, high, low, close, volume }
    """
    today = date.today().strftime("%Y-%m-%d")
    url   = f"{API_BASE}/charts/intraday"
    body  = {
        "securityId":    security_id,
        "exchangeSegment": exchange_segment,
        "instrument":    instrument_type,
        "interval":      "1",
        "fromDate":      today,
        "toDate":        today,
    }

    async with httpx.AsyncClient(headers=_headers(token), timeout=TIMEOUT) as c:
        resp = await c.post(url, json=body)

    if resp.status_code != 200:
        print(f"[market_data] Candles failed {resp.status_code}: {resp.text[:200]}")
        return []

    data      = resp.json()
    opens     = data.get("open", [])
    highs     = data.get("high", [])
    lows      = data.get("low", [])
    closes    = data.get("close", [])
    volumes   = data.get("volume", [])
    timestamps= data.get("timestamp", [])

    candles = []
    for i in range(len(closes)):
        candles.append({
            "timestamp": timestamps[i] if i < len(timestamps) else None,
            "open":   float(opens[i])   if i < len(opens)   else 0,
            "high":   float(highs[i])   if i < len(highs)   else 0,
            "low":    float(lows[i])    if i < len(lows)    else 0,
            "close":  float(closes[i])  if i < len(closes)  else 0,
            "volume": int(volumes[i])   if i < len(volumes) else 0,
        })

    return candles


async def fetch_first_candle(
    token: str,
    security_id: str,
    exchange_segment: str = "NSE_EQ",
) -> Optional[dict]:
    """
    Fetch the first 1-min candle (9:15–9:16 AM) for a security.
    Returns the candle dict or None if not yet available.
    """
    candles = await fetch_intraday_candles(token, security_id, exchange_segment)
    if candles:
        return candles[0]
    return None
