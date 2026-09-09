"""
app/services/market_data.py

Fetches market data from Dhan using the MASTER data account token.
Built directly from https://dhanhq.co/docs/v2/market-quote/
                  and https://dhanhq.co/docs/v2/historical-data/

Endpoints:
  POST /v2/marketfeed/ltp      — LTP only, up to 1000 instruments
  POST /v2/marketfeed/ohlc     — OHLC + LTP, up to 1000 instruments
  POST /v2/charts/intraday     — 1-min candles

Request body: { "NSE_EQ": [11536, 1333] }   ← INTEGER security IDs
Response:     { "data": { "NSE_EQ": { "11536": { "last_price": ... } } } }

Rate limit: 1 req/sec on marketfeed. client-id header required.
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


def _get_account(db: Session) -> Optional[MasterDataAccount]:
    return db.query(MasterDataAccount).first()


def _token_valid(acc: MasterDataAccount) -> bool:
    if not acc or not acc.is_active:
        return False
    try:
        token = decrypt(acc.access_token)
        if not token or not token.strip():
            return False
    except Exception:
        return False
    if not acc.token_expires_at:
        return False
    now     = datetime.now(timezone.utc)
    expires = acc.token_expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return now < (expires - timedelta(minutes=30))


async def get_master_token(db: Session) -> tuple[str, str]:
    """
    Return (access_token, client_id).
    Auto-refreshes via TOTP if expired.
    """
    acc = _get_account(db)
    if not acc:
        raise RuntimeError(
            "No master data account configured. "
            "Go to Master → Settings → Data Account to set it up."
        )
    client_id = decrypt(acc.dhan_client_id)

    if _token_valid(acc):
        return decrypt(acc.access_token), client_id

    pin         = decrypt(acc.pin)
    totp_secret = decrypt(acc.totp_secret)
    print("[market_data] Refreshing master token...")
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
    return result["access_token"], client_id


def _h(token: str, client_id: str) -> dict:
    return {
        "access-token": token,
        "client-id":    client_id,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }


async def fetch_ohlc_batch(
    token: str,
    client_id: str,
    security_ids: list[str],
    exchange_segment: str = "NSE_EQ",
) -> dict:
    """
    POST /v2/marketfeed/ohlc
    Returns: { sid_str: { ltp, open, high, low, prev_close } }
    NOTE: ohlc.close in Dhan response = PREVIOUS DAY close price
    """
    body = {exchange_segment: [int(s) for s in security_ids]}
    async with httpx.AsyncClient(headers=_h(token, client_id), timeout=TIMEOUT) as c:
        resp = await c.post(f"{API_BASE}/marketfeed/ohlc", json=body)

    if resp.status_code != 200:
        print(f"[market_data] OHLC {resp.status_code}: {resp.text[:200]}")
        return {}

    seg_data = (resp.json().get("data") or {}).get(exchange_segment, {})
    result   = {}
    for sid, item in seg_data.items():
        ohlc = item.get("ohlc") or {}
        result[str(sid)] = {
            "ltp":        float(item.get("last_price") or 0),
            "open":       float(ohlc.get("open")  or 0),
            "high":       float(ohlc.get("high")  or 0),
            "low":        float(ohlc.get("low")   or 0),
            "prev_close": float(ohlc.get("close") or 0),  # prev day close
        }
    return result


async def fetch_ltp(
    token: str,
    client_id: str,
    security_ids: list[str],
    exchange_segment: str = "NSE_EQ",
) -> dict:
    """
    POST /v2/marketfeed/ltp
    Returns: { sid_str: ltp_float }
    """
    body = {exchange_segment: [int(s) for s in security_ids]}
    async with httpx.AsyncClient(headers=_h(token, client_id), timeout=TIMEOUT) as c:
        resp = await c.post(f"{API_BASE}/marketfeed/ltp", json=body)

    if resp.status_code != 200:
        print(f"[market_data] LTP {resp.status_code}: {resp.text[:200]}")
        return {}

    seg_data = (resp.json().get("data") or {}).get(exchange_segment, {})
    return {str(sid): float(item.get("last_price") or 0) for sid, item in seg_data.items()}


async def fetch_intraday_candles(
    token: str,
    client_id: str,
    security_id: str,
    exchange_segment: str = "NSE_EQ",
    instrument: str = "EQUITY",
    interval: str = "1",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> list[dict]:
    """
    POST /v2/charts/intraday
    fromDate/toDate format: "YYYY-MM-DD HH:MM:SS"
    Returns: [{ timestamp, open, high, low, close, volume }]
    """
    today   = date.today()
    from_dt = from_date or f"{today} 09:15:00"
    to_dt   = to_date   or f"{today} 15:30:00"

    body = {
        "securityId":      security_id,
        "exchangeSegment": exchange_segment,
        "instrument":      instrument,
        "interval":        interval,
        "oi":              False,
        "fromDate":        from_dt,
        "toDate":          to_dt,
    }

    async with httpx.AsyncClient(headers=_h(token, client_id), timeout=TIMEOUT) as c:
        resp = await c.post(f"{API_BASE}/charts/intraday", json=body)

    if resp.status_code != 200:
        print(f"[market_data] Candles {resp.status_code}: {resp.text[:200]}")
        return []

    data = resp.json()
    opens      = data.get("open",      [])
    highs      = data.get("high",      [])
    lows       = data.get("low",       [])
    closes     = data.get("close",     [])
    volumes    = data.get("volume",    [])
    timestamps = data.get("timestamp", [])

    return [
        {
            "timestamp": timestamps[i] if i < len(timestamps) else None,
            "open":   float(opens[i])  if i < len(opens)  else 0.0,
            "high":   float(highs[i])  if i < len(highs)  else 0.0,
            "low":    float(lows[i])   if i < len(lows)   else 0.0,
            "close":  float(closes[i]) if i < len(closes) else 0.0,
            "volume": int(volumes[i])  if i < len(volumes) else 0,
        }
        for i in range(len(closes))
    ]


async def fetch_first_candle(
    token: str,
    client_id: str,
    security_id: str,
    exchange_segment: str = "NSE_EQ",
) -> Optional[dict]:
    """First 1-min candle of the day (9:15–9:16 AM)."""
    today = date.today()
    candles = await fetch_intraday_candles(
        token, client_id, security_id, exchange_segment,
        from_date=f"{today} 09:15:00",
        to_date=f"{today} 09:17:00",
    )
    return candles[0] if candles else None


async def fetch_daily_ohlcv(
    token: str,
    client_id: str,
    security_id: str,
    from_date: str,
    to_date: str,
    exchange_segment: str = "NSE_EQ",
    instrument: str = "EQUITY",
) -> list[dict]:
    """
    POST /v2/charts/historical
    Returns daily OHLCV candles.
    fromDate/toDate format: "YYYY-MM-DD"
    Response: { open:[...], high:[...], low:[...], close:[...], volume:[...], timestamp:[...] }
    """
    body = {
        "securityId":      security_id,
        "exchangeSegment": exchange_segment,
        "instrument":      instrument,
        "expiryCode":      0,
        "oi":              False,
        "fromDate":        from_date,
        "toDate":          to_date,
    }

    async with httpx.AsyncClient(
        headers=_h(token, client_id), timeout=TIMEOUT
    ) as c:
        resp = await c.post(f"{API_BASE}/charts/historical", json=body)

    if resp.status_code != 200:
        print(f"[market_data] Daily OHLCV {security_id} {resp.status_code}: {resp.text[:200]}")
        return []

    data       = resp.json()
    opens      = data.get("open",      [])
    highs      = data.get("high",      [])
    lows       = data.get("low",       [])
    closes     = data.get("close",     [])
    volumes    = data.get("volume",    [])
    timestamps = data.get("timestamp", [])

    return [
        {
            "timestamp": timestamps[i] if i < len(timestamps) else None,
            "open":      float(opens[i])   if i < len(opens)   else 0.0,
            "high":      float(highs[i])   if i < len(highs)   else 0.0,
            "low":       float(lows[i])    if i < len(lows)    else 0.0,
            "close":     float(closes[i])  if i < len(closes)  else 0.0,
            "volume":    int(volumes[i])   if i < len(volumes) else 0,
        }
        for i in range(len(closes))
    ]
