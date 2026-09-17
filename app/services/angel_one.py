"""
app/services/angel_one.py

Angel One SmartAPI integration for market data.

Used for:
  - LTP fetch (8:45 AM prev close, 9:12:30 open price) — 50 stocks/batch, 1 req/sec
  - FULL quote (circuit limits) — after stock selection, only selected stocks
  - 1-min candle data — first candle at 9:16:05

Angel One API:
  Base URL : https://apiconnect.angelbroking.com
  Auth     : POST /rest/auth/angelbroking/user/v1/loginByPassword
  LTP      : POST /rest/secure/angelbroking/market/v1/quote/ (mode=LTP)
  FULL     : POST /rest/secure/angelbroking/market/v1/quote/ (mode=FULL)
  Candle   : POST /rest/secure/angelbroking/historical/v1/getCandleData

Rate limit : 1 request/second
Batch size : 50 stocks per request (LTP mode)
Token      : JWT valid 24h + refresh token
"""

import asyncio
import httpx
import pyotp
from datetime import datetime, date, timezone, timedelta
from typing import Optional

API_BASE  = "https://apiconnect.angelbroking.com"
TIMEOUT   = 20.0
BATCH_SIZE = 50  # Angel One max per LTP request


def _generate_totp(secret: str) -> str:
    return pyotp.TOTP(secret).now()


def _headers(jwt_token: str, api_key: str, client_id: str) -> dict:
    return {
        "Authorization":      f"Bearer {jwt_token}",
        "Content-Type":       "application/json",
        "Accept":             "application/json",
        "X-UserType":         "USER",
        "X-SourceID":         "WEB",
        "X-ClientLocalIP":    "192.168.1.1",
        "X-ClientPublicIP":   "106.193.147.98",
        "X-MACAddress":       "fe80::216e:6507:4b90:3719",
        "X-PrivateKey":       api_key,
    }


# ── Authentication ────────────────────────────────────────────────────────────

async def angel_generate_token(
    client_id: str,
    pin: str,
    totp_secret: str,
    api_key: str,
) -> dict:
    """
    Login to Angel One SmartAPI.
    Returns { success, jwt_token, refresh_token, feed_token, message }
    """
    totp = _generate_totp(totp_secret)
    body = {
        "clientcode": client_id,
        "password":   pin,
        "totp":       totp,
    }
    headers = {
        "Content-Type": "application/json",
        "Accept":       "application/json",
        "X-UserType":   "USER",
        "X-SourceID":   "WEB",
        "X-PrivateKey": api_key,
    }

    print(f"[angel_one] Logging in client={client_id} totp={totp}...")
    async with httpx.AsyncClient(headers=headers, timeout=TIMEOUT) as c:
        resp = await c.post(
            f"{API_BASE}/rest/auth/angelbroking/user/v1/loginByPassword",
            json=body,
        )

    print(f"[angel_one] Login response: HTTP {resp.status_code}")
    if resp.status_code != 200:
        return {"success": False, "message": f"HTTP {resp.status_code}: {resp.text[:200]}"}

    data = resp.json()
    if not data.get("status") or data.get("errorcode") not in ("", None, "0", 0):
        msg = data.get("message", "Login failed")
        return {"success": False, "message": msg}

    d = data.get("data") or {}
    jwt_token     = d.get("jwtToken")
    refresh_token = d.get("refreshToken")
    feed_token    = d.get("feedToken")

    if not jwt_token:
        return {"success": False, "message": f"No JWT in response: {data}"}

    print(f"[angel_one] ✓ Login OK — client={client_id}")
    return {
        "success":       True,
        "jwt_token":     jwt_token,
        "refresh_token": refresh_token,
        "feed_token":    feed_token,
        "message":       "Login successful",
    }


async def angel_refresh_token(refresh_token: str, api_key: str) -> dict:
    """Refresh JWT using refresh token — avoids full TOTP re-login."""
    headers = {
        "Content-Type": "application/json",
        "Accept":       "application/json",
        "X-PrivateKey": api_key,
    }
    body = {"refreshToken": refresh_token}

    async with httpx.AsyncClient(headers=headers, timeout=TIMEOUT) as c:
        resp = await c.post(
            f"{API_BASE}/rest/secure/angelbroking/user/v1/getToken",
            json=body,
        )

    if resp.status_code != 200:
        return {"success": False, "message": f"HTTP {resp.status_code}"}

    data = resp.json()
    if not data.get("status"):
        return {"success": False, "message": data.get("message", "Refresh failed")}

    d = data.get("data") or {}
    return {
        "success":       True,
        "jwt_token":     d.get("jwtToken"),
        "refresh_token": d.get("refreshToken"),
        "feed_token":    d.get("feedToken"),
    }


# ── LTP fetch (8:45 AM + 9:12:30) ────────────────────────────────────────────

async def angel_fetch_ltp(
    jwt_token: str,
    api_key: str,
    client_id: str,
    security_ids: list[str],
    exchange: str = "NSE",
) -> dict[str, float]:
    """
    Fetch LTP for up to 50 stocks per request.
    For 501 stocks: 11 batches × 1 sec = ~11 seconds.
    Returns { security_id_str: ltp_float }
    """
    result: dict[str, float] = {}
    total_batches = (len(security_ids) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(security_ids), BATCH_SIZE):
        batch     = security_ids[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1

        body = {
            "mode": "LTP",
            "exchangeTokens": {exchange: batch},
        }

        t_fp = f"{jwt_token[:12]}...{jwt_token[-6:]}" if len(jwt_token) > 18 else "SHORT"
        print(f"[angel_one] LTP batch {batch_num}/{total_batches} "
              f"({len(batch)} stocks) token={t_fp}")

        async with httpx.AsyncClient(
            headers=_headers(jwt_token, api_key, client_id),
            timeout=TIMEOUT,
        ) as c:
            resp = await c.post(
                f"{API_BASE}/rest/secure/angelbroking/market/v1/quote/",
                json=body,
            )

        if resp.status_code != 200:
            print(f"[angel_one] LTP {resp.status_code}: {resp.text[:200]}")
        else:
            data    = resp.json()
            fetched = (data.get("data") or {}).get("fetched") or []
            for item in fetched:
                sid = str(item.get("symbolToken", ""))
                ltp = float(item.get("ltp") or 0)
                if sid and ltp > 0:
                    result[sid] = ltp

        if i + BATCH_SIZE < len(security_ids):
            await asyncio.sleep(1.1)  # Angel One: 1 req/sec

    print(f"[angel_one] LTP done: {len(result)}/{len(security_ids)} stocks fetched")
    return result


# ── FULL quote (circuit limits) ───────────────────────────────────────────────

async def angel_fetch_full_quote(
    jwt_token: str,
    api_key: str,
    client_id: str,
    security_ids: list[str],
    exchange: str = "NSE",
) -> dict[str, dict]:
    """
    Fetch FULL market data including circuit limits for selected stocks.
    Returns {
      security_id: {
        ltp, open, high, low, close,
        upper_circuit, lower_circuit,
        volume, net_change, percent_change
      }
    }
    """
    result: dict[str, dict] = {}

    for i in range(0, len(security_ids), BATCH_SIZE):
        batch = security_ids[i:i + BATCH_SIZE]
        body  = {
            "mode": "FULL",
            "exchangeTokens": {exchange: batch},
        }

        async with httpx.AsyncClient(
            headers=_headers(jwt_token, api_key, client_id),
            timeout=TIMEOUT,
        ) as c:
            resp = await c.post(
                f"{API_BASE}/rest/secure/angelbroking/market/v1/quote/",
                json=body,
            )

        if resp.status_code != 200:
            print(f"[angel_one] FULL {resp.status_code}: {resp.text[:200]}")
            continue

        data    = resp.json()
        fetched = (data.get("data") or {}).get("fetched") or []
        for item in fetched:
            sid = str(item.get("symbolToken", ""))
            if not sid:
                continue
            result[sid] = {
                "ltp":           float(item.get("ltp")          or 0),
                "open":          float(item.get("open")         or 0),
                "high":          float(item.get("high")         or 0),
                "low":           float(item.get("low")          or 0),
                "close":         float(item.get("close")        or 0),
                "upper_circuit": float(item.get("upperCircuit") or 0),
                "lower_circuit": float(item.get("lowerCircuit") or 0),
                "volume":        int(item.get("tradeVolume")    or 0),
                "net_change":    float(item.get("netChange")    or 0),
                "pct_change":    float(item.get("percentChange") or 0),
                "symbol":        item.get("tradingSymbol", ""),
            }

        if i + BATCH_SIZE < len(security_ids):
            await asyncio.sleep(1.1)

    return result


# ── Candle data ────────────────────────────────────────────────────────────────

async def angel_fetch_candle(
    jwt_token: str,
    api_key: str,
    client_id: str,
    security_id: str,
    from_dt: str,
    to_dt: str,
    interval: str = "ONE_MINUTE",
    exchange: str = "NSE",
) -> list[dict]:
    """
    Fetch candle data from Angel One.
    interval: ONE_MINUTE, THREE_MINUTE, FIVE_MINUTE, etc.
    from_dt/to_dt: "YYYY-MM-DD HH:MM"
    Returns [{ timestamp, open, high, low, close, volume }]
    """
    body = {
        "exchange":    exchange,
        "symboltoken": security_id,
        "interval":    interval,
        "fromdate":    from_dt,
        "todate":      to_dt,
    }

    async with httpx.AsyncClient(
        headers=_headers(jwt_token, api_key, client_id),
        timeout=TIMEOUT,
    ) as c:
        resp = await c.post(
            f"{API_BASE}/rest/secure/angelbroking/historical/v1/getCandleData",
            json=body,
        )

    if resp.status_code != 200:
        print(f"[angel_one] Candle {resp.status_code}: {resp.text[:200]}")
        return []

    data   = resp.json()
    candles = (data.get("data") or [])
    return [
        {
            "timestamp": c[0],
            "open":      float(c[1]),
            "high":      float(c[2]),
            "low":       float(c[3]),
            "close":     float(c[4]),
            "volume":    int(c[5]) if len(c) > 5 else 0,
        }
        for c in candles
    ]


async def angel_fetch_first_candle(
    jwt_token: str,
    api_key: str,
    client_id: str,
    security_id: str,
) -> Optional[dict]:
    """First 1-min candle of the day (9:15–9:16 AM IST)."""
    today   = date.today()
    from_dt = f"{today} 09:15"
    to_dt   = f"{today} 09:17"
    candles = await angel_fetch_candle(
        jwt_token, api_key, client_id, security_id,
        from_dt, to_dt,
    )
    return candles[0] if candles else None
