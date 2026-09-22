"""
app/routers/market_watch.py  — WOI Market Watch Verification

Endpoint: GET /api/market-watch/candles
  - Fetches last N minutes of 1-min candles for all active algo stocks
  - Keeps only the last 5 minutes in the in-memory store
  - Prunes candles older than 5 minutes on each poll

Endpoint: GET /api/market-watch/candles/stream  (SSE)
  - Server-Sent Events — browser auto-reconnects, no WebSocket needed
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.models.trading import AlgoStrategy, AlgoStock, ClientProfile
from app.services.master_token import get_master_token
from app.services.angel_one import angel_fetch_candle

router = APIRouter(prefix="/api/market-watch", tags=["market-watch"])

IST = timezone(timedelta(hours=5, minutes=30))
KEEP_MINUTES = 5          # rolling window kept in memory
POLL_INTERVAL = 60        # seconds between candle fetches

# ── In-memory candle store ────────────────────────────────────────────────────
# Structure: { security_id: { "symbol": str, "candles": [{ timestamp, o, h, l, c, v }] } }
_candle_store: dict[str, dict] = {}
_store_lock = asyncio.Lock()


def _prune(candles: list[dict]) -> list[dict]:
    """Keep only candles within the last KEEP_MINUTES minutes."""
    cutoff = datetime.now(IST) - timedelta(minutes=KEEP_MINUTES)
    return [c for c in candles if c["timestamp"] >= cutoff.isoformat()[:19]]


async def _refresh_once(db: Session) -> dict:
    """
    Fetch the latest 1-min candles for every active algo stock and
    update the in-memory store.  Returns the current store snapshot.
    """
    # ── 1. Get master Angel One token ────────────────────────────────────────
    token_result = get_master_token(db)
    if not token_result:
        return {"error": "No master token — check AngelOneCredential in DB"}
    jwt_token, api_key, client_id = token_result

    # ── 2. Collect active algo stocks (any enabled strategy) ─────────────────
    stocks: list[AlgoStock] = (
        db.query(AlgoStock)
        .join(AlgoStrategy)
        .filter(AlgoStrategy.is_enabled == True)
        .all()
    )
    if not stocks:
        return {"error": "No active algo stocks found"}

    # Deduplicate by security_id; map security_id → symbol name
    stock_map: dict[str, str] = {}
    for s in stocks:
        if s.security_id not in stock_map:
            stock_map[s.security_id] = s.symbol or s.security_id

    # ── 3. Build time window: last KEEP_MINUTES minutes of market time ────────
    now_ist = datetime.now(IST)
    # If before market open use 9:15 as start
    market_open = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    from_dt_obj = max(now_ist - timedelta(minutes=KEEP_MINUTES), market_open)
    from_dt = from_dt_obj.strftime("%Y-%m-%d %H:%M")
    to_dt   = now_ist.strftime("%Y-%m-%d %H:%M")

    # ── 4. Fetch candles for each stock (sequential to respect rate limit) ────
    results: dict[str, dict] = {}
    for security_id, symbol in stock_map.items():
        try:
            candles = await angel_fetch_candle(
                jwt_token=jwt_token,
                api_key=api_key,
                client_id=client_id,
                security_id=security_id,
                from_dt=from_dt,
                to_dt=to_dt,
                interval="ONE_MINUTE",
                exchange="NSE",
            )
            # Prune to window
            candles = _prune(candles)
        except Exception as exc:
            candles = []
            print(f"[market-watch] fetch error for {symbol} ({security_id}): {exc}")

        results[security_id] = {
            "symbol": symbol,
            "candles": candles,
            "last_updated": now_ist.strftime("%H:%M:%S"),
        }
        await asyncio.sleep(1.1)          # Angel One rate limit: 1 req/s

    # ── 5. Merge into global store (keep previous candles that are still fresh)
    async with _store_lock:
        for sid, data in results.items():
            existing = _candle_store.get(sid, {}).get("candles", [])
            existing_ts = {c["timestamp"] for c in existing}
            merged = existing + [c for c in data["candles"] if c["timestamp"] not in existing_ts]
            merged = _prune(merged)
            merged.sort(key=lambda c: c["timestamp"])
            _candle_store[sid] = {**data, "candles": merged}
        # Remove stale entries (stock was removed from strategy)
        for sid in list(_candle_store.keys()):
            if sid not in results:
                del _candle_store[sid]
        return dict(_candle_store)


# ── REST endpoint: single poll ────────────────────────────────────────────────

@router.get("/candles")
async def get_market_watch_candles(
    db: Session = Depends(get_db),
):
    """
    Fetch and return the latest rolling 5-min 1-min candles for all
    active algo stocks.  Call this every 60 s from the frontend.
    """
    snapshot = await _refresh_once(db)
    now_ist = datetime.now(IST)
    return {
        "as_of": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        "window_minutes": KEEP_MINUTES,
        "stocks": snapshot,
    }


# ── SSE streaming endpoint ────────────────────────────────────────────────────

@router.get("/candles/stream")
async def stream_market_watch(
    db: Session = Depends(get_db),
):
    """
    Server-Sent Events stream — emits a fresh JSON payload every 60 s.
    Browser: const es = new EventSource('/api/market-watch/candles/stream');
    """
    import json as _json

    async def event_generator():
        while True:
            try:
                snapshot = await _refresh_once(db)
                now_ist = datetime.now(IST)
                payload = _json.dumps({
                    "as_of": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
                    "window_minutes": KEEP_MINUTES,
                    "stocks": snapshot,
                })
                yield f"data: {payload}\n\n"
            except Exception as exc:
                yield f"data: {{\"error\": \"{exc}\"}}\n\n"
            await asyncio.sleep(POLL_INTERVAL)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
