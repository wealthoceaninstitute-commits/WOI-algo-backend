"""
app/services/angel_ws.py

Angel One SmartAPI WebSocket v2 — live price streaming.

Architecture:
  - ONE persistent WebSocket connection for ALL stocks
  - Subscribe 501 stocks at startup → continuous LTP stream
  - Market Watch reads from in-memory dict (no REST call needed)
  - After stock selection → FULL REST quote for circuit limits only

WebSocket docs: https://smartapi.angelone.in/docs/WebSocket2

Modes:
  1 = LTP only
  2 = LTP + OHLC
  3 = Full (depth)

Token IDs format: "nse_cm|{security_id}" for NSE cash
"""

import asyncio
import json
import struct
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable

# ── In-memory price store ──────────────────────────────────────────────────────
# { security_id_str: { ltp, timestamp } }
_live_prices: dict[str, dict] = {}
_ws_connected: bool = False
_ws_task: Optional[asyncio.Task] = None
_subscribed_ids: list[str] = []
_last_tick_time: float = 0.0


def get_live_ltp(security_id: str) -> Optional[float]:
    """Get current LTP from in-memory store."""
    d = _live_prices.get(str(security_id))
    return d["ltp"] if d else None


def get_live_prices(security_ids: list[str]) -> dict[str, float]:
    """Get LTP for multiple stocks from in-memory store."""
    return {
        sid: _live_prices[str(sid)]["ltp"]
        for sid in security_ids
        if str(sid) in _live_prices
    }


def is_ws_connected() -> bool:
    return _ws_connected


def ws_stats() -> dict:
    return {
        "connected":      _ws_connected,
        "subscribed":     len(_subscribed_ids),
        "prices_cached":  len(_live_prices),
        "last_tick_ago":  round(time.time() - _last_tick_time, 1) if _last_tick_time else None,
    }


# ── WebSocket v2 binary protocol ──────────────────────────────────────────────

def _build_subscribe_msg(token_ids: list[str], mode: int = 1) -> str:
    """
    Build Angel One WebSocket v2 subscribe message.
    token_ids format: ["nse_cm|11536", "nse_cm|1333", ...]
    mode: 1=LTP, 2=OHLC, 3=Full
    """
    return json.dumps({
        "correlationID": "market_watch",
        "action":        1,  # 1=subscribe, 0=unsubscribe
        "params": {
            "mode":       mode,
            "tokenList": [
                {
                    "exchangeType": 1,  # 1=NSE CM
                    "tokens":       token_ids,
                }
            ],
        },
    })


def _parse_ltp_packet(data: bytes) -> Optional[dict]:
    """
    Parse Angel One WebSocket v2 LTP binary packet.
    Packet structure (LTP mode=1):
      Bytes 0:    subscription_mode (1 byte)
      Bytes 1:    exchange_type (1 byte)
      Bytes 2-27: token (26 bytes, ASCII)
      Bytes 28-35: sequence_number (8 bytes, int64)
      Bytes 36-43: exchange_timestamp (8 bytes, int64, epoch ms)
      Bytes 44-51: ltp (8 bytes, float64, paise → divide by 100)
    """
    try:
        if len(data) < 52:
            return None
        mode  = data[0]
        token = data[2:28].decode("ascii").strip("\x00").strip()
        ltp   = struct.unpack("<d", data[44:52])[0] / 100.0
        return {"token": token, "ltp": ltp, "mode": mode}
    except Exception:
        return None


# ── WebSocket connection ───────────────────────────────────────────────────────

async def start_ws_stream(
    jwt_token: str,
    feed_token: str,
    client_id: str,
    security_ids: list[str],
):
    """
    Start Angel One WebSocket v2 stream.
    Subscribes all security_ids and updates _live_prices continuously.
    Auto-reconnects on disconnect.
    """
    global _ws_connected, _subscribed_ids, _last_tick_time

    try:
        import websockets
    except ImportError:
        print("[angel_ws] websockets not installed — pip install websockets")
        return

    WS_URL = "wss://smartapisocket.angelone.in/smart-stream"
    _subscribed_ids = security_ids

    # Format token IDs for Angel One: "nse_cm|{security_id}"
    token_ids    = [str(sid) for sid in security_ids]
    subscribe_msg = _build_subscribe_msg(token_ids, mode=1)

    print(f"[angel_ws] Connecting to Angel One WebSocket...")
    print(f"[angel_ws] Subscribing {len(token_ids)} stocks (LTP mode)")

    retry_count = 0
    while True:
        try:
            headers = {
                "Authorization": jwt_token,
                "x-feed-token":  feed_token,
                "x-client-code": client_id,
            }
            async with websockets.connect(
                WS_URL,
                extra_headers=headers,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                _ws_connected = True
                retry_count   = 0
                print(f"[angel_ws] ✓ Connected — sending subscribe request")

                await ws.send(subscribe_msg)
                print(f"[angel_ws] Subscribe sent for {len(token_ids)} stocks")

                async for message in ws:
                    if isinstance(message, bytes):
                        parsed = _parse_ltp_packet(message)
                        if parsed and parsed["ltp"] > 0:
                            token = parsed["token"]
                            _live_prices[token] = {
                                "ltp":       parsed["ltp"],
                                "timestamp": time.time(),
                            }
                            _last_tick_time = time.time()
                    elif isinstance(message, str):
                        # Control messages
                        try:
                            msg = json.loads(message)
                            print(f"[angel_ws] Control: {msg}")
                        except Exception:
                            pass

        except Exception as e:
            _ws_connected = False
            retry_count  += 1
            wait = min(30, 5 * retry_count)
            print(f"[angel_ws] Disconnected: {e} — retry {retry_count} in {wait}s")
            await asyncio.sleep(wait)


async def stop_ws_stream():
    """Stop the WebSocket stream task."""
    global _ws_task, _ws_connected
    if _ws_task and not _ws_task.done():
        _ws_task.cancel()
        try:
            await _ws_task
        except asyncio.CancelledError:
            pass
    _ws_connected = False
    _ws_task      = None
    print("[angel_ws] WebSocket stopped")


async def ensure_ws_running(
    jwt_token: str,
    feed_token: str,
    client_id: str,
    security_ids: list[str],
):
    """
    Ensure WebSocket is running — start if not, skip if already running.
    Called from algo_engine after token is available.
    """
    global _ws_task
    if _ws_task and not _ws_task.done():
        print(f"[angel_ws] Already running ({len(_live_prices)} prices cached)")
        return
    _ws_task = asyncio.create_task(
        start_ws_stream(jwt_token, feed_token, client_id, security_ids)
    )
    print(f"[angel_ws] WebSocket task started")
    # Brief wait for initial connection
    await asyncio.sleep(2)
