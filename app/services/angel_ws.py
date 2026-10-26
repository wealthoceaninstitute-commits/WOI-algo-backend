"""
app/services/angel_ws.py

Angel One SmartAPI WebSocket v2 — live price streaming.

Architecture:
  - ONE persistent WebSocket connection for ALL stocks
  - Subscribe stocks once → continuous LTP stream into an in-memory dict
  - The algo monitor loop reads that dict (no REST call needed); REST only tops up
    stocks that haven't ticked recently

Protocol (matches the official smartapi-python SmartWebSocketV2):
  URL      : wss://smartapisocket.angelone.in/smart-stream
  Headers  : Authorization (jwt), x-api-key, x-client-code, x-feed-token
  Subscribe: {"correlationID","action":1,"params":{"mode":1,"tokenList":[{"exchangeType":1,"tokens":[...]}]}}
  Heartbeat: client sends the TEXT frame "ping" every ~10s, server answers "pong"
  LTP packet (mode 1, little-endian, 51 bytes):
      0      subscription mode        (uint8)
      1      exchange type            (uint8)   1 = NSE cash
      2-26   token                    (25 bytes, ASCII, NUL padded)
      27-34  sequence number          (int64)
      35-42  exchange timestamp (ms)  (int64)
      43-46  last traded price        (int32, PAISE  → divide by 100)

Previous version never connected: `extra_headers` was removed in websockets>=14
(raises TypeError), `x-api-key` was missing, and the LTP packet was parsed with the
wrong offsets/type. The UI therefore always showed "polling". All three are fixed.
"""

import asyncio
import json
import struct
import time
from typing import Awaitable, Callable, Optional

# ── In-memory price store ──────────────────────────────────────────────────────
# { security_id_str: { ltp, timestamp } }
_live_prices: dict[str, dict] = {}
_ws_connected: bool = False
_ws_task: Optional[asyncio.Task] = None
_ws_conn = None                       # live connection object (for late subscribes)
_subscribed_ids: list[str] = []
_last_tick_time: float = 0.0
_last_error: Optional[str] = None
_connect_count: int = 0

# async () -> (jwt_token, feed_token, client_id, api_key)  — read fresh on every (re)connect
CredsProvider = Callable[[], Awaitable[tuple[str, str, str, str]]]

WS_URL            = "wss://smartapisocket.angelone.in/smart-stream"
HEARTBEAT_SECS    = 10
RECV_TIMEOUT_SECS = 35        # heartbeat replies keep this alive even when no ticks flow
SUBSCRIBE_CHUNK   = 200


def get_live_ltp(security_id: str) -> Optional[float]:
    """Get current LTP from in-memory store."""
    d = _live_prices.get(str(security_id))
    return d["ltp"] if d else None


def get_live_prices(security_ids: list[str], max_age: Optional[float] = None) -> dict[str, float]:
    """
    Get LTP for multiple stocks from in-memory store.
    max_age: if set, ignore prices older than this many seconds (stale-tick guard).
    """
    now = time.time()
    return {
        sid: _live_prices[str(sid)]["ltp"]
        for sid in security_ids
        if str(sid) in _live_prices
        and (max_age is None or now - _live_prices[str(sid)]["timestamp"] <= max_age)
    }


def is_ws_connected() -> bool:
    return _ws_connected


def ws_stats() -> dict:
    return {
        "connected":      _ws_connected,
        "subscribed":     len(_subscribed_ids),
        "prices_cached":  len(_live_prices),
        "last_tick_ago":  round(time.time() - _last_tick_time, 1) if _last_tick_time else None,
        "connects":       _connect_count,
        "last_error":     _last_error,
    }


# ── WebSocket v2 binary protocol ──────────────────────────────────────────────

def _build_subscribe_msg(token_ids: list[str], mode: int = 1, action: int = 1) -> str:
    """
    token_ids: plain NSE tokens, e.g. ["11536", "1333"]
    mode: 1=LTP, 2=Quote, 3=SnapQuote   action: 1=subscribe, 0=unsubscribe
    """
    return json.dumps({
        "correlationID": "woi_algo",
        "action":        action,
        "params": {
            "mode":      mode,
            "tokenList": [{"exchangeType": 1, "tokens": [str(t) for t in token_ids]}],
        },
    })


def _parse_ltp_packet(data: bytes) -> Optional[dict]:
    """Parse one binary tick (any mode — the first 47 bytes share the same layout)."""
    try:
        if len(data) < 47:
            return None
        mode  = data[0]
        exch  = data[1]
        token = data[2:27].split(b"\x00", 1)[0].decode("ascii", "ignore").strip()
        raw   = struct.unpack_from("<i", data, 43)[0]
        if not token or raw <= 0:
            return None
        return {"token": token, "ltp": raw / 100.0, "mode": mode, "exchange": exch}
    except Exception:
        return None


def _connect(headers: dict):
    """websockets.connect() with the right header kwarg for the installed major version."""
    import websockets
    try:
        major = int(str(websockets.__version__).split(".")[0])
    except Exception:
        major = 14
    kw = dict(ping_interval=None, close_timeout=5, max_size=None)
    if major >= 14:
        kw["additional_headers"] = headers
    else:
        kw["extra_headers"] = headers
    return websockets.connect(WS_URL, **kw)


async def _heartbeat(ws):
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_SECS)
            await ws.send("ping")
    except asyncio.CancelledError:
        raise
    except Exception:
        pass   # the receive loop notices the dead socket and reconnects


async def _send_subscribe(ws, ids: list[str]):
    for i in range(0, len(ids), SUBSCRIBE_CHUNK):
        await ws.send(_build_subscribe_msg(ids[i:i + SUBSCRIBE_CHUNK], mode=1))
        await asyncio.sleep(0.15)


# ── WebSocket connection ───────────────────────────────────────────────────────

async def start_ws_stream(creds_provider: CredsProvider, security_ids: list[str]):
    """
    Maintain the Angel One WebSocket forever: connect → subscribe → stream ticks,
    reconnecting (with fresh credentials) on any failure.
    """
    global _ws_connected, _subscribed_ids, _last_tick_time, _last_error, _ws_conn, _connect_count

    try:
        import websockets  # noqa: F401
    except ImportError:
        _last_error = "websockets not installed"
        print("[angel_ws] websockets not installed — pip install websockets")
        return

    _subscribed_ids = [str(s) for s in security_ids]
    retry_count = 0

    while True:
        hb = None
        try:
            jwt, feed_token, client_id, api_key = await creds_provider()
            if not (jwt and feed_token and client_id and api_key):
                raise RuntimeError("missing Angel credentials / feed token (run Test in Settings)")

            headers = {
                "Authorization": jwt,
                "x-api-key":     api_key,
                "x-client-code": client_id,
                "x-feed-token":  feed_token,
            }
            print(f"[angel_ws] Connecting… ({len(_subscribed_ids)} stocks)")
            async with _connect(headers) as ws:
                _ws_conn      = ws
                _ws_connected = True
                _connect_count += 1
                retry_count   = 0
                _last_error   = None
                print("[angel_ws] ✓ Connected — subscribing")

                await _send_subscribe(ws, list(_subscribed_ids))
                hb = asyncio.create_task(_heartbeat(ws))

                while True:
                    message = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT_SECS)
                    if isinstance(message, (bytes, bytearray)):
                        p = _parse_ltp_packet(bytes(message))
                        if p:
                            now = time.time()
                            _live_prices[p["token"]] = {"ltp": p["ltp"], "timestamp": now}
                            _last_tick_time = now
                    elif isinstance(message, str):
                        if message == "pong":
                            continue
                        try:
                            msg = json.loads(message)
                            if isinstance(msg, dict) and (msg.get("errorCode") or msg.get("error")):
                                print(f"[angel_ws] Server message: {msg}")
                        except Exception:
                            pass

        except asyncio.CancelledError:
            raise
        except Exception as e:
            _last_error = f"{type(e).__name__}: {e}"
            retry_count += 1
            wait = min(15, 2 * retry_count)
            print(f"[angel_ws] Disconnected: {_last_error} — retry {retry_count} in {wait}s")
            await asyncio.sleep(wait)
        finally:
            _ws_connected = False
            _ws_conn = None
            if hb:
                hb.cancel()


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


async def ensure_ws_running(creds_provider: CredsProvider, security_ids: list[str]):
    """
    Ensure the WebSocket task is running (start it if not) and that every id in
    security_ids is subscribed. Safe to call repeatedly.
    """
    global _ws_task, _subscribed_ids
    ids = [str(s) for s in security_ids]

    if _ws_task and not _ws_task.done():
        new = [i for i in ids if i not in set(_subscribed_ids)]
        if new:
            _subscribed_ids = _subscribed_ids + new
            if _ws_conn is not None and _ws_connected:
                try:
                    await _send_subscribe(_ws_conn, new)
                except Exception as e:
                    print(f"[angel_ws] late subscribe failed (will apply on reconnect): {e}")
        return

    _ws_task = asyncio.create_task(start_ws_stream(creds_provider, ids))
    print("[angel_ws] WebSocket task started")
    # Give the first connection a moment so the first ticks are cached
    for _ in range(40):
        await asyncio.sleep(0.1)
        if _ws_connected:
            break
