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
    token_result = await get_master_token(db)
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


# ── Serve the HTML dashboard directly from Railway ────────────────────────────
# Open: https://your-railway-url.up.railway.app/api/market-watch/dashboard
# No file:// restriction, no CORS issue — served from the same origin.

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>WOI Market Watch</title>
<style>
  :root{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#e6edf3;--muted:#8b949e;--green:#3fb950;--red:#f85149;--yellow:#d29922;--blue:#58a6ff;--accent:#1f6feb}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh}
  header{background:var(--card);border-bottom:1px solid var(--border);padding:14px 24px;display:flex;align-items:center;gap:16px}
  header h1{font-size:1.1rem;font-weight:600}
  .badge{font-size:.72rem;padding:3px 8px;border-radius:12px;font-weight:600}
  .badge-live{background:#1a3a1a;color:var(--green);border:1px solid var(--green)}
  .badge-err{background:#3a1a1a;color:var(--red);border:1px solid var(--red)}
  .badge-wait{background:#2a2a10;color:var(--yellow);border:1px solid var(--yellow)}
  #status-bar{font-size:.8rem;color:var(--muted);margin-left:auto}
  main{padding:20px 24px}
  .controls{display:flex;gap:12px;margin-bottom:20px;align-items:center;flex-wrap:wrap}
  button{background:var(--accent);color:#fff;border:none;border-radius:6px;padding:7px 16px;font-size:.85rem;cursor:pointer;font-weight:600}
  button:hover{background:#388bfd}
  button.secondary{background:var(--card);border:1px solid var(--border);color:var(--text)}
  button.secondary:hover{background:#21262d}
  #countdown{font-size:.82rem;color:var(--muted);margin-left:auto}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(520px,1fr));gap:16px}
  .stock-card{background:var(--card);border:1px solid var(--border);border-radius:10px;overflow:hidden}
  .stock-header{display:flex;align-items:center;gap:10px;padding:12px 16px;border-bottom:1px solid var(--border);background:#1c2128}
  .stock-symbol{font-weight:700;font-size:1rem}
  .stock-sid{font-size:.72rem;color:var(--muted)}
  .candle-count{font-size:.72rem;color:var(--muted);margin-left:auto}
  .updated{font-size:.72rem;color:var(--blue)}
  table{width:100%;border-collapse:collapse;font-size:.82rem}
  th{text-align:right;padding:7px 12px;color:var(--muted);font-weight:500;font-size:.75rem;border-bottom:1px solid var(--border)}
  th:first-child{text-align:left}
  td{text-align:right;padding:6px 12px;border-bottom:1px solid #21262d;font-variant-numeric:tabular-nums}
  td:first-child{text-align:left;color:var(--muted);font-size:.78rem}
  tr.newest td{background:rgba(63,185,80,.07)}
  tr:last-child td{border-bottom:none}
  .up{color:var(--green)}.dn{color:var(--red)}
  .empty{padding:24px 16px;color:var(--muted);font-size:.85rem;text-align:center}
  .error-card{background:#1a0d0d;border:1px solid var(--red);border-radius:10px;padding:20px;color:var(--red);font-size:.9rem}
  #loading{text-align:center;padding:60px;color:var(--muted);font-size:.9rem}
</style>
</head>
<body>
<header>
  <h1>📊 WOI Market Watch</h1>
  <span id="status-badge" class="badge badge-wait">Connecting…</span>
  <span id="status-bar">—</span>
</header>
<main>
  <div class="controls">
    <span style="font-size:.82rem;color:var(--muted)">Auto-polling every 62s from this server</span>
    <button onclick="fetchOnce()">↺ Refresh Now</button>
    <button class="secondary" onclick="stopPolling()">⏸ Pause</button>
    <span id="countdown">—</span>
  </div>
  <div id="grid" class="grid"><div id="loading">Loading candle data…</div></div>
</main>
<script>
const POLL_MS=62000;
let timer=null,countdown=null,secondsLeft=0;
function setBadge(t,x){const b=document.getElementById('status-badge');b.className='badge badge-'+t;b.textContent=x}
function setStatus(t){document.getElementById('status-bar').textContent=t}
function startCountdown(){clearInterval(countdown);secondsLeft=Math.round(POLL_MS/1000);const el=document.getElementById('countdown');countdown=setInterval(()=>{if(secondsLeft<=0){el.textContent='Refreshing…';return}el.textContent='Next: '+(secondsLeft--)+'s'},1000)}
async function fetchOnce(){
  try{
    setBadge('wait','Fetching…');
    const res=await fetch('/api/market-watch/candles');
    if(!res.ok)throw new Error('HTTP '+res.status);
    const data=await res.json();
    renderData(data);
    setBadge('live','🟢 LIVE');
    setStatus('as of '+data.as_of+' · window: '+data.window_minutes+' min');
    startCountdown();
  }catch(e){
    setBadge('err','Error');
    setStatus(e.message);
    document.getElementById('grid').innerHTML='<div class="error-card">❌ '+e.message+'</div>';
  }
}
function renderData(data){
  const stocks=data.stocks||{};
  const keys=Object.keys(stocks);
  const grid=document.getElementById('grid');
  if('error' in stocks){grid.innerHTML='<div class="error-card">⚠️ '+stocks.error+'</div>';return}
  if(keys.length===0){grid.innerHTML='<div id="loading">No active algo stocks. Enable a strategy first.</div>';return}
  grid.innerHTML='';
  keys.forEach(sid=>{
    const s=stocks[sid];
    const candles=(s.candles||[]).slice().reverse();
    const card=document.createElement('div');card.className='stock-card';
    const ltp=candles.length?candles[0].close:null;
    const prev=candles.length>1?candles[1].close:null;
    const dir=ltp&&prev?(ltp>=prev?'up':'dn'):'';
    card.innerHTML=`<div class="stock-header">
      <span class="stock-symbol">${s.symbol}</span>
      <span class="stock-sid">#${sid}</span>
      ${ltp?`<span class="${dir}">${dir==='up'?'▲':'▼'} ₹${ltp.toFixed(2)}</span>`:''}
      <span class="candle-count">${candles.length} candle${candles.length!==1?'s':''}</span>
      <span class="updated">⏱ ${s.last_updated}</span>
    </div>
    ${candles.length===0?'<div class="empty">No candle data yet — market may not be open.</div>':`
    <table><thead><tr><th>Time</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th></tr></thead>
    <tbody>${candles.map((c,i)=>{
      const ts=c.timestamp?c.timestamp.substring(11,16):'—';
      const chg=i<candles.length-1?(c.close-candles[i+1].close)/candles[i+1].close*100:0;
      const cls=chg>0?'up':chg<0?'dn':'';
      return `<tr class="${i===0?'newest':''}"><td>${ts}</td><td>${c.open?.toFixed(2)??'—'}</td><td>${c.high?.toFixed(2)??'—'}</td><td>${c.low?.toFixed(2)??'—'}</td><td class="${cls}">${c.close?.toFixed(2)??'—'}${chg!==0?` <small>(${chg>0?'+':''}${chg.toFixed(2)}%)</small>`:''}</td><td>${c.volume?.toLocaleString('en-IN')??'—'}</td></tr>`;
    }).join('')}</tbody></table>`}`;
    grid.appendChild(card);
  });
}
function stopPolling(){clearInterval(timer);clearInterval(countdown);timer=null;setBadge('wait','Paused');document.getElementById('countdown').textContent='Paused'}
fetchOnce();
timer=setInterval(fetchOnce,POLL_MS);
</script>
</body>
</html>"""


@router.get("/dashboard", response_class=None)
async def market_watch_dashboard():
    """
    Open this in your browser: https://your-railway-url/api/market-watch/dashboard
    Served from Railway itself — no file:// CORS issues.
    """
    from fastapi.responses import HTMLResponse
    return HTMLResponse(content=_DASHBOARD_HTML)
