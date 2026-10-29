"""
WOI algo engine worker — the trading engine, running on its own.

Deploy as a SECOND Railway service on the same repo, pointed at the same
DATABASE_URL and carrying the same env vars as the web service:

    startCommand = "uvicorn app.worker:app --host 0.0.0.0 --port $PORT"

Then set RUN_ENGINE=0 (or remove it) on the existing web service, and set
ENGINE_URL on the web service to this worker's URL so that the master's
"Trigger Run Now" button still reaches the engine.

Dashboard traffic can no longer starve the engine, and the engine can no
longer block a login: they share only Postgres, through separate pools.

Endpoints — small on purpose, this service does not serve the app:

    GET  /health          always 200 while the process is up
    GET  /health/engine   503 when the monitor tick has gone stale during
                          market hours, so Railway restarts the worker and the
                          existing resume logic picks the open positions back up
    GET  /engine/status   same payload as /api/algo/engine/status
    POST /engine/trigger  internal; the web service forwards the manual kick here
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Header, HTTPException

from app.core.database import create_tables
from app.core.engine_lock import acquire_engine_lock, internal_token, is_engine_process

IST = timezone(timedelta(hours=5, minutes=30))

# How long the monitor may go quiet during market hours before this worker
# reports itself unhealthy. The loop ticks every 0.5s when the WebSocket is up.
STALE_TICK_SECS = 10

_tasks: list = []
_kicks: set = set()          # keeps manual-trigger tasks from being garbage collected


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _tasks
    create_tables()

    if not acquire_engine_lock():
        print("[worker] ANOTHER PROCESS HOLDS THE ENGINE LOCK — this worker will NOT trade.")
        print("[worker] Check that RUN_ENGINE is not 1 on the web service, then redeploy.")
        yield
        return

    from app.main import start_engine_tasks, stop_engine_tasks
    _tasks = start_engine_tasks()
    print("[worker] Engine lock held — this process is the trading engine")

    yield

    await stop_engine_tasks(_tasks)


app = FastAPI(
    title="WealthOcean Algo Engine",
    description="The trading engine, split out of the API service",
    version="1.0.0",
    lifespan=lifespan,
)


def _in_session(now: datetime) -> bool:
    return (now.weekday() < 5
            and now.replace(hour=9, minute=16, second=0, microsecond=0)
            <= now < now.replace(hour=15, minute=20, second=0, microsecond=0))


@app.get("/", tags=["health"])
def root():
    return {"status": "ok", "service": "WealthOcean Algo Engine", "is_engine": is_engine_process()}


@app.get("/health", tags=["health"])
def health():
    """Liveness only — use /health/engine for the Railway health check."""
    return {"status": "healthy", "is_engine": is_engine_process()}


@app.get("/health/engine", tags=["health"])
def health_engine():
    """
    Readiness. Returns 503 when the monitor is supposed to be ticking and is not,
    which lets Railway restart this worker; resume_monitoring_if_needed() then
    recovers the open positions on the way back up.
    """
    from app.services.algo_engine import engine_status
    st = engine_status()
    now = datetime.now(IST)
    session = _in_session(now)
    ago = st.get("last_tick_ago_secs")

    if not is_engine_process():
        raise HTTPException(status_code=503, detail={"reason": "engine lock not held", **st})
    if session and st.get("monitor_active") and ago is not None and ago > STALE_TICK_SECS:
        raise HTTPException(status_code=503, detail={"reason": "monitor tick stale", **st})
    return {"status": "healthy", "in_session": session, **st}


@app.get("/engine/status", tags=["engine"])
def engine_status_route():
    from app.services.algo_engine import engine_status
    return engine_status()


@app.post("/engine/trigger", tags=["engine"])
async def engine_trigger(x_woi_internal: str = Header(default="")):
    """Internal. The web service forwards the master's 'Trigger Run Now' to this."""
    if not x_woi_internal or x_woi_internal != internal_token():
        raise HTTPException(status_code=401, detail="bad internal token")
    if not is_engine_process():
        raise HTTPException(status_code=409, detail="this worker does not hold the engine lock")

    from app.services.algo_engine import trigger_or_resume
    task = asyncio.create_task(trigger_or_resume())
    _kicks.add(task)
    task.add_done_callback(_kicks.discard)
    return {"message": "engine kick accepted", "started_at": datetime.now(timezone.utc).isoformat()}
