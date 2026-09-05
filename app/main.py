import asyncio
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.config import get_settings
from app.core.database import create_tables, SessionLocal
from app.core.bootstrap import create_master_if_needed
from app.routers import auth, clients, credentials, trading, algo
from app.routers import master_account
from app.services.token_manager import scheduled_morning_refresh
from app.services.algo_engine import run_daily_algo

settings = get_settings()
IST = timezone(timedelta(hours=5, minutes=30))


async def algo_scheduler():
    """
    Fires run_daily_algo() every weekday at 9:00 AM IST.
    Runs only for paper trading clients initially.
    """
    while True:
        now_ist = datetime.now(IST)

        # Next 9:00 AM IST on a weekday
        target = now_ist.replace(hour=9, minute=0, second=0, microsecond=0)
        if now_ist >= target:
            target = target + timedelta(days=1)

        # Skip weekends
        while target.weekday() >= 5:  # 5=Sat, 6=Sun
            target += timedelta(days=1)

        wait_secs = (target - now_ist).total_seconds()
        print(f"[scheduler] Next algo run in {wait_secs/3600:.1f}h "
              f"(at {target.strftime('%Y-%m-%d %H:%M IST')})")

        await asyncio.sleep(wait_secs)

        print(f"[scheduler] === Starting daily algo run {target.strftime('%Y-%m-%d')} ===")
        try:
            await run_daily_algo()
        except Exception as e:
            print(f"[scheduler] Algo run error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    create_master_if_needed()

    # Token refresh — 8 AM IST daily
    token_task = asyncio.create_task(scheduled_morning_refresh(SessionLocal))
    print("[startup] Token refresh scheduler started")

    # Algo engine — 9 AM IST weekdays
    algo_task = asyncio.create_task(algo_scheduler())
    print("[startup] Algo scheduler started")

    yield

    token_task.cancel()
    algo_task.cancel()
    for t in [token_task, algo_task]:
        try:
            await t
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="WealthOcean Trading API",
    description="Algo trading platform — Dhan broker + WOI strategy",
    version="1.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(clients.router)
app.include_router(credentials.router)
app.include_router(trading.router)
app.include_router(algo.router)
app.include_router(master_account.router)


@app.get("/", tags=["health"])
def root():
    return {"status": "ok", "service": "WealthOcean Trading API", "version": "1.2.0"}


@app.get("/health", tags=["health"])
def health():
    return {"status": "healthy"}
