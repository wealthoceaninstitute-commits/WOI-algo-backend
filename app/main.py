import asyncio
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.config import get_settings
from app.core.database import create_tables, SessionLocal
from app.core.bootstrap import create_master_if_needed
from app.routers import auth, clients, credentials, trading, algo, market
from app.routers import master_account, universe, algo_stats
from app.services.token_manager import scheduled_morning_refresh
from app.services.algo_engine import run_daily_algo, cleanup_old_snapshots
from app.services.scrip_downloader import download_and_update

settings = get_settings()
IST = timezone(timedelta(hours=5, minutes=30))


async def morning_scheduler():
    """
    Every weekday at 8:00 AM IST:
      1. Clean up yesterday's price snapshots from DB
      2. Download + update scrip master
      3. Refresh all client Dhan tokens
    Then at 8:45 AM IST:
      4. Run daily algo (Step 1: prev close fetch + DB save)
    """
    while True:
        now_ist = datetime.now(IST)

        # Next 8:00 AM IST weekday
        target_8 = now_ist.replace(hour=8, minute=0, second=0, microsecond=0)
        if now_ist >= target_8:
            target_8 += timedelta(days=1)
        while target_8.weekday() >= 5:
            target_8 += timedelta(days=1)

        wait_secs = (target_8 - now_ist).total_seconds()
        print(f"[scheduler] Next morning tasks in {wait_secs/3600:.1f}h "
              f"(at {target_8.strftime('%Y-%m-%d %H:%M IST')})")
        await asyncio.sleep(wait_secs)

        print(f"[scheduler] === 8:00 AM tasks starting {target_8.strftime('%Y-%m-%d')} ===")

        # 1. Clean up old price snapshots BEFORE new fetch
        db = SessionLocal()
        try:
            cleanup_old_snapshots(db)
        except Exception as e:
            print(f"[scheduler] Snapshot cleanup error: {e}")
        finally:
            db.close()

        # 2. Update scrip master
        db = SessionLocal()
        try:
            result = await download_and_update(db)
            print(f"[scheduler] Scrip master: {result.get('total_downloaded',0)} stocks updated")
        except Exception as e:
            print(f"[scheduler] Scrip master error: {e}")
        finally:
            db.close()

        # 3. Refresh master data token first (market data account)
        db = SessionLocal()
        try:
            from app.services.master_token import refresh_master_token_now
            token, cid = await refresh_master_token_now(db)
            print(f"[scheduler] Master data token refreshed (client_id: {cid})")
        except Exception as e:
            print(f"[scheduler] Master token refresh error: {e}")
        finally:
            db.close()

        # 4. Refresh client tokens (for order placement only)
        try:
            await scheduled_morning_refresh(SessionLocal)
        except Exception as e:
            print(f"[scheduler] Client token refresh error: {e}")

        # 4. Wait until 8:45 AM then launch algo
        now_ist    = datetime.now(IST)
        target_845 = now_ist.replace(hour=8, minute=45, second=0, microsecond=0)
        if now_ist < target_845:
            wait = (target_845 - now_ist).total_seconds()
            print(f"[scheduler] Waiting {wait:.0f}s until 8:45 AM for algo...")
            await asyncio.sleep(wait)

        print(f"[scheduler] === 8:45 AM algo run starting ===")
        try:
            await run_daily_algo()
        except Exception as e:
            print(f"[scheduler] Algo run error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    create_master_if_needed()

    task = asyncio.create_task(morning_scheduler())
    print("[startup] Morning scheduler started (8AM cleanup+scrip+tokens, 8:45AM algo)")

    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="WealthOcean Trading API",
    description="Algo trading platform — Dhan broker + WOI strategy",
    version="1.4.0",
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
app.include_router(universe.router)
app.include_router(algo_stats.router)
app.include_router(market.router)


@app.get("/", tags=["health"])
def root():
    return {"status": "ok", "service": "WealthOcean Trading API", "version": "1.4.0"}


@app.get("/health", tags=["health"])
def health():
    return {"status": "healthy"}
