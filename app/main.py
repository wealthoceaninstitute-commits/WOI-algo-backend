import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.config import get_settings
from app.core.database import create_tables, SessionLocal
from app.core.bootstrap import create_master_if_needed
from app.routers import auth, clients, credentials, trading
from app.services.token_manager import scheduled_morning_refresh

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Create DB tables
    create_tables()

    # 2. Create master user if env vars are set
    create_master_if_needed()

    # 3. Start 8 AM IST daily token refresh scheduler
    task = asyncio.create_task(scheduled_morning_refresh(SessionLocal))
    print("[startup] 8 AM token refresh scheduler started")

    yield

    # Cleanup on shutdown
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="WealthOcean Trading API",
    description="Algo trading platform backend with Dhan broker integration",
    version="1.0.0",
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


@app.get("/", tags=["health"])
def root():
    return {"status": "ok", "service": "WealthOcean Trading API", "version": "1.0.0"}


@app.get("/health", tags=["health"])
def health():
    return {"status": "healthy"}
