import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.config import get_settings
from app.core.database import create_tables, SessionLocal
from app.core.bootstrap import create_master_if_needed
from app.routers import auth, clients, credentials, trading, algo
from app.services.token_manager import scheduled_morning_refresh

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    create_master_if_needed()
    task = asyncio.create_task(scheduled_morning_refresh(SessionLocal))
    print("[startup] 8 AM token refresh scheduler started")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="WealthOcean Trading API",
    description="Algo trading platform — Dhan broker integration + WOI strategy",
    version="1.1.0",
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


@app.get("/", tags=["health"])
def root():
    return {"status": "ok", "service": "WealthOcean Trading API", "version": "1.1.0"}


@app.get("/health", tags=["health"])
def health():
    return {"status": "healthy"}
