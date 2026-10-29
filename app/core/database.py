from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import get_settings

settings = get_settings()
db_url = settings.DATABASE_URL.replace("postgres://", "postgresql://", 1)

# API pool: dashboards / login / routers (get_db). Bursty — 28 clients polling can fill it.
engine = create_engine(db_url, pool_pre_ping=True, pool_size=10, max_overflow=10, pool_timeout=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ENGINE pool: the algo engine (scan, monitor loop, live-order tasks, token jobs) only.
# Separate from the API pool so dashboard traffic can never starve trading (and vice-versa).
# Total = 20 (API) + 20 (engine) max connections; check Postgres max_connections (Railway default is ~100).
engine_engine = create_engine(db_url, pool_pre_ping=True, pool_size=8, max_overflow=12, pool_timeout=30)
EngineSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine_engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    """
    Import ALL models before create_all so SQLAlchemy
    registers every table with Base.metadata.
    """
    from app.models.user import User                          # noqa
    from app.models.trading import (                          # noqa
        ClientProfile, DhanCredential, ProxySetting, Fund,
        Order, Position, DailyPnl,
        AlgoStrategy, AlgoRun, AlgoStock,
    )
    from app.models.master_account import MasterDataAccount  # noqa
    from app.models.scrip_master import (                    # noqa
        ScripMaster, StockUniverse, UniverseStock,
    )
    Base.metadata.create_all(bind=engine)
    # Lightweight migrations for columns added after first deploy (idempotent)
    from sqlalchemy import text
    try:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE algo_stocks ADD COLUMN IF NOT EXISTS peak_price NUMERIC(10,2)"))
            conn.execute(text("ALTER TABLE algo_stocks ADD COLUMN IF NOT EXISTS live_status VARCHAR"))
            conn.execute(text("ALTER TABLE algo_stocks ADD COLUMN IF NOT EXISTS live_entry_price NUMERIC(10,2)"))
            conn.execute(text("ALTER TABLE algo_stocks ADD COLUMN IF NOT EXISTS live_exit_price NUMERIC(10,2)"))
    except Exception as e:
        print(f"[db] migration warning: {e}")
    print("[db] All tables created/verified")
