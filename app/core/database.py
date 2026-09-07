from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import get_settings

settings = get_settings()

# Railway PostgreSQL uses postgres:// — SQLAlchemy needs postgresql://
db_url = settings.DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(db_url, pool_pre_ping=True, pool_size=5, max_overflow=10)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    """
    Create all tables on startup.
    ALL models must be imported here so SQLAlchemy registers them
    with Base.metadata before create_all() is called.
    """
    # Core models
    from app.models.user import User                          # noqa: F401
    from app.models.trading import (                          # noqa: F401
        ClientProfile, DhanCredential, ProxySetting, Fund,
        Order, Position, DailyPnl,
        AlgoStrategy, AlgoRun, AlgoStock,
    )
    # New models — must be imported or their tables won't be created
    from app.models.master_account import MasterDataAccount  # noqa: F401
    from app.models.scrip_master import (                    # noqa: F401
        ScripMaster, StockUniverse, UniverseStock,
    )

    Base.metadata.create_all(bind=engine)
    print("[db] Tables created/verified")
