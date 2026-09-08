from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import get_settings

settings = get_settings()
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
    print("[db] All tables created/verified")
