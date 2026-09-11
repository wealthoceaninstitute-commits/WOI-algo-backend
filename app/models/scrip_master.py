"""
app/models/scrip_master.py

ScripMaster   — Dhan instrument master, auto-updated daily at 8 AM
StockUniverse — named group of stocks (e.g. "Nifty 500", "FO Stocks")
UniverseStock — individual stock membership in a universe
"""
import uuid
from sqlalchemy import (
    Column, String, Boolean, Integer, Numeric,
    Date, DateTime, ForeignKey, Text, UniqueConstraint, func,
)
from sqlalchemy.orm import relationship
from app.core.database import Base


class ScripMaster(Base):
    """
    Dhan instrument master — NSE_EQ equity stocks only.
    Downloaded from https://images.dhan.co/api-data/api-scrip-master.csv
    and filtered to series EQ (excludes BE, SM, IL etc.)
    Updated daily at 8:00 AM IST.
    """
    __tablename__ = "scrip_master"

    security_id      = Column(String, primary_key=True)   # Dhan security ID e.g. "1333"
    symbol           = Column(String, nullable=False, index=True)  # e.g. "RELIANCE"
    name             = Column(String, nullable=False)               # e.g. "Reliance Industries Ltd"
    exchange_segment = Column(String, nullable=False, default="NSE_EQ")
    series           = Column(String, nullable=False, default="EQ")
    isin             = Column(String, nullable=True)
    lot_size         = Column(Integer, default=1)
    tick_size        = Column(Numeric(8, 4), default=0.05)
    last_updated     = Column(Date, nullable=True)


class StockUniverse(Base):
    """
    Named stock universe — e.g. "Nifty 500", "FO Stocks", "Custom List".
    Created by master. One universe can be assigned to multiple strategies.
    """
    __tablename__ = "stock_universes"

    id          = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name        = Column(String, nullable=False, unique=True)   # e.g. "Nifty 500"
    description = Column(Text, nullable=True)
    source      = Column(String, default="csv")   # csv | manual | fo_list
    is_active   = Column(Boolean, default=True)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at  = Column(DateTime(timezone=True), onupdate=func.now())

    stocks = relationship("UniverseStock", back_populates="universe",
                          cascade="all, delete-orphan")


class UniverseStock(Base):
    """
    Individual stock in a universe.
    security_id is resolved from ScripMaster at upload time.
    not_found=True means the symbol from the CSV wasn't in scrip_master.
    """
    __tablename__ = "universe_stocks"
    __table_args__ = (
        UniqueConstraint("universe_id", "security_id", name="uq_universe_stock"),
    )

    id           = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    universe_id  = Column(String, ForeignKey("stock_universes.id", ondelete="CASCADE"),
                          nullable=False, index=True)
    security_id  = Column(String, nullable=True)   # None if not found in scrip_master
    symbol       = Column(String, nullable=False)  # as uploaded in CSV
    name         = Column(String, nullable=True)
    is_active    = Column(Boolean, default=True)
    not_found    = Column(Boolean, default=False)  # symbol missing in scrip_master
    added_at     = Column(DateTime(timezone=True), server_default=func.now())

    universe = relationship("StockUniverse", back_populates="stocks")
