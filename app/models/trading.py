import uuid
from sqlalchemy import (
    Column, String, Boolean, Integer, Numeric,
    DateTime, Date, ForeignKey, Text, Enum, func
)
from sqlalchemy.orm import relationship
from app.core.database import Base


class ClientProfile(Base):
    __tablename__ = "client_profiles"

    id            = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id       = Column(String, ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False)
    city          = Column(String, nullable=True)
    state         = Column(String, nullable=True)
    pan           = Column(String, nullable=True)
    active_index  = Column(String, default="SENSEX")
    paper_trading = Column(Boolean, default=True)
    capital       = Column(Numeric(15, 2), default=0)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    updated_at    = Column(DateTime(timezone=True), onupdate=func.now())

    user          = relationship("User", back_populates="client_profile")
    dhan_cred     = relationship("DhanCredential", back_populates="client_profile", uselist=False, cascade="all, delete-orphan")
    proxy_setting = relationship("ProxySetting", back_populates="client_profile", uselist=False, cascade="all, delete-orphan")
    fund          = relationship("Fund", back_populates="client_profile", uselist=False, cascade="all, delete-orphan")
    orders        = relationship("Order", back_populates="client_profile", cascade="all, delete-orphan")
    positions     = relationship("Position", back_populates="client_profile", cascade="all, delete-orphan")
    daily_pnl     = relationship("DailyPnl", back_populates="client_profile", cascade="all, delete-orphan")


class DhanCredential(Base):
    __tablename__ = "dhan_credentials"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), unique=True, nullable=False)

    # All stored encrypted
    dhan_client_id = Column(String, nullable=False)
    pin            = Column(String, nullable=False)
    totp_secret    = Column(String, nullable=False)
    access_token   = Column(Text, nullable=False)

    is_active        = Column(Boolean, default=False)
    last_verified    = Column(DateTime(timezone=True), nullable=True)
    token_expires_at = Column(DateTime(timezone=True), nullable=True)
    last_error       = Column(String, nullable=True)
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), onupdate=func.now())

    client_profile = relationship("ClientProfile", back_populates="dhan_cred")


class ProxySetting(Base):
    __tablename__ = "proxy_settings"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), unique=True, nullable=False)
    scheme            = Column(String, default="https")   # http | https
    host              = Column(String, nullable=False)
    port              = Column(Integer, default=443)
    username          = Column(String, nullable=True)
    password          = Column(String, nullable=True)     # Encrypted
    is_active         = Column(Boolean, default=True)
    set_by_master     = Column(Boolean, default=False)
    created_at        = Column(DateTime(timezone=True), server_default=func.now())
    updated_at        = Column(DateTime(timezone=True), onupdate=func.now())

    client_profile = relationship("ClientProfile", back_populates="proxy_setting")


class Fund(Base):
    __tablename__ = "funds"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), unique=True, nullable=False)
    available         = Column(Numeric(15, 2), default=0)
    used_margin       = Column(Numeric(15, 2), default=0)
    total_balance     = Column(Numeric(15, 2), default=0)
    updated_at        = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    client_profile = relationship("ClientProfile", back_populates="fund")


class Order(Base):
    __tablename__ = "orders"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), nullable=False, index=True)
    dhan_order_id     = Column(String, unique=True, nullable=True)
    symbol            = Column(String, nullable=False)
    underlying        = Column(String, nullable=False)
    expiry            = Column(String, nullable=True)
    strike_price      = Column(Numeric(10, 2), nullable=True)
    option_type       = Column(String, nullable=True)
    order_type        = Column(Enum("BUY", "SELL", name="order_type"), nullable=False)
    quantity          = Column(Integer, nullable=False)
    price             = Column(Numeric(10, 2), nullable=False)
    trigger_price     = Column(Numeric(10, 2), nullable=True)
    executed_price    = Column(Numeric(10, 2), nullable=True)
    status            = Column(Enum("PENDING", "EXECUTED", "REJECTED", "CANCELLED", name="order_status"), default="PENDING")
    is_paper_trade    = Column(Boolean, default=False)
    rejection_reason  = Column(String, nullable=True)
    placed_at         = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    executed_at       = Column(DateTime(timezone=True), nullable=True)
    updated_at        = Column(DateTime(timezone=True), onupdate=func.now())

    client_profile = relationship("ClientProfile", back_populates="orders")


class Position(Base):
    __tablename__ = "positions"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), nullable=False, index=True)
    symbol            = Column(String, nullable=False)
    underlying        = Column(String, nullable=False)
    expiry            = Column(String, nullable=True)
    strike_price      = Column(Numeric(10, 2), nullable=True)
    option_type       = Column(String, nullable=True)
    quantity          = Column(Integer, nullable=False)
    avg_cost          = Column(Numeric(10, 2), nullable=False)
    ltp               = Column(Numeric(10, 2), nullable=False)
    realized_pnl      = Column(Numeric(12, 2), default=0)
    unrealized_pnl    = Column(Numeric(12, 2), default=0)
    status            = Column(Enum("OPEN", "CLOSED", name="position_status"), default="OPEN")
    opened_at         = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    closed_at         = Column(DateTime(timezone=True), nullable=True)
    updated_at        = Column(DateTime(timezone=True), onupdate=func.now())

    client_profile = relationship("ClientProfile", back_populates="positions")


class DailyPnl(Base):
    __tablename__ = "daily_pnl"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), nullable=False, index=True)
    date              = Column(Date, nullable=False, index=True)
    closed_pnl        = Column(Numeric(12, 2), default=0)
    running_pnl       = Column(Numeric(12, 2), default=0)
    total_pnl         = Column(Numeric(12, 2), default=0)
    trade_count       = Column(Integer, default=0)
    updated_at        = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    client_profile = relationship("ClientProfile", back_populates="daily_pnl")
