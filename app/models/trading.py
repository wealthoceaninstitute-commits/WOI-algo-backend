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


class AlgoStrategy(Base):
    """
    WOI Strategy configuration — one row per client.
    All editable parameters stored here. Master or client can update.
    """
    __tablename__ = "algo_strategies"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), unique=True, nullable=False)
    name              = Column(String, default="WOI")           # strategy name (editable label)
    is_active         = Column(Boolean, default=False)           # master on/off switch
    paper_trading     = Column(Boolean, default=True)            # paper vs live

    # ── Universe / Scanner ─────────────────────────────────────
    gap_min           = Column(Numeric(5, 2), default=3.0)       # min gap % for pre-open scan
    gap_max           = Column(Numeric(5, 2), default=8.0)       # max gap % for pre-open scan
    max_stocks_per_day= Column(Integer, default=5)               # max stocks to trade per day
    live_scan_seconds = Column(Integer, default=60)              # post-open live scan window

    # ── Risk ───────────────────────────────────────────────────
    risk_per_trade    = Column(Numeric(10, 2), default=400.0)    # ₹ risk per trade

    # ── Entry ──────────────────────────────────────────────────
    entry_buffer_pct  = Column(Numeric(6, 4), default=0.004)     # 0.4% entry buffer from close
    sl_pct            = Column(Numeric(6, 4), default=0.004)     # 0.4% SL from entry trigger
    entry_reference   = Column(String, default="close")          # "close" or "highlow"
    use_first_candle  = Column(Boolean, default=True)            # use first 1-min candle only
    disable_shift     = Column(Boolean, default=True)            # no dynamic order shifting
    boring_ratio      = Column(Numeric(5, 3), default=0.35)      # body/range ≤ this = boring candle

    # ── Missed move guard ──────────────────────────────────────
    entry_missed_cancel   = Column(Boolean, default=True)
    entry_missed_cancel_r = Column(Numeric(5, 2), default=1.5)   # cancel if price runs 1.5R past trigger

    # ── Target & Trail SL ──────────────────────────────────────
    target_r          = Column(Numeric(5, 2), default=4.0)       # take profit at 4R
    # Trail steps stored as JSON string: [[r_trigger, lock_r], ...]
    trail_sl_steps    = Column(Text, default='[[2.5,0.0],[3.0,0.5],[3.7,2.0]]')

    # ── TP on exchange ─────────────────────────────────────────
    tp_on_exchange        = Column(Boolean, default=True)
    tp_exchange_place_r   = Column(Numeric(5, 2), default=2.0)   # place TP once profit hits 2R
    tp_exchange_cancel_r  = Column(Numeric(5, 2), default=1.0)   # pull TP if profit drops below 1R

    # ── Re-entry ───────────────────────────────────────────────
    reentry_mode          = Column(String, default="both_sides")  # "same_side" or "both_sides"
    max_reentry_attempts  = Column(Integer, default=0)            # 0 = no re-entry

    # ── Gap direction ──────────────────────────────────────────
    gap_direction_bias    = Column(Boolean, default=False)        # False = OCO both sides
    sl_basis              = Column(String, default="trigger")     # "trigger" or "fill"

    # ── Stock universe & filters ────────────────────────────────
    universe_id           = Column(String, ForeignKey("stock_universes.id", ondelete="SET NULL"), nullable=True)
    min_price             = Column(Numeric(10, 2), default=50.0)    # skip stocks below ₹50
    max_price             = Column(Numeric(10, 2), default=10000.0) # skip stocks above ₹10,000
    min_volume            = Column(Integer, default=500000)          # min prev day volume (shares)
    min_turnover_cr       = Column(Numeric(10, 2), default=10.0)    # min prev day turnover (₹ crores)
    exclude_be_series     = Column(Boolean, default=False)           # exclude BE/surveillance stocks

    created_at        = Column(DateTime(timezone=True), server_default=func.now())
    updated_at        = Column(DateTime(timezone=True), onupdate=func.now())

    client_profile = relationship("ClientProfile", backref="algo_strategy")


class AlgoRun(Base):
    """Tracks each time the algo runs — status, stocks picked, results."""
    __tablename__ = "algo_runs"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), nullable=False, index=True)
    strategy_id       = Column(String, ForeignKey("algo_strategies.id", ondelete="SET NULL"), nullable=True)
    run_date          = Column(Date, nullable=False, index=True)
    status            = Column(String, default="idle")  # idle | scanning | running | done | error
    stocks_scanned    = Column(Integer, default=0)
    stocks_selected   = Column(Integer, default=0)
    stocks_traded     = Column(Integer, default=0)
    total_pnl         = Column(Numeric(12, 2), default=0)
    log               = Column(Text, default="")        # append-only run log
    started_at        = Column(DateTime(timezone=True), nullable=True)
    finished_at       = Column(DateTime(timezone=True), nullable=True)
    created_at        = Column(DateTime(timezone=True), server_default=func.now())

    client_profile = relationship("ClientProfile", backref="algo_runs")


class AlgoStock(Base):
    """Stocks selected by scanner for a given run — with entry/exit details."""
    __tablename__ = "algo_stocks"

    id                = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id            = Column(String, ForeignKey("algo_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    client_profile_id = Column(String, ForeignKey("client_profiles.id", ondelete="CASCADE"), nullable=False)
    symbol            = Column(String, nullable=False)
    security_id       = Column(String, nullable=False)
    gap_pct           = Column(Numeric(6, 2), nullable=True)
    direction         = Column(String, nullable=True)       # UP | DOWN
    prev_close        = Column(Numeric(10, 2), nullable=True)
    candle_high       = Column(Numeric(10, 2), nullable=True)
    candle_low        = Column(Numeric(10, 2), nullable=True)
    candle_close      = Column(Numeric(10, 2), nullable=True)
    buy_trigger       = Column(Numeric(10, 2), nullable=True)
    sell_trigger      = Column(Numeric(10, 2), nullable=True)
    entry_direction   = Column(String, nullable=True)       # BUY | SELL (which side triggered)
    entry_price       = Column(Numeric(10, 2), nullable=True)
    exit_price        = Column(Numeric(10, 2), nullable=True)
    quantity          = Column(Integer, nullable=True)
    pnl               = Column(Numeric(12, 2), nullable=True)
    status            = Column(String, default="watching")  # watching | entered | exited | cancelled
    buy_order_id      = Column(String, nullable=True)
    sell_order_id     = Column(String, nullable=True)
    source            = Column(String, default="preopen")   # preopen | live
    entry_time        = Column(DateTime(timezone=True), nullable=True)
    exit_time         = Column(DateTime(timezone=True), nullable=True)
    exit_reason       = Column(String, nullable=True)       # TARGET | SL | EOD | MANUAL
    created_at        = Column(DateTime(timezone=True), server_default=func.now())
    updated_at        = Column(DateTime(timezone=True), onupdate=func.now())

    run = relationship("AlgoRun", backref="stocks")
