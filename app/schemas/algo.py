from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, date
import json


class TrailStep(BaseModel):
    r_trigger: float
    lock_r: float


class AlgoStrategyRequest(BaseModel):
    name: str = "WOI"
    is_active: bool = False
    paper_trading: bool = True

    # Scanner
    gap_min: float = Field(3.0, ge=0.1, le=20.0)
    gap_max: float = Field(8.0, ge=0.1, le=30.0)
    max_stocks_per_day: int = Field(5, ge=1, le=20)
    live_scan_seconds: int = Field(60, ge=10, le=300)

    # Risk
    risk_per_trade: float = Field(400.0, ge=50.0, le=100000.0)

    # Entry
    entry_buffer_pct: float = Field(0.004, ge=0.001, le=0.05)
    sl_pct: float = Field(0.004, ge=0.001, le=0.05)
    entry_reference: str = "close"          # "close" | "highlow"
    use_first_candle: bool = True
    disable_shift: bool = True
    boring_ratio: float = Field(0.35, ge=0.1, le=0.9)

    # Missed move guard
    entry_missed_cancel: bool = True
    entry_missed_cancel_r: float = Field(1.5, ge=0.5, le=5.0)

    # Target & trail
    target_r: float = Field(4.0, ge=1.0, le=20.0)
    trail_sl_steps: List[TrailStep] = [
        TrailStep(r_trigger=2.5, lock_r=0.0),
        TrailStep(r_trigger=3.0, lock_r=0.5),
        TrailStep(r_trigger=3.7, lock_r=2.0),
    ]

    # TP on exchange
    tp_on_exchange: bool = True
    tp_exchange_place_r: float = Field(2.0, ge=0.5, le=10.0)
    tp_exchange_cancel_r: float = Field(1.0, ge=0.1, le=10.0)

    # Re-entry
    reentry_mode: str = "both_sides"        # "same_side" | "both_sides"
    max_reentry_attempts: int = Field(0, ge=0, le=3)

    # Direction
    gap_direction_bias: bool = False
    sl_basis: str = "trigger"               # "trigger" | "fill"

    # Stock universe & filters (0 = disabled for all numeric filters)
    universe_id:       Optional[str]   = None
    min_price:         float           = Field(0.0,  ge=0)
    max_price:         float           = Field(0.0,  ge=0)
    min_volume:        int             = Field(0,    ge=0)
    min_turnover_cr:   float           = Field(0.0,  ge=0)
    exclude_be_series: bool            = False


class AlgoStrategyResponse(AlgoStrategyRequest):
    id: str
    client_profile_id: str
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True

    @classmethod
    def from_orm_model(cls, m):
        steps = []
        try:
            from app.services.algo_engine import parse_trail_steps
            steps = [TrailStep(r_trigger=a, lock_r=b) for a, b in parse_trail_steps(m.trail_sl_steps)]
        except Exception:
            steps = []

        # NULL / bad values in an old row must never take the page down — fall back to the defaults.
        def F(v, d):
            try:
                return float(v) if v is not None else d
            except Exception:
                return d

        def I(v, d):
            try:
                return int(v) if v is not None else d
            except Exception:
                return d

        def B(v, d):
            return bool(v) if v is not None else d

        def S(v, d):
            return v if v else d

        # model_construct: values are returned as stored (no range re-validation on the way OUT)
        return cls.model_construct(
            id=m.id,
            client_profile_id=m.client_profile_id,
            name=S(m.name, "WOI"),
            is_active=B(m.is_active, False),
            paper_trading=B(m.paper_trading, True),
            gap_min=F(m.gap_min, 3.0),
            gap_max=F(m.gap_max, 8.0),
            max_stocks_per_day=I(m.max_stocks_per_day, 5),
            live_scan_seconds=I(m.live_scan_seconds, 60),
            risk_per_trade=F(m.risk_per_trade, 400.0),
            entry_buffer_pct=F(m.entry_buffer_pct, 0.004),
            sl_pct=F(m.sl_pct, 0.004),
            entry_reference=S(m.entry_reference, "close"),
            use_first_candle=B(m.use_first_candle, True),
            disable_shift=B(m.disable_shift, True),
            boring_ratio=F(m.boring_ratio, 0.35),
            entry_missed_cancel=B(m.entry_missed_cancel, True),
            entry_missed_cancel_r=F(m.entry_missed_cancel_r, 1.5),
            target_r=F(m.target_r, 4.0),
            trail_sl_steps=steps,
            tp_on_exchange=B(m.tp_on_exchange, True),
            tp_exchange_place_r=F(m.tp_exchange_place_r, 2.0),
            tp_exchange_cancel_r=F(m.tp_exchange_cancel_r, 1.0),
            reentry_mode=S(m.reentry_mode, "both_sides"),
            max_reentry_attempts=I(m.max_reentry_attempts, 0),
            gap_direction_bias=B(m.gap_direction_bias, False),
            sl_basis=S(m.sl_basis, "trigger"),
            universe_id=m.universe_id,
            min_price=float(m.min_price) if m.min_price else 50.0,
            max_price=float(m.max_price) if m.max_price else 10000.0,
            min_volume=m.min_volume or 500000,
            min_turnover_cr=float(m.min_turnover_cr) if m.min_turnover_cr else 10.0,
            exclude_be_series=m.exclude_be_series or False,
            created_at=m.created_at,
            updated_at=m.updated_at,
        )


class AlgoStockResponse(BaseModel):
    id: str
    symbol: str
    security_id: str
    gap_pct: Optional[float]
    direction: Optional[str]
    prev_close: Optional[float]
    candle_high: Optional[float]
    candle_low: Optional[float]
    candle_close: Optional[float]
    buy_trigger: Optional[float]
    sell_trigger: Optional[float]
    entry_direction: Optional[str]
    entry_price: Optional[float]
    exit_price: Optional[float]
    quantity: Optional[int]
    pnl: Optional[float]
    status: str
    buy_order_id: Optional[str]
    sell_order_id: Optional[str]
    source: str

    class Config:
        from_attributes = True


class AlgoRunResponse(BaseModel):
    id: str
    run_date: date
    status: str
    stocks_scanned: int
    stocks_selected: int
    stocks_traded: int
    total_pnl: float
    log: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    stocks: List[AlgoStockResponse] = []

    class Config:
        from_attributes = True
