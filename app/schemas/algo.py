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
            raw = json.loads(m.trail_sl_steps or "[]")
            steps = [TrailStep(r_trigger=s[0], lock_r=s[1]) for s in raw]
        except Exception:
            steps = []

        return cls(
            id=m.id,
            client_profile_id=m.client_profile_id,
            name=m.name,
            is_active=m.is_active,
            paper_trading=m.paper_trading,
            gap_min=float(m.gap_min),
            gap_max=float(m.gap_max),
            max_stocks_per_day=m.max_stocks_per_day,
            live_scan_seconds=m.live_scan_seconds,
            risk_per_trade=float(m.risk_per_trade),
            entry_buffer_pct=float(m.entry_buffer_pct),
            sl_pct=float(m.sl_pct),
            entry_reference=m.entry_reference,
            use_first_candle=m.use_first_candle,
            disable_shift=m.disable_shift,
            boring_ratio=float(m.boring_ratio),
            entry_missed_cancel=m.entry_missed_cancel,
            entry_missed_cancel_r=float(m.entry_missed_cancel_r),
            target_r=float(m.target_r),
            trail_sl_steps=steps,
            tp_on_exchange=m.tp_on_exchange,
            tp_exchange_place_r=float(m.tp_exchange_place_r),
            tp_exchange_cancel_r=float(m.tp_exchange_cancel_r),
            reentry_mode=m.reentry_mode,
            max_reentry_attempts=m.max_reentry_attempts,
            gap_direction_bias=m.gap_direction_bias,
            sl_basis=m.sl_basis,
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
