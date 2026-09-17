"""
Algo router — WOI strategy configuration + run management.

Endpoints:
  GET/PUT  /api/algo/strategy          — get/update client's strategy config
  GET      /api/algo/strategy/defaults — get default values with labels
  GET      /api/algo/runs              — list runs for client
  GET      /api/algo/runs/latest       — latest run status + stocks
  POST     /api/algo/runs/{id}/stocks  — add a stock manually to a run
  DELETE   /api/algo/runs/{id}/stocks/{stock_id} — remove a stock

Master:
  GET      /api/algo/all              — all clients' strategy configs
  PUT      /api/algo/{profile_id}/strategy — update any client's strategy
  GET      /api/algo/{profile_id}/runs     — any client's runs
"""

import json
from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from fastapi import BackgroundTasks
from datetime import datetime, timezone
from app.core.security import get_current_user, require_master
from app.models.user import User
from app.models.trading import ClientProfile, AlgoStrategy, AlgoRun, AlgoStock
from app.schemas.algo import (
    AlgoStrategyRequest, AlgoStrategyResponse,
    AlgoRunResponse, AlgoStockResponse,
)

router = APIRouter(prefix="/api/algo", tags=["algo"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _profile(user: User, db: Session) -> ClientProfile:
    p = db.query(ClientProfile).filter(ClientProfile.user_id == user.id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Client profile not found")
    return p


def _get_or_create_strategy(profile_id: str, db: Session) -> AlgoStrategy:
    s = db.query(AlgoStrategy).filter(AlgoStrategy.client_profile_id == profile_id).first()
    if not s:
        s = AlgoStrategy(client_profile_id=profile_id)
        db.add(s)
        db.commit()
        db.refresh(s)
    return s


def _apply_strategy(s: AlgoStrategy, payload: AlgoStrategyRequest) -> AlgoStrategy:
    s.name                  = payload.name
    s.is_active             = payload.is_active
    s.paper_trading         = payload.paper_trading
    s.gap_min               = payload.gap_min
    s.gap_max               = payload.gap_max
    s.max_stocks_per_day    = payload.max_stocks_per_day
    s.live_scan_seconds     = payload.live_scan_seconds
    s.risk_per_trade        = payload.risk_per_trade
    s.entry_buffer_pct      = payload.entry_buffer_pct
    s.sl_pct                = payload.sl_pct
    s.entry_reference       = payload.entry_reference
    s.use_first_candle      = payload.use_first_candle
    s.disable_shift         = payload.disable_shift
    s.boring_ratio          = payload.boring_ratio
    s.entry_missed_cancel   = payload.entry_missed_cancel
    s.entry_missed_cancel_r = payload.entry_missed_cancel_r
    s.target_r              = payload.target_r
    s.trail_sl_steps        = json.dumps([[t.r_trigger, t.lock_r] for t in payload.trail_sl_steps])
    s.tp_on_exchange        = payload.tp_on_exchange
    s.tp_exchange_place_r   = payload.tp_exchange_place_r
    s.tp_exchange_cancel_r  = payload.tp_exchange_cancel_r
    s.reentry_mode          = payload.reentry_mode
    s.max_reentry_attempts  = payload.max_reentry_attempts
    s.gap_direction_bias    = payload.gap_direction_bias
    s.sl_basis              = payload.sl_basis
    # Stock universe & filters
    s.universe_id           = payload.universe_id
    s.min_price             = payload.min_price
    s.max_price             = payload.max_price
    s.min_volume            = payload.min_volume
    s.min_turnover_cr       = payload.min_turnover_cr
    s.exclude_be_series     = payload.exclude_be_series
    return s


def _ser_run(r: AlgoRun) -> dict:
    return {
        "id": r.id,
        "run_date": r.run_date.isoformat() if r.run_date else None,
        "status": r.status,
        "stocks_scanned": r.stocks_scanned,
        "stocks_selected": r.stocks_selected,
        "stocks_traded": r.stocks_traded,
        "total_pnl": float(r.total_pnl or 0),
        "log": r.log or "",
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        "stocks": [_ser_stock(s) for s in (r.stocks or [])],
    }


def _ser_stock(s: AlgoStock) -> dict:
    return {
        "id": s.id,
        "symbol": s.symbol,
        "security_id": s.security_id,
        "gap_pct": float(s.gap_pct) if s.gap_pct else None,
        "direction": s.direction,
        "prev_close": float(s.prev_close) if s.prev_close else None,
        "candle_high": float(s.candle_high) if s.candle_high else None,
        "candle_low": float(s.candle_low) if s.candle_low else None,
        "candle_close": float(s.candle_close) if s.candle_close else None,
        "buy_trigger": float(s.buy_trigger) if s.buy_trigger else None,
        "sell_trigger": float(s.sell_trigger) if s.sell_trigger else None,
        "entry_direction": s.entry_direction,
        "entry_price": float(s.entry_price) if s.entry_price else None,
        "exit_price": float(s.exit_price) if s.exit_price else None,
        "quantity": s.quantity,
        "pnl": float(s.pnl) if s.pnl else None,
        "status": s.status,
        "buy_order_id": s.buy_order_id,
        "sell_order_id": s.sell_order_id,
        "source": s.source,
    }


# ── Strategy — client self ─────────────────────────────────────────────────────

@router.get("/strategy")
def get_my_strategy(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    s = _get_or_create_strategy(profile.id, db)
    return AlgoStrategyResponse.from_orm_model(s)


@router.put("/strategy")
def update_my_strategy(
    payload: AlgoStrategyRequest,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    s = _get_or_create_strategy(profile.id, db)
    s = _apply_strategy(s, payload)
    db.commit()
    db.refresh(s)
    return AlgoStrategyResponse.from_orm_model(s)


@router.get("/strategy/defaults")
def get_defaults():
    """Returns default values with field labels — used to render the form."""
    return {
        "sections": [
            {
                "title": "Strategy Info",
                "fields": [
                    {"key": "name", "label": "Strategy name", "type": "text", "default": "WOI"},
                    {"key": "paper_trading", "label": "Paper trading (simulate only)", "type": "bool", "default": True},
                    {"key": "is_active", "label": "Strategy active", "type": "bool", "default": False},
                ],
            },
            {
                "title": "Scanner",
                "fields": [
                    {"key": "gap_min", "label": "Min gap %", "type": "number", "default": 3.0, "min": 0.1, "max": 20.0, "step": 0.5},
                    {"key": "gap_max", "label": "Max gap %", "type": "number", "default": 8.0, "min": 0.5, "max": 30.0, "step": 0.5},
                    {"key": "max_stocks_per_day", "label": "Max stocks per day", "type": "int", "default": 5, "min": 1, "max": 20},
                    {"key": "live_scan_seconds", "label": "Live scan window (seconds)", "type": "int", "default": 60, "min": 10, "max": 300},
                ],
            },
            {
                "title": "Risk",
                "fields": [
                    {"key": "risk_per_trade", "label": "Risk per trade (₹)", "type": "number", "default": 400.0, "min": 50, "max": 100000, "step": 50},
                ],
            },
            {
                "title": "Entry",
                "fields": [
                    {"key": "use_first_candle", "label": "Use first 1-min candle only (ignore boring filter)", "type": "bool", "default": True},
                    {"key": "entry_reference", "label": "Entry reference", "type": "select", "default": "close", "options": [{"value": "close", "label": "Close price ± buffer"}, {"value": "highlow", "label": "High/Low ± buffer"}]},
                    {"key": "entry_buffer_pct", "label": "Entry buffer %", "type": "percent", "default": 0.004, "min": 0.001, "max": 0.05, "step": 0.001},
                    {"key": "sl_pct", "label": "Stop loss %", "type": "percent", "default": 0.004, "min": 0.001, "max": 0.05, "step": 0.001},
                    {"key": "boring_ratio", "label": "Boring candle ratio (body/range ≤)", "type": "number", "default": 0.35, "min": 0.1, "max": 0.9, "step": 0.05},
                    {"key": "disable_shift", "label": "Lock entry levels (no dynamic shifting)", "type": "bool", "default": True},
                    {"key": "sl_basis", "label": "SL basis", "type": "select", "default": "trigger", "options": [{"value": "trigger", "label": "From trigger price"}, {"value": "fill", "label": "From actual fill price"}]},
                    {"key": "gap_direction_bias", "label": "Gap direction bias (enter only gap direction)", "type": "bool", "default": False},
                ],
            },
            {
                "title": "Missed Move Guard",
                "fields": [
                    {"key": "entry_missed_cancel", "label": "Cancel if missed move", "type": "bool", "default": True},
                    {"key": "entry_missed_cancel_r", "label": "Cancel after price moves (R multiples)", "type": "number", "default": 1.5, "min": 0.5, "max": 5.0, "step": 0.5},
                ],
            },
            {
                "title": "Target & Trailing SL",
                "fields": [
                    {"key": "target_r", "label": "Target (R multiples, e.g. 4 = 1:4)", "type": "number", "default": 4.0, "min": 1.0, "max": 20.0, "step": 0.5},
                    {"key": "trail_sl_steps", "label": "Trail SL steps [[trigger_R, lock_R], ...]", "type": "trail_steps", "default": [[2.5, 0.0], [3.0, 0.5], [3.7, 2.0]]},
                    {"key": "tp_on_exchange", "label": "Place TP on exchange (margin-saving)", "type": "bool", "default": True},
                    {"key": "tp_exchange_place_r", "label": "Place exchange TP at (R)", "type": "number", "default": 2.0, "min": 0.5, "max": 10.0, "step": 0.5},
                    {"key": "tp_exchange_cancel_r", "label": "Pull exchange TP below (R)", "type": "number", "default": 1.0, "min": 0.1, "max": 10.0, "step": 0.5},
                ],
            },
            {
                "title": "Re-entry",
                "fields": [
                    {"key": "max_reentry_attempts", "label": "Max re-entry attempts (0 = none)", "type": "int", "default": 0, "min": 0, "max": 3},
                    {"key": "reentry_mode", "label": "Re-entry mode", "type": "select", "default": "both_sides", "options": [{"value": "both_sides", "label": "Both sides (new OCO)"}, {"value": "same_side", "label": "Same side only"}]},
                ],
            },
        ]
    }


# ── Runs — client ─────────────────────────────────────────────────────────────

@router.get("/runs")
def get_my_runs(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    runs = (
        db.query(AlgoRun)
        .filter(AlgoRun.client_profile_id == profile.id)
        .order_by(AlgoRun.run_date.desc())
        .limit(30)
        .all()
    )
    return [_ser_run(r) for r in runs]


@router.get("/runs/latest")
def get_latest_run(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    run = (
        db.query(AlgoRun)
        .filter(AlgoRun.client_profile_id == profile.id)
        .order_by(AlgoRun.run_date.desc(), AlgoRun.created_at.desc())
        .first()
    )
    if not run:
        return {"status": "idle", "stocks": [], "log": "No runs yet"}
    return _ser_run(run)


@router.post("/runs/{run_id}/stocks")
def add_stock_to_run(
    run_id: str,
    payload: dict,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Manually add a stock to today's run."""
    profile = _profile(user, db)
    run = db.query(AlgoRun).filter(
        AlgoRun.id == run_id,
        AlgoRun.client_profile_id == profile.id
    ).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    symbol = (payload.get("symbol") or "").upper().strip()
    security_id = str(payload.get("security_id") or "")
    if not symbol or not security_id:
        raise HTTPException(status_code=400, detail="symbol and security_id required")

    stock = AlgoStock(
        run_id=run_id,
        client_profile_id=profile.id,
        symbol=symbol,
        security_id=security_id,
        source="manual",
        status="watching",
    )
    db.add(stock)
    run.stocks_selected = (run.stocks_selected or 0) + 1
    db.commit()
    db.refresh(stock)
    return _ser_stock(stock)


@router.delete("/runs/{run_id}/stocks/{stock_id}", status_code=204)
def remove_stock_from_run(
    run_id: str,
    stock_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    stock = db.query(AlgoStock).filter(
        AlgoStock.id == stock_id,
        AlgoStock.run_id == run_id,
        AlgoStock.client_profile_id == profile.id,
    ).first()
    if stock:
        run = db.query(AlgoRun).filter(AlgoRun.id == run_id).first()
        if run:
            run.stocks_selected = max(0, (run.stocks_selected or 1) - 1)
        db.delete(stock)
        db.commit()


# ── Master — view all clients ─────────────────────────────────────────────────

@router.get("/all")
def get_all_strategies(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    strategies = db.query(AlgoStrategy).all()
    return [AlgoStrategyResponse.from_orm_model(s) for s in strategies]


@router.put("/{client_profile_id}/strategy")
def update_client_strategy(
    client_profile_id: str,
    payload: AlgoStrategyRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    profile = db.query(ClientProfile).filter(ClientProfile.id == client_profile_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Client not found")
    s = _get_or_create_strategy(client_profile_id, db)
    s = _apply_strategy(s, payload)
    db.commit()
    db.refresh(s)
    return AlgoStrategyResponse.from_orm_model(s)


@router.get("/{client_profile_id}/runs")
def get_client_runs(
    client_profile_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    runs = (
        db.query(AlgoRun)
        .filter(AlgoRun.client_profile_id == client_profile_id)
        .order_by(AlgoRun.run_date.desc())
        .limit(30)
        .all()
    )
    return [_ser_run(r) for r in runs]


# ── Manual trigger — master can force-start algo run ─────────────────────────

@router.post("/trigger-run")
async def trigger_algo_run(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """
    Manually trigger the algo run immediately.
    Useful for testing or if scheduled run was missed.
    Runs in background — returns immediately.
    """
    from app.services.algo_engine import run_daily_algo
    background_tasks.add_task(run_daily_algo)
    return {
        "message": "Algo run triggered. Check Railway logs and client WOI Algo page for progress.",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Debug: price snapshot ─────────────────────────────────────────────────────

@router.get("/debug/snapshot")
def get_price_snapshot(
    date: Optional[str] = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """
    GET /api/algo/debug/snapshot?date=2026-09-10
    Returns the daily_price_snapshots table for a given date (default: today).
    Shows prev_close (8:45 AM), open_price (9:12:30), and computed gap_pct
    for all 501 universe stocks — visible without restarting the server.
    """
    from datetime import date as date_type
    from app.models.trading import DailyPriceSnapshot

    target = date_type.today()
    if date:
        try:
            target = date_type.fromisoformat(date)
        except ValueError:
            from fastapi import HTTPException
            raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD")

    rows = (
        db.query(DailyPriceSnapshot)
        .filter(DailyPriceSnapshot.trade_date == target)
        .order_by(DailyPriceSnapshot.symbol)
        .all()
    )

    data = [
        {
            "security_id": r.security_id,
            "symbol":      r.symbol,
            "prev_close":  float(r.prev_close)  if r.prev_close  else None,
            "open_price":  float(r.open_price)  if r.open_price  else None,
            "gap_pct":     float(r.gap_pct)     if r.gap_pct     else None,
        }
        for r in rows
    ]

    # Summary stats
    with_prev  = sum(1 for d in data if d["prev_close"])
    with_open  = sum(1 for d in data if d["open_price"])
    with_gap   = sum(1 for d in data if d["gap_pct"] is not None)
    gap_3_8    = [d for d in data if d["gap_pct"] and 3.0 <= abs(d["gap_pct"]) <= 8.0]
    gap_above8 = [d for d in data if d["gap_pct"] and abs(d["gap_pct"]) > 8.0]

    return {
        "date":        target.isoformat(),
        "total_rows":  len(data),
        "prev_close_filled":  with_prev,
        "open_price_filled":  with_open,
        "gap_computed":       with_gap,
        "gap_3_8pct_count":   len(gap_3_8),
        "gap_above8pct_count":len(gap_above8),
        "gap_3_8_stocks":     sorted(gap_3_8, key=lambda x: abs(x["gap_pct"]), reverse=True),
        "all_stocks":         data,
    }
