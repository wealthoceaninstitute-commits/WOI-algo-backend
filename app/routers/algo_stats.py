"""
app/routers/algo_stats.py — Algo performance statistics

GET /api/algo/stats/daily/{profile_id}?date=YYYY-MM-DD
GET /api/algo/stats/summary/{profile_id}?from_date=&to_date=
GET /api/algo/stats/all?from_date=&to_date=
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from datetime import date
from typing import Optional

from app.core.database import get_db
from app.core.security import require_master
from app.models.user import User
from app.models.trading import ClientProfile, AlgoRun, AlgoStock, AlgoStrategy

router = APIRouter(prefix="/api/algo/stats", tags=["algo-stats"])


def _ser_trade(s: AlgoStock, run_date, sl_pct: float = 0.004) -> dict:
    entry  = float(s.entry_price or 0)
    pnl    = float(s.pnl or 0)
    qty    = s.quantity or 1
    rps    = entry * sl_pct
    rr     = round(pnl / (rps * qty), 2) if rps > 0 and s.status == "exited" else None
    return {
        "id":              s.id,
        "run_date":        run_date.isoformat() if run_date else None,
        "symbol":          s.symbol,
        "gap_pct":         float(s.gap_pct or 0),
        "direction":       s.direction,
        "entry_direction": s.entry_direction,
        "entry_price":     entry,
        "exit_price":      float(s.exit_price or 0),
        "quantity":        qty,
        "pnl":             pnl,
        "status":          s.status,
        "exit_reason":     getattr(s, "exit_reason", None),
        "entry_time":      s.entry_time.isoformat()  if getattr(s, "entry_time",  None) else None,
        "exit_time":       s.exit_time.isoformat()   if getattr(s, "exit_time",   None) else None,
        "rr_achieved":     rr,
        "risk_per_trade":  round(rps * qty, 2),
        "candle_close":    float(s.candle_close  or 0),
        "buy_trigger":     float(s.buy_trigger   or 0),
        "sell_trigger":    float(s.sell_trigger  or 0),
    }


def _run_summary(run: AlgoRun) -> dict:
    stocks  = run.stocks or []
    traded  = [s for s in stocks if s.status == "exited"]
    winners = [s for s in traded if float(s.pnl or 0) > 0]
    losers  = [s for s in traded if float(s.pnl or 0) <= 0]
    total   = float(run.total_pnl or 0)
    return {
        "run_date":        run.run_date.isoformat() if run.run_date else None,
        "status":          run.status,
        "stocks_scanned":  run.stocks_scanned  or 0,
        "stocks_selected": run.stocks_selected or 0,
        "stocks_traded":   run.stocks_traded   or 0,
        "winners":         len(winners),
        "losers":          len(losers),
        "win_rate":        round(len(winners) / len(traded) * 100, 1) if traded else 0,
        "total_pnl":       total,
        "gross_profit":    round(sum(float(s.pnl or 0) for s in winners), 2),
        "gross_loss":      round(sum(float(s.pnl or 0) for s in losers),  2),
    }


def _overall(runs, fd, td) -> dict:
    all_t   = [s for r in runs for s in (r.stocks or []) if s.status == "exited"]
    winners = [s for s in all_t if float(s.pnl or 0) > 0]
    losers  = [s for s in all_t if float(s.pnl or 0) <= 0]
    tp      = sum(float(s.pnl or 0) for s in all_t)
    gp      = sum(float(s.pnl or 0) for s in winners)
    gl      = sum(float(s.pnl or 0) for s in losers)
    aw      = gp / len(winners) if winners else 0
    al      = abs(gl / len(losers)) if losers else 0
    pf      = abs(gp / gl) if gl else 0
    td_cnt  = len(runs)
    wd      = sum(1 for r in runs if float(r.total_pnl or 0) > 0)
    return {
        "from_date":          fd.isoformat() if fd else None,
        "to_date":            td.isoformat() if td else None,
        "trading_days":       td_cnt,
        "winning_days":       wd,
        "losing_days":        td_cnt - wd,
        "total_trades":       len(all_t),
        "winners":            len(winners),
        "losers":             len(losers),
        "win_rate":           round(len(winners) / len(all_t) * 100, 1) if all_t else 0,
        "total_pnl":          round(tp, 2),
        "gross_profit":       round(gp, 2),
        "gross_loss":         round(gl, 2),
        "avg_win":            round(aw, 2),
        "avg_loss":           round(al, 2),
        "profit_factor":      round(pf, 2),
        "total_scanned":      sum(r.stocks_scanned  or 0 for r in runs),
        "total_selected":     sum(r.stocks_selected or 0 for r in runs),
    }


@router.get("/daily/{profile_id}")
def daily_stats(
    profile_id: str,
    run_date: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    target = date.fromisoformat(run_date) if run_date else date.today()
    run = db.query(AlgoRun).filter(
        AlgoRun.client_profile_id == profile_id,
        AlgoRun.run_date == target,
    ).first()
    if not run:
        return {"run_date": target.isoformat(), "run": None, "trades": [], "watching": [], "log": ""}

    strategy = db.query(AlgoStrategy).filter(
        AlgoStrategy.client_profile_id == profile_id
    ).first()
    sl_pct = float(strategy.sl_pct) if strategy else 0.004

    stocks  = run.stocks or []
    trades  = [_ser_trade(s, run.run_date, sl_pct) for s in stocks
               if s.status in ("exited", "entered")]
    others  = [_ser_trade(s, run.run_date, sl_pct) for s in stocks
               if s.status in ("watching", "cancelled")]

    return {
        "run_date": target.isoformat(),
        "run":      _run_summary(run),
        "trades":   trades,
        "watching": others,
        "log":      run.log or "",
    }


@router.get("/summary/{profile_id}")
def summary_stats(
    profile_id: str,
    from_date: Optional[str] = Query(None),
    to_date:   Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    fd = date.fromisoformat(from_date) if from_date else None
    td = date.fromisoformat(to_date)   if to_date   else date.today()
    q  = db.query(AlgoRun).filter(
        AlgoRun.client_profile_id == profile_id,
        AlgoRun.status == "done",
    )
    if fd: q = q.filter(AlgoRun.run_date >= fd)
    if td: q = q.filter(AlgoRun.run_date <= td)
    runs = q.order_by(AlgoRun.run_date.desc()).all()

    strategy = db.query(AlgoStrategy).filter(
        AlgoStrategy.client_profile_id == profile_id
    ).first()

    return {
        "overall":       _overall(runs, fd, td),
        "daily":         [_run_summary(r) for r in runs],
        "strategy_name": strategy.name      if strategy else "WOI",
        "target_r":      float(strategy.target_r) if strategy else 4.0,
        "sl_pct":        float(strategy.sl_pct)   if strategy else 0.004,
    }


@router.get("/all")
def all_clients_summary(
    from_date: Optional[str] = Query(None),
    to_date:   Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    fd = date.fromisoformat(from_date) if from_date else None
    td = date.fromisoformat(to_date)   if to_date   else date.today()
    profiles = db.query(ClientProfile).all()
    result   = []
    for profile in profiles:
        q = db.query(AlgoRun).filter(
            AlgoRun.client_profile_id == profile.id,
            AlgoRun.status == "done",
        )
        if fd: q = q.filter(AlgoRun.run_date >= fd)
        if td: q = q.filter(AlgoRun.run_date <= td)
        runs = q.all()
        result.append({
            "profile_id":  profile.id,
            "client_name": profile.user.name if profile.user else profile.id[:8],
            **_overall(runs, fd, td),
        })
    return result
