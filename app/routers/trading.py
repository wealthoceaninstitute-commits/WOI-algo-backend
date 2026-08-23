from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload
from datetime import date, datetime
from typing import Optional

from app.core.database import get_db
from app.core.security import get_current_user, require_master
from app.models.user import User
from app.models.trading import ClientProfile, Order, Position, DailyPnl, Fund

router = APIRouter(prefix="/api", tags=["trading"])


def _profile(user: User, db: Session) -> ClientProfile:
    p = db.query(ClientProfile).filter(ClientProfile.user_id == user.id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Client profile not found")
    return p


def _order_dict(o: Order, client_name: str = None) -> dict:
    return {
        "id": o.id,
        "dhan_order_id": o.dhan_order_id,
        "symbol": o.symbol,
        "underlying": o.underlying,
        "expiry": o.expiry,
        "strike_price": float(o.strike_price) if o.strike_price else None,
        "option_type": o.option_type,
        "order_type": o.order_type,
        "quantity": o.quantity,
        "price": float(o.price),
        "executed_price": float(o.executed_price) if o.executed_price else None,
        "status": o.status,
        "is_paper_trade": o.is_paper_trade,
        "rejection_reason": o.rejection_reason,
        "placed_at": o.placed_at.isoformat() if o.placed_at else None,
        "executed_at": o.executed_at.isoformat() if o.executed_at else None,
        "client_name": client_name,
    }


def _position_dict(p: Position, client_name: str = None) -> dict:
    return {
        "id": p.id,
        "symbol": p.symbol,
        "underlying": p.underlying,
        "expiry": p.expiry,
        "strike_price": float(p.strike_price) if p.strike_price else None,
        "option_type": p.option_type,
        "quantity": p.quantity,
        "avg_cost": float(p.avg_cost),
        "ltp": float(p.ltp),
        "realized_pnl": float(p.realized_pnl),
        "unrealized_pnl": float(p.unrealized_pnl),
        "total_pnl": float(p.realized_pnl) + float(p.unrealized_pnl),
        "status": p.status,
        "opened_at": p.opened_at.isoformat() if p.opened_at else None,
        "closed_at": p.closed_at.isoformat() if p.closed_at else None,
        "client_name": client_name,
    }


# ── Orders ────────────────────────────────────────────────────────────────────

@router.get("/orders")
def list_orders(
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    q = db.query(Order).filter(Order.client_profile_id == profile.id)
    if status and status.upper() != "ALL":
        q = q.filter(Order.status == status.upper())
    orders = q.order_by(Order.placed_at.desc()).limit(200).all()
    return [_order_dict(o) for o in orders]


@router.get("/orders/all")
def list_all_orders(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Master: all orders across all clients."""
    orders = (
        db.query(Order)
        .options(joinedload(Order.client_profile).joinedload(ClientProfile.user))
        .order_by(Order.placed_at.desc())
        .limit(300)
        .all()
    )
    return [_order_dict(o, o.client_profile.user.name) for o in orders]


# ── Positions ─────────────────────────────────────────────────────────────────

@router.get("/positions")
def list_positions(
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    q = db.query(Position).filter(Position.client_profile_id == profile.id)
    if status and status.upper() != "ALL":
        q = q.filter(Position.status == status.upper())
    positions = q.order_by(Position.opened_at.desc()).limit(200).all()
    return [_position_dict(p) for p in positions]


@router.get("/positions/all")
def list_all_positions(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    positions = (
        db.query(Position)
        .options(joinedload(Position.client_profile).joinedload(ClientProfile.user))
        .order_by(Position.opened_at.desc())
        .limit(300)
        .all()
    )
    return [_position_dict(p, p.client_profile.user.name) for p in positions]


# ── Portfolio ─────────────────────────────────────────────────────────────────

@router.get("/portfolio")
def portfolio(
    month: Optional[str] = Query(None, description="YYYY-MM"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    m = month or datetime.now().strftime("%Y-%m")
    year, mon = map(int, m.split("-"))
    start = date(year, mon, 1)
    end = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)

    rows = (
        db.query(DailyPnl)
        .filter(
            DailyPnl.client_profile_id == profile.id,
            DailyPnl.date >= start,
            DailyPnl.date < end,
        )
        .order_by(DailyPnl.date)
        .all()
    )

    daily = [
        {
            "date": r.date.isoformat(),
            "closed_pnl": float(r.closed_pnl),
            "running_pnl": float(r.running_pnl),
            "total_pnl": float(r.total_pnl),
            "trade_count": r.trade_count,
        }
        for r in rows
    ]

    fund = profile.fund
    return {
        "daily_pnl": daily,
        "month_total": sum(d["total_pnl"] for d in daily),
        "funds": {
            "available": float(fund.available) if fund else 0.0,
            "used_margin": float(fund.used_margin) if fund else 0.0,
            "total_balance": float(fund.total_balance) if fund else 0.0,
        },
    }


@router.get("/portfolio/summary")
def pnl_summary(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    today = date.today()
    row = (
        db.query(DailyPnl)
        .filter(DailyPnl.client_profile_id == profile.id, DailyPnl.date == today)
        .first()
    )
    return {
        "today_pnl": float(row.total_pnl) if row else 0.0,
        "closed_pnl": float(row.closed_pnl) if row else 0.0,
        "running_pnl": float(row.running_pnl) if row else 0.0,
        "trade_count": row.trade_count if row else 0,
    }
