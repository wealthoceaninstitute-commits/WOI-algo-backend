"""
Trading router — fetches live data from Dhan API for the logged-in client.
Falls back to DB records if Dhan creds not set or API fails.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import date, datetime
from typing import Optional

from app.core.database import get_db
from app.core.security import get_current_user, require_master
from app.core.encryption import decrypt
from app.models.user import User
from app.models.trading import ClientProfile, Order, Position, DailyPnl, Fund
from app.services.dhan_trade import (
    get_order_book, get_trade_book, get_positions, get_holdings,
    get_fund_limit, get_trades_by_order, get_order_by_id,
    normalize_order, normalize_position, normalize_trade,
)

router = APIRouter(prefix="/api", tags=["trading"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _profile(user: User, db: Session) -> ClientProfile:
    p = db.query(ClientProfile).filter(ClientProfile.user_id == user.id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Client profile not found")
    return p


def _dhan_creds(profile: ClientProfile):
    """Return (access_token, dhan_client_id) or (None, None)."""
    cred = profile.dhan_cred
    if not cred or not cred.is_active:
        return None, None
    try:
        return decrypt(cred.access_token), decrypt(cred.dhan_client_id)
    except Exception:
        return None, None


def _proxy_kwargs(profile: ClientProfile) -> dict:
    p = profile.proxy_setting
    if not p or not p.is_active:
        return {}
    return {
        "proxy_host": p.host,
        "proxy_port": p.port,
        "proxy_user": p.username,
        "proxy_pass": decrypt(p.password) if p.password else None,
    }


def _db_order(o: Order, client_name: str = None) -> dict:
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
        "source": "db",
    }


def _db_position(p: Position, client_name: str = None) -> dict:
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
        "source": "db",
    }


# ── Orders ────────────────────────────────────────────────────────────────────

@router.get("/orders")
async def list_orders(
    status: Optional[str] = Query(None),
    source: Optional[str] = Query("dhan", description="dhan | db"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Fetch today's orders.
    source=dhan  → live from Dhan API (default)
    source=db    → from our local database
    """
    profile = _profile(user, db)

    if source == "dhan":
        token, _ = _dhan_creds(profile)
        if token:
            result = await get_order_book(token, **_proxy_kwargs(profile))
            if result["success"]:
                orders = [normalize_order(o) for o in (result["data"] or [])]
                # Filter by status if requested
                if status and status.upper() != "ALL":
                    mapped = {"PENDING": ["PENDING", "TRANSIT", "PART_TRADED"],
                              "EXECUTED": ["TRADED"], "REJECTED": ["REJECTED"],
                              "CANCELLED": ["CANCELLED", "EXPIRED"]}
                    allow = mapped.get(status.upper(), [status.upper()])
                    orders = [o for o in orders if o["status"].upper() in
                              [_map_status_reverse(a) for a in allow]]
                return {"source": "dhan", "data": orders}

    # Fallback to DB
    q = db.query(Order).filter(Order.client_profile_id == profile.id)
    if status and status.upper() != "ALL":
        q = q.filter(Order.status == status.upper())
    rows = q.order_by(Order.placed_at.desc()).limit(200).all()
    return {"source": "db", "data": [_db_order(o) for o in rows]}


def _map_status_reverse(s: str) -> str:
    m = {"PENDING": "PENDING", "EXECUTED": "TRADED",
         "REJECTED": "REJECTED", "CANCELLED": "CANCELLED"}
    return m.get(s, s)


@router.get("/orders/{order_id}")
async def get_order(
    order_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get a single order by Dhan order ID."""
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)
    if token:
        result = await get_order_by_id(token, order_id, **_proxy_kwargs(profile))
        if result["success"]:
            return {"source": "dhan", "data": normalize_order(result["data"])}
    raise HTTPException(status_code=404, detail="Order not found or API not connected")


@router.get("/orders/all")
async def list_all_orders(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Master: all orders from DB across all clients."""
    from sqlalchemy.orm import joinedload
    orders = (
        db.query(Order)
        .options(joinedload(Order.client_profile).joinedload(ClientProfile.user))
        .order_by(Order.placed_at.desc())
        .limit(500)
        .all()
    )
    return {"source": "db", "data": [_db_order(o, o.client_profile.user.name) for o in orders]}


# ── Trades ────────────────────────────────────────────────────────────────────

@router.get("/trades")
async def list_trades(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Today's executed trades — live from Dhan trade book.
    Only orders with TRADED status (actually filled).
    """
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)
    if not token:
        raise HTTPException(status_code=400, detail="Dhan API not connected. Save and test credentials first.")

    result = await get_trade_book(token, **_proxy_kwargs(profile))
    if not result["success"]:
        raise HTTPException(status_code=502, detail=result["message"])

    return {"source": "dhan", "data": [normalize_trade(t) for t in (result["data"] or [])]}


@router.get("/trades/{order_id}")
async def get_trades_for_order(
    order_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get all trades generated for a specific order (partial fills etc.)."""
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)
    if not token:
        raise HTTPException(status_code=400, detail="Dhan API not connected")

    result = await get_trades_by_order(token, order_id, **_proxy_kwargs(profile))
    if not result["success"]:
        raise HTTPException(status_code=502, detail=result["message"])

    data = result["data"]
    if isinstance(data, dict):
        data = [data]
    return {"source": "dhan", "data": [normalize_trade(t) for t in data]}


# ── Positions ─────────────────────────────────────────────────────────────────

@router.get("/positions")
async def list_positions(
    status: Optional[str] = Query(None),
    source: Optional[str] = Query("dhan"),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Open/closed positions.
    source=dhan → live from Dhan (default)
    source=db   → from our local DB
    """
    profile = _profile(user, db)

    if source == "dhan":
        token, _ = _dhan_creds(profile)
        if token:
            result = await get_positions(token, **_proxy_kwargs(profile))
            if result["success"]:
                positions = [normalize_position(p) for p in (result["data"] or [])]
                if status and status.upper() != "ALL":
                    positions = [p for p in positions if p["status"] == status.upper()]
                return {"source": "dhan", "data": positions}

    # Fallback to DB
    q = db.query(Position).filter(Position.client_profile_id == profile.id)
    if status and status.upper() != "ALL":
        q = q.filter(Position.status == status.upper())
    rows = q.order_by(Position.opened_at.desc()).limit(200).all()
    return {"source": "db", "data": [_db_position(p) for p in rows]}


@router.get("/positions/all")
async def list_all_positions(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Master: all positions from DB."""
    from sqlalchemy.orm import joinedload
    rows = (
        db.query(Position)
        .options(joinedload(Position.client_profile).joinedload(ClientProfile.user))
        .order_by(Position.opened_at.desc())
        .limit(500)
        .all()
    )
    return {"source": "db", "data": [_db_position(p, p.client_profile.user.name) for p in rows]}


# ── Holdings ──────────────────────────────────────────────────────────────────

@router.get("/holdings")
async def list_holdings(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Demat holdings — live from Dhan."""
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)
    if not token:
        raise HTTPException(status_code=400, detail="Dhan API not connected")

    result = await get_holdings(token, **_proxy_kwargs(profile))
    if not result["success"]:
        raise HTTPException(status_code=502, detail=result["message"])

    return {"source": "dhan", "data": result["data"]}


# ── Fund Limit ────────────────────────────────────────────────────────────────

@router.get("/funds")
async def fund_limit(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Live fund limit from Dhan — available balance, utilized, withdrawable.
    Also updates the local funds table for portfolio display.
    """
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)

    if token:
        result = await get_fund_limit(token, **_proxy_kwargs(profile))
        if result["success"]:
            # Sync to local DB
            fund = profile.fund
            if fund:
                fund.available    = result["available_balance"]
                fund.used_margin  = result["utilized_amount"]
                fund.total_balance = result["available_balance"] + result["utilized_amount"]
                db.commit()
            return {
                "source": "dhan",
                "available_balance":    result["available_balance"],
                "utilized_amount":      result["utilized_amount"],
                "withdrawable_balance": result["withdrawable_balance"],
                "sod_limit":            result["sod_limit"],
                "collateral_amount":    result["collateral_amount"],
            }

    # Fallback to DB
    fund = profile.fund
    if not fund:
        return {"source": "db", "available_balance": 0, "utilized_amount": 0,
                "withdrawable_balance": 0, "sod_limit": 0, "collateral_amount": 0}
    return {
        "source": "db",
        "available_balance":    float(fund.available),
        "utilized_amount":      float(fund.used_margin),
        "withdrawable_balance": float(fund.available),
        "sod_limit":            float(fund.total_balance),
        "collateral_amount":    0,
    }


# ── Portfolio / Daily PnL ─────────────────────────────────────────────────────

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
