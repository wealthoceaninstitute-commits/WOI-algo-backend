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
    if not cred:
        return None, None
    # Allow fetch even if is_active=False — token may still be valid
    try:
        token = decrypt(cred.access_token)
        client_id = decrypt(cred.dhan_client_id)
        # Empty token means credentials saved but never tested
        if not token or token.strip() == "":
            return None, None
        return token, client_id
    except Exception:
        return None, None


def _proxy_kwargs(profile: ClientProfile) -> dict:
    p = profile.proxy_setting
    if not p or not p.is_active:
        return {}
    try:
        return {
            "proxy_host": p.host,
            "proxy_port": p.port,
            "proxy_user": p.username,
            "proxy_pass": decrypt(p.password) if p.password else None,
        }
    except Exception:
        return {}


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


def _filter_by_status(orders: list, status: str) -> list:
    """Filter normalized orders by status string."""
    if not status or status.upper() == "ALL":
        return orders
    target = status.upper()
    # Map frontend filter → possible Dhan statuses already normalized
    return [o for o in orders if o.get("status", "").upper() == target]


# ── Orders ────────────────────────────────────────────────────────────────────

@router.get("/orders")
async def list_orders(
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """
    Fetch today's orders live from Dhan API.
    Falls back to DB if token not set or Dhan call fails.
    """
    profile = _profile(user, db)
    token, dhan_client_id = _dhan_creds(profile)

    if token:
        try:
            result = await get_order_book(token, **_proxy_kwargs(profile))
            if result["success"]:
                raw = result.get("data") or []
                # Dhan returns a list directly
                if isinstance(raw, dict):
                    raw = [raw]
                orders = [normalize_order(o) for o in raw if isinstance(o, dict)]
                orders = _filter_by_status(orders, status)
                return {"source": "dhan", "data": orders, "count": len(orders)}
            else:
                # Log the error but fall through to DB
                print(f"[orders] Dhan error: {result.get('message')}")
        except Exception as e:
            print(f"[orders] Exception calling Dhan: {e}")

    # Fallback to DB
    q = db.query(Order).filter(Order.client_profile_id == profile.id)
    if status and status.upper() != "ALL":
        q = q.filter(Order.status == status.upper())
    rows = q.order_by(Order.placed_at.desc()).limit(200).all()
    return {"source": "db", "data": [_db_order(o) for o in rows], "count": len(rows)}


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


@router.get("/orders/{order_id}")
async def get_order(
    order_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)
    if token:
        try:
            result = await get_order_by_id(token, order_id, **_proxy_kwargs(profile))
            if result["success"]:
                return {"source": "dhan", "data": normalize_order(result["data"])}
        except Exception as e:
            print(f"[get_order] Exception: {e}")
    raise HTTPException(status_code=404, detail="Order not found or API not connected")


# ── Trades ────────────────────────────────────────────────────────────────────

@router.get("/trades")
async def list_trades(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)

    if not token:
        # Return empty instead of raising — frontend handles gracefully
        return {"source": "none", "data": [], "message": "Dhan API credentials not connected"}

    try:
        result = await get_trade_book(token, **_proxy_kwargs(profile))
        if result["success"]:
            raw = result.get("data") or []
            if isinstance(raw, dict):
                raw = [raw]
            return {"source": "dhan", "data": [normalize_trade(t) for t in raw if isinstance(t, dict)]}
        return {"source": "dhan", "data": [], "message": result.get("message")}
    except Exception as e:
        print(f"[trades] Exception: {e}")
        return {"source": "error", "data": [], "message": str(e)}


@router.get("/trades/{order_id}")
async def get_trades_for_order(
    order_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)
    if not token:
        raise HTTPException(status_code=400, detail="Dhan API not connected")
    try:
        result = await get_trades_by_order(token, order_id, **_proxy_kwargs(profile))
        if not result["success"]:
            raise HTTPException(status_code=502, detail=result["message"])
        data = result["data"]
        if isinstance(data, dict):
            data = [data]
        return {"source": "dhan", "data": [normalize_trade(t) for t in data]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Positions ─────────────────────────────────────────────────────────────────

@router.get("/positions")
async def list_positions(
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)

    if token:
        try:
            result = await get_positions(token, **_proxy_kwargs(profile))
            if result["success"]:
                raw = result.get("data") or []
                if isinstance(raw, dict):
                    raw = [raw]
                positions = [normalize_position(p) for p in raw if isinstance(p, dict)]
                if status and status.upper() != "ALL":
                    positions = [p for p in positions if p["status"] == status.upper()]
                return {"source": "dhan", "data": positions}
            print(f"[positions] Dhan error: {result.get('message')}")
        except Exception as e:
            print(f"[positions] Exception: {e}")

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
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)
    if not token:
        return {"source": "none", "data": [], "message": "Dhan API not connected"}
    try:
        result = await get_holdings(token, **_proxy_kwargs(profile))
        if result["success"]:
            return {"source": "dhan", "data": result["data"]}
        return {"source": "dhan", "data": [], "message": result.get("message")}
    except Exception as e:
        return {"source": "error", "data": [], "message": str(e)}


# ── Fund Limit ────────────────────────────────────────────────────────────────

@router.get("/funds")
async def fund_limit(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    token, _ = _dhan_creds(profile)

    if token:
        try:
            result = await get_fund_limit(token, **_proxy_kwargs(profile))
            if result["success"]:
                fund = profile.fund
                if fund:
                    fund.available     = result["available_balance"]
                    fund.used_margin   = result["utilized_amount"]
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
        except Exception as e:
            print(f"[funds] Exception: {e}")

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
