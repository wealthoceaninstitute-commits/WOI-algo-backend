"""
Trading router — fetches live data from Dhan API.

Token strategy:
  1. Check expiry FIRST — if token is valid, use it immediately (no Dhan call)
  2. If expired or missing — refresh once via TOTP (with lock)
  3. If DH-906 returned mid-session — refresh and retry once
  4. 8 AM IST scheduled refresh keeps tokens always fresh before market open

This eliminates redundant refreshes and parallel refresh storms.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import date, datetime, timezone
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
from app.services.token_manager import (
    token_is_valid, get_stored_token, refresh_token,
)

router = APIRouter(prefix="/api", tags=["trading"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _profile(user: User, db: Session) -> ClientProfile:
    p = db.query(ClientProfile).filter(ClientProfile.user_id == user.id).first()
    if not p:
        raise HTTPException(status_code=404, detail="Client profile not found")
    return p


def _creds(profile: ClientProfile):
    """Decrypt all four credential fields. Returns (client_id, pin, totp) or all None."""
    cred = profile.dhan_cred
    if not cred:
        return None, None, None
    try:
        client_id = decrypt(cred.dhan_client_id) or None
        pin       = decrypt(cred.pin) or None
        totp      = decrypt(cred.totp_secret) or None
        return client_id, pin, totp
    except Exception:
        return None, None, None


def _proxy(profile: ClientProfile) -> dict:
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


async def _get_valid_token(
    profile: ClientProfile,
    db: Session,
) -> Optional[str]:
    """
    Return a valid Dhan access token.

    Strategy:
      - Token not expired → return stored token immediately (no Dhan call)
      - Token expired / missing → refresh via TOTP (with lock)
      - No credentials → return None
    """
    client_id, pin, totp = _creds(profile)
    if not client_id or not pin or not totp:
        return None  # No credentials configured

    proxy_kw = _proxy(profile)

    # Token still valid — use it directly
    if token_is_valid(profile):
        return get_stored_token(profile)

    # Token expired or missing — refresh
    return await refresh_token(
        profile, client_id, pin, totp, db,
        reason="expired", **proxy_kw
    )


async def _dhan(fn, profile: ClientProfile, db: Session, *args):
    """
    Call a Dhan API function with smart token handling:
      1. Get valid token (from DB if not expired, refresh if expired)
      2. Call Dhan
      3. If DH-906 mid-call (shouldn't happen but can) → refresh + retry once
    """
    client_id, pin, totp = _creds(profile)
    if not client_id or not pin or not totp:
        return {"success": False, "data": None,
                "message": "No Dhan credentials configured"}

    proxy_kw = _proxy(profile)
    token = await _get_valid_token(profile, db)

    if not token:
        return {"success": False, "data": None,
                "message": "Could not obtain valid Dhan token"}

    # Call Dhan API
    result = await fn(token, *args, **proxy_kw)

    # DH-906 mid-session (token invalidated server-side) — refresh once and retry
    if result.get("token_expired"):
        print(f"[trading] DH-906 mid-session for profile {profile.id} — refreshing")
        new_token = await refresh_token(
            profile, client_id, pin, totp, db,
            reason="dh906", **proxy_kw
        )
        if new_token:
            result = await fn(new_token, *args, **proxy_kw)
        else:
            result = {"success": False, "token_expired": False,
                      "message": "Token refresh failed — check TOTP secret"}

    return result


def _to_list(raw) -> list:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return [raw]
    return []


def _filter_status(items: list, status: Optional[str]) -> list:
    if not status or status.upper() == "ALL":
        return items
    target = status.upper()
    return [o for o in items if (o.get("status") or "").upper() == target]


# ── DB serializers (fallback when Dhan is unreachable) ────────────────────────

def _ser_order(o: Order, client_name: str = None) -> dict:
    return {
        "id":               o.id,
        "dhan_order_id":    o.dhan_order_id,
        "symbol":           o.symbol,
        "underlying":       o.underlying,
        "expiry":           o.expiry,
        "strike_price":     float(o.strike_price) if o.strike_price else None,
        "option_type":      o.option_type,
        "order_type":       o.order_type,
        "quantity":         o.quantity,
        "price":            float(o.price),
        "executed_price":   float(o.executed_price) if o.executed_price else None,
        "status":           o.status,
        "is_paper_trade":   o.is_paper_trade,
        "rejection_reason": o.rejection_reason,
        "placed_at":        o.placed_at.isoformat() if o.placed_at else None,
        "executed_at":      o.executed_at.isoformat() if o.executed_at else None,
        "client_name":      client_name,
        "source":           "db",
    }


def _ser_position(p: Position, client_name: str = None) -> dict:
    return {
        "id":              p.id,
        "symbol":          p.symbol,
        "underlying":      p.underlying,
        "expiry":          p.expiry,
        "strike_price":    float(p.strike_price) if p.strike_price else None,
        "option_type":     p.option_type,
        "quantity":        p.quantity,
        "avg_cost":        float(p.avg_cost),
        "ltp":             float(p.ltp),
        "realized_pnl":    float(p.realized_pnl),
        "unrealized_pnl":  float(p.unrealized_pnl),
        "total_pnl":       float(p.realized_pnl) + float(p.unrealized_pnl),
        "status":          p.status,
        "opened_at":       p.opened_at.isoformat() if p.opened_at else None,
        "closed_at":       p.closed_at.isoformat() if p.closed_at else None,
        "client_name":     client_name,
        "source":          "db",
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/orders")
async def list_orders(
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    result  = await _dhan(get_order_book, profile, db)

    if result["success"]:
        orders = [normalize_order(o) for o in _to_list(result.get("data"))]
        return {"source": "dhan", "data": _filter_status(orders, status)}

    print(f"[orders] DB fallback: {result.get('message')}")
    q = db.query(Order).filter(Order.client_profile_id == profile.id)
    if status and status.upper() != "ALL":
        q = q.filter(Order.status == status.upper())
    rows = q.order_by(Order.placed_at.desc()).limit(200).all()
    return {"source": "db", "data": [_ser_order(o) for o in rows]}


@router.get("/orders/all")
async def list_all_orders(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    from sqlalchemy.orm import joinedload
    rows = (
        db.query(Order)
        .options(joinedload(Order.client_profile).joinedload(ClientProfile.user))
        .order_by(Order.placed_at.desc())
        .limit(500)
        .all()
    )
    return {"source": "db",
            "data": [_ser_order(o, o.client_profile.user.name) for o in rows]}


@router.get("/orders/{order_id}")
async def get_order(
    order_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    result  = await _dhan(get_order_by_id, profile, db, order_id)
    if result["success"]:
        return {"source": "dhan", "data": normalize_order(result["data"])}
    raise HTTPException(status_code=404, detail="Order not found")


@router.get("/trades")
async def list_trades(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    result  = await _dhan(get_trade_book, profile, db)
    if result["success"]:
        return {"source": "dhan",
                "data": [normalize_trade(t) for t in _to_list(result.get("data"))]}
    return {"source": "none", "data": [], "message": result.get("message")}


@router.get("/trades/{order_id}")
async def get_trades_for_order(
    order_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    result  = await _dhan(get_trades_by_order, profile, db, order_id)
    if not result["success"]:
        raise HTTPException(status_code=502, detail=result.get("message"))
    return {"source": "dhan",
            "data": [normalize_trade(t) for t in _to_list(result.get("data"))]}


@router.get("/positions")
async def list_positions(
    status: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    result  = await _dhan(get_positions, profile, db)

    if result["success"]:
        positions = [normalize_position(p) for p in _to_list(result.get("data"))]
        return {"source": "dhan", "data": _filter_status(positions, status)}

    q = db.query(Position).filter(Position.client_profile_id == profile.id)
    if status and status.upper() != "ALL":
        q = q.filter(Position.status == status.upper())
    rows = q.order_by(Position.opened_at.desc()).limit(200).all()
    return {"source": "db", "data": [_ser_position(p) for p in rows]}


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
    return {"source": "db",
            "data": [_ser_position(p, p.client_profile.user.name) for p in rows]}


@router.get("/holdings")
async def list_holdings(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    result  = await _dhan(get_holdings, profile, db)
    if result["success"]:
        return {"source": "dhan", "data": result["data"]}
    return {"source": "none", "data": [], "message": result.get("message")}


@router.get("/funds")
async def fund_limit(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    result  = await _dhan(get_fund_limit, profile, db)

    if result["success"]:
        fund = profile.fund
        if fund:
            fund.available     = result["available_balance"]
            fund.used_margin   = result["utilized_amount"]
            fund.total_balance = result["available_balance"] + result["utilized_amount"]
            db.commit()
        return {
            "source":               "dhan",
            "available_balance":    result["available_balance"],
            "utilized_amount":      result["utilized_amount"],
            "withdrawable_balance": result["withdrawable_balance"],
            "sod_limit":            result["sod_limit"],
            "collateral_amount":    result["collateral_amount"],
        }

    fund = profile.fund
    if not fund:
        return {"source": "db", "available_balance": 0, "utilized_amount": 0,
                "withdrawable_balance": 0, "sod_limit": 0, "collateral_amount": 0}
    return {
        "source":               "db",
        "available_balance":    float(fund.available),
        "utilized_amount":      float(fund.used_margin),
        "withdrawable_balance": float(fund.available),
        "sod_limit":            float(fund.total_balance),
        "collateral_amount":    0,
    }


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
    end   = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)

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
            "date":        r.date.isoformat(),
            "closed_pnl":  float(r.closed_pnl),
            "running_pnl": float(r.running_pnl),
            "total_pnl":   float(r.total_pnl),
            "trade_count": r.trade_count,
        }
        for r in rows
    ]

    fund = profile.fund
    return {
        "daily_pnl":   daily,
        "month_total": sum(d["total_pnl"] for d in daily),
        "funds": {
            "available":     float(fund.available)     if fund else 0.0,
            "used_margin":   float(fund.used_margin)   if fund else 0.0,
            "total_balance": float(fund.total_balance) if fund else 0.0,
        },
    }


@router.get("/portfolio/summary")
def pnl_summary(
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    profile = _profile(user, db)
    today   = date.today()
    row     = (
        db.query(DailyPnl)
        .filter(DailyPnl.client_profile_id == profile.id, DailyPnl.date == today)
        .first()
    )
    return {
        "today_pnl":   float(row.total_pnl)   if row else 0.0,
        "closed_pnl":  float(row.closed_pnl)  if row else 0.0,
        "running_pnl": float(row.running_pnl) if row else 0.0,
        "trade_count": row.trade_count         if row else 0,
    }
