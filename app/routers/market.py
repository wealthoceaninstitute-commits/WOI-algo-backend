"""
app/routers/market.py — Live market data endpoints

GET  /api/market/ltp   — fetch LTP for given security_ids
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional

from app.core.database import get_db
from app.core.security import require_master
from app.models.user import User
from app.models.trading import DailyPriceSnapshot
from app.services.market_data import get_master_token, fetch_ltp
from datetime import date

router = APIRouter(prefix="/api/market", tags=["market"])


class LTPRequest(BaseModel):
    security_ids: list[str]
    exchange: Optional[str] = "NSE_EQ"


@router.post("/ltp")
async def get_ltp(
    payload: LTPRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """
    Fetch live LTP for given security_ids via Dhan API.
    Also returns prev_close from today's DB snapshot if available.

    Returns: { "security_id": { "ltp": 123.4, "prev_close": 120.0 } }
    """
    if not payload.security_ids:
        raise HTTPException(400, "No security_ids provided")

    if len(payload.security_ids) > 1000:
        raise HTTPException(400, "Max 1000 security_ids per request")

    # Get master token
    try:
        token, client_id = await get_master_token(db)
    except Exception as e:
        raise HTTPException(503, f"Master token error: {e}")

    # Fetch LTP from Dhan
    try:
        prices = await fetch_ltp(token, client_id, payload.security_ids)
    except Exception as e:
        raise HTTPException(503, f"Dhan LTP fetch failed: {e}")

    # Load prev_close from today's snapshot
    today = date.today()
    snap_rows = db.query(DailyPriceSnapshot).filter(
        DailyPriceSnapshot.trade_date  == today,
        DailyPriceSnapshot.security_id.in_(payload.security_ids),
    ).all()
    snap_map = {str(r.security_id): float(r.prev_close) for r in snap_rows if r.prev_close}

    # Build response
    result = {}
    for sid in payload.security_ids:
        ltp = prices.get(str(sid))
        prev = snap_map.get(str(sid))

        # If no prev_close in snapshot, use LTP as rough prev (will show 0% change)
        result[sid] = {
            "ltp":        ltp,
            "prev_close": prev,
            "has_snapshot": prev is not None,
        }

    return {
        "fetched":    len([v for v in result.values() if v["ltp"]]),
        "total":      len(payload.security_ids),
        "exchange":   payload.exchange,
        "data":       result,
    }
