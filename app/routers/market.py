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
from app.services.angel_one import angel_fetch_ltp
from app.services.master_token import get_master_token
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

    # Get master data token (cached — no repeated Dhan logins)
    try:
        jwt, api_key, client_id = await get_master_token(db)
    except Exception as e:
        raise HTTPException(503, f"Master token error: {e}")

    # Fetch LTP from Angel One
    try:
        prices = await angel_fetch_ltp(jwt, api_key, client_id, payload.security_ids)
    except Exception as e:
        raise HTTPException(503, f"Angel One LTP fetch failed: {e}")

    print(f"[market/ltp] requested={len(payload.security_ids)} "
          f"got={len(prices)} keys_sample={list(prices.keys())[:3]}")

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

    # Flatten to { sid: {ltp, prev_close} } — simpler for frontend
    flat = {
        sid: {"ltp": v["ltp"], "prev_close": v["prev_close"]}
        for sid, v in result.items()
    }
    return {
        "fetched":  len([v for v in flat.values() if v["ltp"]]),
        "total":    len(payload.security_ids),
        "exchange": payload.exchange,
        "data":     flat,
    }


class SnapshotRequest(BaseModel):
    type: str  # "prev_close" or "open_price"
    security_ids: Optional[list[str]] = None  # None = use full universe


@router.post("/snapshot")
async def save_snapshot(
    payload: SnapshotRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """
    Fetch live LTP for universe stocks and save to daily_price_snapshots.
    type = "prev_close" → saves as prev_close (simulates 8:45 AM step)
    type = "open_price" → saves as open_price + computes gap_pct (simulates 9:12:30 step)

    Used by Market Watch "Snapshot Now" button to test DB write path.
    """
    from app.models.scrip_master import UniverseStock
    from app.models.trading import DailyPriceSnapshot
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from datetime import date
    import asyncio

    if payload.type not in ("prev_close", "open_price"):
        raise HTTPException(400, "type must be 'prev_close' or 'open_price'")

    # Get security_ids — from payload or full universe
    if payload.security_ids:
        # Use provided IDs directly (e.g. from watchlist)
        sec_ids = payload.security_ids
    else:
        # Load full active universe
        rows = db.query(UniverseStock).filter(
            UniverseStock.is_active   == True,
            UniverseStock.not_found   == False,
            UniverseStock.security_id != None,
        ).all()
        sec_ids = [r.security_id for r in rows]
        if not sec_ids:
            raise HTTPException(400, "Universe is empty — add stocks to universe first")

    if not sec_ids:
        raise HTTPException(400, "No security_ids found")

    # Get master data token (cached)
    try:
        token, client_id = await get_master_token(db)
    except Exception as e:
        raise HTTPException(503, f"Master token error: {e}")

    # Wait before fetch to avoid 429 (market watch LTP may have just fired)
    await asyncio.sleep(2.5)

    # Fetch LTP in batches
    all_prices: dict[str, float] = {}
    for i in range(0, len(sec_ids), 900):
        batch  = sec_ids[i:i+900]
        prices = await fetch_ltp(token, client_id, batch)
        all_prices.update(prices)
        print(f"[snapshot] batch {i//900+1}: got {len(prices)} prices")
        if i + 900 < len(sec_ids):
            await asyncio.sleep(1.5)

    valid = {k: v for k, v in all_prices.items() if v and v > 0}
    print(f"[snapshot] total valid prices: {len(valid)}/{len(sec_ids)}")
    today = date.today()

    if payload.type == "prev_close":
        # Build symbol lookup from UniverseStock + ScripMaster
        from app.models.scrip_master import ScripMaster
        scrip_map = {
            str(r.security_id): r.symbol
            for r in db.query(ScripMaster).filter(
                ScripMaster.security_id.in_(list(valid.keys()))
            ).all()
        }

        # Upsert prev_close rows
        rows_to_insert = [
            {
                "trade_date":  today,
                "security_id": sid,
                "symbol":      scrip_map.get(str(sid), sid),
                "prev_close":  price,
                "open_price":  None,
                "gap_pct":     None,
            }
            for sid, price in valid.items()
        ]
        if rows_to_insert:
            stmt = pg_insert(DailyPriceSnapshot).values(rows_to_insert)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_snapshot_date_sid",
                set_={"prev_close": stmt.excluded.prev_close},
            )
            db.execute(stmt)
            db.commit()

    else:  # open_price
        # Update open_price + compute gap_pct for existing rows
        existing = db.query(DailyPriceSnapshot).filter(
            DailyPriceSnapshot.trade_date == today
        ).all()

        updated = 0
        for row in existing:
            ltp = valid.get(str(row.security_id))
            if not ltp:
                continue
            prev = float(row.prev_close or 0)
            row.open_price = ltp
            if prev > 0:
                row.gap_pct = round(((ltp - prev) / prev) * 100, 2)
            updated += 1
        db.commit()

    return {
        "type":         payload.type,
        "date":         today.isoformat(),
        "total_ids":    len(sec_ids),
        "fetched":      len(valid),
        "saved":        len(valid) if payload.type == "prev_close" else updated,
        "message":      f"{payload.type} snapshot saved — {len(valid)} stocks written to daily_price_snapshots",
    }
