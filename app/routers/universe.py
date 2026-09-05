"""
app/routers/universe.py

Stock universe management — master only.

Endpoints:
  GET    /api/universe/scrip/status    — scrip master download status
  POST   /api/universe/scrip/refresh   — trigger manual scrip download
  GET    /api/universe/scrip/search    — search scrip master by symbol/name

  GET    /api/universe                 — list all universes
  POST   /api/universe                 — create universe
  GET    /api/universe/{id}            — get universe + stocks
  DELETE /api/universe/{id}            — delete universe
  POST   /api/universe/{id}/upload     — upload CSV of symbols → resolve + store
  POST   /api/universe/{id}/stocks     — add single stock manually
  DELETE /api/universe/{id}/stocks/{stock_id} — remove stock
  PATCH  /api/universe/{id}/stocks/{stock_id} — toggle is_active
"""

import io
import csv
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone

from app.core.database import get_db
from app.core.security import require_master
from app.models.user import User
from app.models.scrip_master import ScripMaster, StockUniverse, UniverseStock
from app.services.scrip_downloader import download_and_update

router = APIRouter(prefix="/api/universe", tags=["universe"])


# ── Scrip master endpoints ────────────────────────────────────────────────────

@router.get("/scrip/status")
def scrip_status(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Scrip master stats — total stocks, last update date."""
    total   = db.query(ScripMaster).count()
    latest  = db.query(ScripMaster.last_updated).order_by(
        ScripMaster.last_updated.desc()
    ).first()
    return {
        "total_stocks": total,
        "last_updated": latest[0].isoformat() if latest and latest[0] else None,
        "is_ready":     total > 0,
    }


@router.post("/scrip/refresh")
async def scrip_refresh(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Manually trigger scrip master download + DB upsert."""
    result = await download_and_update(db)
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result["message"])
    return result


@router.get("/scrip/search")
def scrip_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, le=50),
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Search scrip master by symbol or name prefix."""
    q = q.upper().strip()
    rows = (
        db.query(ScripMaster)
        .filter(
            (ScripMaster.symbol.startswith(q)) |
            (ScripMaster.name.ilike(f"{q}%"))
        )
        .order_by(ScripMaster.symbol)
        .limit(limit)
        .all()
    )
    return [_ser_scrip(r) for r in rows]


def _ser_scrip(s: ScripMaster) -> dict:
    return {
        "security_id":      s.security_id,
        "symbol":           s.symbol,
        "name":             s.name,
        "exchange_segment": s.exchange_segment,
        "lot_size":         s.lot_size,
        "tick_size":        float(s.tick_size) if s.tick_size else 0.05,
        "isin":             s.isin,
    }


# ── Universe CRUD ─────────────────────────────────────────────────────────────

class UniverseCreate(BaseModel):
    name: str
    description: Optional[str] = None


@router.get("/")
def list_universes(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    universes = db.query(StockUniverse).order_by(StockUniverse.created_at.desc()).all()
    return [_ser_universe(u, db) for u in universes]


@router.post("/")
def create_universe(
    payload: UniverseCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    existing = db.query(StockUniverse).filter(
        StockUniverse.name == payload.name
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="Universe name already exists")

    u = StockUniverse(name=payload.name, description=payload.description)
    db.add(u)
    db.commit()
    db.refresh(u)
    return _ser_universe(u, db)


@router.get("/{universe_id}")
def get_universe(
    universe_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    u = _get_universe(universe_id, db)
    return _ser_universe_full(u)


@router.delete("/{universe_id}", status_code=204)
def delete_universe(
    universe_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    u = _get_universe(universe_id, db)
    db.delete(u)
    db.commit()


# ── CSV upload ────────────────────────────────────────────────────────────────

@router.post("/{universe_id}/upload")
async def upload_universe_csv(
    universe_id: str,
    file: UploadFile = File(...),
    replace: bool = Query(False, description="Replace all existing stocks"),
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """
    Upload a CSV of stock symbols to add to this universe.

    CSV format — just needs a column named 'Symbol' (case-insensitive).
    NSE Nifty 500 CSV from niftyindices.com works directly.
    Any CSV with a Symbol or symbol column works.

    Each symbol is looked up in scrip_master to resolve security_id.
    Symbols not found are stored with not_found=True for review.
    """
    u = _get_universe(universe_id, db)

    content = await file.read()
    try:
        text   = content.decode("utf-8-sig")   # handles BOM from Excel
        reader = csv.DictReader(io.StringIO(text))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {e}")

    # Find the symbol column (case-insensitive)
    fieldnames = reader.fieldnames or []
    sym_col    = next(
        (f for f in fieldnames if f.strip().lower() in ("symbol", "symbols", "ticker")),
        None
    )
    if not sym_col:
        raise HTTPException(
            status_code=400,
            detail=f"No 'Symbol' column found. Columns: {fieldnames}"
        )

    # Build scrip master lookup
    scrips = {
        r.symbol: r
        for r in db.query(ScripMaster)
        .filter(ScripMaster.exchange_segment == "NSE_EQ")
        .all()
    }

    if replace:
        db.query(UniverseStock).filter(UniverseStock.universe_id == universe_id).delete()
        db.commit()

    added      = 0
    not_found  = 0
    duplicates = 0
    not_found_symbols = []

    for row in reader:
        raw_symbol = (row.get(sym_col) or "").strip().upper()
        if not raw_symbol:
            continue

        # Check duplicate
        exists = db.query(UniverseStock).filter(
            UniverseStock.universe_id == universe_id,
            UniverseStock.symbol      == raw_symbol,
        ).first()
        if exists:
            duplicates += 1
            continue

        scrip       = scrips.get(raw_symbol)
        security_id = scrip.security_id if scrip else None
        name        = scrip.name if scrip else None
        found       = scrip is not None

        if not found:
            not_found += 1
            not_found_symbols.append(raw_symbol)

        db.add(UniverseStock(
            universe_id = universe_id,
            security_id = security_id,
            symbol      = raw_symbol,
            name        = name,
            is_active   = found,     # auto-disable if not found
            not_found   = not found,
        ))
        added += 1

    db.commit()
    u.updated_at = datetime.now(timezone.utc)
    db.commit()

    return {
        "added":             added,
        "not_found":         not_found,
        "duplicates":        duplicates,
        "not_found_symbols": not_found_symbols[:20],  # show first 20
        "total_stocks":      db.query(UniverseStock)
                               .filter(UniverseStock.universe_id == universe_id).count(),
    }


# ── Individual stock management ───────────────────────────────────────────────

class StockAdd(BaseModel):
    symbol: str
    security_id: Optional[str] = None


@router.post("/{universe_id}/stocks")
def add_stock(
    universe_id: str,
    payload: StockAdd,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Manually add a single stock. Auto-resolves security_id from scrip_master if not provided."""
    _get_universe(universe_id, db)
    symbol = payload.symbol.upper().strip()

    exists = db.query(UniverseStock).filter(
        UniverseStock.universe_id == universe_id,
        UniverseStock.symbol      == symbol,
    ).first()
    if exists:
        raise HTTPException(status_code=400, detail=f"{symbol} already in universe")

    # Auto-resolve security_id
    security_id = payload.security_id
    name        = None
    not_found   = False

    if not security_id:
        scrip = db.query(ScripMaster).filter(
            ScripMaster.symbol == symbol,
            ScripMaster.exchange_segment == "NSE_EQ",
        ).first()
        if scrip:
            security_id = scrip.security_id
            name        = scrip.name
        else:
            not_found = True

    stock = UniverseStock(
        universe_id = universe_id,
        security_id = security_id,
        symbol      = symbol,
        name        = name,
        is_active   = not not_found,
        not_found   = not_found,
    )
    db.add(stock)
    db.commit()
    db.refresh(stock)
    return _ser_stock(stock)


@router.patch("/{universe_id}/stocks/{stock_id}")
def toggle_stock(
    universe_id: str,
    stock_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Toggle is_active for a stock in the universe."""
    stock = _get_stock(universe_id, stock_id, db)
    if stock.not_found and not stock.is_active:
        raise HTTPException(status_code=400, detail="Stock not in scrip master — resolve security_id first")
    stock.is_active = not stock.is_active
    db.commit()
    return _ser_stock(stock)


@router.delete("/{universe_id}/stocks/{stock_id}", status_code=204)
def remove_stock(
    universe_id: str,
    stock_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    stock = _get_stock(universe_id, stock_id, db)
    db.delete(stock)
    db.commit()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_universe(universe_id: str, db: Session) -> StockUniverse:
    u = db.query(StockUniverse).filter(StockUniverse.id == universe_id).first()
    if not u:
        raise HTTPException(status_code=404, detail="Universe not found")
    return u


def _get_stock(universe_id: str, stock_id: str, db: Session) -> UniverseStock:
    s = db.query(UniverseStock).filter(
        UniverseStock.id           == stock_id,
        UniverseStock.universe_id  == universe_id,
    ).first()
    if not s:
        raise HTTPException(status_code=404, detail="Stock not found")
    return s


def _ser_universe(u: StockUniverse, db: Session) -> dict:
    total  = db.query(UniverseStock).filter(UniverseStock.universe_id == u.id).count()
    active = db.query(UniverseStock).filter(
        UniverseStock.universe_id == u.id,
        UniverseStock.is_active   == True,
    ).count()
    nf = db.query(UniverseStock).filter(
        UniverseStock.universe_id == u.id,
        UniverseStock.not_found   == True,
    ).count()
    return {
        "id":           u.id,
        "name":         u.name,
        "description":  u.description,
        "is_active":    u.is_active,
        "total_stocks": total,
        "active_stocks":active,
        "not_found":    nf,
        "created_at":   u.created_at.isoformat() if u.created_at else None,
        "updated_at":   u.updated_at.isoformat() if u.updated_at else None,
    }


def _ser_universe_full(u: StockUniverse) -> dict:
    base = {
        "id":          u.id,
        "name":        u.name,
        "description": u.description,
        "is_active":   u.is_active,
        "stocks":      [_ser_stock(s) for s in u.stocks],
    }
    return base


def _ser_stock(s: UniverseStock) -> dict:
    return {
        "id":          s.id,
        "symbol":      s.symbol,
        "name":        s.name,
        "security_id": s.security_id,
        "is_active":   s.is_active,
        "not_found":   s.not_found,
        "added_at":    s.added_at.isoformat() if s.added_at else None,
    }
