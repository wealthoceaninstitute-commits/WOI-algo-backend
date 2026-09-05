"""
app/services/scrip_downloader.py

Downloads Dhan instrument master CSV and upserts into scrip_master table.
Runs daily at 8:00 AM IST (before token refresh, before algo scan).

Source: https://images.dhan.co/api-data/api-scrip-master.csv
Columns used (compact format):
  SEM_EXM_EXCH_ID     → exchange (NSE / BSE)
  SEM_SEGMENT         → segment (E = Equity)
  SEM_SMST_SECURITY_ID → security_id
  SM_SYMBOL_NAME      → symbol
  SEM_INSTRUMENT_NAME → instrument (EQUITY / FUTIDX / etc.)
  SEM_SERIES          → series (EQ / BE / SM etc.)
  SM_FULL_NAME        → company name (compact CSV has this)
  SEM_CUSTOM_SYMBOL   → display symbol
  SEM_LOT_UNITS       → lot size
  SEM_TICK_SIZE       → tick size
  SM_ISIN_NUMBER      → ISIN

We keep only: NSE exchange + E segment + EQUITY instrument + EQ series
This gives ~1800 clean NSE equity stocks.
"""

import io
import csv
import httpx
from datetime import date, datetime, timezone
from sqlalchemy.orm import Session

from app.models.scrip_master import ScripMaster

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
TIMEOUT   = 30.0


async def download_and_update(db: Session) -> dict:
    """
    Download Dhan scrip master CSV and upsert into DB.
    Returns: { total_downloaded, inserted, updated, skipped, errors }
    """
    print("[scrip] Downloading Dhan instrument master...")

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as c:
            resp = await c.get(SCRIP_URL)

        if resp.status_code != 200:
            msg = f"Download failed: HTTP {resp.status_code}"
            print(f"[scrip] {msg}")
            return {"success": False, "message": msg}

        content = resp.text
        print(f"[scrip] Downloaded {len(content):,} bytes")

    except Exception as e:
        msg = f"Network error: {e}"
        print(f"[scrip] {msg}")
        return {"success": False, "message": msg}

    # Parse CSV
    rows        = []
    inserted    = 0
    updated     = 0
    skipped     = 0
    errors      = 0
    today       = date.today()

    try:
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            try:
                # Filter: NSE equity EQ series only
                exch       = (row.get("SEM_EXM_EXCH_ID") or "").strip().upper()
                segment    = (row.get("SEM_SEGMENT") or "").strip().upper()
                instrument = (row.get("SEM_INSTRUMENT_NAME") or "").strip().upper()
                series     = (row.get("SEM_SERIES") or "").strip().upper()

                if exch != "NSE" or segment != "E" or instrument != "EQUITY" or series != "EQ":
                    skipped += 1
                    continue

                security_id = (row.get("SEM_SMST_SECURITY_ID") or "").strip()
                symbol      = (row.get("SM_SYMBOL_NAME") or "").strip().upper()
                name        = (row.get("SM_FULL_NAME") or
                               row.get("SEM_CUSTOM_SYMBOL") or "").strip()
                isin        = (row.get("SM_ISIN_NUMBER") or "").strip()

                try:
                    lot_size = int(float(row.get("SEM_LOT_UNITS") or 1))
                except Exception:
                    lot_size = 1

                try:
                    tick_size = float(row.get("SEM_TICK_SIZE") or 0.05)
                except Exception:
                    tick_size = 0.05

                if not security_id or not symbol:
                    skipped += 1
                    continue

                rows.append({
                    "security_id":      security_id,
                    "symbol":           symbol,
                    "name":             name,
                    "exchange_segment": "NSE_EQ",
                    "series":           series,
                    "isin":             isin,
                    "lot_size":         lot_size,
                    "tick_size":        tick_size,
                    "last_updated":     today,
                })

            except Exception as e:
                errors += 1
                continue

    except Exception as e:
        msg = f"CSV parse error: {e}"
        print(f"[scrip] {msg}")
        return {"success": False, "message": msg}

    print(f"[scrip] Parsed {len(rows)} NSE EQ stocks (skipped {skipped}, errors {errors})")

    # Upsert into DB
    for r in rows:
        try:
            existing = db.query(ScripMaster).filter(
                ScripMaster.security_id == r["security_id"]
            ).first()

            if existing:
                existing.symbol           = r["symbol"]
                existing.name             = r["name"]
                existing.exchange_segment = r["exchange_segment"]
                existing.series           = r["series"]
                existing.isin             = r["isin"]
                existing.lot_size         = r["lot_size"]
                existing.tick_size        = r["tick_size"]
                existing.last_updated     = r["last_updated"]
                updated += 1
            else:
                db.add(ScripMaster(**r))
                inserted += 1

        except Exception as e:
            errors += 1
            db.rollback()
            continue

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        return {"success": False, "message": f"DB commit failed: {e}"}

    summary = {
        "success":          True,
        "total_downloaded": len(rows),
        "inserted":         inserted,
        "updated":          updated,
        "skipped":          skipped,
        "errors":           errors,
        "updated_at":       datetime.now(timezone.utc).isoformat(),
    }
    print(f"[scrip] Done — inserted={inserted} updated={updated} errors={errors}")
    return summary


def get_scrip(db: Session, security_id: str) -> ScripMaster | None:
    return db.query(ScripMaster).filter(ScripMaster.security_id == security_id).first()


def get_scrip_by_symbol(db: Session, symbol: str) -> ScripMaster | None:
    return db.query(ScripMaster).filter(
        ScripMaster.symbol == symbol.upper(),
        ScripMaster.exchange_segment == "NSE_EQ",
    ).first()


def build_symbol_map(db: Session) -> dict[str, str]:
    """Return { symbol: security_id } for all NSE EQ stocks."""
    rows = db.query(ScripMaster.symbol, ScripMaster.security_id).all()
    return {r.symbol: r.security_id for r in rows}


def build_id_map(db: Session) -> dict[str, dict]:
    """Return { security_id: { symbol, name, lot_size } }"""
    rows = db.query(ScripMaster).all()
    return {
        r.security_id: {
            "symbol":   r.symbol,
            "name":     r.name,
            "lot_size": r.lot_size,
        }
        for r in rows
    }
