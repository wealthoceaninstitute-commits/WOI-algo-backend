"""
app/services/scrip_downloader.py

Downloads Dhan instrument master and upserts into scrip_master table.
Source: https://images.dhan.co/api-data/api-scrip-master.csv

Confirmed column names from actual CSV:
  SEM_EXM_EXCH_ID        → exchange (NSE / BSE / MCX)
  SEM_SEGMENT            → E = Equity segment
  SEM_SMST_SECURITY_ID   → security_id (Dhan's unique ID)
  SEM_INSTRUMENT_NAME    → EQUITY / FUTIDX / OPTIDX etc
  SEM_TRADING_SYMBOL     → trading symbol e.g. RELIANCE, HFCL  ← col F
  SEM_LOT_UNITS          → lot size
  SEM_CUSTOM_SYMBOL      → display name
  SEM_TICK_SIZE          → tick size
  SEM_SERIES             → EQ / BE / SG / SM / MF etc
  SM_SYMBOL_NAME         → short symbol name

Filter: NSE + E segment + EQUITY instrument + (EQ or BE) series
  EQ = regular equity (2,677 stocks)
  BE = trade-for-trade surveillance (242 stocks — e.g. HFCL, HEG — fully tradeable)
  All other series excluded (SG=bonds, SM=SME, MF=mutual funds, etc.)

Total stored: ~2,919 NSE equity stocks
"""

import io
import csv
import httpx
from datetime import date, datetime, timezone
from sqlalchemy.orm import Session

from app.models.scrip_master import ScripMaster

SCRIP_URL      = "https://images.dhan.co/api-data/api-scrip-master.csv"
TIMEOUT        = 60.0
ALLOWED_SERIES = {"EQ", "BE"}   # EQ = normal, BE = trade-for-trade (both tradeable)


async def download_and_update(db: Session) -> dict:
    """
    Download Dhan scrip master CSV and upsert NSE equity stocks into DB.
    Keeps EQ and BE series — covers all Nifty 500 stocks including surveillance ones.
    """
    print("[scrip] Downloading Dhan instrument master...")

    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        ) as c:
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

    rows     = []
    skipped  = 0
    errors   = 0
    today    = date.today()

    try:
        reader  = csv.DictReader(io.StringIO(content))
        headers = reader.fieldnames or []
        print(f"[scrip] Columns found: {headers}")

        for row in reader:
            try:
                exch       = (row.get("SEM_EXM_EXCH_ID")       or "").strip().upper()
                segment    = (row.get("SEM_SEGMENT")             or "").strip().upper()
                instrument = (row.get("SEM_INSTRUMENT_NAME")     or "").strip().upper()
                series     = (row.get("SEM_SERIES")              or "").strip().upper()

                # Keep only NSE equity in EQ or BE series
                if exch != "NSE" or segment != "E" or instrument != "EQUITY":
                    skipped += 1
                    continue
                if series not in ALLOWED_SERIES:
                    skipped += 1
                    continue

                security_id = (row.get("SEM_SMST_SECURITY_ID") or "").strip()
                # SEM_TRADING_SYMBOL is the NSE trading symbol (col F)
                symbol      = (row.get("SEM_TRADING_SYMBOL")    or
                               row.get("SM_SYMBOL_NAME")         or "").strip().upper()
                name        = (row.get("SEM_CUSTOM_SYMBOL")      or symbol).strip()
                isin        = (row.get("SM_ISIN_NUMBER")         or "").strip()

                try:
                    lot_size = max(1, int(float(row.get("SEM_LOT_UNITS") or 1)))
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

            except Exception:
                errors += 1
                continue

    except Exception as e:
        return {"success": False, "message": f"CSV parse error: {e}"}

    if not rows:
        return {
            "success": False,
            "message": f"No NSE EQ/BE stocks found. CSV columns: {headers}",
        }

    print(f"[scrip] Parsed {len(rows)} NSE EQ+BE stocks ({skipped} skipped, {errors} errors)")

    # Upsert
    inserted  = 0
    updated   = 0
    db_errors = 0

    for r in rows:
        try:
            existing = db.query(ScripMaster).filter(
                ScripMaster.security_id == r["security_id"]
            ).first()
            if existing:
                existing.symbol       = r["symbol"]
                existing.name         = r["name"]
                existing.series       = r["series"]
                existing.isin         = r["isin"]
                existing.lot_size     = r["lot_size"]
                existing.tick_size    = r["tick_size"]
                existing.last_updated = r["last_updated"]
                updated += 1
            else:
                db.add(ScripMaster(**r))
                inserted += 1
        except Exception:
            db_errors += 1
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
        "errors":           errors + db_errors,
        "updated_at":       datetime.now(timezone.utc).isoformat(),
    }
    print(f"[scrip] Done — {len(rows)} stocks | inserted={inserted} updated={updated}")
    return summary


def get_scrip(db: Session, security_id: str) -> ScripMaster | None:
    return db.query(ScripMaster).filter(
        ScripMaster.security_id == security_id
    ).first()


def get_scrip_by_symbol(db: Session, symbol: str) -> ScripMaster | None:
    return db.query(ScripMaster).filter(
        ScripMaster.symbol           == symbol.upper().strip(),
        ScripMaster.exchange_segment == "NSE_EQ",
    ).first()


def build_id_map(db: Session) -> dict[str, dict]:
    """Return { security_id: { symbol, name, lot_size } } — used by algo engine."""
    rows = db.query(ScripMaster).filter(
        ScripMaster.exchange_segment == "NSE_EQ"
    ).all()
    return {
        r.security_id: {
            "symbol":   r.symbol,
            "name":     r.name,
            "lot_size": r.lot_size,
        }
        for r in rows
    }
