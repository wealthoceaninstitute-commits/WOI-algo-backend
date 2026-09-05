"""
app/services/scrip_downloader.py

Downloads Dhan instrument master and upserts into scrip_master table.
Source: https://images.dhan.co/api-data/api-scrip-master.csv

Column mapping (confirmed from actual CSV screenshot):
  SEM_EXM_EXCH_ID       → exchange (NSE / BSE / MCX)
  SEM_SEGMENT           → segment  (E = Equity)
  SEM_SMST_SECURITY_ID  → security_id  ← primary key
  SEM_INSTRUMENT_NAME   → instrument type (EQUITY / FUTIDX / OPTIDX etc)
  SEM_TRADING_SYMBOL    → trading symbol (ARE&M, RELIANCE, etc)  ← col F
  SEM_LOT_UNITS         → lot size
  SEM_CUSTOM_SYMBOL     → display name / company short name
  SEM_SERIES            → series (EQ / BE / SG / SM / IL etc)
  SM_SYMBOL_NAME        → short symbol name
  OL_NAME               → full company name

Filter: NSE + E segment + EQUITY instrument + EQ series only
This gives ~1,800 clean NSE equity stocks (excludes bonds, ETFs, SME, BE series).
"""

import io
import csv
import httpx
from datetime import date, datetime, timezone
from sqlalchemy.orm import Session

from app.models.scrip_master import ScripMaster

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
TIMEOUT   = 60.0


async def download_and_update(db: Session) -> dict:
    """
    Download Dhan scrip master CSV and upsert NSE EQ stocks into DB.
    Returns summary dict.
    """
    print("[scrip] Downloading Dhan instrument master from Dhan CDN...")

    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},  # required by Dhan CDN
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

    # Parse CSV
    rows     = []
    skipped  = 0
    errors   = 0
    today    = date.today()

    try:
        reader = csv.DictReader(io.StringIO(content))
        headers = reader.fieldnames or []
        print(f"[scrip] CSV columns: {headers[:10]}...")  # log first 10 cols for debugging

        for row in reader:
            try:
                # ── Filters ───────────────────────────────────────────────
                exch       = (row.get("SEM_EXM_EXCH_ID")    or "").strip().upper()
                segment    = (row.get("SEM_SEGMENT")          or "").strip().upper()
                instrument = (row.get("SEM_INSTRUMENT_NAME")  or "").strip().upper()
                series     = (row.get("SEM_SERIES")           or
                              row.get("SM_SERIES")             or "").strip().upper()

                # Keep only NSE equity EQ series
                if exch != "NSE":
                    skipped += 1
                    continue
                if segment != "E":
                    skipped += 1
                    continue
                if instrument != "EQUITY":
                    skipped += 1
                    continue
                if series != "EQ":
                    skipped += 1
                    continue

                # ── Extract fields ────────────────────────────────────────
                security_id = (row.get("SEM_SMST_SECURITY_ID") or "").strip()

                # Trading symbol — column F in the CSV (SEM_TRADING_SYMBOL)
                symbol = (
                    row.get("SEM_TRADING_SYMBOL") or
                    row.get("SM_SYMBOL_NAME")     or
                    ""
                ).strip().upper()

                # Company name — OL_NAME is the full name
                name = (
                    row.get("OL_NAME")            or
                    row.get("SEM_CUSTOM_SYMBOL")  or
                    row.get("SM_FULL_NAME")        or
                    symbol
                ).strip()

                isin = (row.get("SM_ISIN_NUMBER") or
                        row.get("SEM_ISIN")        or "").strip()

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
                    "series":           "EQ",
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

    if not rows:
        # Column names may have changed — log all headers for debugging
        msg = (
            f"No NSE EQ stocks found after filtering. "
            f"CSV may have different column names. "
            f"Check headers: {headers}"
        )
        print(f"[scrip] {msg}")
        return {"success": False, "message": msg}

    print(f"[scrip] Parsed {len(rows)} NSE EQ stocks (skipped {skipped}, errors {errors})")

    # Upsert into DB
    inserted = 0
    updated  = 0
    db_errors = 0

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
    print(
        f"[scrip] Done — {len(rows)} total | "
        f"inserted={inserted} updated={updated} "
        f"skipped={skipped} errors={errors+db_errors}"
    )
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
