"""
app/services/algo_engine.py — WOI Paper Trade Engine

Architecture (DB-persisted price snapshot):
  8:45 AM  → fetch_ltp(501) → save prev_close to DailyPriceSnapshot table
  9:12:30  → fetch_ltp(501) → update open_price in same table → compute gaps
  8:00 AM (next day) → DELETE previous day snapshot before fresh fetch

Benefits:
  - Survives server restart / double-run (upsert, not overwrite)
  - No 429 rate limit from concurrent runs (DB write is idempotent)
  - Debug endpoint: GET /api/algo/debug/snapshot?date=YYYY-MM-DD
"""

import asyncio, json
from datetime import datetime, date, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text

from app.core.database import SessionLocal
from app.models.trading import (
    ClientProfile, AlgoStrategy, AlgoRun, AlgoStock,
    DailyPnl, DailyPriceSnapshot,
)
from app.models.scrip_master import UniverseStock, ScripMaster
from app.services.angel_one import (
    angel_fetch_ltp, angel_fetch_full_quote, angel_fetch_first_candle
)
from app.services.angel_ws import (
    ensure_ws_running, get_live_prices, get_live_ltp,
    is_ws_connected, ws_stats, stop_ws_stream
)
from app.services.master_token import get_master_token, clear_master_token_cache

IST = timezone(timedelta(hours=5, minutes=30))

FALLBACK_UNIVERSE = [
    "1333","11536","10895","15083","4963","3456","14977","1232","5258","11630",
    "2475","16675","7229","1526","13611","10940","467","11184","6705","3787",
]


# ── Logging ───────────────────────────────────────────────────────────────────

def _log(run: AlgoRun, msg: str, db: Session):
    ts      = datetime.now(IST).strftime("%H:%M:%S")
    line    = f"[{ts}] {msg}"
    run.log = (run.log or "") + line + "\n"
    db.commit()
    print(f"[algo] {msg}")


def _log_separator(run: AlgoRun, title: str, db: Session):
    sep = f"{'─' * 10} {title} {'─' * 10}"
    _log(run, sep, db)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _subscribed_clients(db: Session) -> list[tuple[ClientProfile, AlgoStrategy]]:
    strategies = db.query(AlgoStrategy).filter(AlgoStrategy.is_active == True).all()
    result = []
    for s in strategies:
        p = db.query(ClientProfile).filter(ClientProfile.id == s.client_profile_id).first()
        if p:
            result.append((p, s))
    return result


def _get_or_create_run(profile_id: str, strategy_id: str, db: Session) -> AlgoRun:
    today = date.today()
    run   = db.query(AlgoRun).filter(
        AlgoRun.client_profile_id == profile_id,
        AlgoRun.run_date          == today,
    ).first()
    if not run:
        run = AlgoRun(
            client_profile_id=profile_id,
            strategy_id=strategy_id,
            run_date=today,
            status="idle",
            log="",
        )
        db.add(run)
        db.commit()
        db.refresh(run)
    return run


def _load_universe(strategy: AlgoStrategy, db: Session) -> list[dict]:
    if not strategy.universe_id:
        print("[algo] WARNING: No universe set — using fallback list of 20 stocks")
        rows = db.query(ScripMaster).filter(
            ScripMaster.security_id.in_(FALLBACK_UNIVERSE)
        ).all()
        return [{"security_id": r.security_id, "symbol": r.symbol,
                 "series": r.series, "lot_size": r.lot_size} for r in rows]

    stocks = (
        db.query(UniverseStock)
        .filter(
            UniverseStock.universe_id == strategy.universe_id,
            UniverseStock.is_active   == True,
            UniverseStock.not_found   == False,
            UniverseStock.security_id != None,
        )
        .all()
    )

    if not stocks:
        print("[algo] WARNING: Universe empty — using fallback")
        return [{"security_id": sid, "symbol": sid, "series": "EQ", "lot_size": 1}
                for sid in FALLBACK_UNIVERSE]

    result = []
    for s in stocks:
        scrip = db.query(ScripMaster).filter(ScripMaster.security_id == s.security_id).first()
        result.append({
            "security_id": s.security_id,
            "symbol":      s.symbol,
            "series":      scrip.series   if scrip else "EQ",
            "lot_size":    scrip.lot_size if scrip else 1,
        })

    print(f"[algo] Universe loaded: {len(result)} active stocks")
    return result


async def _wait_until(hour: int, minute: int, second: int = 0, label: str = ""):
    now_ist = datetime.now(IST)
    target  = now_ist.replace(hour=hour, minute=minute, second=second, microsecond=0)
    wait    = (target - now_ist).total_seconds()
    if wait > 0:
        print(f"[algo] Waiting {wait:.0f}s until {hour:02d}:{minute:02d}:{second:02d} IST — {label}")
        await asyncio.sleep(wait)
    else:
        print(f"[algo] {hour:02d}:{minute:02d}:{second:02d} already past — proceeding immediately")


# ── DB Snapshot helpers ───────────────────────────────────────────────────────

def _upsert_prev_close(prices: dict[str, float], universe: list[dict], db: Session):
    """
    Bulk upsert prev_close into DailyPriceSnapshot for today.
    Uses INSERT ... ON CONFLICT DO UPDATE so double-runs are safe.
    """
    today    = date.today()
    meta_map = {u["security_id"]: u for u in universe}

    rows = [
        {
            "trade_date":  today,
            "security_id": sid,
            "symbol":      meta_map.get(sid, {}).get("symbol", sid),
            "prev_close":  price,
            "open_price":  None,
            "gap_pct":     None,
        }
        for sid, price in prices.items() if price and price > 0
    ]
    if not rows:
        return

    stmt = pg_insert(DailyPriceSnapshot).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_snapshot_date_sid",
        set_={"prev_close": stmt.excluded.prev_close,
              "symbol":     stmt.excluded.symbol},
    )
    db.execute(stmt)
    db.commit()
    print(f"[snapshot] prev_close saved: {len(rows)} rows → daily_price_snapshots")


def _upsert_open_price(prices: dict[str, float], db: Session):
    """
    Bulk upsert open_price into today's snapshot rows and compute gap_pct.
    Only updates rows that already have prev_close.
    """
    today = date.today()
    rows  = db.query(DailyPriceSnapshot).filter(
        DailyPriceSnapshot.trade_date == today
    ).all()

    updated = 0
    for row in rows:
        open_p = prices.get(str(row.security_id))
        if not open_p or open_p <= 0:
            continue
        prev = float(row.prev_close or 0)
        row.open_price = open_p
        if prev > 0:
            row.gap_pct = round(((open_p - prev) / prev) * 100, 2)
        updated += 1

    db.commit()
    print(f"[snapshot] open_price + gap_pct updated: {updated} rows → daily_price_snapshots")


def _load_gaps_from_db(universe: list[dict], db: Session) -> dict:
    """
    Read today's snapshot from DB and return gap_results dict
    (same shape as old _compute_gaps output).
    """
    today    = date.today()
    meta_map = {u["security_id"]: u for u in universe}

    rows = db.query(DailyPriceSnapshot).filter(
        DailyPriceSnapshot.trade_date == today,
        DailyPriceSnapshot.prev_close != None,
        DailyPriceSnapshot.open_price != None,
        DailyPriceSnapshot.gap_pct    != None,
    ).all()

    results  = {}
    zero_gap = 0
    no_prev  = 0

    for row in rows:
        sid  = str(row.security_id)
        prev = float(row.prev_close or 0)
        open_p = float(row.open_price or 0)
        gap  = float(row.gap_pct or 0)

        if prev <= 0 or open_p <= 0:
            no_prev += 1
            continue
        if gap == 0:
            zero_gap += 1

        m = meta_map.get(sid, {})
        results[sid] = {
            "symbol":     row.symbol or m.get("symbol", sid),
            "series":     m.get("series", "EQ"),
            "lot_size":   m.get("lot_size", 1),
            "gap_pct":    gap,
            "prev_close": prev,
            "open_price": open_p,
        }

    print(f"[algo] Gap from DB: {len(results)} stocks | zero-gap={zero_gap} | no-prev={no_prev}")

    bands = {"0–1%": 0, "1–3%": 0, "3–8%": 0, "8%+": 0}
    for d in results.values():
        g = abs(d["gap_pct"])
        if g < 1:   bands["0–1%"] += 1
        elif g < 3: bands["1–3%"] += 1
        elif g < 8: bands["3–8%"] += 1
        else:       bands["8%+"]  += 1
    print(f"[algo] Gap distribution: {bands}")

    return results


def cleanup_old_snapshots(db: Session):
    """Delete snapshots older than today. Called at 8:00 AM before new fetch."""
    today  = date.today()
    result = db.execute(
        text("DELETE FROM daily_price_snapshots WHERE trade_date < :today"),
        {"today": today},
    )
    db.commit()
    deleted = result.rowcount
    print(f"[snapshot] Cleaned up {deleted} old snapshot rows (before today)")


# ── Step 1: 8:45 AM — fetch LTP as prev close, save to DB ────────────────────

async def _fetch_and_save_prev_close(
    jwt: str, api_key: str, master_client_id: str, universe: list[dict], db: Session
) -> int:
    """Fetch LTP for all universe stocks, save as prev_close to DB. Returns count saved."""
    sec_ids = [u["security_id"] for u in universe]
    print(f"[algo] Fetching prev close LTP for {len(sec_ids)} stocks in {(len(sec_ids)+899)//900} batch(es)...")

    snapshot: dict[str, float] = {}
    for i in range(0, len(sec_ids), 900):
        batch  = sec_ids[i:i+900]
        prices = await angel_fetch_ltp(jwt, api_key, master_client_id, batch)
        snapshot.update(prices)
        print(f"[algo] Batch {i//900 + 1}: got {len(prices)} prices")
        if i + 900 < len(sec_ids):
            await asyncio.sleep(1.1)

    valid = {k: v for k, v in snapshot.items() if v and v > 0}
    print(f"[algo] Prev close fetched: {len(valid)} valid / {len(sec_ids)} total")

    _upsert_prev_close(valid, universe, db)
    return len(valid)


# ── Step 2: 9:12:30 — fetch LTP as open price, update DB, compute gaps ────────

async def _fetch_and_save_open_price(
    jwt: str, api_key: str, master_client_id: str, universe: list[dict], db: Session
) -> int:
    """Fetch LTP as opening price, update DB rows, compute gap_pct. Returns count updated."""
    sec_ids = [u["security_id"] for u in universe]
    print(f"[algo] Fetching opening LTP for {len(sec_ids)} stocks in {(len(sec_ids)+899)//900} batch(es)...")

    opening: dict[str, float] = {}
    for i in range(0, len(sec_ids), 900):
        batch  = sec_ids[i:i+900]
        prices = await angel_fetch_ltp(jwt, api_key, master_client_id, batch)
        opening.update(prices)
        print(f"[algo] Batch {i//900 + 1}: got {len(prices)} prices")
        if i + 900 < len(sec_ids):
            await asyncio.sleep(1.1)

    valid = {k: v for k, v in opening.items() if v and v > 0}
    print(f"[algo] Opening prices fetched: {len(valid)} valid / {len(sec_ids)} total")

    _upsert_open_price(valid, db)
    return len(valid)


# ── Step 3: Apply filters ─────────────────────────────────────────────────────

def _apply_filters(gap_results: dict, strategy: AlgoStrategy, run: AlgoRun, db: Session) -> list[dict]:
    gap_min   = float(strategy.gap_min)
    gap_max   = float(strategy.gap_max)
    min_price = float(strategy.min_price or 0)
    max_price = float(strategy.max_price or 0)
    excl_be   = bool(strategy.exclude_be_series)
    max_n     = int(strategy.max_stocks_per_day)

    _log_separator(run, "STOCK SELECTION", db)
    _log(run, f"Universe: {len(gap_results)} stocks with gap data", db)
    _log(run, f"Filters: gap={gap_min}–{gap_max}% | min_price=₹{min_price} | max_price=₹{max_price} | excl_BE={excl_be}", db)

    passed    = []
    rej_gap   = []
    rej_price = []
    rej_be    = []

    for sid, d in gap_results.items():
        gap  = abs(d["gap_pct"])
        prev = d["prev_close"]
        ser  = d.get("series", "EQ")
        sym  = d.get("symbol", sid)

        if not (gap_min <= gap <= gap_max):
            rej_gap.append(f"{sym}({d['gap_pct']:+.1f}%)")
            continue
        if min_price > 0 and prev < min_price:
            rej_price.append(f"{sym}(₹{prev})")
            continue
        if max_price > 0 and prev > max_price:
            rej_price.append(f"{sym}(₹{prev})")
            continue
        if excl_be and ser == "BE":
            rej_be.append(sym)
            continue

        passed.append({"security_id": sid, **d})

    _log(run, f"Rejected by gap filter: {len(rej_gap)} stocks", db)
    _log(run, f"Rejected by price filter: {len(rej_price)} stocks", db)
    if excl_be:
        _log(run, f"Rejected BE series: {len(rej_be)} stocks", db)
    _log(run, f"Passed all filters: {len(passed)} stocks qualify", db)

    if not passed:
        _log(run, "WARNING: No stocks passed filters! Check gap% settings vs today's market.", db)
        top = sorted(gap_results.values(), key=lambda x: abs(x["gap_pct"]), reverse=True)[:10]
        _log(run, "Top 10 gap movers today (for reference):", db)
        for s in top:
            _log(run, f"  {s['symbol']}: {s['gap_pct']:+.2f}% | prev=₹{s['prev_close']} | open=₹{s['open_price']}", db)
        return []

    passed.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)
    selected = passed[:max_n]

    _log(run, f"Selected top {len(selected)} of {len(passed)} qualifying stocks:", db)
    for s in selected:
        _log(run,
            f"  {s['symbol']}: gap={s['gap_pct']:+.2f}% | "
            f"prev=₹{s['prev_close']} | open=₹{s['open_price']} | series={s['series']}",
            db)

    return selected


# ── Step 4: First candle ──────────────────────────────────────────────────────

async def _compute_entry(security_id: str, strategy: AlgoStrategy, jwt: str, api_key: str, client_id: str) -> Optional[dict]:
    candle = await angel_fetch_first_candle(jwt, api_key, client_id, security_id)
    if not candle:
        return None

    buf  = float(strategy.entry_buffer_pct)
    sl   = float(strategy.sl_pct)
    risk = float(strategy.risk_per_trade)

    base_buy  = candle["close"] if strategy.entry_reference == "close" else candle["high"]
    base_sell = candle["close"] if strategy.entry_reference == "close" else candle["low"]

    buy_trigger  = round(base_buy  * (1 + buf), 2)
    sell_trigger = round(base_sell * (1 - buf), 2)
    rps          = buy_trigger * sl
    quantity     = max(1, int(risk / rps)) if rps > 0 else 1

    return {
        "candle_open":  candle["open"],
        "candle_high":  candle["high"],
        "candle_low":   candle["low"],
        "candle_close": candle["close"],
        "buy_trigger":  buy_trigger,
        "sell_trigger": sell_trigger,
        "quantity":     quantity,
    }


# ── Trail SL ──────────────────────────────────────────────────────────────────

def _check_exit(stock: AlgoStock, ltp: float, strategy: AlgoStrategy, trail_steps: list) -> tuple[bool, str]:
    entry  = float(stock.entry_price or 0)
    one_r  = entry * float(strategy.sl_pct)
    target = float(strategy.target_r)
    if one_r <= 0 or entry <= 0:
        return False, ""

    pnl_r = (
        (ltp - entry) / one_r if stock.entry_direction == "BUY"
        else (entry - ltp)    / one_r
    )

    if pnl_r >= target:
        return True, "TARGET"

    locked_r = -1.0
    for step in sorted(trail_steps, key=lambda s: s[0], reverse=True):
        if pnl_r >= step[0]:
            locked_r = step[1]
            break

    sl_price = (
        entry + (locked_r * one_r) if stock.entry_direction == "BUY"
        else entry - (locked_r * one_r)
    )

    if stock.entry_direction == "BUY"  and ltp <= sl_price: return True, "SL"
    if stock.entry_direction == "SELL" and ltp >= sl_price: return True, "SL"
    return False, ""


def _calc_pnl(stock: AlgoStock, exit_price: float) -> float:
    qty   = stock.quantity or 1
    entry = float(stock.entry_price or 0)
    return round(
        (exit_price - entry) * qty if stock.entry_direction == "BUY"
        else (entry - exit_price) * qty, 2
    )


def _update_daily_pnl(profile_id: str, pnl: float, db: Session):
    today = date.today()
    row   = db.query(DailyPnl).filter(
        DailyPnl.client_profile_id == profile_id, DailyPnl.date == today
    ).first()
    if row:
        row.closed_pnl  = float(row.closed_pnl  or 0) + pnl
        row.total_pnl   = float(row.total_pnl   or 0) + pnl
        row.trade_count = (row.trade_count or 0) + 1
    else:
        db.add(DailyPnl(
            client_profile_id=profile_id, date=today,
            closed_pnl=pnl, running_pnl=0, total_pnl=pnl, trade_count=1,
        ))
    db.commit()


# ── Monitor tick ──────────────────────────────────────────────────────────────

async def _monitor_tick(db: Session, jwt: str, api_key: str, client_id: str):
    today  = date.today()
    stocks = db.query(AlgoStock).filter(AlgoStock.status.in_(["watching", "entered"])).all()
    if not stocks:
        return

    sec_ids = list({s.security_id for s in stocks})

    # Use WebSocket cache if connected — no REST call needed
    if is_ws_connected():
        ltp_map = get_live_prices(sec_ids)
        if not ltp_map:
            # WebSocket not populated yet — fall back to REST
            ltp_map = await angel_fetch_ltp(jwt, api_key, client_id, sec_ids)
    else:
        ltp_map = await angel_fetch_ltp(jwt, api_key, client_id, sec_ids)

    for stock in stocks:
        ltp = ltp_map.get(str(stock.security_id))
        if not ltp:
            continue

        run = db.query(AlgoRun).filter(AlgoRun.id == stock.run_id).first()
        if not run or run.run_date != today:
            continue

        strategy = db.query(AlgoStrategy).filter(
            AlgoStrategy.client_profile_id == stock.client_profile_id
        ).first()
        if not strategy:
            continue

        try:
            trail_steps = json.loads(strategy.trail_sl_steps or "[]")
        except Exception:
            trail_steps = []

        if stock.status == "watching":
            buy_t  = float(stock.buy_trigger  or 0)
            sell_t = float(stock.sell_trigger or 0)
            entered = False

            if not strategy.gap_direction_bias:
                if buy_t and ltp >= buy_t:
                    stock.entry_direction = "BUY";  entered = True
                elif sell_t and ltp <= sell_t:
                    stock.entry_direction = "SELL"; entered = True
            else:
                if stock.direction == "UP"   and buy_t  and ltp >= buy_t:
                    stock.entry_direction = "BUY";  entered = True
                elif stock.direction == "DOWN" and sell_t and ltp <= sell_t:
                    stock.entry_direction = "SELL"; entered = True

            if entered:
                stock.status      = "entered"
                stock.entry_price = ltp
                stock.entry_time  = datetime.now(timezone.utc)
                run.stocks_traded = (run.stocks_traded or 0) + 1
                trig     = buy_t if stock.entry_direction == "BUY" else sell_t
                sl_price = ltp * (1 - float(strategy.sl_pct)) if stock.entry_direction == "BUY" \
                           else ltp * (1 + float(strategy.sl_pct))
                tgt_price = ltp * (1 + float(strategy.sl_pct) * float(strategy.target_r)) \
                            if stock.entry_direction == "BUY" \
                            else ltp * (1 - float(strategy.sl_pct) * float(strategy.target_r))
                _log(run,
                    f"PAPER {stock.entry_direction} {stock.symbol} @ ₹{ltp:.2f} "
                    f"| trigger=₹{trig:.2f} | SL=₹{sl_price:.2f} | target=₹{tgt_price:.2f} "
                    f"| qty={stock.quantity}",
                    db)
            db.commit()
            continue

        if stock.status == "entered" and stock.entry_price and stock.entry_direction:
            should_exit, reason = _check_exit(stock, ltp, strategy, trail_steps)
            if should_exit:
                pnl               = _calc_pnl(stock, ltp)
                entry             = float(stock.entry_price)
                one_r             = entry * float(strategy.sl_pct)
                rr                = round(pnl / (one_r * (stock.quantity or 1)), 2) if one_r > 0 else 0
                stock.status      = "exited"
                stock.exit_price  = ltp
                stock.exit_time   = datetime.now(timezone.utc)
                stock.exit_reason = reason
                stock.pnl         = pnl
                run.total_pnl     = float(run.total_pnl or 0) + pnl
                _log(run,
                    f"EXIT {stock.symbol} [{reason}] @ ₹{ltp:.2f} "
                    f"| entry=₹{entry:.2f} | P&L=₹{pnl:+.2f} | R:R={rr:+.2f}R",
                    db)
                _update_daily_pnl(stock.client_profile_id, pnl, db)
        db.commit()


# ── EOD force exit ────────────────────────────────────────────────────────────

async def _force_exit_all(db: Session, jwt: str, api_key: str, client_id: str):
    stocks = db.query(AlgoStock).filter(AlgoStock.status.in_(["watching", "entered"])).all()
    if not stocks:
        return

    sec_ids = list({s.security_id for s in stocks})
    # Try WebSocket first, fall back to REST
    ltp_map = get_live_prices(sec_ids) if is_ws_connected() else {}
    if not ltp_map:
        ltp_map = await angel_fetch_ltp(jwt, api_key, client_id, sec_ids)

    for stock in stocks:
        ltp = ltp_map.get(str(stock.security_id), float(stock.entry_price or 0))
        pnl = _calc_pnl(stock, ltp) if stock.status == "entered" else 0
        stock.status      = "exited"
        stock.exit_price  = ltp
        stock.exit_time   = datetime.now(timezone.utc)
        stock.exit_reason = "EOD"
        stock.pnl         = pnl
        run = db.query(AlgoRun).filter(AlgoRun.id == stock.run_id).first()
        if run:
            run.total_pnl = float(run.total_pnl or 0) + pnl
            _log(run, f"FORCE EXIT {stock.symbol} @ ₹{ltp:.2f} [EOD] | P&L=₹{pnl:+.2f}", db)
        if pnl != 0:
            _update_daily_pnl(stock.client_profile_id, pnl, db)

    db.commit()


# ── Main daily run ────────────────────────────────────────────────────────────

async def _fresh_master_token() -> tuple[str, str, str]:
    """
    Return (jwt_token, api_key, client_id) for Angel One master account.
    Reads from DB — no in-memory cache.
    """
    db = SessionLocal()
    try:
        return await get_master_token(db)
    finally:
        db.close()


async def _keep_token_alive_until(
    hour: int, minute: int, second: int,
    jwt: str, api_key: str, client_id: str
):
    """
    Ping Dhan LTP every 20 minutes to prevent session timeout.
    Dhan market data tokens expire after ~30 min of inactivity.
    Uses a single stock (security_id=1333 = HDFCBANK) as heartbeat.
    """
    PING_INTERVAL = 20 * 60   # 20 minutes
    HEARTBEAT_SID = ["1333"]  # HDFCBANK — always in NSE_EQ

    while True:
        now_ist = datetime.now(IST)
        target  = now_ist.replace(
            hour=hour, minute=minute, second=second, microsecond=0
        )
        remaining = (target - now_ist).total_seconds()

        if remaining <= 0:
            break

        if remaining <= PING_INTERVAL:
            # Close enough — just wait out the rest
            print(f"[algo] Waiting {remaining:.0f}s until {hour:02d}:{minute:02d}:{second:02d} IST")
            await asyncio.sleep(remaining)
            break

        # Sleep 20 min then ping
        print(f"[algo] Token keep-alive: {remaining:.0f}s to go — "
              f"pinging in {PING_INTERVAL}s")
        await asyncio.sleep(PING_INTERVAL)

        # Ping with single stock LTP to keep session alive
        try:
            prices = await angel_fetch_ltp(jwt, api_key, client_id, HEARTBEAT_SID)
            if prices:
                print(f"[algo] Token keep-alive ping OK "
                      f"(HDFCBANK LTP=₹{list(prices.values())[0]:.2f})")
            else:
                print("[algo] Token keep-alive ping returned empty — "
                      "token may have expired, will refresh at step 2")
        except Exception as e:
            print(f"[algo] Token keep-alive ping error: {e}")


# Guard against double-runs
_algo_running = False


async def run_daily_algo():
    global _algo_running
    if _algo_running:
        print("[algo] Already running — skipping duplicate trigger")
        return
    _algo_running = True
    # Do NOT clear token cache here — master token was refreshed at 8:00 AM
    # and is still valid. Clearing would force a new TOTP login at 8:45
    # which would invalidate the master token and cause 401 errors.

    db = SessionLocal()
    runs = {}
    try:
        now_ist = datetime.now(IST)
        print(f"[algo] ═══════════════════════════════════════")
        print(f"[algo] WOI Algo Engine starting — {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"[algo] ═══════════════════════════════════════")

        clients = _subscribed_clients(db)
        if not clients:
            print("[algo] No subscribed clients — nothing to do")
            return

        print(f"[algo] Subscribed clients: {len(clients)}")

        print("[algo] Getting master data account token...")
        jwt, api_key, master_client_id = await _fresh_master_token()
        print(f"[algo] Master token OK (client_id: {master_client_id})")

        # Get feed token for WebSocket from DB
        from app.models.master_account import MasterDataAccount
        _acc = db.query(MasterDataAccount).first()
        _feed_token = _acc.angel_feed_token if _acc and _acc.angel_feed_token else ""
        if _feed_token:
            print(f"[algo] Feed token available for WebSocket")
        else:
            print("[algo] WARNING: No feed token — WebSocket will not start. Run Test in Settings.")

        # Create/reuse today's run for each client
        for profile, strategy in clients:
            run = _get_or_create_run(profile.id, strategy.id, db)
            run.status     = "scanning"
            run.started_at = datetime.now(timezone.utc)
            db.commit()
            runs[profile.id] = (run, strategy)
            _log(run, f"═══ WOI Algo run started — {now_ist.strftime('%Y-%m-%d')} ═══", db)
            _log(run,
                f"Strategy: {strategy.name} | Gap: {strategy.gap_min}–{strategy.gap_max}% | "
                f"Risk: ₹{strategy.risk_per_trade} | Target: 1:{strategy.target_r}R | "
                f"Max stocks: {strategy.max_stocks_per_day}", db)

        # Load universes
        client_universes: dict[str, list[dict]] = {}
        all_meta: dict[str, dict] = {}
        for profile, strategy in clients:
            universe = _load_universe(strategy, db)
            client_universes[profile.id] = universe
            for u in universe:
                all_meta[u["security_id"]] = u
            run, _ = runs[profile.id]
            _log(run, f"Universe loaded: {len(universe)} active stocks", db)

        combined_universe = list(all_meta.values())
        print(f"[algo] Combined universe: {len(combined_universe)} unique stocks across all clients")

        # ── Start WebSocket stream for live prices ─────────────────────────
        # Subscribe all 501 stocks once — continuous LTP stream
        # Used during monitor loop instead of repeated REST calls
        if _feed_token:
            sid_list = [u["security_id"] for u in combined_universe]
            await ensure_ws_running(jwt, _feed_token, master_client_id, sid_list)
            print(f"[algo] WebSocket: {ws_stats()}")
        else:
            print("[algo] Skipping WebSocket — no feed token")

        # ── STEP 1: 8:45 AM — prev close snapshot → DB ─────────────────
        for _, (run, _) in runs.items():
            _log_separator(run, "STEP 1: PREV CLOSE SNAPSHOT (8:45 AM)", db)
            _log(run, "Waiting for 8:45 AM to fetch prev close LTP...", db)

        await _wait_until(8, 45, 0, "prev close snapshot")
        jwt, api_key, master_client_id = await _fresh_master_token()

        # Check if today's snapshot already has prev_close (double-run guard)
        existing = db.query(DailyPriceSnapshot).filter(
            DailyPriceSnapshot.trade_date == date.today(),
            DailyPriceSnapshot.prev_close != None,
        ).count()

        for _, (run, _) in runs.items():
            if existing >= len(combined_universe) * 0.9:
                _log(run, f"Prev close already in DB ({existing} rows) — skipping re-fetch", db)
            else:
                _log(run, f"Fetching LTP for {len(combined_universe)} stocks as prev close baseline...", db)

        if existing < len(combined_universe) * 0.9:
            saved = await _fetch_and_save_prev_close(jwt, api_key, master_client_id, combined_universe, db)
            for _, (run, _) in runs.items():
                _log(run, f"Prev close snapshot: {saved}/{len(combined_universe)} stocks saved to DB", db)
                if saved < len(combined_universe) * 0.9:
                    _log(run, f"WARNING: Only {saved} of {len(combined_universe)} stocks returned LTP — check master data account", db)
        else:
            print(f"[algo] Prev close already in DB ({existing} rows) — skipping fetch")

        # ── STEP 2: 9:12:30 — opening price → DB → compute gaps ────────
        for _, (run, _) in runs.items():
            _log_separator(run, "STEP 2: OPENING PRICE SCAN (9:12:30)", db)
            _log(run, "Waiting for 9:12:30 to fetch pre-open prices...", db)

        # Keep token alive during wait — ping Dhan every 20 min with 1 stock LTP
        # Dhan market data tokens expire after ~30 min of inactivity
        await _keep_token_alive_until(9, 12, 30, jwt, api_key, master_client_id)
        # Re-use same token (no new login — cache serves it)
        jwt, api_key, master_client_id = await _fresh_master_token()

        for _, (run, _) in runs.items():
            _log(run, f"Fetching opening LTP for {len(combined_universe)} stocks...", db)

        open_count = await _fetch_and_save_open_price(jwt, api_key, master_client_id, combined_universe, db)

        # Load gap results from DB (single source of truth)
        all_gaps = _load_gaps_from_db(combined_universe, db)

        for _, (run, _) in runs.items():
            _log(run, f"Opening prices fetched: {open_count} stocks", db)
            _log(run, f"Gap computed for {len(all_gaps)} stocks (from DB snapshot)", db)
            bands = {"0–1%": 0, "1–3%": 0, "3–8%": 0, "8%+": 0}
            for d in all_gaps.values():
                g = abs(d["gap_pct"])
                if g < 1:   bands["0–1%"] += 1
                elif g < 3: bands["1–3%"] += 1
                elif g < 8: bands["3–8%"] += 1
                else:       bands["8%+"]  += 1
            _log(run, f"Gap distribution: {bands}", db)

        # ── STEP 3: Filter & select ─────────────────────────────────────
        for profile, strategy in clients:
            run, strat = runs[profile.id]
            client_sids = {u["security_id"] for u in client_universes[profile.id]}
            client_gaps = {sid: d for sid, d in all_gaps.items() if sid in client_sids}
            run.stocks_scanned = len(client_sids)
            selected = _apply_filters(client_gaps, strat, run, db)
            run.stocks_selected = len(selected)
            db.commit()

            if not selected:
                _log(run, "No stocks selected — algo will idle until 3:20 PM then exit", db)
            else:
                # Circuit limit filter — remove stocks at upper/lower circuit
                _log(run, f"Checking circuit limits for {len(selected)} stocks via Angel One FULL quote...", db)
                try:
                    sids_to_check = [s["security_id"] for s in selected]
                    full_data = await angel_fetch_full_quote(
                        jwt, api_key, master_client_id, sids_to_check
                    )
                    circuit_passed = []
                    for item in selected:
                        sid  = item["security_id"]
                        quot = full_data.get(str(sid))
                        if not quot:
                            circuit_passed.append(item)
                            continue
                        ltp   = quot.get("ltp", 0)
                        upper = quot.get("upper_circuit", 0)
                        lower = quot.get("lower_circuit", 0)
                        at_upper = upper > 0 and ltp >= upper * 0.999
                        at_lower = lower > 0 and ltp <= lower * 1.001
                        if at_upper:
                            _log(run, f"  REMOVED {item['symbol']}: at UPPER circuit ₹{upper} — cannot buy", db)
                        elif at_lower:
                            _log(run, f"  REMOVED {item['symbol']}: at LOWER circuit ₹{lower} — cannot sell", db)
                        else:
                            item["upper_circuit"] = upper
                            item["lower_circuit"] = lower
                            circuit_passed.append(item)
                            _log(run, f"  OK {item['symbol']}: LTP=₹{ltp} UC=₹{upper} LC=₹{lower}", db)
                    _log(run, f"Circuit filter: {len(circuit_passed)}/{len(selected)} stocks passed", db)
                    selected = circuit_passed
                except Exception as e:
                    _log(run, f"Circuit limit check failed: {e} — proceeding without filter", db)

                for item in selected:
                    exists = db.query(AlgoStock).filter(
                        AlgoStock.run_id      == run.id,
                        AlgoStock.security_id == item["security_id"],
                    ).first()
                    if not exists:
                        db.add(AlgoStock(
                            run_id=run.id,
                            client_profile_id=profile.id,
                            symbol=item["symbol"],
                            security_id=item["security_id"],
                            gap_pct=item["gap_pct"],
                            direction="UP" if item["gap_pct"] > 0 else "DOWN",
                            prev_close=item["prev_close"],
                            status="watching",
                            source="preopen",
                        ))
                db.commit()

        # ── STEP 4: 9:16:05 first candle ───────────────────────────────
        for _, (run, _) in runs.items():
            _log_separator(run, "STEP 3: FIRST CANDLE (9:16:05)", db)
            _log(run, "Waiting for 9:16:05 — first 1-min candle closes at 9:16:00...", db)

        await _wait_until(9, 16, 5, "first candle")
        jwt, api_key, master_client_id = await _fresh_master_token()

        for profile, strategy in clients:
            run, strat = runs[profile.id]
            run.status = "running"
            db.commit()

            stocks = db.query(AlgoStock).filter(
                AlgoStock.run_id == run.id, AlgoStock.status == "watching"
            ).all()

            if not stocks:
                _log(run, "No stocks to fetch candle for (none selected in previous step)", db)
                continue

            _log(run, f"Fetching first 1-min candle for {len(stocks)} selected stocks...", db)

            for stock in stocks:
                _log(run, f"Fetching candle for {stock.symbol} (security_id={stock.security_id})...", db)
                entry = await _compute_entry(stock.security_id, strat, jwt, api_key, master_client_id)
                if entry:
                    stock.candle_high   = entry["candle_high"]
                    stock.candle_low    = entry["candle_low"]
                    stock.candle_close  = entry["candle_close"]
                    stock.buy_trigger   = entry["buy_trigger"]
                    stock.sell_trigger  = entry["sell_trigger"]
                    stock.quantity      = entry["quantity"]
                    _log(run,
                        f"{stock.symbol}: O=₹{entry['candle_open']:.2f} "
                        f"H=₹{entry['candle_high']:.2f} "
                        f"L=₹{entry['candle_low']:.2f} "
                        f"C=₹{entry['candle_close']:.2f} | "
                        f"BUY trigger=₹{entry['buy_trigger']:.2f} | "
                        f"SELL trigger=₹{entry['sell_trigger']:.2f} | "
                        f"Qty={entry['quantity']}",
                        db)
                else:
                    stock.status = "cancelled"
                    _log(run, f"{stock.symbol}: No candle data — skipped", db)
                db.commit()
                await asyncio.sleep(0.3)

        # ── STEP 5: Monitor loop ────────────────────────────────────────
        for _, (run, _) in runs.items():
            _log_separator(run, "STEP 4: MONITORING", db)
            _log(run, "Monitor loop started — checking LTP every 5s until 3:20 PM", db)

        eod_ist    = datetime.now(IST).replace(hour=15, minute=20, second=0, microsecond=0)
        tick_count = 0

        while datetime.now(IST) < eod_ist:
            await asyncio.sleep(5)
            tick_count += 1
            try:
                jwt, api_key, master_client_id = await _fresh_master_token()
                await _monitor_tick(db, jwt, api_key, master_client_id)
            except Exception as e:
                print(f"[algo] Monitor tick error: {e}")
                for _, (run, _) in runs.items():
                    _log(run, f"Monitor tick error: {e}", db)

            if tick_count % 60 == 0:
                elapsed    = (datetime.now(IST) - now_ist).seconds // 60
                open_count = db.query(AlgoStock).filter(
                    AlgoStock.status.in_(["watching", "entered"])
                ).count()
                for _, (run, _) in runs.items():
                    _log(run, f"Heartbeat: {elapsed}min elapsed | {open_count} position(s) still open", db)

            open_count = db.query(AlgoStock).filter(
                AlgoStock.status.in_(["watching", "entered"])
            ).count()
            if open_count == 0:
                for _, (run, _) in runs.items():
                    _log(run, "All positions closed — stopping monitor loop early", db)
                break

        # ── STEP 6: EOD force exit ──────────────────────────────────────
        for _, (run, _) in runs.items():
            _log_separator(run, "STEP 5: EOD FORCE EXIT (3:20 PM)", db)

        jwt, api_key, master_client_id = await _fresh_master_token()
        await _force_exit_all(db, jwt, api_key, master_client_id)

        # Final summary
        for profile, _ in clients:
            run, _ = runs[profile.id]
            run.status      = "done"
            run.finished_at = datetime.now(timezone.utc)
            _log_separator(run, "RUN COMPLETE", db)
            _log(run, f"Total P&L: ₹{float(run.total_pnl or 0):.2f}", db)
            _log(run, f"Stocks: scanned={run.stocks_scanned} | selected={run.stocks_selected} | traded={run.stocks_traded}", db)
            winners = db.query(AlgoStock).filter(
                AlgoStock.run_id == run.id, AlgoStock.status == "exited", AlgoStock.pnl > 0
            ).count()
            losers = db.query(AlgoStock).filter(
                AlgoStock.run_id == run.id, AlgoStock.status == "exited", AlgoStock.pnl <= 0
            ).count()
            _log(run,
                f"Trades: {winners} winners | {losers} losers | "
                f"Win rate: {round(winners/(winners+losers)*100,1) if (winners+losers) > 0 else 0}%", db)

        print("[algo] ═══ Daily run complete ═══")

    except Exception as e:
        print(f"[algo] FATAL ERROR: {e}")
        import traceback; traceback.print_exc()
        for _, (run, _) in (runs.items() if runs else []):
            try:
                _log(run, f"FATAL ERROR: {e}", db)
                run.status = "error"
                db.commit()
            except Exception:
                pass
    finally:
        _algo_running = False
        db.close()
