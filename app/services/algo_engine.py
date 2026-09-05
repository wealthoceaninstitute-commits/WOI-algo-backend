"""
app/services/algo_engine.py — WOI Paper Trade Engine

Stock selection flow:
  08:45 AM → Fetch LTP for entire universe → store as prev_close snapshot
  09:12:30 → Fetch LTP again → this is the opening/pre-open price
  09:12:30 → Compute gap% = (open_ltp - prev_close) / prev_close × 100
  09:12:30 → Apply filters → sort by |gap%| → pick top N
  09:15:00 → Wait for first 1-min candle (9:15–9:16)
  09:15:30 → Fetch first candle → compute BUY/SELL triggers
  09:16:00+ → Monitor loop every 5s → trail SL → exit at target
  15:20:00 → Force exit all open paper positions
"""

import asyncio, json
from datetime import datetime, date, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.trading import ClientProfile, AlgoStrategy, AlgoRun, AlgoStock, DailyPnl
from app.models.scrip_master import UniverseStock, ScripMaster
from app.services.market_data import (
    get_master_token, fetch_ltp, fetch_first_candle,
)

IST = timezone(timedelta(hours=5, minutes=30))

FALLBACK_UNIVERSE = [
    "1333","11536","10895","15083","4963","3456","14977","1232","5258","11630",
    "2475","16675","7229","1526","13611","10940","467","11184","6705","3787",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log(run: AlgoRun, msg: str, db: Session):
    ts      = datetime.now(IST).strftime("%H:%M:%S")
    run.log = (run.log or "") + f"[{ts}] {msg}\n"
    db.commit()
    print(f"[algo] {msg}")


def _subscribed_clients(db: Session) -> list[tuple[ClientProfile, AlgoStrategy]]:
    strategies = db.query(AlgoStrategy).filter(AlgoStrategy.is_active == True).all()
    result = []
    for s in strategies:
        p = db.query(ClientProfile).filter(
            ClientProfile.id == s.client_profile_id
        ).first()
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


# ── Universe loader ───────────────────────────────────────────────────────────

def _load_universe(strategy: AlgoStrategy, db: Session) -> list[dict]:
    """
    Load active stocks from the strategy's assigned universe.
    Returns list of { security_id, symbol, series, lot_size }
    """
    if not strategy.universe_id:
        print(f"[algo] No universe set — using fallback list")
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
        print(f"[algo] Universe empty — using fallback")
        return [{"security_id": sid, "symbol": sid, "series": "EQ", "lot_size": 1}
                for sid in FALLBACK_UNIVERSE]

    result = []
    for s in stocks:
        scrip = db.query(ScripMaster).filter(
            ScripMaster.security_id == s.security_id
        ).first()
        result.append({
            "security_id": s.security_id,
            "symbol":      s.symbol,
            "series":      scrip.series    if scrip else "EQ",
            "lot_size":    scrip.lot_size  if scrip else 1,
        })

    print(f"[algo] Universe loaded: {len(result)} stocks")
    return result


# ── Wait until a specific IST time ───────────────────────────────────────────

async def _wait_until(hour: int, minute: int, second: int = 0, label: str = ""):
    now_ist = datetime.now(IST)
    target  = now_ist.replace(hour=hour, minute=minute, second=second, microsecond=0)
    wait    = (target - now_ist).total_seconds()
    if wait > 0:
        print(f"[algo] Waiting {wait:.0f}s until {hour:02d}:{minute:02d}:{second:02d} IST {label}")
        await asyncio.sleep(wait)


# ── Step 1: 8:45 AM — prev close snapshot ────────────────────────────────────

async def _fetch_prev_close_snapshot(
    token: str, client_id: str, universe: list[dict]
) -> dict[str, float]:
    """
    Fetch LTP at 8:45 AM for all universe stocks.
    This becomes the 'previous close' baseline for gap calculation.
    Returns { security_id: ltp }
    """
    sec_ids = [u["security_id"] for u in universe]
    print(f"[algo] 8:45 AM snapshot: fetching LTP for {len(sec_ids)} stocks...")

    snapshot = {}
    for i in range(0, len(sec_ids), 900):   # Dhan allows 1000 per request
        batch  = sec_ids[i:i+900]
        prices = await fetch_ltp(token, client_id, batch)
        snapshot.update(prices)
        if i + 900 < len(sec_ids):
            await asyncio.sleep(1.1)         # 1 req/sec rate limit

    valid = {k: v for k, v in snapshot.items() if v and v > 0}
    print(f"[algo] 8:45 AM snapshot: {len(valid)} stocks with valid LTP")
    return valid


# ── Step 2: 9:12:30 — opening price scan + gap calculation ───────────────────

async def _fetch_opening_prices(
    token: str, client_id: str, universe: list[dict]
) -> dict[str, float]:
    """
    Fetch LTP at 9:12:30 AM — this is the pre-open discovered price
    (indicative opening price before actual market open at 9:15).
    Returns { security_id: ltp }
    """
    sec_ids = [u["security_id"] for u in universe]
    print(f"[algo] 9:12:30 scan: fetching opening LTP for {len(sec_ids)} stocks...")

    opening = {}
    for i in range(0, len(sec_ids), 900):
        batch  = sec_ids[i:i+900]
        prices = await fetch_ltp(token, client_id, batch)
        opening.update(prices)
        if i + 900 < len(sec_ids):
            await asyncio.sleep(1.1)

    valid = {k: v for k, v in opening.items() if v and v > 0}
    print(f"[algo] 9:12:30 scan: {len(valid)} stocks with valid price")
    return valid


def _compute_gaps(
    prev_close: dict[str, float],
    opening:    dict[str, float],
    universe:   list[dict],
) -> dict[str, dict]:
    """
    gap% = (opening_ltp - prev_close_ltp) / prev_close_ltp × 100
    Returns { security_id: { symbol, gap_pct, prev_close, open_price, series, lot_size } }
    """
    meta    = {u["security_id"]: u for u in universe}
    results = {}

    for sid, open_price in opening.items():
        prev = prev_close.get(sid)
        if not prev or prev <= 0 or open_price <= 0:
            continue
        gap_pct = ((open_price - prev) / prev) * 100
        m       = meta.get(sid, {})
        results[sid] = {
            "symbol":      m.get("symbol", sid),
            "series":      m.get("series", "EQ"),
            "lot_size":    m.get("lot_size", 1),
            "gap_pct":     round(gap_pct, 2),
            "prev_close":  round(prev, 2),
            "open_price":  round(open_price, 2),
        }

    return results


# ── Step 3: Apply filters & select top N ─────────────────────────────────────

def _apply_filters(
    gap_results: dict,
    strategy:    AlgoStrategy,
    run:         AlgoRun,
    db:          Session,
) -> list[dict]:
    """
    Filter sequence:
      1. Gap % band
      2. Min price (prev_close ≥ min_price)
      3. Max price (prev_close ≤ max_price, if set)
      4. BE series exclusion
      5. Sort by |gap%| descending
      6. Take top max_stocks_per_day
    Note: volume/turnover not available from LTP — skipped in this flow.
    """
    gap_min   = float(strategy.gap_min)
    gap_max   = float(strategy.gap_max)
    min_price = float(strategy.min_price or 0)
    max_price = float(strategy.max_price or 0)
    excl_be   = bool(strategy.exclude_be_series)
    max_n     = int(strategy.max_stocks_per_day)

    passed   = []
    rejected = {"gap": 0, "price": 0, "be": 0}

    for sid, d in gap_results.items():
        gap  = abs(d["gap_pct"])
        prev = d["prev_close"]
        ser  = d.get("series", "EQ")

        # 1. Gap band
        if not (gap_min <= gap <= gap_max):
            rejected["gap"] += 1
            continue

        # 2. Min price filter (0 = disabled)
        if min_price > 0 and prev < min_price:
            rejected["price"] += 1
            continue

        # 3. Max price filter (0 = disabled)
        if max_price > 0 and prev > max_price:
            rejected["price"] += 1
            continue

        # 4. BE series exclusion
        if excl_be and ser == "BE":
            rejected["be"] += 1
            continue

        passed.append({"security_id": sid, **d})

    _log(run,
        f"Filters: {len(gap_results)} stocks scanned | "
        f"gap={rejected['gap']} rejected | price={rejected['price']} rejected | "
        f"BE={rejected['be']} excluded | {len(passed)} qualify",
        db)

    # Sort by absolute gap% descending — biggest movers first
    passed.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)
    selected = passed[:max_n]

    _log(run,
        f"Selected top {len(selected)}: " +
        ", ".join(f"{s['symbol']} ({s['gap_pct']:+.2f}%)" for s in selected),
        db)

    return selected


# ── Step 4: First candle entry triggers ──────────────────────────────────────

async def _compute_entry(
    security_id: str,
    strategy:    AlgoStrategy,
    token:       str,
    client_id:   str,
) -> Optional[dict]:
    candle = await fetch_first_candle(token, client_id, security_id)
    if not candle:
        return None

    buf  = float(strategy.entry_buffer_pct)
    sl   = float(strategy.sl_pct)
    risk = float(strategy.risk_per_trade)

    base_buy  = candle["close"] if strategy.entry_reference == "close" else candle["high"]
    base_sell = candle["close"] if strategy.entry_reference == "close" else candle["low"]

    buy_trigger  = round(base_buy  * (1 + buf), 2)
    sell_trigger = round(base_sell * (1 - buf), 2)

    risk_per_share = buy_trigger * sl
    quantity = max(1, int(risk / risk_per_share)) if risk_per_share > 0 else 1

    return {
        "candle_high":  candle["high"],
        "candle_low":   candle["low"],
        "candle_close": candle["close"],
        "buy_trigger":  buy_trigger,
        "sell_trigger": sell_trigger,
        "quantity":     quantity,
    }


# ── Trail SL & exit ───────────────────────────────────────────────────────────

def _check_exit(
    stock:       AlgoStock,
    ltp:         float,
    strategy:    AlgoStrategy,
    trail_steps: list,
) -> tuple[bool, str]:
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
        else (entry - exit_price) * qty,
        2,
    )


def _update_daily_pnl(profile_id: str, pnl: float, db: Session):
    today = date.today()
    row   = db.query(DailyPnl).filter(
        DailyPnl.client_profile_id == profile_id,
        DailyPnl.date              == today,
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

async def _monitor_tick(db: Session, token: str, client_id: str):
    today  = date.today()
    stocks = db.query(AlgoStock).filter(
        AlgoStock.status.in_(["watching", "entered"])
    ).all()
    if not stocks:
        return

    sec_ids = list({s.security_id for s in stocks})
    ltp_map = await fetch_ltp(token, client_id, sec_ids)

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

        # Watching → check if trigger hit
        if stock.status == "watching":
            buy_t  = float(stock.buy_trigger  or 0)
            sell_t = float(stock.sell_trigger or 0)

            entered = False
            if not strategy.gap_direction_bias:
                # OCO — either side triggers
                if buy_t and ltp >= buy_t:
                    stock.entry_direction = "BUY";  entered = True
                elif sell_t and ltp <= sell_t:
                    stock.entry_direction = "SELL"; entered = True
            else:
                # Only enter in gap direction
                if stock.direction == "UP"   and buy_t  and ltp >= buy_t:
                    stock.entry_direction = "BUY";  entered = True
                elif stock.direction == "DOWN" and sell_t and ltp <= sell_t:
                    stock.entry_direction = "SELL"; entered = True

            if entered:
                stock.status      = "entered"
                stock.entry_price = ltp
                run.stocks_traded = (run.stocks_traded or 0) + 1
                _log(run,
                    f"PAPER {stock.entry_direction} {stock.symbol} @ ₹{ltp:.2f} "
                    f"(trigger ₹{buy_t if stock.entry_direction == 'BUY' else sell_t:.2f})",
                    db)

            db.commit()
            continue

        # Entered → check trail SL / target exit
        if stock.status == "entered" and stock.entry_price and stock.entry_direction:
            should_exit, reason = _check_exit(stock, ltp, strategy, trail_steps)
            if should_exit:
                pnl          = _calc_pnl(stock, ltp)
                stock.status = "exited"
                stock.exit_price = ltp
                stock.pnl        = pnl
                run.total_pnl    = float(run.total_pnl or 0) + pnl
                _log(run,
                    f"EXIT {stock.symbol} @ ₹{ltp:.2f} [{reason}] "
                    f"P&L: {pnl:+.2f}",
                    db)
                _update_daily_pnl(stock.client_profile_id, pnl, db)

        db.commit()


# ── EOD force exit ────────────────────────────────────────────────────────────

async def _force_exit_all(db: Session, token: str, client_id: str):
    print("[algo] 3:20 PM — force-exiting all open paper positions")
    stocks  = db.query(AlgoStock).filter(AlgoStock.status.in_(["watching","entered"])).all()
    if not stocks:
        return

    sec_ids = list({s.security_id for s in stocks})
    ltp_map = await fetch_ltp(token, client_id, sec_ids)

    for stock in stocks:
        ltp = ltp_map.get(str(stock.security_id), float(stock.entry_price or 0))
        pnl = _calc_pnl(stock, ltp) if stock.status == "entered" else 0
        stock.status     = "exited"
        stock.exit_price = ltp
        stock.pnl        = pnl
        run = db.query(AlgoRun).filter(AlgoRun.id == stock.run_id).first()
        if run:
            run.total_pnl = float(run.total_pnl or 0) + pnl
            _log(run, f"FORCE EXIT {stock.symbol} @ ₹{ltp:.2f} [EOD] P&L: {pnl:+.2f}", db)
        if pnl != 0:
            _update_daily_pnl(stock.client_profile_id, pnl, db)

    db.commit()
    print(f"[algo] Force-exited {len(stocks)} positions")


# ── Main daily run ────────────────────────────────────────────────────────────

async def run_daily_algo():
    """
    Full daily paper trading cycle.
    Scheduled at 8:45 AM IST by morning_scheduler in main.py.
    """
    db = SessionLocal()
    try:
        clients = _subscribed_clients(db)
        if not clients:
            print("[algo] No subscribed clients — skipping")
            return

        print(f"[algo] Starting daily run for {len(clients)} client(s)")
        token, master_client_id = await get_master_token(db)

        # Create today's run records
        runs = {}
        for profile, strategy in clients:
            run = _get_or_create_run(profile.id, strategy.id, db)
            run.status     = "scanning"
            run.started_at = datetime.now(timezone.utc)
            db.commit()
            runs[profile.id] = (run, strategy)

        # Load all universes — scan union of all to minimise API calls
        client_universes = {}
        all_meta         = {}   # security_id → { symbol, series, lot_size }
        for profile, strategy in clients:
            universe = _load_universe(strategy, db)
            client_universes[profile.id] = universe
            for u in universe:
                all_meta[u["security_id"]] = u

        combined_universe = list(all_meta.values())
        print(f"[algo] Combined universe: {len(combined_universe)} unique stocks")

        # ── STEP 1: 8:45 AM — prev close snapshot ──────────────────────
        await _wait_until(8, 45, 0, "(prev close snapshot)")
        token, master_client_id = await get_master_token(db)
        prev_close_snap = await _fetch_prev_close_snapshot(
            token, master_client_id, combined_universe
        )
        for _, (run, _) in runs.items():
            _log(run, f"8:45 AM snapshot: {len(prev_close_snap)} stocks captured", db)

        # ── STEP 2: 9:12:30 — opening price scan ───────────────────────
        await _wait_until(9, 12, 30, "(opening price scan)")
        token, master_client_id = await get_master_token(db)
        opening_prices = await _fetch_opening_prices(
            token, master_client_id, combined_universe
        )

        # Compute gaps for full universe
        all_gaps = _compute_gaps(prev_close_snap, opening_prices, combined_universe)
        print(f"[algo] Gap computed for {len(all_gaps)} stocks")

        # ── STEP 3: Apply per-client filters → select top N ────────────
        for profile, strategy in clients:
            run, strat = runs[profile.id]

            # Filter gap_results to this client's universe only
            client_sids = {u["security_id"] for u in client_universes[profile.id]}
            client_gaps = {sid: d for sid, d in all_gaps.items() if sid in client_sids}

            run.stocks_scanned = len(client_sids)
            selected = _apply_filters(client_gaps, strat, run, db)
            run.stocks_selected = len(selected)
            db.commit()

            for item in selected:
                exists = db.query(AlgoStock).filter(
                    AlgoStock.run_id      == run.id,
                    AlgoStock.security_id == item["security_id"],
                ).first()
                if not exists:
                    db.add(AlgoStock(
                        run_id            = run.id,
                        client_profile_id = profile.id,
                        symbol            = item["symbol"],
                        security_id       = item["security_id"],
                        gap_pct           = item["gap_pct"],
                        direction         = "UP" if item["gap_pct"] > 0 else "DOWN",
                        prev_close        = item["prev_close"],
                        status            = "watching",
                        source            = "preopen",
                    ))
            db.commit()

        # ── STEP 4: 9:16:05 — first 1-min candle (candle closes at 9:16:00) ──
        await _wait_until(9, 16, 5, "(first candle — 5s after 9:16:00 close)")
        token, master_client_id = await get_master_token(db)

        for profile, strategy in clients:
            run, strat = runs[profile.id]
            run.status = "running"
            db.commit()

            stocks = db.query(AlgoStock).filter(
                AlgoStock.run_id == run.id,
                AlgoStock.status == "watching",
            ).all()

            for stock in stocks:
                entry = await _compute_entry(
                    stock.security_id, strat, token, master_client_id
                )
                if entry:
                    stock.candle_high   = entry["candle_high"]
                    stock.candle_low    = entry["candle_low"]
                    stock.candle_close  = entry["candle_close"]
                    stock.buy_trigger   = entry["buy_trigger"]
                    stock.sell_trigger  = entry["sell_trigger"]
                    stock.quantity      = entry["quantity"]
                    _log(run,
                        f"{stock.symbol}: candle close ₹{entry['candle_close']:.2f} | "
                        f"BUY trigger ₹{entry['buy_trigger']:.2f} | "
                        f"SELL trigger ₹{entry['sell_trigger']:.2f} | "
                        f"Qty {entry['quantity']}",
                        db)
                else:
                    stock.status = "cancelled"
                    _log(run, f"{stock.symbol}: No candle data yet — skipped", db)
                db.commit()
                await asyncio.sleep(0.3)

        # ── STEP 5: Monitor loop until 3:20 PM ─────────────────────────
        eod_ist = datetime.now(IST).replace(hour=15, minute=20, second=0, microsecond=0)
        print("[algo] Monitor loop started (every 5s)")

        while datetime.now(IST) < eod_ist:
            await asyncio.sleep(5)
            try:
                token, master_client_id = await get_master_token(db)
                await _monitor_tick(db, token, master_client_id)
            except Exception as e:
                print(f"[algo] Monitor tick error: {e}")

            # Stop early if all positions closed
            open_count = db.query(AlgoStock).filter(
                AlgoStock.status.in_(["watching", "entered"])
            ).count()
            if open_count == 0:
                print("[algo] All positions closed — stopping monitor loop")
                break

        # ── STEP 6: 3:20 PM EOD force exit ─────────────────────────────
        token, master_client_id = await get_master_token(db)
        await _force_exit_all(db, token, master_client_id)

        # Mark all runs done
        for profile, _ in clients:
            run, _ = runs[profile.id]
            run.status      = "done"
            run.finished_at = datetime.now(timezone.utc)
            _log(run, f"Run complete. Total P&L: ₹{float(run.total_pnl or 0):.2f}", db)

        print("[algo] Daily run complete")

    except Exception as e:
        print(f"[algo] Fatal error: {e}")
        import traceback; traceback.print_exc()
    finally:
        db.close()
