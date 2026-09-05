"""
app/services/algo_engine.py — WOI Paper Trade Engine

Full WOI gap strategy — paper mode for all subscribed clients.
Master Dhan account → ALL market data (OHLC, candles, LTP).
Client accounts → order placement (paper mode = no real orders yet).

Daily schedule (IST weekdays):
  09:00 → pre-open gap scan
  09:15 → first 1-min candle, compute entry triggers
  09:15+ → monitor loop every 5s (trail SL, exit at target)
  15:20 → force-exit all open positions
"""

import asyncio, json
from datetime import datetime, date, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.trading import ClientProfile, AlgoStrategy, AlgoRun, AlgoStock, DailyPnl
from app.services.market_data import (
    get_master_token, fetch_ohlc_batch, fetch_ltp, fetch_first_candle,
)

IST = timezone(timedelta(hours=5, minutes=30))

# Nifty 500 starter list — security IDs (NSE_EQ)
UNIVERSE = [
    "1333","11536","10895","15083","4963","3456","14977","1232","5258","11630",
    "2475","16675","7229","1526","13611","10940","467","11184","6705","3787",
    "6897","21808","25","1394","11703","2029","5900","9500","9819","6066",
    "10999","3312","2303","1660","14413",
]


def _log(run: AlgoRun, msg: str, db: Session):
    ts      = datetime.now(IST).strftime("%H:%M:%S")
    run.log = (run.log or "") + f"[{ts}] {msg}\n"
    db.commit()
    print(f"[algo] {msg}")


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
        AlgoRun.run_date == today,
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


# ── Gap scanner ───────────────────────────────────────────────────────────────

async def _preopen_scan(token: str, client_id: str) -> dict:
    """Fetch OHLC for universe, compute gap%. Returns { sid: { symbol, gap_pct, prev_close, open } }"""
    print("[algo] Pre-open gap scan starting...")
    all_quotes = {}
    for i in range(0, len(UNIVERSE), 100):
        batch  = UNIVERSE[i:i+100]
        quotes = await fetch_ohlc_batch(token, client_id, batch)
        all_quotes.update(quotes)
        await asyncio.sleep(1.1)   # 1 req/sec rate limit

    results = {}
    for sid, q in all_quotes.items():
        prev = q.get("prev_close", 0)
        open_ = q.get("open", 0)
        if prev <= 0 or open_ <= 0:
            continue
        gap_pct = ((open_ - prev) / prev) * 100
        results[sid] = {
            "gap_pct":    round(gap_pct, 2),
            "prev_close": prev,
            "open":       open_,
        }

    print(f"[algo] Gap scan: {len(all_quotes)} fetched, {len(results)} with valid prices")
    return results


def _filter_stocks(gap_results: dict, gap_min: float, gap_max: float, max_n: int) -> list[dict]:
    filtered = [
        {"security_id": sid, **d}
        for sid, d in gap_results.items()
        if gap_min <= abs(d["gap_pct"]) <= gap_max
    ]
    filtered.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)
    return filtered[:max_n]


# ── Entry trigger computation ─────────────────────────────────────────────────

async def _compute_entry(
    security_id: str, strategy: AlgoStrategy,
    token: str, client_id: str,
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


# ── Trail SL logic ────────────────────────────────────────────────────────────

def _check_exit(
    stock: AlgoStock,
    ltp: float,
    strategy: AlgoStrategy,
    trail_steps: list,
) -> tuple[bool, str]:
    """Returns (should_exit, reason). reason: 'TARGET' | 'SL' | ''"""
    entry  = float(stock.entry_price or 0)
    sl_pct = float(strategy.sl_pct)
    target = float(strategy.target_r)
    one_r  = entry * sl_pct
    if one_r <= 0 or entry <= 0:
        return False, ""

    direction = stock.entry_direction
    pnl_r = (
        (ltp - entry) / one_r if direction == "BUY"
        else (entry - ltp)   / one_r
    )

    # Target hit
    if pnl_r >= target:
        return True, "TARGET"

    # Find locked SL from trail steps
    locked_r = -1.0
    for step in sorted(trail_steps, key=lambda s: s[0], reverse=True):
        if pnl_r >= step[0]:
            locked_r = step[1]
            break

    sl_price = (
        entry + (locked_r * one_r) if direction == "BUY"
        else entry - (locked_r * one_r)
    )

    if direction == "BUY"  and ltp <= sl_price:
        return True, "SL"
    if direction == "SELL" and ltp >= sl_price:
        return True, "SL"

    return False, ""


def _calc_pnl(stock: AlgoStock, exit_price: float) -> float:
    qty   = stock.quantity or 1
    entry = float(stock.entry_price or 0)
    if stock.entry_direction == "BUY":
        return round((exit_price - entry) * qty, 2)
    return round((entry - exit_price) * qty, 2)


def _update_daily_pnl(profile_id: str, pnl: float, db: Session):
    today = date.today()
    row   = db.query(DailyPnl).filter(
        DailyPnl.client_profile_id == profile_id,
        DailyPnl.date == today,
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


# ── Monitor loop ──────────────────────────────────────────────────────────────

async def _monitor_tick(db: Session, token: str, client_id: str):
    """One monitoring tick — fetch LTP for all open stocks, check triggers/exits."""
    today  = date.today()
    stocks = db.query(AlgoStock).filter(
        AlgoStock.status.in_(["watching", "entered"]),
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

            if not strategy.gap_direction_bias:
                # OCO — either side
                if buy_t  and ltp >= buy_t:
                    stock.status = "entered"; stock.entry_direction = "BUY";  stock.entry_price = ltp
                    run.stocks_traded = (run.stocks_traded or 0) + 1
                    _log(run, f"PAPER BUY  {stock.symbol} @ ₹{ltp:.2f} (trigger ₹{buy_t:.2f})", db)
                elif sell_t and ltp <= sell_t:
                    stock.status = "entered"; stock.entry_direction = "SELL"; stock.entry_price = ltp
                    run.stocks_traded = (run.stocks_traded or 0) + 1
                    _log(run, f"PAPER SELL {stock.symbol} @ ₹{ltp:.2f} (trigger ₹{sell_t:.2f})", db)
            else:
                # Direction bias — only enter in gap direction
                if stock.direction == "UP"   and buy_t  and ltp >= buy_t:
                    stock.status = "entered"; stock.entry_direction = "BUY";  stock.entry_price = ltp
                    run.stocks_traded = (run.stocks_traded or 0) + 1
                    _log(run, f"PAPER BUY  {stock.symbol} @ ₹{ltp:.2f}", db)
                elif stock.direction == "DOWN" and sell_t and ltp <= sell_t:
                    stock.status = "entered"; stock.entry_direction = "SELL"; stock.entry_price = ltp
                    run.stocks_traded = (run.stocks_traded or 0) + 1
                    _log(run, f"PAPER SELL {stock.symbol} @ ₹{ltp:.2f}", db)

            db.commit()
            continue

        # Entered → check SL/target
        if stock.status == "entered" and stock.entry_price and stock.entry_direction:
            should_exit, reason = _check_exit(stock, ltp, strategy, trail_steps)
            if should_exit:
                pnl = _calc_pnl(stock, ltp)
                stock.status     = "exited"
                stock.exit_price = ltp
                stock.pnl        = pnl
                run.total_pnl    = float(run.total_pnl or 0) + pnl
                _log(run, f"EXIT {stock.symbol} @ ₹{ltp:.2f} [{reason}] P&L: {pnl:+.2f}", db)
                _update_daily_pnl(stock.client_profile_id, pnl, db)

        db.commit()


# ── EOD force exit ────────────────────────────────────────────────────────────

async def _force_exit_all(db: Session, token: str, client_id: str):
    print("[algo] 3:20 PM — force-exiting all open paper positions")
    stocks  = db.query(AlgoStock).filter(AlgoStock.status == "entered").all()
    if not stocks:
        return

    sec_ids = list({s.security_id for s in stocks})
    ltp_map = await fetch_ltp(token, client_id, sec_ids)

    for stock in stocks:
        ltp = ltp_map.get(str(stock.security_id), float(stock.entry_price or 0))
        pnl = _calc_pnl(stock, ltp)
        stock.status     = "exited"
        stock.exit_price = ltp
        stock.pnl        = pnl
        run = db.query(AlgoRun).filter(AlgoRun.id == stock.run_id).first()
        if run:
            run.total_pnl = float(run.total_pnl or 0) + pnl
            _log(run, f"FORCE EXIT {stock.symbol} @ ₹{ltp:.2f} [EOD] P&L: {pnl:+.2f}", db)
        _update_daily_pnl(stock.client_profile_id, pnl, db)

    db.commit()
    print(f"[algo] Force-exited {len(stocks)} positions")


# ── Main daily run ────────────────────────────────────────────────────────────

async def run_daily_algo():
    """Full daily algo cycle. Called by scheduler at 9:00 AM IST on weekdays."""
    db = SessionLocal()
    try:
        clients = _subscribed_clients(db)
        if not clients:
            print("[algo] No subscribed clients — skipping")
            return

        print(f"[algo] Starting daily run for {len(clients)} client(s)")

        # Get master data token
        token, master_client_id = await get_master_token(db)

        # Create today's runs
        runs = {}
        for profile, strategy in clients:
            run = _get_or_create_run(profile.id, strategy.id, db)
            run.status     = "scanning"
            run.started_at = datetime.now(timezone.utc)
            db.commit()
            runs[profile.id] = (run, strategy)

        # ── Phase 1: Pre-open scan ──────────────────────────────────────────
        gap_results = await _preopen_scan(token, master_client_id)

        for profile, strategy in clients:
            run, strat = runs[profile.id]
            run.stocks_scanned = len(gap_results)
            selected = _filter_stocks(
                gap_results,
                float(strat.gap_min), float(strat.gap_max),
                strat.max_stocks_per_day,
            )
            run.stocks_selected = len(selected)
            _log(run, f"Scan done: {len(gap_results)} stocks, {len(selected)} selected", db)

            for item in selected:
                exists = db.query(AlgoStock).filter(
                    AlgoStock.run_id     == run.id,
                    AlgoStock.security_id == item["security_id"],
                ).first()
                if not exists:
                    db.add(AlgoStock(
                        run_id            = run.id,
                        client_profile_id = profile.id,
                        symbol            = item.get("security_id", ""),  # symbol populated after candle
                        security_id       = item["security_id"],
                        gap_pct           = item["gap_pct"],
                        direction         = "UP" if item["gap_pct"] > 0 else "DOWN",
                        prev_close        = item["prev_close"],
                        status            = "watching",
                        source            = "preopen",
                    ))
            db.commit()

        # ── Phase 2: Wait for 9:15 AM ──────────────────────────────────────
        now_ist  = datetime.now(IST)
        open_ist = now_ist.replace(hour=9, minute=15, second=30, microsecond=0)
        wait_sec = (open_ist - now_ist).total_seconds()
        if 0 < wait_sec < 900:
            print(f"[algo] Waiting {wait_sec:.0f}s for market open (9:15 AM)...")
            await asyncio.sleep(wait_sec)

        # ── Phase 3: First candle entry ────────────────────────────────────
        token, master_client_id = await get_master_token(db)  # refresh if needed

        for profile, strategy in clients:
            run, strat = runs[profile.id]
            run.status = "running"
            db.commit()

            stocks = db.query(AlgoStock).filter(
                AlgoStock.run_id == run.id,
                AlgoStock.status == "watching",
            ).all()

            for stock in stocks:
                entry = await _compute_entry(stock.security_id, strat, token, master_client_id)
                if entry:
                    stock.candle_high   = entry["candle_high"]
                    stock.candle_low    = entry["candle_low"]
                    stock.candle_close  = entry["candle_close"]
                    stock.buy_trigger   = entry["buy_trigger"]
                    stock.sell_trigger  = entry["sell_trigger"]
                    stock.quantity      = entry["quantity"]
                    _log(run,
                        f"{stock.security_id}: BUY ₹{entry['buy_trigger']:.2f} | "
                        f"SELL ₹{entry['sell_trigger']:.2f} | Qty {entry['quantity']}", db)
                else:
                    stock.status = "cancelled"
                    _log(run, f"{stock.security_id}: No candle data — skipped", db)
                db.commit()
                await asyncio.sleep(0.5)

        # ── Phase 4: Monitor until 3:20 PM ─────────────────────────────────
        eod_ist = datetime.now(IST).replace(hour=15, minute=20, second=0, microsecond=0)
        print("[algo] Monitor loop started")

        while datetime.now(IST) < eod_ist:
            await asyncio.sleep(5)
            try:
                token, master_client_id = await get_master_token(db)
                await _monitor_tick(db, token, master_client_id)
            except Exception as e:
                print(f"[algo] Monitor tick error: {e}")

            open_count = db.query(AlgoStock).filter(
                AlgoStock.status.in_(["watching", "entered"])
            ).count()
            if open_count == 0:
                print("[algo] All positions closed — stopping monitor")
                break

        # ── Phase 5: EOD force exit ────────────────────────────────────────
        token, master_client_id = await get_master_token(db)
        await _force_exit_all(db, token, master_client_id)

        for profile, _ in clients:
            run, _ = runs[profile.id]
            run.status      = "done"
            run.finished_at = datetime.now(timezone.utc)
            _log(run, f"Run complete. Total P&L: ₹{run.total_pnl:.2f}", db)

        print("[algo] Daily run complete")

    except Exception as e:
        print(f"[algo] Fatal error: {e}")
        import traceback; traceback.print_exc()
    finally:
        db.close()
