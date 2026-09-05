"""
app/services/algo_engine.py

WOI Paper Trade Engine
======================

Runs the full WOI gap strategy in paper mode for all subscribed clients.
Uses master Dhan account for ALL market data (quotes, candles).
In paper mode: no real orders placed — fills simulated from live LTP.

Daily schedule (IST):
  09:00 → start_preopen_scan()   — fetch OHLC, find gap stocks
  09:15 → start_candle_entry()   — first 1-min candle, compute triggers
  09:16 → start_monitor_loop()   — every 5s: check LTP, trail SL, exit
  15:20 → force_exit_all()       — close all open paper positions

All state kept in DB (AlgoRun, AlgoStock).
"""

import asyncio
import json
import math
from datetime import datetime, date, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.core.encryption import decrypt
from app.models.trading import (
    ClientProfile, AlgoStrategy, AlgoRun, AlgoStock, DailyPnl,
)
from app.services.market_data import (
    get_master_token, fetch_ohlc_batch, fetch_ltp, fetch_first_candle,
)

IST = timezone(timedelta(hours=5, minutes=30))

# Nifty 500 security IDs — a starter list of liquid NSE_EQ stocks
# In production this comes from the Nifty 500 CSV or auto-ranked by turnover
NIFTY500_SECURITY_IDS = [
    "1333",   # HDFC Bank
    "11536",  # Reliance
    "10895",  # PNB
    "15083",  # SBI
    "4963",   # Infosys
    "3456",   # TCS
    "14977",  # ICICI Bank
    "1232",   # Bajaj Finance
    "5258",   # Kotak Bank
    "11630",  # ITC
    "2475",   # L&T
    "16675",  # HUL
    "7229",   # Asian Paints
    "1526",   # Bajaj Auto
    "13611",  # Maruti
    "10940",  # Tata Motors
    "467",    # Axis Bank
    "11184",  # Wipro
    "6705",   # HCL Tech
    "3787",   # Sun Pharma
    "6897",   # Tech Mahindra
    "21808",  # Adani Ports
    "25",     # Adani Enterprises
    "1394",   # Bharti Airtel
    "11703",  # ONGC
    "2029",   # Coal India
    "5900",   # M&M
    "9500",   # Power Grid
    "9819",   # NTPC
    "6066",   # ATGL
    "10999",  # Tata Steel
    "3312",   # JSW Steel
    "2303",   # Hindalco
    "1660",   # Hero MotoCorp
    "14413",  # Eicher Motors
]


# ── Logging helper ────────────────────────────────────────────────────────────

def _log(run: AlgoRun, msg: str, db: Session):
    ts  = datetime.now(IST).strftime("%H:%M:%S")
    run.log = (run.log or "") + f"[{ts}] {msg}\n"
    db.commit()
    print(f"[algo_engine] {msg}")


# ── Get subscribed clients ────────────────────────────────────────────────────

def _get_subscribed_clients(db: Session) -> list[tuple[ClientProfile, AlgoStrategy]]:
    """Return all (profile, strategy) pairs where strategy is active."""
    strategies = (
        db.query(AlgoStrategy)
        .filter(AlgoStrategy.is_active == True)
        .all()
    )
    result = []
    for s in strategies:
        profile = db.query(ClientProfile).filter(
            ClientProfile.id == s.client_profile_id
        ).first()
        if profile:
            result.append((profile, s))
    return result


# ── Create / get today's run ──────────────────────────────────────────────────

def _get_or_create_run(profile_id: str, strategy_id: str, db: Session) -> AlgoRun:
    today = date.today()
    run = (
        db.query(AlgoRun)
        .filter(AlgoRun.client_profile_id == profile_id, AlgoRun.run_date == today)
        .first()
    )
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

async def run_preopen_scan(db: Session) -> dict:
    """
    9:00–9:14 AM: Fetch OHLC for universe, calculate gap%, rank by abs gap.
    Returns: { security_id: { symbol, gap_pct, prev_close, open } }
    """
    print("[algo_engine] Starting pre-open gap scan...")
    token    = await get_master_token(db)
    sec_ids  = NIFTY500_SECURITY_IDS

    # Batch in chunks of 100
    all_quotes = {}
    for i in range(0, len(sec_ids), 100):
        batch  = sec_ids[i:i+100]
        quotes = await fetch_ohlc_batch(token, batch)
        all_quotes.update(quotes)
        await asyncio.sleep(0.3)

    print(f"[algo_engine] Got quotes for {len(all_quotes)} securities")

    gap_results = {}
    for sid, q in all_quotes.items():
        prev_close = float(q.get("prev_close") or 0)
        open_price = float(q.get("open") or 0)
        if prev_close <= 0 or open_price <= 0:
            continue
        gap_pct = ((open_price - prev_close) / prev_close) * 100
        gap_results[sid] = {
            "symbol":     q.get("symbol", ""),
            "gap_pct":    round(gap_pct, 2),
            "prev_close": prev_close,
            "open":       open_price,
        }

    return gap_results


def _filter_gap_stocks(
    gap_results: dict,
    gap_min: float,
    gap_max: float,
    max_stocks: int,
) -> list[dict]:
    """Filter by gap band, sort by abs gap, take top N."""
    filtered = [
        {"security_id": sid, **data}
        for sid, data in gap_results.items()
        if gap_min <= abs(data["gap_pct"]) <= gap_max
    ]
    filtered.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)
    return filtered[:max_stocks]


# ── First candle entry ────────────────────────────────────────────────────────

async def compute_entry(
    security_id: str,
    strategy: AlgoStrategy,
    token: str,
) -> Optional[dict]:
    """
    Fetch first 1-min candle (9:15–9:16), compute BUY/SELL triggers.
    Returns: { candle_open, high, low, close, buy_trigger, sell_trigger, quantity }
    """
    candle = await fetch_first_candle(token, security_id)
    if not candle:
        return None

    ref   = float(strategy.entry_buffer_pct)
    sl    = float(strategy.sl_pct)
    risk  = float(strategy.risk_per_trade)

    if strategy.entry_reference == "close":
        base_buy  = candle["close"]
        base_sell = candle["close"]
    else:
        base_buy  = candle["high"]
        base_sell = candle["low"]

    buy_trigger  = round(base_buy  * (1 + ref), 2)
    sell_trigger = round(base_sell * (1 - ref), 2)

    # Quantity = risk ÷ (trigger × sl_pct)
    risk_per_share = buy_trigger * sl
    quantity = max(1, int(risk / risk_per_share)) if risk_per_share > 0 else 1

    return {
        "candle_open":   candle["open"],
        "candle_high":   candle["high"],
        "candle_low":    candle["low"],
        "candle_close":  candle["close"],
        "buy_trigger":   buy_trigger,
        "sell_trigger":  sell_trigger,
        "quantity":      quantity,
    }


# ── Paper trade monitoring ────────────────────────────────────────────────────

def _trail_sl(
    entry_price: float,
    current_price: float,
    direction: str,       # BUY or SELL
    sl_pct: float,
    trail_steps: list,
    target_r: float,
) -> tuple[bool, bool, float]:
    """
    Returns: (should_exit_target, should_exit_sl, current_sl_price)

    trail_steps: [[r_trigger, lock_r], ...]
    """
    one_r = entry_price * sl_pct

    if direction == "BUY":
        pnl_r = (current_price - entry_price) / one_r if one_r > 0 else 0
    else:
        pnl_r = (entry_price - current_price) / one_r if one_r > 0 else 0

    # Target hit
    if pnl_r >= target_r:
        return True, False, 0.0

    # Determine SL level based on trail steps
    locked_r = -1.0  # Start with full SL (-1R)
    for step in sorted(trail_steps, key=lambda s: s[0], reverse=True):
        if pnl_r >= step[0]:
            locked_r = step[1]
            break

    # Calculate SL price
    sl_price = (
        entry_price + (locked_r * one_r) if direction == "BUY"
        else entry_price - (locked_r * one_r)
    )

    # SL hit
    if direction == "BUY"  and current_price <= sl_price:
        return False, True, sl_price
    if direction == "SELL" and current_price >= sl_price:
        return False, True, sl_price

    return False, False, sl_price


async def monitor_paper_positions(db: Session):
    """
    Every 5 seconds: check LTP for all watching/entered paper stocks.
    Simulates fills and trails SL.
    """
    token = await get_master_token(db)
    today = date.today()

    # Get all active stocks across all clients
    stocks = (
        db.query(AlgoStock)
        .filter(
            AlgoStock.status.in_(["watching", "entered"]),
        )
        .all()
    )

    if not stocks:
        return

    # Group by security_id for batch LTP fetch
    sec_ids    = list({s.security_id for s in stocks})
    ltp_map    = await fetch_ltp(token, sec_ids)

    for stock in stocks:
        ltp = ltp_map.get(stock.security_id)
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

        # Parse trail steps
        try:
            trail_steps = json.loads(strategy.trail_sl_steps or "[]")
        except Exception:
            trail_steps = []

        # ── Watching → check if trigger hit ──────────────────────────────────
        if stock.status == "watching":
            buy_t  = float(stock.buy_trigger  or 0)
            sell_t = float(stock.sell_trigger or 0)

            if buy_t and ltp >= buy_t:
                # BUY triggered
                stock.status         = "entered"
                stock.entry_direction= "BUY"
                stock.entry_price    = ltp
                run.stocks_traded    = (run.stocks_traded or 0) + 1
                _log(run, f"PAPER BUY {stock.symbol} @ ₹{ltp:.2f} (trigger ₹{buy_t:.2f})", db)

            elif sell_t and ltp <= sell_t:
                # SELL triggered
                stock.status         = "entered"
                stock.entry_direction= "SELL"
                stock.entry_price    = ltp
                run.stocks_traded    = (run.stocks_traded or 0) + 1
                _log(run, f"PAPER SELL {stock.symbol} @ ₹{ltp:.2f} (trigger ₹{sell_t:.2f})", db)

            db.commit()
            continue

        # ── Entered → check SL / target ──────────────────────────────────────
        if stock.status == "entered" and stock.entry_price and stock.entry_direction:
            hit_target, hit_sl, sl_price = _trail_sl(
                entry_price   = float(stock.entry_price),
                current_price = ltp,
                direction     = stock.entry_direction,
                sl_pct        = float(strategy.sl_pct),
                trail_steps   = trail_steps,
                target_r      = float(strategy.target_r),
            )

            if hit_target or hit_sl:
                reason = "TARGET" if hit_target else "SL"
                pnl    = _calc_pnl(stock, ltp)
                stock.status     = "exited"
                stock.exit_price = ltp
                stock.pnl        = pnl
                run.total_pnl    = float(run.total_pnl or 0) + pnl
                _log(run, f"PAPER EXIT {stock.symbol} @ ₹{ltp:.2f} [{reason}] P&L: {pnl:+.2f}", db)

                # Update daily P&L
                _update_daily_pnl(stock.client_profile_id, pnl, db)

        db.commit()


def _calc_pnl(stock: AlgoStock, exit_price: float) -> float:
    qty   = stock.quantity or 1
    entry = float(stock.entry_price or 0)
    if stock.entry_direction == "BUY":
        return round((exit_price - entry) * qty, 2)
    else:
        return round((entry - exit_price) * qty, 2)


def _update_daily_pnl(profile_id: str, pnl: float, db: Session):
    today = date.today()
    row   = db.query(DailyPnl).filter(
        DailyPnl.client_profile_id == profile_id,
        DailyPnl.date == today,
    ).first()
    if row:
        row.closed_pnl  = float(row.closed_pnl or 0) + pnl
        row.total_pnl   = float(row.total_pnl or 0) + pnl
        row.trade_count = (row.trade_count or 0) + 1
    else:
        db.add(DailyPnl(
            client_profile_id=profile_id,
            date=today,
            closed_pnl=pnl,
            running_pnl=0,
            total_pnl=pnl,
            trade_count=1,
        ))
    db.commit()


# ── Force exit all at 3:20 PM ─────────────────────────────────────────────────

async def force_exit_all(db: Session):
    """Close all open paper positions at 3:20 PM IST using last LTP."""
    print("[algo_engine] 3:20 PM — force-exiting all paper positions")
    token  = await get_master_token(db)
    stocks = db.query(AlgoStock).filter(AlgoStock.status == "entered").all()
    if not stocks:
        return

    sec_ids = list({s.security_id for s in stocks})
    ltp_map = await fetch_ltp(token, sec_ids)

    for stock in stocks:
        ltp = ltp_map.get(stock.security_id, float(stock.entry_price or 0))
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
    print(f"[algo_engine] Force-exited {len(stocks)} positions")


# ── Main daily run ────────────────────────────────────────────────────────────

async def run_daily_algo():
    """
    Full daily algo cycle for all subscribed clients.
    Called by scheduler at 9:00 AM IST.
    """
    db = SessionLocal()
    try:
        clients = _get_subscribed_clients(db)
        if not clients:
            print("[algo_engine] No subscribed clients — skipping")
            return

        print(f"[algo_engine] Starting daily run for {len(clients)} client(s)")

        # Create today's run for each client
        runs = {}
        for profile, strategy in clients:
            run = _get_or_create_run(profile.id, strategy.id, db)
            run.status    = "scanning"
            run.started_at = datetime.now(timezone.utc)
            db.commit()
            runs[profile.id] = (run, strategy)

        # ── Phase 1: Pre-open gap scan (9:00–9:14) ─────────────────────────
        gap_results = await run_preopen_scan(db)

        for profile, strategy in clients:
            run, strat = runs[profile.id]
            run.stocks_scanned = len(gap_results)

            selected = _filter_gap_stocks(
                gap_results,
                float(strat.gap_min),
                float(strat.gap_max),
                strat.max_stocks_per_day,
            )

            run.stocks_selected = len(selected)
            _log(run, f"Pre-open scan complete: {len(gap_results)} scanned, {len(selected)} selected", db)

            for item in selected:
                existing = db.query(AlgoStock).filter(
                    AlgoStock.run_id    == run.id,
                    AlgoStock.security_id == item["security_id"],
                ).first()
                if not existing:
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

        # ── Phase 2: Wait for market open (9:15 AM) ────────────────────────
        # In production, scheduler calls this at exactly 9:15. Here we add a
        # small sleep to simulate the wait if called early.
        now_ist = datetime.now(IST)
        open_ist = now_ist.replace(hour=9, minute=15, second=30, microsecond=0)
        wait_sec = (open_ist - now_ist).total_seconds()
        if 0 < wait_sec < 600:
            print(f"[algo_engine] Waiting {wait_sec:.0f}s for market open...")
            await asyncio.sleep(wait_sec)

        # ── Phase 3: First candle entry (9:15–9:16) ─────────────────────────
        token = await get_master_token(db)
        for profile, strategy in clients:
            run, strat = runs[profile.id]
            run.status = "running"
            db.commit()

            stocks = db.query(AlgoStock).filter(
                AlgoStock.run_id == run.id,
                AlgoStock.status == "watching",
            ).all()

            for stock in stocks:
                entry = await compute_entry(stock.security_id, strat, token)
                if entry:
                    stock.candle_high   = entry["candle_high"]
                    stock.candle_low    = entry["candle_low"]
                    stock.candle_close  = entry["candle_close"]
                    stock.buy_trigger   = entry["buy_trigger"]
                    stock.sell_trigger  = entry["sell_trigger"]
                    stock.quantity      = entry["quantity"]
                    _log(run,
                        f"{stock.symbol}: BUY trigger ₹{entry['buy_trigger']:.2f} | "
                        f"SELL trigger ₹{entry['sell_trigger']:.2f} | Qty {entry['quantity']}",
                        db)
                else:
                    stock.status = "cancelled"
                    _log(run, f"{stock.symbol}: No candle data — cancelled", db)
                db.commit()
                await asyncio.sleep(0.2)

        # ── Phase 4: Monitor loop until 3:20 PM ────────────────────────────
        eod_ist = now_ist.replace(hour=15, minute=20, second=0, microsecond=0)
        print("[algo_engine] Entering monitor loop...")

        while datetime.now(IST) < eod_ist:
            await asyncio.sleep(5)
            await monitor_paper_positions(db)

            # Check if all positions are closed
            open_stocks = db.query(AlgoStock).filter(
                AlgoStock.status.in_(["watching", "entered"])
            ).count()
            if open_stocks == 0:
                print("[algo_engine] All positions closed — stopping monitor")
                break

        # ── Phase 5: EOD force exit ─────────────────────────────────────────
        await force_exit_all(db)

        # Mark all runs as done
        for profile, _ in clients:
            run, _ = runs[profile.id]
            run.status      = "done"
            run.finished_at = datetime.now(timezone.utc)
            _log(run, f"Run complete. Total P&L: ₹{run.total_pnl:.2f}", db)

        print("[algo_engine] Daily run complete")

    except Exception as e:
        print(f"[algo_engine] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()
