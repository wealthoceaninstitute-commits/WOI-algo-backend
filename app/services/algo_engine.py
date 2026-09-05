"""
app/services/algo_engine.py — WOI Paper Trade Engine

Stock selection sequence:
  1. Load universe from DB (strategy.universe_id → UniverseStock table)
  2. Pre-open OHLC scan via master Dhan account
  3. Apply filters:
       - Gap %        : gap_min ≤ |gap%| ≤ gap_max
       - Price        : min_price ≤ prev_close ≤ max_price
       - Volume       : prev_day_volume ≥ min_volume
       - Turnover     : prev_close × volume ≥ min_turnover_cr × 1cr
       - BE exclusion : exclude_be_series = True → skip BE series
  4. Sort by absolute gap % descending
  5. Take top max_stocks_per_day

Entry (9:15 AM):
  - First 1-min candle → compute BUY/SELL triggers
  - Paper trade: simulate fills from live LTP

Monitor (every 5s):
  - Trail SL per ladder
  - Exit at target_r or SL

EOD (3:20 PM):
  - Force-exit all open paper positions
"""

import asyncio, json
from datetime import datetime, date, timezone, timedelta
from typing import Optional
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.trading import ClientProfile, AlgoStrategy, AlgoRun, AlgoStock, DailyPnl
from app.models.scrip_master import UniverseStock, ScripMaster
from app.services.market_data import (
    get_master_token, fetch_ohlc_batch, fetch_ltp, fetch_first_candle,
)

IST = timezone(timedelta(hours=5, minutes=30))

# Fallback universe — used only if no universe_id set on strategy
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
    Load stock universe from DB for this strategy.
    Returns list of { security_id, symbol, series, lot_size }
    Falls back to hardcoded list if no universe configured.
    """
    if not strategy.universe_id:
        # No universe set — use fallback hardcoded list
        print(f"[algo] No universe set for strategy {strategy.id} — using fallback list")
        rows = db.query(ScripMaster).filter(
            ScripMaster.security_id.in_(FALLBACK_UNIVERSE)
        ).all()
        return [{"security_id": r.security_id, "symbol": r.symbol,
                 "series": r.series, "lot_size": r.lot_size} for r in rows]

    # Load from universe_stocks table
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
        print(f"[algo] Universe {strategy.universe_id} is empty — using fallback")
        return [{"security_id": sid, "symbol": sid, "series": "EQ", "lot_size": 1}
                for sid in FALLBACK_UNIVERSE]

    # Enrich with scrip_master data (series, lot_size)
    result = []
    for s in stocks:
        scrip = db.query(ScripMaster).filter(
            ScripMaster.security_id == s.security_id
        ).first()
        result.append({
            "security_id": s.security_id,
            "symbol":      s.symbol,
            "series":      scrip.series if scrip else "EQ",
            "lot_size":    scrip.lot_size if scrip else 1,
        })

    print(f"[algo] Universe loaded: {len(result)} active stocks")
    return result


# ── Pre-open gap scan ─────────────────────────────────────────────────────────

async def _preopen_scan(
    token: str,
    client_id: str,
    universe: list[dict],
) -> dict:
    """
    Fetch OHLC for all universe stocks in batches of 100.
    Returns { security_id: { symbol, gap_pct, prev_close, open, volume, series } }
    Rate limit: 1 req/sec on Dhan marketfeed API.
    """
    sec_ids = [u["security_id"] for u in universe]
    meta    = {u["security_id"]: u for u in universe}

    print(f"[algo] Pre-open scan: {len(sec_ids)} stocks in {(len(sec_ids)+99)//100} batches")
    all_quotes = {}

    for i in range(0, len(sec_ids), 100):
        batch  = sec_ids[i:i+100]
        quotes = await fetch_ohlc_batch(token, client_id, batch)
        all_quotes.update(quotes)
        await asyncio.sleep(1.1)   # 1 req/sec rate limit

    results = {}
    for sid, q in all_quotes.items():
        prev  = float(q.get("prev_close") or 0)
        open_ = float(q.get("open")       or 0)
        vol   = int(q.get("volume")       or 0)
        if prev <= 0 or open_ <= 0:
            continue
        gap_pct = ((open_ - prev) / prev) * 100
        m       = meta.get(sid, {})
        results[sid] = {
            "symbol":     m.get("symbol", q.get("symbol", sid)),
            "series":     m.get("series", "EQ"),
            "lot_size":   m.get("lot_size", 1),
            "gap_pct":    round(gap_pct, 2),
            "prev_close": prev,
            "open":       open_,
            "volume":     vol,
            "turnover_cr": round((prev * vol) / 1e7, 2),  # ₹ crores
        }

    print(f"[algo] Got quotes for {len(results)} stocks")
    return results


# ── Stock filter & selection ──────────────────────────────────────────────────

def _apply_filters(
    gap_results: dict,
    strategy: AlgoStrategy,
    run: AlgoRun,
    db: Session,
) -> list[dict]:
    """
    Apply all configured filters to gap results.
    Returns filtered + sorted list, capped at max_stocks_per_day.

    Sequence:
      1. Gap % band filter
      2. Price filter (min/max prev close)
      3. Volume filter
      4. Turnover filter
      5. BE series exclusion
      6. Sort by |gap%| descending
      7. Take top N
    """
    gap_min    = float(strategy.gap_min)
    gap_max    = float(strategy.gap_max)
    # 0 = disabled for all filters except max_price (0 would block everything)
    min_price  = float(strategy.min_price or 0)
    max_price  = float(strategy.max_price or 0)   # 0 = no max limit
    min_vol    = int(strategy.min_volume  or 0)   # 0 = no volume filter
    min_turn   = float(strategy.min_turnover_cr or 0)  # 0 = no turnover filter
    excl_be    = bool(strategy.exclude_be_series)
    max_n      = int(strategy.max_stocks_per_day)

    passed = []
    rejected = {"gap": 0, "price": 0, "volume": 0, "turnover": 0, "be": 0}

    for sid, d in gap_results.items():
        gap  = abs(d["gap_pct"])
        prev = d["prev_close"]
        vol  = d["volume"]
        turn = d["turnover_cr"]
        ser  = d.get("series", "EQ")

        if not (gap_min <= gap <= gap_max):
            rejected["gap"] += 1; continue
        # Price filter — 0 = disabled
        if min_price > 0 and prev < min_price:
            rejected["price"] += 1; continue
        if max_price > 0 and prev > max_price:
            rejected["price"] += 1; continue
        # Volume filter — 0 = disabled
        if min_vol > 0 and vol < min_vol:
            rejected["volume"] += 1; continue
        # Turnover filter — 0 = disabled
        if min_turn > 0 and turn < min_turn:
            rejected["turnover"] += 1; continue
        if excl_be and ser == "BE":
            rejected["be"] += 1; continue

        passed.append({"security_id": sid, **d})

    _log(run,
        f"Filters: {len(gap_results)} total → "
        f"{rejected['gap']} gap · {rejected['price']} price · "
        f"{rejected['volume']} volume · {rejected['turnover']} turnover · "
        f"{rejected['be']} BE → {len(passed)} passed",
        db)

    # Sort by absolute gap % descending — biggest movers first
    passed.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)

    selected = passed[:max_n]
    _log(run, f"Selected top {len(selected)} of {len(passed)} qualifying stocks", db)
    return selected


# ── Entry trigger computation ─────────────────────────────────────────────────

async def _compute_entry(
    security_id: str,
    strategy: AlgoStrategy,
    token: str,
    client_id: str,
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
    entry  = float(stock.entry_price or 0)
    one_r  = entry * float(strategy.sl_pct)
    target = float(strategy.target_r)
    if one_r <= 0 or entry <= 0:
        return False, ""

    direction = stock.entry_direction
    pnl_r = (
        (ltp - entry) / one_r if direction == "BUY"
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
        entry + (locked_r * one_r) if direction == "BUY"
        else entry - (locked_r * one_r)
    )

    if direction == "BUY"  and ltp <= sl_price: return True, "SL"
    if direction == "SELL" and ltp >= sl_price: return True, "SL"
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

        if stock.status == "watching":
            buy_t  = float(stock.buy_trigger  or 0)
            sell_t = float(stock.sell_trigger or 0)

            if not strategy.gap_direction_bias:
                if buy_t  and ltp >= buy_t:
                    stock.status = "entered"; stock.entry_direction = "BUY"
                    stock.entry_price = ltp; run.stocks_traded = (run.stocks_traded or 0) + 1
                    _log(run, f"PAPER BUY  {stock.symbol} @ ₹{ltp:.2f} (trigger ₹{buy_t:.2f})", db)
                elif sell_t and ltp <= sell_t:
                    stock.status = "entered"; stock.entry_direction = "SELL"
                    stock.entry_price = ltp; run.stocks_traded = (run.stocks_traded or 0) + 1
                    _log(run, f"PAPER SELL {stock.symbol} @ ₹{ltp:.2f} (trigger ₹{sell_t:.2f})", db)
            else:
                if stock.direction == "UP"   and buy_t  and ltp >= buy_t:
                    stock.status = "entered"; stock.entry_direction = "BUY"
                    stock.entry_price = ltp; run.stocks_traded = (run.stocks_traded or 0) + 1
                    _log(run, f"PAPER BUY  {stock.symbol} @ ₹{ltp:.2f}", db)
                elif stock.direction == "DOWN" and sell_t and ltp <= sell_t:
                    stock.status = "entered"; stock.entry_direction = "SELL"
                    stock.entry_price = ltp; run.stocks_traded = (run.stocks_traded or 0) + 1
                    _log(run, f"PAPER SELL {stock.symbol} @ ₹{ltp:.2f}", db)

            db.commit()
            continue

        if stock.status == "entered" and stock.entry_price and stock.entry_direction:
            should_exit, reason = _check_exit(stock, ltp, strategy, trail_steps)
            if should_exit:
                pnl = _calc_pnl(stock, ltp)
                stock.status = "exited"; stock.exit_price = ltp; stock.pnl = pnl
                run.total_pnl = float(run.total_pnl or 0) + pnl
                _log(run, f"EXIT {stock.symbol} @ ₹{ltp:.2f} [{reason}] P&L: {pnl:+.2f}", db)
                _update_daily_pnl(stock.client_profile_id, pnl, db)

        db.commit()


# ── EOD force exit ────────────────────────────────────────────────────────────

async def _force_exit_all(db: Session, token: str, client_id: str):
    print("[algo] 3:20 PM force-exit all open paper positions")
    stocks  = db.query(AlgoStock).filter(AlgoStock.status == "entered").all()
    if not stocks:
        return
    sec_ids = list({s.security_id for s in stocks})
    ltp_map = await fetch_ltp(token, client_id, sec_ids)
    for stock in stocks:
        ltp = ltp_map.get(str(stock.security_id), float(stock.entry_price or 0))
        pnl = _calc_pnl(stock, ltp)
        stock.status = "exited"; stock.exit_price = ltp; stock.pnl = pnl
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
        token, master_client_id = await get_master_token(db)

        # Create today's runs
        runs = {}
        for profile, strategy in clients:
            run = _get_or_create_run(profile.id, strategy.id, db)
            run.status     = "scanning"
            run.started_at = datetime.now(timezone.utc)
            db.commit()
            runs[profile.id] = (run, strategy)

        # ── Phase 1: Load universe + pre-open scan ──────────────────────
        # Each client may have a different universe — scan union of all
        # then filter per client to avoid duplicate API calls

        all_sec_ids = set()
        client_universes = {}
        for profile, strategy in clients:
            universe = _load_universe(strategy, db)
            client_universes[profile.id] = universe
            all_sec_ids.update(u["security_id"] for u in universe)

        _log(runs[clients[0][0].id][0],
             f"Combined universe: {len(all_sec_ids)} unique stocks across all clients", db)

        # Scan all unique security IDs once
        meta_universe = []
        for sid in all_sec_ids:
            # Find meta from any client's universe
            for cid, univ in client_universes.items():
                m = next((u for u in univ if u["security_id"] == sid), None)
                if m:
                    meta_universe.append(m)
                    break

        gap_results = await _preopen_scan(token, master_client_id, meta_universe)

        # Apply per-client filters and store selected stocks
        for profile, strategy in clients:
            run, strat = runs[profile.id]
            universe   = client_universes[profile.id]
            run.stocks_scanned = len(universe)

            # Filter gap_results to only this client's universe
            client_sec_ids = {u["security_id"] for u in universe}
            client_gaps    = {
                sid: d for sid, d in gap_results.items()
                if sid in client_sec_ids
            }

            # Enrich with series info from universe
            sid_meta = {u["security_id"]: u for u in universe}
            for sid in client_gaps:
                client_gaps[sid]["series"]   = sid_meta.get(sid, {}).get("series", "EQ")
                client_gaps[sid]["lot_size"]  = sid_meta.get(sid, {}).get("lot_size", 1)

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

        # ── Phase 2: Wait for 9:15 AM ──────────────────────────────────
        now_ist  = datetime.now(IST)
        open_ist = now_ist.replace(hour=9, minute=15, second=30, microsecond=0)
        wait_sec = (open_ist - now_ist).total_seconds()
        if 0 < wait_sec < 900:
            print(f"[algo] Waiting {wait_sec:.0f}s for market open...")
            await asyncio.sleep(wait_sec)

        # ── Phase 3: First candle entry ────────────────────────────────
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
                        f"{stock.symbol}: BUY ₹{entry['buy_trigger']:.2f} | "
                        f"SELL ₹{entry['sell_trigger']:.2f} | Qty {entry['quantity']}", db)
                else:
                    stock.status = "cancelled"
                    _log(run, f"{stock.symbol}: No candle data — skipped", db)
                db.commit()
                await asyncio.sleep(0.3)

        # ── Phase 4: Monitor until 3:20 PM ────────────────────────────
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

        # ── Phase 5: EOD force exit ────────────────────────────────────
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
