"""
app/services/algo_engine.py — WOI Paper Trade Engine

Gap calculation using Daily Historical Data (more reliable than LTP snapshots):
  POST /v2/charts/historical with yesterday + today date range
  → yesterday close = prev_close
  → today open      = opening price
  → gap% = (today_open - yesterday_close) / yesterday_close × 100
  → volume from yesterday's candle for volume filter

Schedule:
  08:00 AM → Scrip master + token refresh (in main.py scheduler)
  09:16:05 → Fetch daily OHLCV for universe → compute gaps → filter → select
  09:16:05 → Fetch first 1-min candle → BUY/SELL triggers
  09:16:05+ → Monitor every 5s → trail SL → exit at target
  15:20:00 → Force exit all open positions
"""

import asyncio, json
from datetime import datetime, date, timedelta, timezone
from typing import Optional
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.trading import ClientProfile, AlgoStrategy, AlgoRun, AlgoStock, DailyPnl
from app.models.scrip_master import UniverseStock, ScripMaster
from app.services.market_data import (
    get_master_token, fetch_ltp, fetch_first_candle, fetch_daily_ohlcv, fetch_ohlc_batch,
)

IST = timezone(timedelta(hours=5, minutes=30))

FALLBACK_UNIVERSE = [
    "1333","11536","10895","15083","4963","3456","14977","1232","5258","11630",
    "2475","16675","7229","1526","13611","10940","467","11184","6705","3787",
]


# ── Logging ───────────────────────────────────────────────────────────────────

def _log(run: AlgoRun, msg: str, db: Session):
    ts      = datetime.now(IST).strftime("%H:%M:%S")
    run.log = (run.log or "") + f"[{ts}] {msg}\n"
    db.commit()
    print(f"[algo] {msg}")


def _sep(run: AlgoRun, title: str, db: Session):
    _log(run, f"{'─'*10} {title} {'─'*10}", db)


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
            run_date=today, status="idle", log="",
        )
        db.add(run)
        db.commit()
        db.refresh(run)
    return run


def _load_universe(strategy: AlgoStrategy, db: Session) -> list[dict]:
    if not strategy.universe_id:
        print("[algo] No universe — using fallback")
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
        ).all()
    )
    if not stocks:
        print("[algo] Universe empty — using fallback")
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
    print(f"[algo] Universe: {len(result)} stocks")
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


# ── Gap scan using Daily Historical Data ──────────────────────────────────────

async def _fetch_gap_data(
    token: str,
    client_id: str,
    universe: list[dict],
    pre_open_snap: dict,
    run: AlgoRun,
    db: Session,
) -> dict:
    """
    Hybrid approach:
    - Yesterday close: from Daily Historical API (accurate, finalized)
    - Today open:      from LTP at 9:12:30 AM pre-open (live indicative price)
    - Volume:          from yesterday's historical candle (for volume filter)
    gap% = (today_open - yesterday_close) / yesterday_close × 100

    Returns { security_id: { symbol, gap_pct, prev_close, open_price, volume, series } }
    """
    today         = date.today()
    yesterday     = today - timedelta(days=1)
    # Use yesterday as toDate — avoids partial today candle issue
    # fromDate = 7 days back to cover weekends/holidays
    today_str     = today.strftime("%Y-%m-%d")
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    from_date_str = (today - timedelta(days=7)).strftime("%Y-%m-%d")

    _sep(run, "GAP CALCULATION — DAILY HISTORICAL DATA", db)
    _log(run, f"Fetching daily OHLCV: fromDate={from_date_str} toDate={yesterday_str}", db)
    _log(run, f"  yesterday close = prev_close for gap calc", db)
    _log(run, f"  today open = fetched via LTP at 9:12:30 AM (pre-open price)", db)
    _log(run, f"Stocks to fetch: {len(universe)}", db)

    results   = {}
    errors    = 0
    no_today  = 0
    no_yest   = 0

    # Rate limit: 1 req/sec — batch with delay
    for i, u in enumerate(universe):
        sid    = u["security_id"]
        symbol = u["symbol"]
        series = u.get("series", "EQ")

        try:
            # Fetch historical up to yesterday — avoids partial today candle
            candles = await fetch_daily_ohlcv(
                token, client_id, sid,
                from_date=from_date_str,
                to_date=yesterday_str,
            )

            if not candles:
                no_yest += 1
                continue

            # Last candle = yesterday's actual close
            yest_candle = candles[-1]
            yest_close  = float(yest_candle["close"]  or 0)
            yest_volume = int(yest_candle["volume"]   or 0)

            if yest_close <= 0:
                no_yest += 1
                continue

            # today_open comes from pre_close_snap (LTP at 9:12:30 pre-open)
            today_open = pre_open_snap.get(sid, 0)
            if today_open <= 0:
                no_today += 1
                continue

            gap_pct     = ((today_open - yest_close) / yest_close) * 100
            turnover_cr = round((yest_close * yest_volume) / 1e7, 2)

            results[sid] = {
                "symbol":      symbol,
                "series":      series,
                "gap_pct":     round(gap_pct, 2),
                "prev_close":  round(yest_close, 2),
                "open_price":  round(today_open, 2),
                "volume":      yest_volume,
                "turnover_cr": turnover_cr,
            }

        except Exception as e:
            errors += 1
            print(f"[algo] Error fetching {symbol}: {e}")

        # Rate limit: 1 req/sec for historical endpoint
        await asyncio.sleep(1.1)

        # Log progress every 50 stocks
        if (i + 1) % 50 == 0:
            _log(run, f"Progress: {i+1}/{len(universe)} stocks fetched ({len(results)} valid so far)", db)

    _log(run, f"Daily OHLCV complete: {len(results)} stocks with valid data | "
              f"no_yesterday={no_yest} | no_today={no_today} | errors={errors}", db)

    # Gap distribution
    bands = {"0–1%": 0, "1–3%": 0, "3–8%": 0, "8%+": 0}
    for d in results.values():
        g = abs(d["gap_pct"])
        if g < 1: bands["0–1%"] += 1
        elif g < 3: bands["1–3%"] += 1
        elif g < 8: bands["3–8%"] += 1
        else: bands["8%+"] += 1
    _log(run, f"Gap distribution: {bands}", db)

    return results


# ── Stock filter ──────────────────────────────────────────────────────────────

def _apply_filters(gap_results: dict, strategy: AlgoStrategy, run: AlgoRun, db: Session) -> list[dict]:
    gap_min   = float(strategy.gap_min)
    gap_max   = float(strategy.gap_max)
    min_price = float(strategy.min_price   or 0)
    max_price = float(strategy.max_price   or 0)
    min_vol   = int(strategy.min_volume    or 0)
    min_turn  = float(strategy.min_turnover_cr or 0)
    excl_be   = bool(strategy.exclude_be_series)
    max_n     = int(strategy.max_stocks_per_day)

    _sep(run, "STOCK FILTER", db)
    _log(run, f"Applying filters to {len(gap_results)} stocks:", db)
    _log(run, f"  Gap: {gap_min}% – {gap_max}% (absolute)", db)
    _log(run, f"  Price: ₹{min_price} – ₹{max_price} (0=disabled)", db)
    _log(run, f"  Min volume: {min_vol:,} shares prev day (0=disabled)", db)
    _log(run, f"  Min turnover: ₹{min_turn} cr prev day (0=disabled)", db)
    _log(run, f"  Exclude BE series: {excl_be}", db)

    passed   = []
    rej_gap  = 0
    rej_price= 0
    rej_vol  = 0
    rej_turn = 0
    rej_be   = 0

    for sid, d in gap_results.items():
        gap  = abs(d["gap_pct"])
        prev = d["prev_close"]
        vol  = d.get("volume", 0)
        turn = d.get("turnover_cr", 0)
        ser  = d.get("series", "EQ")
        sym  = d.get("symbol", sid)

        if not (gap_min <= gap <= gap_max):
            rej_gap += 1; continue
        if min_price > 0 and prev < min_price:
            rej_price += 1; continue
        if max_price > 0 and prev > max_price:
            rej_price += 1; continue
        if min_vol > 0 and vol < min_vol:
            rej_vol += 1; continue
        if min_turn > 0 and turn < min_turn:
            rej_turn += 1; continue
        if excl_be and ser == "BE":
            rej_be += 1; continue

        passed.append({"security_id": sid, **d})

    _log(run, f"Rejected: gap={rej_gap} | price={rej_price} | volume={rej_vol} | turnover={rej_turn} | BE={rej_be}", db)
    _log(run, f"Passed all filters: {len(passed)} stocks", db)

    if not passed:
        _log(run, "⚠ NO STOCKS PASSED FILTERS", db)
        top = sorted(gap_results.values(), key=lambda x: abs(x["gap_pct"]), reverse=True)[:10]
        _log(run, "Top 10 gap movers today (for reference):", db)
        for s in top:
            _log(run,
                f"  {s['symbol']}: {s['gap_pct']:+.2f}% | "
                f"prev=₹{s['prev_close']} | vol={s.get('volume',0):,} | "
                f"turnover=₹{s.get('turnover_cr',0):.1f}cr | series={s['series']}",
                db)
        return []

    passed.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)
    selected = passed[:max_n]

    _log(run, f"Selected top {len(selected)}:", db)
    for s in selected:
        _log(run,
            f"  {s['symbol']}: gap={s['gap_pct']:+.2f}% | "
            f"prev=₹{s['prev_close']} | open=₹{s['open_price']} | "
            f"vol={s.get('volume',0):,} | series={s['series']}",
            db)

    return selected


# ── First candle ──────────────────────────────────────────────────────────────

async def _compute_entry(security_id: str, strategy: AlgoStrategy, token: str, client_id: str) -> Optional[dict]:
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

async def _monitor_tick(db: Session, token: str, client_id: str):
    today  = date.today()
    stocks = db.query(AlgoStock).filter(AlgoStock.status.in_(["watching", "entered"])).all()
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
            entered = False

            if not strategy.gap_direction_bias:
                if buy_t  and ltp >= buy_t:
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
                trig = buy_t if stock.entry_direction == "BUY" else sell_t
                sl_p = ltp * (1 - float(strategy.sl_pct)) if stock.entry_direction == "BUY" \
                       else ltp * (1 + float(strategy.sl_pct))
                tgt_p = ltp * (1 + float(strategy.sl_pct) * float(strategy.target_r)) \
                        if stock.entry_direction == "BUY" \
                        else ltp * (1 - float(strategy.sl_pct) * float(strategy.target_r))
                _log(run,
                    f"PAPER {stock.entry_direction} {stock.symbol} @ ₹{ltp:.2f} "
                    f"| trigger=₹{trig:.2f} | SL=₹{sl_p:.2f} | target=₹{tgt_p:.2f} | qty={stock.quantity}",
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

async def _force_exit_all(db: Session, token: str, client_id: str):
    stocks = db.query(AlgoStock).filter(AlgoStock.status.in_(["watching", "entered"])).all()
    if not stocks:
        return
    sec_ids = list({s.security_id for s in stocks})
    ltp_map = await fetch_ltp(token, client_id, sec_ids)
    for stock in stocks:
        ltp = ltp_map.get(str(stock.security_id), float(stock.entry_price or 0))
        pnl = _calc_pnl(stock, ltp) if stock.status == "entered" else 0
        stock.status = "exited"; stock.exit_price = ltp
        stock.exit_time = datetime.now(timezone.utc); stock.exit_reason = "EOD"
        stock.pnl = pnl
        run = db.query(AlgoRun).filter(AlgoRun.id == stock.run_id).first()
        if run:
            run.total_pnl = float(run.total_pnl or 0) + pnl
            _log(run, f"FORCE EXIT {stock.symbol} @ ₹{ltp:.2f} [EOD] P&L=₹{pnl:+.2f}", db)
        if pnl != 0:
            _update_daily_pnl(stock.client_profile_id, pnl, db)
    db.commit()


# ── Main daily run ────────────────────────────────────────────────────────────

async def run_daily_algo():
    db = SessionLocal()
    runs = {}
    try:
        now_ist = datetime.now(IST)
        print(f"[algo] ═══ WOI Algo starting — {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')} ═══")

        clients = _subscribed_clients(db)
        if not clients:
            print("[algo] No subscribed clients — skipping")
            return

        print(f"[algo] Subscribed clients: {len(clients)}")
        token, master_client_id = await get_master_token(db)
        print(f"[algo] Master token OK")

        # Create runs
        for profile, strategy in clients:
            run = _get_or_create_run(profile.id, strategy.id, db)
            run.status = "scanning"; run.started_at = datetime.now(timezone.utc)
            db.commit()
            runs[profile.id] = (run, strategy)
            _log(run, f"═══ WOI Algo — {now_ist.strftime('%Y-%m-%d')} ═══", db)
            _log(run,
                f"Strategy: {strategy.name} | Gap: {strategy.gap_min}–{strategy.gap_max}% | "
                f"Risk: ₹{strategy.risk_per_trade} | Target: 1:{strategy.target_r}R | "
                f"Max: {strategy.max_stocks_per_day} stocks",
                db)

        # Load universes
        client_universes = {}
        all_meta = {}
        for profile, strategy in clients:
            universe = _load_universe(strategy, db)
            client_universes[profile.id] = universe
            for u in universe:
                all_meta[u["security_id"]] = u
            run, _ = runs[profile.id]
            _log(run, f"Universe: {len(universe)} active stocks", db)

        combined_universe = list(all_meta.values())

        # ── Wait until 9:16:05 ─────────────────────────────────────────
        # Gap data fetched after market opens — we have actual today's open
        for _, (run, _) in runs.items():
            _log(run, "Waiting for 9:16:05 — will fetch daily OHLCV after first candle closes", db)

        await _wait_until(9, 16, 5, "daily OHLCV gap scan + first candle")
        token, master_client_id = await get_master_token(db)

        # ── Gap scan using daily historical data ───────────────────────
        # Fetch for all stocks in combined universe at once
        # Since historical is per-stock (1 req each), we fetch for each client's stocks
        for profile, strategy in clients:
            run, strat = runs[profile.id]
            universe   = client_universes[profile.id]

            _sep(run, "STEP 1: PRE-OPEN PRICE SCAN (9:12:30)", db)
            _log(run, f"Fetching pre-open LTP for {len(universe)} stocks (today's opening price)...", db)

            # Fetch today's pre-open prices via LTP
            pre_open_snap = {}
            sec_ids = [u["security_id"] for u in universe]
            for i in range(0, len(sec_ids), 900):
                batch  = sec_ids[i:i+900]
                prices = await fetch_ltp(token, master_client_id, batch)
                pre_open_snap.update(prices)
                if i + 900 < len(sec_ids):
                    await asyncio.sleep(1.1)
            valid_ltp = sum(1 for v in pre_open_snap.values() if v > 0)
            _log(run, f"Pre-open LTP: {valid_ltp}/{len(universe)} stocks with price", db)

            _sep(run, "STEP 2: YESTERDAY CLOSE — DAILY HISTORICAL", db)
            _log(run, f"Fetching yesterday's close for {len(universe)} stocks "
                      f"(1 req/stock @ 1 req/sec — takes ~{len(universe)}s)", db)

            gap_results = await _fetch_gap_data(token, master_client_id, universe, pre_open_snap, run, db)

            run.stocks_scanned = len(universe)
            selected = _apply_filters(gap_results, strat, run, db)
            run.stocks_selected = len(selected)
            db.commit()

            for item in selected:
                exists = db.query(AlgoStock).filter(
                    AlgoStock.run_id == run.id,
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

        # ── First candle ───────────────────────────────────────────────
        for profile, strategy in clients:
            run, strat = runs[profile.id]
            run.status = "running"; db.commit()

            stocks = db.query(AlgoStock).filter(
                AlgoStock.run_id == run.id, AlgoStock.status == "watching"
            ).all()

            if not stocks:
                _log(run, "No stocks selected — nothing to trade today", db)
                continue

            _sep(run, "FIRST CANDLE ENTRY", db)
            _log(run, f"Fetching first 1-min candle for {len(stocks)} stocks...", db)

            for stock in stocks:
                entry = await _compute_entry(stock.security_id, strat, token, master_client_id)
                if entry:
                    stock.candle_high  = entry["candle_high"]
                    stock.candle_low   = entry["candle_low"]
                    stock.candle_close = entry["candle_close"]
                    stock.buy_trigger  = entry["buy_trigger"]
                    stock.sell_trigger = entry["sell_trigger"]
                    stock.quantity     = entry["quantity"]
                    _log(run,
                        f"{stock.symbol}: O=₹{entry['candle_open']:.2f} "
                        f"H=₹{entry['candle_high']:.2f} "
                        f"L=₹{entry['candle_low']:.2f} "
                        f"C=₹{entry['candle_close']:.2f} | "
                        f"BUY=₹{entry['buy_trigger']:.2f} | "
                        f"SELL=₹{entry['sell_trigger']:.2f} | "
                        f"Qty={entry['quantity']}",
                        db)
                else:
                    stock.status = "cancelled"
                    _log(run, f"{stock.symbol}: No candle data — skipped", db)
                db.commit()
                await asyncio.sleep(0.3)

        # ── Monitor loop ───────────────────────────────────────────────
        eod_ist    = datetime.now(IST).replace(hour=15, minute=20, second=0, microsecond=0)
        tick_count = 0
        for _, (run, _) in runs.items():
            _sep(run, "MONITORING", db)
            _log(run, "Monitor loop started — checking LTP every 5s until 3:20 PM", db)

        while datetime.now(IST) < eod_ist:
            await asyncio.sleep(5)
            tick_count += 1
            try:
                token, master_client_id = await get_master_token(db)
                await _monitor_tick(db, token, master_client_id)
            except Exception as e:
                print(f"[algo] Tick error: {e}")

            # Heartbeat every 5 min
            if tick_count % 60 == 0:
                for _, (run, _) in runs.items():
                    open_c = db.query(AlgoStock).filter(
                        AlgoStock.run_id == run.id,
                        AlgoStock.status.in_(["watching", "entered"])
                    ).count()
                    _log(run, f"Heartbeat: {tick_count * 5 // 60}min | {open_c} position(s) open | "
                              f"P&L: ₹{float(run.total_pnl or 0):.2f}", db)

            open_count = db.query(AlgoStock).filter(
                AlgoStock.status.in_(["watching", "entered"])
            ).count()
            if open_count == 0:
                for _, (run, _) in runs.items():
                    _log(run, "All positions closed — stopping monitor", db)
                break

        # ── EOD ────────────────────────────────────────────────────────
        token, master_client_id = await get_master_token(db)
        await _force_exit_all(db, token, master_client_id)

        for profile, _ in clients:
            run, _ = runs[profile.id]
            run.status = "done"; run.finished_at = datetime.now(timezone.utc)
            _sep(run, "RUN COMPLETE", db)
            _log(run, f"Total P&L: ₹{float(run.total_pnl or 0):.2f}", db)
            w = db.query(AlgoStock).filter(AlgoStock.run_id == run.id,
                AlgoStock.status == "exited", AlgoStock.pnl > 0).count()
            l = db.query(AlgoStock).filter(AlgoStock.run_id == run.id,
                AlgoStock.status == "exited", AlgoStock.pnl <= 0).count()
            wr = round(w / (w + l) * 100, 1) if (w + l) > 0 else 0
            _log(run, f"Trades: {w}W / {l}L | Win rate: {wr}%", db)

        print("[algo] ═══ Run complete ═══")

    except Exception as e:
        print(f"[algo] FATAL: {e}")
        import traceback; traceback.print_exc()
        for pid, (run, _) in runs.items():
            try:
                _log(run, f"FATAL ERROR: {e}", db)
                run.status = "error"; db.commit()
            except Exception:
                pass
    finally:
        db.close()
