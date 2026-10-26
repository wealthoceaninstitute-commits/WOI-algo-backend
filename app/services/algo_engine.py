"""
app/services/algo_engine.py — WOI Hybrid Trade Engine

Architecture (DB-persisted price snapshot):
  8:45 AM  → fetch_ltp(501) → save prev_close to DailyPriceSnapshot table
  9:12:30  → fetch_ltp(501) → update open_price in same table → compute gaps
  8:00 AM (next day) → DELETE previous day snapshot before fresh fetch

Trading mode (per client, switchable anytime via UI) — AlgoStrategy.paper_trading:
  paper_trading = True  → simulate only, no real orders
  paper_trading = False → place real MARKET orders via Dhan (through the client's static-IP proxy)

PAPER and LIVE clients run side by side in ONE monitor loop:
  • the loop only does fast, in-memory + one-commit-per-tick work (never waits on a broker),
  • every LIVE order is handed to its own background task (timeouts, retries, fill check),
    so a slow / failing Dhan call can never delay anyone's SL / target / entry.

Reliability:
  • every tick uses a fresh short-lived DB session (a DB hiccup can't poison the loop)
  • any error inside a tick is logged and the loop carries on — it cannot die silently
  • resume_monitoring_if_needed() + engine_watchdog() restart monitoring after a crash/redeploy
    and recover SL / target hits that happened while the engine was down

Benefits:
  - Survives server restart / double-run (upsert, not overwrite)
  - No 429 rate limit from concurrent runs (DB write is idempotent)
  - Debug endpoint: GET /api/algo/debug/snapshot?date=YYYY-MM-DD
"""

import asyncio, json, math, time, traceback
from dataclasses import dataclass, field
from datetime import datetime, date, timezone, timedelta
from types import SimpleNamespace
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text, update, func, case

from app.core.database import SessionLocal
from app.core.encryption import decrypt
from app.models.trading import (
    ClientProfile, AlgoStrategy, AlgoRun, AlgoStock,
    DailyPnl, DailyPriceSnapshot, DhanCredential,
)
from app.models.scrip_master import UniverseStock, ScripMaster
from app.services.angel_one import (
    angel_fetch_ltp, angel_fetch_full_quote, angel_fetch_first_candle, angel_fetch_candle
)
from app.services.angel_ws import (
    ensure_ws_running, get_live_prices, get_live_ltp,
    is_ws_connected, ws_stats, stop_ws_stream
)
from app.services.master_token import get_master_token, clear_master_token_cache
from app.services.dhan_trade import (
    place_order, cancel_order, get_order_by_id, get_positions,
    find_order_by_correlation, normalize_order,
)
from app.services.token_manager import (
    token_is_valid, get_stored_token, refresh_token, _get_proxy_kwargs,
)

IST = timezone(timedelta(hours=5, minutes=30))


FALLBACK_UNIVERSE = [
    "1333","11536","10895","15083","4963","3456","14977","1232","5258","11630",
    "2475","16675","7229","1526","13611","10940","467","11184","6705","3787",
]


# ── Logging ───────────────────────────────────────────────────────────────────

def _stamp() -> str:
    return datetime.now(IST).strftime("%H:%M:%S")


def _append_run_log(db: Session, run_id: str, lines: list[str]):
    """
    Atomic SQL append to algo_runs.log.
    Never loads / rewrites the (large) log column and cannot clobber lines written at the
    same moment from another session (live-order tasks log while the tick loop runs).
    The caller commits.
    """
    if not lines:
        return
    db.execute(
        update(AlgoRun)
        .where(AlgoRun.id == run_id)
        .values(log=func.coalesce(AlgoRun.log, "") + "".join(l + "\n" for l in lines))
    )


def _log(run: AlgoRun, msg: str, db: Session, commit: bool = True):
    line = f"[{_stamp()}] {msg}"
    _append_run_log(db, run.id, [line])
    if commit:
        db.commit()
    print(f"[algo] {msg}")


def _log_separator(run: AlgoRun, title: str, db: Session):
    sep = f"{'─' * 10} {title} {'─' * 10}"
    _log(run, sep, db)


def _log_runs_sync(run_ids: list[str], msg: str):
    """Append one line to many runs from a worker thread (own session). Never raises."""
    db = SessionLocal()
    try:
        line = f"[{_stamp()}] {msg}"
        for rid in run_ids:
            _append_run_log(db, rid, [line])
        db.commit()
    except Exception as e:
        print(f"[algo] log write failed: {type(e).__name__}: {e}")
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()
    print(f"[algo] {msg}")


# ── Trading mode ──────────────────────────────────────────────────────────────

def _is_paper(profile: Optional[ClientProfile], strategy: Optional[AlgoStrategy] = None) -> bool:
    """
    True if this client is in paper trading mode.

    SINGLE SOURCE OF TRUTH = AlgoStrategy.paper_trading — the flag the master flips on the
    Clients page (Paper / Live badge). The engine used to read ClientProfile.paper_trading
    instead, which that toggle never touches, so a client switched to "Live" kept running as
    paper and no order was ever sent. Both flags are now kept in sync on every write
    (see routers/algo.py + routers/clients.py), and the strategy flag wins here.
    """
    if strategy is not None and strategy.paper_trading is not None:
        return bool(strategy.paper_trading)
    if profile is not None and profile.paper_trading is not None:
        return bool(profile.paper_trading)
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  LIVE ORDER EXECUTION (Dhan)
#  Runs in background tasks so it can never delay the monitor loop.
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class LiveIntent:
    kind: str                    # "ENTRY" | "EXIT"
    stock_id: str
    run_id: str
    profile_id: str
    symbol: str
    security_id: str
    side: str                    # order side: BUY | SELL
    qty: int
    reason: str = ""
    signal_price: float = 0.0    # price the engine saw when it decided
    verify_position: bool = False  # EOD: check broker position first (it may already be flat)


_live_tasks: set = set()
_entry_tasks: dict[str, asyncio.Task] = {}
LIVE_ORDER_TIMEOUT   = 12.0
LIVE_ORDER_ATTEMPTS  = 3


def _corr_id(stock_id: str, kind: str) -> str:
    """≤24 chars, unique per stock+leg — lets us find the order again after a network error."""
    return stock_id.replace("-", "")[:23] + ("E" if kind == "ENTRY" else "X")


def _dispatch_live(intents: list[LiveIntent]):
    """Fire-and-forget: each order runs in its own task (entry always completes before its exit)."""
    for it in intents:
        prior = _entry_tasks.get(it.stock_id) if it.kind == "EXIT" else None
        task = asyncio.create_task(_run_live_intent(it, prior))
        _live_tasks.add(task)
        task.add_done_callback(_live_tasks.discard)
        if it.kind == "ENTRY":
            _entry_tasks[it.stock_id] = task
            task.add_done_callback(lambda _t, sid=it.stock_id: _entry_tasks.pop(sid, None))


async def _wait_live_tasks(timeout: float = 45.0):
    pending = [t for t in list(_live_tasks) if not t.done()]
    if pending:
        print(f"[live] waiting for {len(pending)} live order task(s)…")
        await asyncio.wait(pending, timeout=timeout)


def _load_live_ctx(profile_id: str, stock_id: str) -> dict:
    """Credentials + proxy for one client (worker thread, own session)."""
    db = SessionLocal()
    try:
        profile = db.query(ClientProfile).filter(ClientProfile.id == profile_id).first()
        cred = profile.dhan_cred if profile else None
        stock = db.query(AlgoStock).filter(AlgoStock.id == stock_id).first()
        out = {"live_status": stock.live_status if stock else None}
        if not profile or not cred:
            out["error"] = "no Dhan credentials saved for this client"
            return out
        try:
            out["client_id"] = decrypt(cred.dhan_client_id)
        except Exception:
            out["error"] = "could not decrypt Dhan client id"
            return out
        out["token"]    = get_stored_token(profile)          # DECRYPTED access token
        out["token_ok"] = bool(out["token"]) and token_is_valid(profile)
        out["proxy"]    = _get_proxy_kwargs(profile)          # static-IP proxy (SEBI) — must be used for orders
        return out
    except Exception as e:
        return {"error": f"credential load failed: {type(e).__name__}: {e}"}
    finally:
        db.close()


async def _refresh_live_token(profile_id: str, failed_token: Optional[str]) -> Optional[str]:
    """Generate a fresh Dhan token (TOTP). Reuses one a parallel task already refreshed."""
    db = SessionLocal()
    try:
        profile = db.query(ClientProfile).filter(ClientProfile.id == profile_id).first()
        cred = profile.dhan_cred if profile else None
        if not cred:
            return None
        current = get_stored_token(profile)
        if current and failed_token and current != failed_token:
            return current                                    # someone else already refreshed
        client_id = decrypt(cred.dhan_client_id)
        pin       = decrypt(cred.pin)
        totp      = decrypt(cred.totp_secret)
        return await refresh_token(profile, client_id, pin, totp, db,
                                   reason="forced", **_get_proxy_kwargs(profile))
    except Exception as e:
        print(f"[live] token refresh failed for {profile_id}: {type(e).__name__}: {e}")
        return None
    finally:
        db.close()


def _retryable(res: dict) -> bool:
    msg = str(res.get("message", ""))
    return any(k in msg for k in ("Dhan 5", "Dhan 429", "DH-904", "DH-908", "DH-909", "network", "timeout"))


async def _place_with_recovery(it: LiveIntent, ctx: dict, token: str, qty: int) -> dict:
    """
    POST the market order. Handles: expired token (refresh once + retry), network errors /
    timeouts (look the order up by correlationId BEFORE retrying so it is never placed twice),
    and transient 5xx / rate limits (short retry). Returns {success, order_id, message}.
    """
    corr  = _corr_id(it.stock_id, it.kind)
    proxy = ctx.get("proxy") or {}
    refreshed = False
    last = {"success": False, "message": "not attempted"}

    for attempt in range(1, LIVE_ORDER_ATTEMPTS + 1):
        try:
            res = await asyncio.wait_for(
                place_order(
                    access_token=token,
                    dhan_client_id=ctx["client_id"],
                    transaction_type=it.side,
                    exchange_segment="NSE_EQ",
                    product_type="INTRADAY",
                    order_type="MARKET",
                    security_id=str(it.security_id),
                    quantity=int(qty),
                    price=0,
                    correlation_id=corr,
                    **proxy,
                ),
                timeout=LIVE_ORDER_TIMEOUT,
            )
        except Exception as e:
            # Ambiguous: Dhan may have received it. Check before sending again.
            try:
                found = await asyncio.wait_for(find_order_by_correlation(token, corr, **proxy), 8)
            except Exception:
                found = None
            if found:
                return {"success": True, "order_id": found.get("orderId"), "message": "recovered via correlationId"}
            last = {"success": False, "message": f"network error ({type(e).__name__}: {e})"}
            await asyncio.sleep(0.4)
            continue

        if res.get("success"):
            oid = res.get("order_id") or (res.get("data") or {}).get("orderId")
            return {"success": True, "order_id": oid, "message": ""}

        if res.get("token_expired") and not refreshed:
            refreshed = True
            new = await _refresh_live_token(it.profile_id, token)
            if new:
                token = new
                continue
            return {"success": False, "message": "Dhan token expired and automatic refresh failed"}

        last = {"success": False, "message": res.get("message") or str(res)}
        if _retryable(last) and attempt < LIVE_ORDER_ATTEMPTS:
            await asyncio.sleep(0.5 * attempt)
            continue
        break

    return last


async def _poll_fill(token: str, proxy: dict, order_id: str) -> dict:
    """Short poll of the order status → actual average fill price (≈ a few seconds max)."""
    last: dict = {}
    for i in range(8):
        await asyncio.sleep(0.25 if i == 0 else 0.5)
        try:
            res = await asyncio.wait_for(get_order_by_id(token, order_id, **proxy), 6)
        except Exception:
            continue
        if not res.get("success"):
            continue
        d = res.get("data")
        if isinstance(d, list):
            d = d[0] if d else {}
        if not isinstance(d, dict) or not d:
            continue
        last = normalize_order(d)
        if last["status"] in ("EXECUTED", "REJECTED", "CANCELLED"):
            break
    return {
        "status": last.get("status", "UNKNOWN"),
        "avg":    float(last.get("executed_price") or 0),
        "filled": int(last.get("filled_qty") or 0),
        "reason": last.get("rejection_reason"),
    }


async def _broker_position_qty(token: str, proxy: dict, security_id: str, timeout: float = 8.0) -> Optional[int]:
    """Signed net quantity the broker holds for this security today (None = could not tell)."""
    try:
        res = await asyncio.wait_for(get_positions(token, **proxy), timeout)
    except Exception:
        return None
    if not res.get("success"):
        return None
    data = res.get("data")
    if not isinstance(data, list):
        return None
    for p in data:
        if str(p.get("securityId")) == str(security_id) and str(p.get("productType", "INTRADAY")) in ("INTRADAY", "MIS"):
            try:
                return int(p.get("netQty") or 0)
            except Exception:
                return None
    return 0


def _live_note(it: LiveIntent, msg: str):
    _log_runs_sync([it.run_id], msg)


def _live_record_success(it: LiveIntent, order_id: str, fill: dict, qty: int):
    """Persist order id / status / fill; log one clear line. (worker thread)"""
    db = SessionLocal()
    try:
        stock = db.query(AlgoStock).filter(AlgoStock.id == it.stock_id).first()
        if not stock:
            return
        avg = fill.get("avg") or 0.0
        slip = ""
        if avg > 0 and it.signal_price > 0:
            slip = f" | signal ₹{it.signal_price:.2f} → fill ₹{avg:.2f} (slip {avg - it.signal_price:+.2f})"
        st = fill.get("status", "UNKNOWN")
        if it.kind == "ENTRY":
            if it.side == "BUY":
                stock.buy_order_id = str(order_id)
            else:
                stock.sell_order_id = str(order_id)
            stock.live_status = "ENTRY_OK"
            if avg > 0:
                stock.live_entry_price = avg
        else:
            if it.side == "BUY":
                stock.buy_order_id = str(order_id)
            else:
                stock.sell_order_id = str(order_id)
            stock.live_status = "EXIT_OK"
            if avg > 0:
                stock.live_exit_price = avg
        _append_run_log(db, it.run_id, [
            f"[{_stamp()}] LIVE {it.kind} {it.side} {it.symbol} x{qty} @ MKT ✓ order_id={order_id} "
            f"status={st}{(' filled ' + str(fill.get('filled'))) if fill.get('filled') else ''}{slip}"
            f"{(' [' + it.reason + ']') if it.reason else ''}"
        ])
        db.commit()
        print(f"[live] {it.kind} {it.side} {it.symbol} x{qty} OK order_id={order_id} status={st} avg={avg}")
    except Exception as e:
        print(f"[live] record_success failed: {type(e).__name__}: {e}")
        db.rollback()
    finally:
        db.close()


def _live_record_failure(it: LiveIntent, msg: str):
    """
    ENTRY failed  → nothing was bought/sold at the broker: cancel this client's trade so the
                    algo does not track (or later try to "exit") a position that does not exist.
    EXIT failed   → the broker position may still be OPEN: flag it loudly.
    """
    db = SessionLocal()
    try:
        stock = db.query(AlgoStock).filter(AlgoStock.id == it.stock_id).first()
        if not stock:
            return
        if it.kind == "ENTRY":
            stock.live_status = "ENTRY_FAILED"
            if stock.status in ("entered", "exited"):
                was_exited = stock.status == "exited"
                pnl = float(stock.pnl or 0) if was_exited else 0.0
                stock.status      = "cancelled"
                stock.exit_reason = "ENTRY_FAILED"
                stock.pnl         = 0
                db.execute(update(AlgoRun).where(AlgoRun.id == it.run_id).values(
                    stocks_traded=case((AlgoRun.stocks_traded > 0, AlgoRun.stocks_traded - 1), else_=0),
                    total_pnl=func.coalesce(AlgoRun.total_pnl, 0) - pnl))
                if was_exited and pnl:       # exited within the same second: undo its (virtual) P&L
                    drow = db.query(DailyPnl).filter(
                        DailyPnl.client_profile_id == it.profile_id,
                        DailyPnl.date == date.today()).first()
                    if drow:
                        drow.closed_pnl  = float(drow.closed_pnl or 0) - pnl
                        drow.total_pnl   = float(drow.total_pnl or 0) - pnl
                        drow.trade_count = max(0, (drow.trade_count or 0) - 1)
            line = (f"[{_stamp()}] LIVE ENTRY FAILED {it.side} {it.symbol} x{it.qty}: {msg} "
                    f"— NO position opened at the broker; trade cancelled for this client")
        else:
            stock.live_status = "EXIT_FAILED"
            line = (f"[{_stamp()}] ⚠ LIVE EXIT FAILED {it.side} {it.symbol} x{it.qty} [{it.reason}]: {msg} "
                    f"— POSITION MAY STILL BE OPEN AT THE BROKER, CLOSE IT MANUALLY")
        _append_run_log(db, it.run_id, [line])
        db.commit()
        print(f"[live] {line}")
    except Exception as e:
        print(f"[live] record_failure failed: {type(e).__name__}: {e}")
        db.rollback()
    finally:
        db.close()


async def _execute_live_order(it: LiveIntent):
    ctx = await asyncio.to_thread(_load_live_ctx, it.profile_id, it.stock_id)

    # An exit only makes sense if the matching entry really went through at the broker.
    # (Positions that were opened as paper-only — e.g. before a client was switched to
    # live — must never send a real order: a SELL with no position opens a fresh SHORT.)
    if it.kind == "EXIT" and ctx.get("live_status") not in ("ENTRY_OK", "ENTRY_PENDING"):
        await asyncio.to_thread(
            _live_note, it,
            f"LIVE EXIT {it.symbol} skipped — no live entry order for this trade "
            f"(live_status={ctx.get('live_status')}); nothing to close at the broker")
        return

    if ctx.get("error"):
        await asyncio.to_thread(_live_record_failure, it, ctx["error"])
        return

    token = ctx.get("token")
    if not ctx.get("token_ok"):
        token = await _refresh_live_token(it.profile_id, token)
    if not token:
        await asyncio.to_thread(_live_record_failure, it, "no valid Dhan access token (refresh failed — check PIN/TOTP)")
        return

    qty = it.qty
    if it.kind == "EXIT":
        # Never close more than the broker really holds, and never open a fresh opposite
        # position: if the trade was already closed (manually in the Dhan app, broker auto
        # square-off, or never filled) skip the order. For SL / target the lookup is capped at
        # 2.5s so it cannot meaningfully delay the exit; at EOD we can afford to wait longer.
        net = await _broker_position_qty(token, ctx.get("proxy") or {}, it.security_id,
                                         timeout=8.0 if it.reason == "EOD" else 2.5)
        if net is not None:
            want_close = (net > 0 and it.side == "SELL") or (net < 0 and it.side == "BUY")
            if net == 0 or not want_close:
                await asyncio.to_thread(
                    _live_note, it,
                    f"LIVE EXIT {it.symbol} skipped — broker net qty is {net} (already flat / closed manually)")
                db = SessionLocal()
                try:
                    s = db.query(AlgoStock).filter(AlgoStock.id == it.stock_id).first()
                    if s:
                        s.live_status = "EXIT_OK"
                        db.commit()
                finally:
                    db.close()
                return
            qty = min(abs(net), it.qty)

    res = await _place_with_recovery(it, ctx, token, qty)
    if not res.get("success"):
        await asyncio.to_thread(_live_record_failure, it, res.get("message") or "order rejected")
        return

    order_id = res.get("order_id")
    fill = await _poll_fill(token, ctx.get("proxy") or {}, order_id) if order_id else {"status": "UNKNOWN", "avg": 0.0}

    if fill.get("status") in ("REJECTED", "CANCELLED"):        # accepted by Dhan, refused by the exchange / RMS
        await asyncio.to_thread(
            _live_record_failure, it,
            f"order {order_id} {fill['status']}: {fill.get('reason') or 'no reason given'}")
        return

    await asyncio.to_thread(_live_record_success, it, order_id, fill, qty)


async def _run_live_intent(it: LiveIntent, prior: Optional[asyncio.Task]):
    try:
        if prior is not None and not prior.done():
            try:
                await asyncio.wait_for(asyncio.shield(prior), 20)
            except Exception:
                pass
        await _execute_live_order(it)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[live] unexpected error for {it.kind} {it.symbol}: {e}\n{tb}")
        try:
            await asyncio.to_thread(_live_record_failure, it, f"internal error {type(e).__name__}: {e}")
        except Exception:
            pass

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
# The candle is fetched ONCE per unique security, all securities IN PARALLEL (the old code
# fetched them one after another, each with 3s/6s/9s back-off — ~26s before the first order
# could even be considered). Each stock's levels are written the moment ITS candle arrives
# and the monitor loop is already running, so entries start at ~9:16:01 instead of ~9:16:30.

def _entry_levels(candle: dict, strategy, security_id: str) -> dict:
    """Trigger levels + quantity for ONE client's strategy from the shared first candle."""
    buf  = float(strategy.entry_buffer_pct)
    sl   = float(strategy.sl_pct)
    risk = float(strategy.risk_per_trade)

    base_buy  = candle["close"] if strategy.entry_reference == "close" else candle["high"]
    base_sell = candle["close"] if strategy.entry_reference == "close" else candle["low"]

    tick         = _tick_for(security_id, base_buy)
    buy_trigger  = snap_to_tick(base_buy  * (1 + buf), tick, "up")     # BUY only at/above the level
    sell_trigger = snap_to_tick(base_sell * (1 - buf), tick, "down")   # SELL only at/below the level
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


def _apply_candle_sync(sid: str, candle: Optional[dict], stock_ids: list[str]):
    """Write the candle-derived levels to every client's row for this security (worker thread)."""
    db = SessionLocal()
    try:
        stocks = db.query(AlgoStock).filter(AlgoStock.id.in_(stock_ids)).all()
        pids   = {s.client_profile_id for s in stocks}
        strat_by_pf = {x.client_profile_id: x for x in
                       db.query(AlgoStrategy).filter(AlgoStrategy.client_profile_id.in_(pids)).all()} if pids else {}
        for stock in stocks:
            if stock.status != "watching":
                continue
            strat = strat_by_pf.get(stock.client_profile_id)
            if candle and strat:
                e = _entry_levels(candle, strat, sid)
                stock.candle_high  = e["candle_high"]
                stock.candle_low   = e["candle_low"]
                stock.candle_close = e["candle_close"]
                stock.buy_trigger  = e["buy_trigger"]
                stock.sell_trigger = e["sell_trigger"]
                stock.quantity     = e["quantity"]
                _append_run_log(db, stock.run_id, [
                    f"[{_stamp()}] {stock.symbol}: O=₹{e['candle_open']:.2f} "
                    f"H=₹{e['candle_high']:.2f} L=₹{e['candle_low']:.2f} C=₹{e['candle_close']:.2f} | "
                    f"BUY=₹{e['buy_trigger']:.2f} | SELL=₹{e['sell_trigger']:.2f} | Qty={e['quantity']}"
                ])
            else:
                stock.status = "cancelled"
                stock.exit_reason = "NO_CANDLE"
                _append_run_log(db, stock.run_id, [
                    f"[{_stamp()}] {stock.symbol}: No first-candle data after 45s — skipped"
                ])
        db.commit()
    except Exception as e:
        print(f"[algo] apply_candle {sid} failed: {type(e).__name__}: {e}")
        db.rollback()
    finally:
        db.close()


async def _candle_worker(sid: str, stock_ids: list[str], jwt: str, api_key: str, client_id: str):
    candle = None
    try:
        candle = await angel_fetch_first_candle(jwt, api_key, client_id, sid)
    except Exception as e:
        print(f"[algo] first candle {sid} failed: {type(e).__name__}: {e}")
    try:
        await asyncio.to_thread(_apply_candle_sync, sid, candle, stock_ids)
    except Exception as e:
        print(f"[algo] apply candle {sid} failed: {type(e).__name__}: {e}")


# ── NSE tick size ─────────────────────────────────────────────────────────────
# Every price compared against / sent to the exchange must sit on the instrument's tick.
# NSE circular CMTR67133 (effective 15-Apr-2025): cash-segment tick depends on the
# security's price band (excluding ETFs), reviewed monthly:
#   < ₹250 → 0.01 | 250–1,000 → 0.05 | >1,000–5,000 → 0.10 | >5,000–10,000 → 0.50
#   >10,000–20,000 → 1.00 | >20,000 → 5.00
# Rule used here: tick = max(tick listed in scrip_master, tick of the price band),
# ETFs use the listed tick only. A coarser grid is always valid on a finer one, so the
# max can never produce an off-tick price even if one of the two sources is stale.

DEFAULT_TICK = 0.05
_tick_cache: dict[str, tuple[float, bool]] = {}   # security_id → (listed tick, is_etf)
_tick_loaded_at: float = 0.0


def band_tick(price: float) -> float:
    if price <= 0:      return DEFAULT_TICK
    if price < 250:     return 0.01
    if price <= 1000:   return 0.05
    if price <= 5000:   return 0.10
    if price <= 10000:  return 0.50
    if price <= 20000:  return 1.00
    return 5.00


def _norm_tick(raw) -> float:
    try:
        t = float(raw)
    except Exception:
        return DEFAULT_TICK
    if t <= 0:
        return DEFAULT_TICK
    if t >= 1:          # some feeds give the tick in paise (5 = ₹0.05)
        t = t / 100.0
    return round(t, 4)


def _tick_for(security_id, ref_price: Optional[float] = None) -> float:
    """
    Tick size for a security at a reference price (entry / trigger price).
    scrip_master is reloaded at most every 30 min on a cache miss.
    """
    import time as _t
    global _tick_loaded_at
    sid = str(security_id)
    if sid not in _tick_cache and (not _tick_cache or _t.monotonic() - _tick_loaded_at > 1800):
        _tick_loaded_at = _t.monotonic()
        sdb = SessionLocal()
        try:
            for k, v, nm, sy in sdb.query(
                ScripMaster.security_id, ScripMaster.tick_size, ScripMaster.name, ScripMaster.symbol
            ).all():
                label = f"{nm or ''} {sy or ''}".upper()
                _tick_cache[str(k)] = (_norm_tick(v), "ETF" in label or "BEES" in label)
        except Exception as e:
            print(f"[algo] tick-size load failed: {e}")
        finally:
            sdb.close()
    listed, is_etf = _tick_cache.get(sid, (DEFAULT_TICK, False))
    if ref_price and not is_etf:
        return max(listed, band_tick(float(ref_price)))
    return listed


def snap_to_tick(price: float, tick: float, mode: str = "nearest") -> float:
    """Snap a price to the tick grid. mode: 'up' (ceil), 'down' (floor) or 'nearest'."""
    if not tick or tick <= 0:
        return round(price, 2)
    q = price / tick
    n = math.ceil(q - 1e-6) if mode == "up" else math.floor(q + 1e-6) if mode == "down" else round(q)
    return round(n * tick, 4)


# ── Trail SL ──────────────────────────────────────────────────────────────────

def parse_trail_steps(raw) -> list[tuple[float, float]]:
    """
    Normalise trail SL steps to [(trigger_R, lock_R), ...] sorted ascending.
    Accepts a JSON string or list, with each step as [trigger, lock] OR
    {"r_trigger": x, "lock_r": y} (the master config form saves the latter).
    Bad entries are skipped instead of crashing the monitor loop.
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "[]")
        except Exception:
            return []
    out: list[tuple[float, float]] = []
    for st in raw or []:
        try:
            if isinstance(st, dict):
                out.append((float(st.get("r_trigger", st.get("trigger"))), float(st.get("lock_r", st.get("lock")))))
            else:
                out.append((float(st[0]), float(st[1])))
        except Exception:
            continue
    return sorted(out, key=lambda x: x[0])


def _favourable_peak(stock: AlgoStock, ltp: float) -> float:
    """Best price reached since entry (highest for BUY, lowest for SELL)."""
    entry  = float(stock.entry_price or 0)
    stored = float(stock.peak_price) if getattr(stock, "peak_price", None) else entry
    return max(stored, ltp) if stock.entry_direction == "BUY" else min(stored, ltp)


def _trail_levels(stock: AlgoStock, ltp: float, strategy: AlgoStrategy, trail_steps) -> dict:
    """
    Trailing SL is a RATCHET: it is driven by the best price reached (peak),
    not the current price, so once a step locks in it never moves back.
    """
    entry  = float(stock.entry_price or 0)
    one_r  = entry * float(strategy.sl_pct)
    is_buy = stock.entry_direction == "BUY"
    sign   = 1 if is_buy else -1
    peak   = _favourable_peak(stock, ltp)
    pnl_r  = sign * (ltp  - entry) / one_r
    peak_r = sign * (peak - entry) / one_r
    locked_r = -1.0
    for trig, lock in parse_trail_steps(trail_steps)[::-1]:
        if peak_r >= trig:
            locked_r = lock
            break
    return {
        "entry": entry, "one_r": one_r, "sign": sign,
        "pnl_r": pnl_r, "peak": peak, "peak_r": peak_r, "locked_r": locked_r,
        # SL rounds toward the market (BUY up / SELL down): protective, never riskier than planned
        "sl_price": snap_to_tick(entry + sign * locked_r * one_r, _tick_for(stock.security_id, entry),
                                 "up" if is_buy else "down"),
    }


def _check_exit(stock: AlgoStock, ltp: float, strategy: AlgoStrategy, trail_steps: list) -> tuple[bool, str]:
    entry = float(stock.entry_price or 0)
    one_r = entry * float(strategy.sl_pct)
    if one_r <= 0 or entry <= 0:
        return False, ""

    t = _trail_levels(stock, ltp, strategy, trail_steps)

    # Target: reached now OR reached at any point since entry (e.g. missed during an outage).
    # Compared on the tick-snapped target PRICE (an R-multiple check would miss it after rounding).
    tgt = _target_price(stock, strategy)
    if (stock.entry_direction == "BUY" and t["peak"] >= tgt) or \
       (stock.entry_direction == "SELL" and t["peak"] <= tgt):
        return True, "TARGET"

    if stock.entry_direction == "BUY"  and ltp <= t["sl_price"]: return True, "SL"
    if stock.entry_direction == "SELL" and ltp >= t["sl_price"]: return True, "SL"
    return False, ""


def _target_price(stock: AlgoStock, strategy: AlgoStrategy) -> float:
    entry = float(stock.entry_price or 0)
    one_r = entry * float(strategy.sl_pct)
    is_buy = stock.entry_direction == "BUY"
    sign  = 1 if is_buy else -1
    # Target rounds toward entry (BUY down / SELL up) so the limit level is always tradable
    return snap_to_tick(entry + sign * float(strategy.target_r) * one_r,
                        _tick_for(stock.security_id, entry), "down" if is_buy else "up")


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

# ── Live position helpers (broker-style feel) ─────────────────────────────────

_last_live_log: dict[str, float] = {}   # stock.id → monotonic ts of last live log line
LIVE_LOG_ENTERED_SECS  = 30             # open position: P&L line every 30s
LIVE_LOG_WATCHING_SECS = 60             # waiting stock: LTP vs trigger every 60s


def _live_log_due(key: str, every: float) -> bool:
    import time as _t
    now = _t.monotonic()
    if now - _last_live_log.get(key, 0) >= every:
        _last_live_log[key] = now
        return True
    return False


def position_state(stock: AlgoStock, ltp: float, strategy: AlgoStrategy, trail_steps: list) -> dict:
    """Live numbers for an open position: P&L, R multiple, effective (ratcheted) SL, target."""
    entry = float(stock.entry_price or 0)
    qty   = stock.quantity or 1
    one_r = entry * float(strategy.sl_pct)
    if entry <= 0 or one_r <= 0:
        return {}
    t   = _trail_levels(stock, ltp, strategy, trail_steps)
    pnl = t["sign"] * (ltp - entry) * qty
    tgt = _target_price(stock, strategy)
    return {
        "pnl":       round(pnl, 2),
        "pnl_pct":   round(pnl / (entry * qty) * 100, 2) if qty else 0,
        "pnl_r":     round(t["pnl_r"], 2),
        "peak_r":    round(t["peak_r"], 2),
        "sl_price":  round(t["sl_price"], 2),
        "locked_r":  t["locked_r"],
        "target":    round(tgt, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  MONITOR — one loop for ALL clients, paper + live side by side
# ══════════════════════════════════════════════════════════════════════════════

# Per-client strategy / mode snapshot, refreshed every few seconds (plain values → safe to
# keep after the DB session that loaded them is closed).
_CTX_TTL   = 5.0
_ctx_cache = {"ts": 0.0, "map": {}}


def _load_ctx(db: Session, profile_ids: set) -> dict:
    now    = time.monotonic()
    cached = _ctx_cache["map"]
    if (now - _ctx_cache["ts"]) < _CTX_TTL and profile_ids.issubset(cached.keys()):
        return cached

    strats = {s.client_profile_id: s for s in
              db.query(AlgoStrategy).filter(AlgoStrategy.client_profile_id.in_(profile_ids)).all()}
    profs  = {p.id: p for p in
              db.query(ClientProfile).filter(ClientProfile.id.in_(profile_ids)).all()}
    m: dict = {}
    for pid in profile_ids:
        s = strats.get(pid)
        if s is None:
            m[pid] = None
            continue
        m[pid] = SimpleNamespace(
            sl_pct=s.sl_pct,
            target_r=s.target_r,
            trail=parse_trail_steps(s.trail_sl_steps),
            gap_direction_bias=bool(s.gap_direction_bias),
            missed_guard=bool(s.entry_missed_cancel) if s.entry_missed_cancel is not None else True,
            missed_r=float(s.entry_missed_cancel_r) if s.entry_missed_cancel_r is not None else 1.5,
            paper=_is_paper(profs.get(pid), s),
        )
    _ctx_cache.update(ts=now, map=m)
    return m


@dataclass
class TickResult:
    intents: list = field(default_factory=list)
    active_sids: set = field(default_factory=set)
    open_count: int = 0


class _Book:
    """
    Collects everything one tick changes so it is written with ONE commit:
    per-run log lines (SQL append), per-run counters, per-client daily P&L, live-order intents.
    """

    def __init__(self, db: Session, today: date):
        self.db      = db
        self.today   = today
        self.logs: dict[str, list[str]] = {}
        self.traded: dict[str, int]     = {}
        self.pnl: dict[str, float]      = {}
        self.intents: list[LiveIntent]  = []
        self._daily: dict[str, DailyPnl] = {}

    def log(self, run_id: str, msg: str):
        self.logs.setdefault(run_id, []).append(f"[{_stamp()}] {msg}")
        print(f"[algo] {msg}")

    def add_daily(self, profile_id: str, pnl: float):
        row = self._daily.get(profile_id)
        if row is None:
            row = self.db.query(DailyPnl).filter(
                DailyPnl.client_profile_id == profile_id, DailyPnl.date == self.today
            ).first()
            if row is None:
                row = DailyPnl(client_profile_id=profile_id, date=self.today,
                               closed_pnl=0, running_pnl=0, total_pnl=0, trade_count=0)
                self.db.add(row)
            self._daily[profile_id] = row
        row.closed_pnl  = float(row.closed_pnl or 0) + pnl
        row.total_pnl   = float(row.total_pnl or 0) + pnl
        row.trade_count = (row.trade_count or 0) + 1

    def exit(self, stock: AlgoStock, strat, reason: str, fill: float, ltp: float,
             verify_position: bool = False, when: Optional[datetime] = None, note: str = ""):
        """Close one position (algo bookkeeping) and queue the broker order if it is a real one."""
        pnl   = _calc_pnl(stock, fill)
        entry = float(stock.entry_price)
        one_r = entry * float(strat.sl_pct)
        rr    = round(pnl / (one_r * (stock.quantity or 1)), 2) if one_r > 0 else 0
        live_pos = stock.live_status in ("ENTRY_OK", "ENTRY_PENDING")

        stock.status      = "exited"
        stock.exit_price  = fill
        stock.exit_time   = when or datetime.now(timezone.utc)
        stock.exit_reason = reason
        stock.pnl         = pnl
        self.pnl[stock.run_id] = self.pnl.get(stock.run_id, 0.0) + pnl

        seen = f" (LTP seen ₹{ltp:.2f})" if abs(fill - ltp) > 1e-9 else ""
        self.log(stock.run_id,
                 f"EXIT {stock.symbol} [{reason}] @ ₹{fill:.2f}{seen} "
                 f"| entry=₹{entry:.2f} | P&L=₹{pnl:+.2f} | R:R={rr:+.2f}R{note}")
        self.add_daily(stock.client_profile_id, pnl)

        if live_pos:
            self.intents.append(LiveIntent(
                kind="EXIT", stock_id=stock.id, run_id=stock.run_id, profile_id=stock.client_profile_id,
                symbol=stock.symbol, security_id=str(stock.security_id),
                side="SELL" if stock.entry_direction == "BUY" else "BUY",
                qty=int(stock.quantity or 1), reason=reason, signal_price=ltp,
                verify_position=True))
        elif not strat.paper:
            self.log(stock.run_id,
                     f"LIVE client: {stock.symbol} has no broker position for this trade — exit recorded in the algo only, no order sent")

    def flush(self):
        for rid, n in self.traded.items():
            self.db.execute(update(AlgoRun).where(AlgoRun.id == rid).values(
                stocks_traded=func.coalesce(AlgoRun.stocks_traded, 0) + n))
        for rid, p in self.pnl.items():
            self.db.execute(update(AlgoRun).where(AlgoRun.id == rid).values(
                total_pnl=func.coalesce(AlgoRun.total_pnl, 0) + p))
        for rid, lines in self.logs.items():
            _append_run_log(self.db, rid, lines)
        self.db.commit()


def _process_tick(ltp_map: dict) -> TickResult:
    """
    One monitor pass over every client's watching / entered stock (worker thread).
    Fresh session each tick; one commit; broker orders are returned as intents and sent by
    the caller AFTER the commit, in their own tasks.
    """
    db = SessionLocal()
    try:
        today  = date.today()
        res    = TickResult()
        stocks = (
            db.query(AlgoStock)
            .join(AlgoRun, AlgoRun.id == AlgoStock.run_id)
            .filter(AlgoStock.status.in_(("watching", "entered")), AlgoRun.run_date == today)
            .all()
        )
        if not stocks:
            return res

        # Open positions first — nothing about a pending entry may delay an SL / target exit.
        stocks.sort(key=lambda x: 0 if x.status == "entered" else 1)
        ctx  = _load_ctx(db, {s.client_profile_id for s in stocks})
        book = _Book(db, today)

        for stock in stocks:
            sid = str(stock.security_id)
            res.active_sids.add(sid)
            strat = ctx.get(stock.client_profile_id)
            ltp   = ltp_map.get(sid)
            if not strat or not ltp:
                res.open_count += 1
                continue

            trail_steps = strat.trail

            # ── WATCHING: wait for the trigger ────────────────────────────────
            if stock.status == "watching":
                buy_t  = float(stock.buy_trigger  or 0)
                sell_t = float(stock.sell_trigger or 0)
                if not buy_t and not sell_t:          # first candle not applied yet
                    res.open_count += 1
                    continue

                direction = None
                if not strat.gap_direction_bias:
                    if buy_t and ltp >= buy_t:
                        direction = "BUY"
                    elif sell_t and ltp <= sell_t:
                        direction = "SELL"
                else:
                    if stock.direction == "UP" and buy_t and ltp >= buy_t:
                        direction = "BUY"
                    elif stock.direction == "DOWN" and sell_t and ltp <= sell_t:
                        direction = "SELL"

                if not direction:
                    res.open_count += 1
                    if _live_log_due(f"w-{stock.id}-{stock.run_id}", LIVE_LOG_WATCHING_SECS):
                        buy_txt  = f"BUY ≥₹{buy_t:.2f} ({(buy_t - ltp) / ltp * 100:+.2f}% away)"  if buy_t  else "BUY n/a"
                        sell_txt = f"SELL ≤₹{sell_t:.2f} ({(ltp - sell_t) / ltp * 100:+.2f}% away)" if sell_t else "SELL n/a"
                        book.log(stock.run_id, f"WATCH {stock.symbol} LTP=₹{ltp:.2f} | {buy_txt} | {sell_txt}")
                    continue

                # Missed-move guard: price is already far beyond the trigger when we first
                # act (late start / outage) → the setup is gone; do not chase it.
                trig   = buy_t if direction == "BUY" else sell_t
                moved  = (ltp - trig) if direction == "BUY" else (trig - ltp)
                one_rt = trig * float(strat.sl_pct)
                moved_r = moved / one_rt if one_rt > 0 else 0.0
                if strat.missed_guard and moved_r > strat.missed_r:
                    stock.status      = "cancelled"
                    stock.exit_reason = "ENTRY_MISSED"
                    book.log(stock.run_id,
                             f"SKIP {stock.symbol}: {direction} trigger ₹{trig:.2f} is already {moved_r:.1f}R behind "
                             f"LTP ₹{ltp:.2f} (missed-move guard {strat.missed_r:g}R) — entry cancelled")
                    continue

                # ── ENTER ─────────────────────────────────────────────────────
                stock.entry_direction = direction
                stock.status          = "entered"
                stock.entry_price     = ltp
                stock.peak_price      = ltp
                stock.entry_time      = datetime.now(timezone.utc)
                book.traded[stock.run_id] = book.traded.get(stock.run_id, 0) + 1
                sl_price  = _trail_levels(stock, ltp, strat, trail_steps)["sl_price"]
                tgt_price = _target_price(stock, strat)
                qty       = int(stock.quantity or 1)
                if strat.paper:
                    mode_label, order_note = "PAPER", ""
                else:
                    mode_label, order_note = "LIVE", " | MARKET order → Dhan"
                    stock.live_status = "ENTRY_PENDING"
                    book.intents.append(LiveIntent(
                        kind="ENTRY", stock_id=stock.id, run_id=stock.run_id,
                        profile_id=stock.client_profile_id, symbol=stock.symbol,
                        security_id=sid, side=direction, qty=qty, signal_price=ltp))
                book.log(stock.run_id,
                         f"{mode_label} {direction} {stock.symbol} @ ₹{ltp:.2f} "
                         f"| trigger=₹{trig:.2f} | SL=₹{sl_price:.2f} | target=₹{tgt_price:.2f} "
                         f"| qty={qty}{order_note}")
                res.open_count += 1
                continue

            # ── ENTERED: ratchet peak, check SL / target ──────────────────────
            if stock.entry_price and stock.entry_direction:
                new_peak = _favourable_peak(stock, ltp)
                if stock.peak_price is None or float(stock.peak_price) != new_peak:
                    stock.peak_price = new_peak
                should_exit, reason = _check_exit(stock, ltp, strat, trail_steps)
                if not should_exit:
                    res.open_count += 1
                    if _live_log_due(f"e-{stock.id}-{stock.run_id}", LIVE_LOG_ENTERED_SECS):
                        st = position_state(stock, ltp, strat, trail_steps)
                        if st:
                            tag = "LIVE" if stock.live_status in ("ENTRY_OK", "ENTRY_PENDING") else "OPEN"
                            book.log(stock.run_id,
                                     f"{tag} {stock.entry_direction} {stock.symbol} LTP=₹{ltp:.2f} "
                                     f"| entry=₹{float(stock.entry_price):.2f} | P&L=₹{st['pnl']:+.2f} "
                                     f"({st['pnl_r']:+.2f}R) | SL=₹{st['sl_price']:.2f} | target=₹{st['target']:.2f}")
                    continue

                # TARGET behaves like a resting limit order: it fills AT the target price even
                # if our tick only sees the price after it ran past it (paper). SL is a
                # stop-market: fills at the prevailing LTP. Live positions get a market order,
                # so their recorded price is LTP (the real fill is stored in live_exit_price).
                live_pos = stock.live_status in ("ENTRY_OK", "ENTRY_PENDING")
                fill = ltp
                if reason == "TARGET" and not live_pos:
                    fill = _target_price(stock, strat)
                book.exit(stock, strat, reason, fill, ltp)
                continue

            res.open_count += 1

        book.flush()
        res.intents = book.intents
        return res
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()


def _active_sids_sync() -> set:
    db = SessionLocal()
    try:
        rows = (
            db.query(AlgoStock.security_id)
            .join(AlgoRun, AlgoRun.id == AlgoStock.run_id)
            .filter(AlgoStock.status.in_(("watching", "entered")), AlgoRun.run_date == date.today())
            .distinct().all()
        )
        return {str(r[0]) for r in rows}
    finally:
        db.close()


# ── Prices ────────────────────────────────────────────────────────────────────

_last_rest_ltp_ts: float = 0.0
REST_MIN_GAP = 1.5
_last_price_warn = 0.0


async def _gather_prices(sec_ids: list[str], jwt: str, api_key: str, client_id: str) -> dict[str, float]:
    """WebSocket cache first (instant); REST only tops up what is missing (≥1.5s apart)."""
    global _last_rest_ltp_ts, _last_price_warn
    ltp = get_live_prices(sec_ids, max_age=5) if is_ws_connected() else {}
    missing = [s for s in sec_ids if s not in ltp]
    if missing and (time.monotonic() - _last_rest_ltp_ts) >= REST_MIN_GAP:
        _last_rest_ltp_ts = time.monotonic()
        try:
            rest = await asyncio.wait_for(angel_fetch_ltp(jwt, api_key, client_id, missing), 6.0)
            for k, v in (rest or {}).items():
                ltp.setdefault(str(k), v)
        except Exception as e:
            if time.monotonic() - _last_price_warn > 10:
                _last_price_warn = time.monotonic()
                print(f"[algo] REST LTP fallback failed: {type(e).__name__}: {e}")
    return ltp


async def _ws_creds() -> tuple[str, str, str, str]:
    """(jwt, feed_token, client_id, api_key) for the WebSocket — re-read on every reconnect."""
    try:
        await _fresh_master_token()          # refreshes the stored jwt + feed token when due
    except Exception as e:
        print(f"[algo] ws creds: master token refresh failed: {type(e).__name__}: {e}")

    def _read():
        from app.models.master_account import MasterDataAccount
        db = SessionLocal()
        try:
            acc = db.query(MasterDataAccount).first()
            if not acc:
                return "", "", "", ""
            jwt = decrypt(acc.angel_jwt_token) if acc.angel_jwt_token else ""
            cid = decrypt(acc.angel_client_id) if acc.angel_client_id else ""
            key = decrypt(acc.angel_api_key)   if acc.angel_api_key   else ""
            return jwt, (acc.angel_feed_token or ""), cid, key
        finally:
            db.close()

    return await asyncio.to_thread(_read)


# ── EOD force exit ────────────────────────────────────────────────────────────

def _eod_collect_sync() -> list[str]:
    return sorted(_active_sids_sync())


def _eod_apply_sync(ltp_map: dict) -> list[LiveIntent]:
    db = SessionLocal()
    try:
        today  = date.today()
        stocks = (
            db.query(AlgoStock)
            .join(AlgoRun, AlgoRun.id == AlgoStock.run_id)
            .filter(AlgoStock.status.in_(("watching", "entered")), AlgoRun.run_date == today)
            .all()
        )
        if not stocks:
            return []
        ctx  = _load_ctx(db, {s.client_profile_id for s in stocks})
        book = _Book(db, today)
        for stock in stocks:
            strat = ctx.get(stock.client_profile_id) or SimpleNamespace(sl_pct=0.004, paper=True)
            if stock.status == "entered" and stock.entry_price and stock.entry_direction:
                ltp = ltp_map.get(str(stock.security_id)) or float(stock.entry_price)
                book.exit(stock, strat, "EOD", ltp, ltp, verify_position=True)
            else:
                stock.status      = "cancelled"
                stock.exit_reason = "NO_ENTRY"
                book.log(stock.run_id, f"EOD: {stock.symbol} never triggered — closed without a trade")
        book.flush()
        return book.intents
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()


async def _force_exit_all(jwt: str, api_key: str, client_id: str):
    sids = await asyncio.to_thread(_eod_collect_sync)
    if not sids:
        return
    ltp_map = get_live_prices(sids, max_age=15) if is_ws_connected() else {}
    missing = [s for s in sids if s not in ltp_map]
    if missing:
        try:
            rest = await asyncio.wait_for(angel_fetch_ltp(jwt, api_key, client_id, missing), 12)
            for k, v in (rest or {}).items():
                ltp_map.setdefault(str(k), v)
        except Exception as e:
            print(f"[algo] EOD REST LTP failed: {type(e).__name__}: {e}")
    intents = await asyncio.to_thread(_eod_apply_sync, ltp_map)
    _dispatch_live(intents)


# ── Heartbeat / finish ────────────────────────────────────────────────────────

def _heartbeat_sync(run_ids: list[str], elapsed_min: int):
    db = SessionLocal()
    try:
        for rid in run_ids:
            n = db.query(AlgoStock).filter(
                AlgoStock.run_id == rid, AlgoStock.status.in_(("watching", "entered"))
            ).count()
            _append_run_log(db, rid, [f"[{_stamp()}] Heartbeat: {elapsed_min}min elapsed | {n} position(s) open"])
        db.commit()
    except Exception as e:
        print(f"[algo] heartbeat failed: {type(e).__name__}: {e}")
        db.rollback()
    finally:
        db.close()


def _finish_runs_sync(run_ids: list[str]):
    db = SessionLocal()
    try:
        for rid in run_ids:
            row = db.query(AlgoRun.total_pnl, AlgoRun.stocks_scanned,
                           AlgoRun.stocks_selected, AlgoRun.stocks_traded).filter(AlgoRun.id == rid).first()
            if not row:
                continue
            winners = db.query(AlgoStock).filter(
                AlgoStock.run_id == rid, AlgoStock.status == "exited", AlgoStock.pnl > 0).count()
            losers = db.query(AlgoStock).filter(
                AlgoStock.run_id == rid, AlgoStock.status == "exited", AlgoStock.pnl <= 0).count()
            sep = f"{'─' * 10} RUN COMPLETE {'─' * 10}"
            _append_run_log(db, rid, [
                f"[{_stamp()}] {sep}",
                f"[{_stamp()}] Total P&L: ₹{float(row[0] or 0):.2f}",
                f"[{_stamp()}] Stocks: scanned={row[1]} | selected={row[2]} | traded={row[3]}",
                f"[{_stamp()}] Trades: {winners} winners | {losers} losers | "
                f"Win rate: {round(winners / (winners + losers) * 100, 1) if (winners + losers) > 0 else 0}%",
            ])
            db.execute(update(AlgoRun).where(AlgoRun.id == rid).values(
                status="done", finished_at=datetime.now(timezone.utc)))
        db.commit()
    except Exception as e:
        print(f"[algo] finish_runs failed: {type(e).__name__}: {e}")
        db.rollback()
    finally:
        db.close()


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




# ── Outage recovery (used on resume) ──────────────────────────────────────────

def replay_first_exit(entry_price: float, direction: str, peak_price: Optional[float],
                      security_id: str, strat, candles: list[dict]) -> dict:
    """
    Walk 1-min candles (oldest first) after an entry and find the FIRST SL / target breach.
    Order inside a candle is unknown, so it is replayed pessimistically:
    open → adverse extreme → favourable extreme → close (an SL breach is seen before a target).
    Returns {"reason": "SL"|"TARGET"|None, "price": float, "time": str, "peak": float}.
    """
    is_buy = direction == "BUY"
    fs = SimpleNamespace(entry_price=entry_price, entry_direction=direction,
                         peak_price=peak_price or entry_price, security_id=security_id)
    for c in candles:
        adverse = c["low"]  if is_buy else c["high"]
        fav     = c["high"] if is_buy else c["low"]
        for px in (c["open"], adverse, fav, c["close"]):
            fs.peak_price = _favourable_peak(fs, px)
            hit, reason = _check_exit(fs, px, strat, strat.trail)
            if hit:
                price = _target_price(fs, strat) if reason == "TARGET" \
                    else _trail_levels(fs, px, strat, strat.trail)["sl_price"]
                return {"reason": reason, "price": price, "time": c["timestamp"], "peak": fs.peak_price}
    return {"reason": None, "price": 0.0, "time": "", "peak": fs.peak_price}


def _recovery_candidates_sync(run_ids: list[str]) -> list[dict]:
    db = SessionLocal()
    try:
        out = []
        rows = db.query(AlgoStock).filter(
            AlgoStock.run_id.in_(run_ids), AlgoStock.status == "entered").all()
        for s in rows:
            if not (s.entry_price and s.entry_direction and s.entry_time):
                continue
            et = s.entry_time if s.entry_time.tzinfo else s.entry_time.replace(tzinfo=timezone.utc)
            out.append({
                "stock_id": s.id, "sid": str(s.security_id), "entry_time": et.astimezone(IST),
            })
        return out
    finally:
        db.close()


def _recovery_apply_sync(run_ids: list[str], candles_by_sid: dict, ltp_map: dict) -> int:
    """Apply the replay to every open position. Returns how many were closed."""
    db = SessionLocal()
    try:
        today = date.today()
        stocks = db.query(AlgoStock).filter(
            AlgoStock.run_id.in_(run_ids), AlgoStock.status == "entered").all()
        if not stocks:
            return 0
        ctx  = _load_ctx(db, {s.client_profile_id for s in stocks})
        book = _Book(db, today)
        closed = 0
        for s in stocks:
            strat   = ctx.get(s.client_profile_id)
            candles = candles_by_sid.get(str(s.security_id)) or []
            if not strat or not candles or not (s.entry_price and s.entry_direction and s.entry_time):
                continue
            et = s.entry_time if s.entry_time.tzinfo else s.entry_time.replace(tzinfo=timezone.utc)
            # candles that start after the entry minute
            after = []
            for c in candles:
                try:
                    ct = datetime.fromisoformat(str(c["timestamp"]))
                    if ct.tzinfo is None:
                        ct = ct.replace(tzinfo=IST)
                except Exception:
                    continue
                if ct > et.astimezone(IST).replace(second=0, microsecond=0):
                    after.append(c)
            if not after:
                continue
            r = replay_first_exit(float(s.entry_price), s.entry_direction,
                                  float(s.peak_price) if s.peak_price else None,
                                  str(s.security_id), strat, after)
            live_pos = s.live_status in ("ENTRY_OK", "ENTRY_PENDING")
            if r["reason"]:
                hh = str(r["time"])[11:16]
                if live_pos:
                    # a REAL position: the breach is in the past, the only honest action is to close it now
                    ltp = ltp_map.get(str(s.security_id)) or float(after[-1]["close"])
                    book.exit(s, strat, r["reason"], ltp, ltp,
                              note=f" | RECOVERED: {r['reason']} level was breached at {hh} while the engine was down — closing at market now")
                else:
                    # paper: book it where the rule would have closed it
                    ltp = ltp_map.get(str(s.security_id)) or r["price"]
                    try:
                        when = datetime.fromisoformat(str(r["time"])).astimezone(timezone.utc)
                    except Exception:
                        when = None
                    book.exit(s, strat, r["reason"], r["price"], ltp, when=when,
                              note=f" | RECOVERED: {r['reason']} was breached at {hh} while the engine was down — booked at the rule price")
                closed += 1
            else:
                # no breach — but keep the best price reached during the outage (trail ratchet)
                if r["peak"] and (s.peak_price is None or float(s.peak_price) != r["peak"]):
                    s.peak_price = r["peak"]
        book.flush()
        _dispatch_later.extend(book.intents)
        return closed
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()


_dispatch_later: list = []        # intents produced in worker threads, dispatched on the event loop


async def _recover_outage(run_ids: list[str], jwt: str, api_key: str, client_id: str):
    """
    The engine was down for a while (crash / redeploy). Positions that touched SL or target in
    the meantime are closed now; the rest get their trailing-SL peak restored.
    """
    cands = await asyncio.to_thread(_recovery_candidates_sync, run_ids)
    now   = datetime.now(IST)
    cands = [c for c in cands if (now - c["entry_time"]).total_seconds() > 120]
    if not cands:
        return
    sids = sorted({c["sid"] for c in cands})
    earliest = min(c["entry_time"] for c in cands)
    from_dt = earliest.strftime("%Y-%m-%d %H:%M")
    to_dt   = now.strftime("%Y-%m-%d %H:%M")

    candles_by_sid: dict[str, list] = {}
    for sid in sids:
        try:
            candles_by_sid[sid] = await asyncio.wait_for(
                angel_fetch_candle(jwt, api_key, client_id, sid, from_dt, to_dt), 15)
        except Exception as e:
            print(f"[algo] recovery candles {sid} failed: {type(e).__name__}: {e}")

    ltp_map = await _gather_prices(sids, jwt, api_key, client_id)
    closed = await asyncio.to_thread(_recovery_apply_sync, run_ids, candles_by_sid, ltp_map)
    if _dispatch_later:
        pending = list(_dispatch_later)
        _dispatch_later.clear()
        _dispatch_live(pending)
    print(f"[algo] Outage recovery: {closed} position(s) closed from {len(cands)} checked")


# ── Monitor supervisor ────────────────────────────────────────────────────────

_algo_running = False             # guard against double-runs (also set while resumed)
_monitor_last_tick: float = 0.0
_monitor_active = False
_monitor_started_at: Optional[datetime] = None
_monitor_errors = 0


async def _monitor_and_finish(run_ids: list[str], now_ist: datetime, background: list = ()):
    """
    Monitor loop (every 0.5s until 3:20 PM) → EOD force exit → run summary.
    Shared by the normal daily run and by resume-after-restart.

    This loop is built NOT to die: every tick runs on its own DB session and any exception is
    logged and survived. (The old loop shared one long-lived session; one DB error poisoned it,
    the error handler's own log-write then raised, and the engine stopped silently at 9:16:40
    with positions still open.)
    """
    global _monitor_last_tick, _monitor_active, _monitor_started_at, _monitor_errors
    _monitor_active     = True
    _monitor_started_at = datetime.now(IST)

    try:
        await asyncio.to_thread(_log_runs_sync, run_ids, f"{'─' * 10} STEP 4: MONITORING {'─' * 10}")
        await asyncio.to_thread(
            _log_runs_sync, run_ids,
            "Monitor loop started — entries / SL / target checked every 0.5s until 3:20 PM (paper + live in one loop)")

        eod_ist = datetime.now(IST).replace(hour=15, minute=20, second=0, microsecond=0)
        FAST_TICK, REST_TICK, TOKEN_REFRESH_SECS = 0.5, 1.5, 120
        jwt = api_key = master_cid = ""
        last_token_ts = 0.0
        last_hb_ts    = time.monotonic()
        last_err_log  = 0.0
        active_sids: Optional[set] = None
        consec_err    = 0

        while datetime.now(IST) < eod_ist:
            t0 = time.monotonic()
            _monitor_last_tick = t0
            try:
                if not jwt or t0 - last_token_ts > TOKEN_REFRESH_SECS:
                    jwt, api_key, master_cid = await asyncio.wait_for(_fresh_master_token(), 30)
                    last_token_ts = t0
                if active_sids is None:
                    active_sids = await asyncio.wait_for(asyncio.to_thread(_active_sids_sync), 15)

                ltp_map = await _gather_prices(sorted(active_sids), jwt, api_key, master_cid) if active_sids else {}
                if ltp_map or not active_sids:
                    res = await asyncio.wait_for(asyncio.to_thread(_process_tick, ltp_map), 20)
                    active_sids = res.active_sids
                    if res.intents:
                        _dispatch_live(res.intents)
                    if res.open_count == 0:
                        await asyncio.to_thread(
                            _log_runs_sync, run_ids, "All positions closed — stopping monitor loop early")
                        break
                consec_err = 0

            except asyncio.CancelledError:
                raise
            except Exception as e:
                consec_err += 1
                _monitor_errors += 1
                last_token_ts = 0.0          # force a token re-read
                active_sids   = None         # force a re-query
                if time.monotonic() - last_err_log > 5:
                    last_err_log = time.monotonic()
                    print(f"[algo] Monitor tick error ({consec_err}x): {type(e).__name__}: {e}\n{traceback.format_exc()}")
                    try:
                        await asyncio.wait_for(asyncio.to_thread(
                            _log_runs_sync, run_ids,
                            f"Monitor tick error ({consec_err}x): {type(e).__name__}: {e} — retrying"), 10)
                    except Exception:
                        pass
                await asyncio.sleep(min(2.0, 0.25 * consec_err))
                continue

            if time.monotonic() - last_hb_ts >= 300:
                last_hb_ts = time.monotonic()
                elapsed = int((datetime.now(IST) - now_ist).total_seconds() // 60)
                print(f"[algo] Heartbeat: {elapsed}min elapsed | ws={ws_stats()}")
                try:
                    await asyncio.wait_for(asyncio.to_thread(_heartbeat_sync, run_ids, elapsed), 15)
                except Exception:
                    pass

            # Sleep only the remainder of the interval (processing time counts)
            interval = FAST_TICK if is_ws_connected() else REST_TICK / 3
            await asyncio.sleep(max(0.05, interval - (time.monotonic() - t0)))

        # candle workers must be finished before the EOD sweep
        for t in background:
            try:
                await asyncio.wait_for(asyncio.shield(t), 60)
            except Exception:
                pass

        # ── EOD force exit ───────────────────────────────────────────────
        try:
            await asyncio.to_thread(_log_runs_sync, run_ids, f"{'─' * 10} STEP 5: EOD FORCE EXIT (3:20 PM) {'─' * 10}")
            try:
                jwt, api_key, master_cid = await asyncio.wait_for(_fresh_master_token(), 30)
            except Exception as e:
                print(f"[algo] EOD token refresh failed: {type(e).__name__}: {e}")
            await _force_exit_all(jwt, api_key, master_cid)
        except Exception as e:
            print(f"[algo] EOD force exit error: {type(e).__name__}: {e}\n{traceback.format_exc()}")

        await _wait_live_tasks(45)
        await asyncio.to_thread(_finish_runs_sync, run_ids)
    finally:
        _monitor_active = False


async def resume_monitoring_if_needed() -> bool:
    """
    Called at server start AND by the watchdog. If today's run died while monitoring (deploy,
    crash, error) nothing would be watching open positions — SL / target / trailing would silently
    stop. Pick them back up from the DB, recover anything that was breached while we were down,
    re-derive triggers that never got written, and carry on. Returns True if monitoring ran.
    """
    global _algo_running
    if _algo_running:
        return False
    now = datetime.now(IST)
    if now.weekday() >= 5 or now < now.replace(hour=9, minute=16, second=0, microsecond=0):
        return False

    def _plan():
        db = SessionLocal()
        try:
            today = date.today()
            runs = db.query(AlgoRun.id, AlgoRun.status).filter(
                AlgoRun.run_date == today, AlgoRun.status.in_(("running", "error"))).all()
            if not runs:
                return None
            run_ids = [r[0] for r in runs]
            open_rows = db.query(AlgoStock.id, AlgoStock.security_id, AlgoStock.status,
                                 AlgoStock.buy_trigger, AlgoStock.sell_trigger).filter(
                AlgoStock.run_id.in_(run_ids), AlgoStock.status.in_(("watching", "entered"))).all()
            if not open_rows:
                return None
            missing: dict[str, list[str]] = {}
            for sid_row, sec, st, bt, sl in open_rows:
                if st == "watching" and bt is None and sl is None:
                    missing.setdefault(str(sec), []).append(sid_row)
            return {"run_ids": run_ids, "sec_ids": sorted({str(r[1]) for r in open_rows}), "missing": missing}
        finally:
            db.close()

    took_lock = False
    try:
        plan = await asyncio.to_thread(_plan)
        if not plan or _algo_running:
            return False
        _algo_running = True
        took_lock = True

        run_ids = plan["run_ids"]
        print(f"[algo] RESUME — {len(plan['sec_ids'])} open/watching stock(s), {len(run_ids)} run(s)")
        jwt, api_key, master_cid = await _fresh_master_token()

        await ensure_ws_running(_ws_creds, plan["sec_ids"])
        print(f"[algo] WebSocket (resume): {ws_stats()}")

        def _mark():
            db = SessionLocal()
            try:
                db.execute(update(AlgoRun).where(AlgoRun.id.in_(run_ids), AlgoRun.status == "error")
                           .values(status="running"))
                db.commit()
            finally:
                db.close()
        await asyncio.to_thread(_mark)
        await asyncio.to_thread(_log_runs_sync, run_ids, f"{'─' * 10} RESUMED {'─' * 10}")
        await asyncio.to_thread(_log_runs_sync, run_ids,
                                "Engine was not monitoring (restart / crash) — resumed; checking for SL / target hits while it was down")

        await _recover_outage(run_ids, jwt, api_key, master_cid)

        tasks = []
        for sid, stock_ids in plan["missing"].items():
            tasks.append(asyncio.create_task(_candle_worker(sid, stock_ids, jwt, api_key, master_cid)))

        await _monitor_and_finish(run_ids, now, background=tasks)
        return True
    except Exception as e:
        print(f"[algo] Resume error: {type(e).__name__}: {e}")
        traceback.print_exc()
        return False
    finally:
        if took_lock:
            _algo_running = False


async def engine_watchdog():
    """
    Every 15s during market hours: if the engine is NOT running but today's run still has open
    / watching stocks (it crashed), restart monitoring. Complements the on-boot resume.
    """
    await asyncio.sleep(20)
    while True:
        try:
            now   = datetime.now(IST)
            start = now.replace(hour=9, minute=16, second=30, microsecond=0)
            end   = now.replace(hour=15, minute=20, second=0, microsecond=0)
            if now.weekday() < 5 and start <= now < end and not _algo_running:
                if await resume_monitoring_if_needed():
                    print("[watchdog] monitoring was restarted and has now finished")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[watchdog] error: {type(e).__name__}: {e}")
        await asyncio.sleep(15)


async def catchup_run_if_needed():
    """
    Server (re)started on a trading morning AFTER the 8:00 scheduler slot but before the
    opening scan (9:12): the scheduler already scheduled "tomorrow", so today's run would never
    start. Start it now (run_daily_algo waits for 8:45 / 9:12:30 / 9:16 on its own).
    """
    global _algo_running
    await asyncio.sleep(8)
    now = datetime.now(IST)
    if now.weekday() >= 5 or _algo_running:
        return
    if not (now.replace(hour=8, minute=0, second=0, microsecond=0) <= now < now.replace(hour=9, minute=12, second=0, microsecond=0)):
        return

    def _has_run():
        db = SessionLocal()
        try:
            return db.query(AlgoRun).filter(
                AlgoRun.run_date == date.today(), AlgoRun.status.in_(("running", "done"))).count() > 0
        finally:
            db.close()

    if await asyncio.to_thread(_has_run):
        return
    print("[algo] CATCH-UP: server started after 8:00 AM — starting today's run now")

    # Live clients' Dhan tokens are normally refreshed by the 8:00 job we just missed
    try:
        from app.services.token_manager import _refresh_one_profile
        def _live_profiles():
            db = SessionLocal()
            try:
                return [p for p, s in _subscribed_clients(db) if not _is_paper(p, s)]
            finally:
                db.close()
        for p in await asyncio.to_thread(_live_profiles):
            db = SessionLocal()
            try:
                prof = db.query(ClientProfile).filter(ClientProfile.id == p.id).first()
                if prof:
                    await _refresh_one_profile(prof, db)
            finally:
                db.close()
    except Exception as e:
        print(f"[algo] catch-up live token refresh failed: {type(e).__name__}: {e}")

    await run_daily_algo()


async def trigger_or_resume() -> str:
    """Manual 'Trigger Run Now': before 9:16 start the normal run; after 9:16 restart monitoring."""
    now = datetime.now(IST)
    if now >= now.replace(hour=9, minute=16, second=0, microsecond=0):
        if _algo_running:
            return "engine already running"
        ok = await resume_monitoring_if_needed()
        return "monitoring resumed" if ok else "nothing to resume (no open positions / run not started today)"
    await run_daily_algo()
    return "run started"


def engine_status() -> dict:
    now = time.monotonic()
    return {
        "running":              _algo_running,
        "monitor_active":       _monitor_active,
        "monitor_started_at":   _monitor_started_at.isoformat() if _monitor_started_at else None,
        "last_tick_ago_secs":   round(now - _monitor_last_tick, 1) if _monitor_last_tick else None,
        "tick_errors":          _monitor_errors,
        "live_orders_in_flight": len([t for t in _live_tasks if not t.done()]),
        "ws":                   ws_stats(),
    }



def _log_mode(run: AlgoRun, profile: ClientProfile, strategy: AlgoStrategy, db: Session):
    """Say clearly in the client's log whether this run places REAL orders, and why not if it can't."""
    paper = _is_paper(profile, strategy)
    if paper:
        _log(run, "Mode: PAPER — simulated trades only, no orders are sent", db)
    else:
        _log(run, "Mode: LIVE — real MARKET orders will be sent to Dhan", db)
        problems = []
        cred = profile.dhan_cred
        if not cred:
            problems.append("no Dhan credentials saved")
        elif not token_is_valid(profile):
            problems.append("Dhan access token is expired — will be refreshed (TOTP) when the first order is sent")
        px = profile.proxy_setting
        if not px or not px.is_active:
            problems.append("no static-IP proxy configured — Dhan will reject orders that don't come from the registered IP")
        for pr in problems:
            _log(run, f"LIVE pre-flight warning: {pr}", db)
    if profile.paper_trading != strategy.paper_trading:
        _log(run, f"Note: profile flag paper_trading={profile.paper_trading} differs from algo flag "
                  f"paper_trading={strategy.paper_trading} — the ALGO flag (Clients page badge) is used", db)



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
            _log_mode(run, profile, strategy, db)
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
            await ensure_ws_running(_ws_creds, sid_list)
            print(f"[algo] WebSocket: {ws_stats()}")
        else:
            print("[algo] Skipping WebSocket — no feed token")

        # ══════════════════════════════════════════════════════════════════
        # CENTRAL MARKET DATA STEPS — run ONCE for all subscribers
        # Steps 1–2 are master-account operations; no per-client logging
        # ══════════════════════════════════════════════════════════════════

        # ── STEP 1: 8:45 AM — prev close snapshot → DB ─────────────────
        print("[algo] ─── STEP 1: PREV CLOSE SNAPSHOT (8:45 AM) ───")
        await _wait_until(8, 45, 0, "prev close snapshot")
        jwt, api_key, master_client_id = await _fresh_master_token()

        existing = db.query(DailyPriceSnapshot).filter(
            DailyPriceSnapshot.trade_date == date.today(),
            DailyPriceSnapshot.prev_close != None,
        ).count()

        if existing < len(combined_universe) * 0.9:
            print(f"[algo] Fetching prev close LTP for {len(combined_universe)} stocks...")
            saved = await _fetch_and_save_prev_close(jwt, api_key, master_client_id, combined_universe, db)
            print(f"[algo] Prev close snapshot: {saved}/{len(combined_universe)} stocks saved")
            if saved < len(combined_universe) * 0.9:
                print(f"[algo] WARNING: Only {saved} of {len(combined_universe)} returned LTP")
        else:
            print(f"[algo] Prev close already in DB ({existing} rows) — skipping fetch")

        # Log once to each client's run (summary only)
        for _, (run, _) in runs.items():
            _log_separator(run, "STEP 1: PREV CLOSE SNAPSHOT", db)
            _log(run, f"Master: prev close snapshot done — {max(existing, 0)} stocks in DB", db)

        # ── STEP 2: 9:12:30 — opening price → DB → compute gaps ────────
        print("[algo] ─── STEP 2: OPENING PRICE SCAN (9:12:30) ───")
        await _keep_token_alive_until(9, 12, 30, jwt, api_key, master_client_id)
        jwt, api_key, master_client_id = await _fresh_master_token()

        print(f"[algo] Fetching opening LTP for {len(combined_universe)} stocks...")
        open_saved = await _fetch_and_save_open_price(jwt, api_key, master_client_id, combined_universe, db)

        # Load gap results from DB (single source of truth)
        all_gaps = _load_gaps_from_db(combined_universe, db)

        bands = {"0–1%": 0, "1–3%": 0, "3–8%": 0, "8%+": 0}
        for d in all_gaps.values():
            g = abs(d["gap_pct"])
            if g < 1:   bands["0–1%"] += 1
            elif g < 3: bands["1–3%"] += 1
            elif g < 8: bands["3–8%"] += 1
            else:       bands["8%+"]  += 1
        print(f"[algo] Opening prices: {open_saved} stocks | Gap distribution: {bands}")

        # Log once to each client's run (summary only)
        for _, (run, _) in runs.items():
            _log_separator(run, "STEP 2: OPENING PRICE SCAN", db)
            _log(run, f"Master: {open_saved} opening prices fetched | Gap computed for {len(all_gaps)} stocks", db)
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

        # ── STEP 4: 9:16:00 first candle ───────────────────────────────
        # ONE candle fetch per unique security_id, ALL in parallel; the monitor loop starts
        # immediately and each stock becomes tradable the moment its own candle lands.
        print("[algo] ─── STEP 3: FIRST CANDLE (9:16:00) ───")
        await _wait_until(9, 16, 0, "first candle")
        jwt, api_key, master_client_id = await _fresh_master_token()

        all_watching: dict[str, list[str]] = {}        # security_id → [AlgoStock.id, ...]
        for profile, _ in clients:
            run, strat = runs[profile.id]
            run.status = "running"
            db.commit()
            for stock in db.query(AlgoStock).filter(
                AlgoStock.run_id == run.id, AlgoStock.status == "watching"
            ).all():
                all_watching.setdefault(stock.security_id, []).append(stock.id)

        print(f"[algo] Fetching first candle for {len(all_watching)} unique stocks in parallel (across all clients)...")
        candle_tasks = [
            asyncio.create_task(_candle_worker(sid, ids, jwt, api_key, master_client_id))
            for sid, ids in all_watching.items()
        ]

        run_ids = [runs[p.id][0].id for p, _ in clients]
        # (the monitor works on its own short-lived sessions — this one stays idle until the end)

        await _monitor_and_finish(run_ids, now_ist, background=candle_tasks)

        print("[algo] ═══ Daily run complete ═══")

    except Exception as e:
        print(f"[algo] FATAL ERROR: {e}")
        import traceback; traceback.print_exc()
        try:
            db.rollback()
        except Exception:
            pass
        for _, (run, _) in (runs.items() if runs else []):
            try:
                _log(run, f"FATAL ERROR: {type(e).__name__}: {e}", db)
                db.execute(update(AlgoRun).where(AlgoRun.id == run.id).values(status="error"))
                db.commit()
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
    finally:
        _algo_running = False
        db.close()
