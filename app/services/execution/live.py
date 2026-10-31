"""
LIVE route: real orders on Dhan (https://dhanhq.co/docs/v2/).

Every call goes through the client's static-IP proxy (SEBI / Dhan requirement for
order placement). Each order runs in its own background task, so a slow or failing
Dhan call can never delay the shared monitor loop or any other client.

Safety rules kept from the engine:
  • correlationId per stock + leg — after a network error the order is looked up
    before any retry, so it is never placed twice
  • expired token → one TOTP refresh, then retry
  • an EXIT is sent only if the ENTRY really went through at the broker, and never
    for more than the broker's net position (no accidental fresh short / long)
  • a failed ENTRY cancels that client's trade; a failed EXIT is flagged loudly
"""

import asyncio, traceback
from datetime import date
from typing import Optional
from sqlalchemy import update, func, case

from app.core.database import EngineSessionLocal as SessionLocal
from app.core.encryption import decrypt
from app.models.trading import ClientProfile, AlgoRun, AlgoStock, DailyPnl
from app.services.dhan_trade import (
    place_order, get_order_by_id, get_positions, find_order_by_correlation, normalize_order,
)
from app.services.token_manager import (
    token_is_valid, get_stored_token, refresh_token, _get_proxy_kwargs,
)
from app.services.execution import OrderIntent
from app.services import algo_engine as engine     # shared run-log helpers (resolved at call time)


def preflight_problems(profile: ClientProfile) -> list[str]:
    """Why live orders for this client might fail — written to the client's log at run start."""
    problems = []
    if not profile.dhan_cred:
        problems.append("no Dhan credentials saved")
    elif not token_is_valid(profile):
        problems.append("Dhan access token is expired — will be refreshed (TOTP) when the first order is sent")
    px = profile.proxy_setting
    if not px or not px.is_active:
        problems.append("no static-IP proxy configured — Dhan will reject orders that don't come from the registered IP")
    return problems

_live_tasks: set = set()
_entry_tasks: dict[str, asyncio.Task] = {}
LIVE_ORDER_TIMEOUT   = 12.0
LIVE_ORDER_ATTEMPTS  = 3


def _corr_id(stock_id: str, kind: str) -> str:
    """≤24 chars, unique per stock+leg — lets us find the order again after a network error."""
    return stock_id.replace("-", "")[:23] + ("E" if kind == "ENTRY" else "X")


def submit(it: OrderIntent):
    """Fire-and-forget: the order runs in its own task (an entry always completes before its exit)."""
    prior = _entry_tasks.get(it.stock_id) if it.kind == "EXIT" else None
    task = asyncio.create_task(_run_live_intent(it, prior))
    _live_tasks.add(task)
    task.add_done_callback(_live_tasks.discard)
    if it.kind == "ENTRY":
        _entry_tasks[it.stock_id] = task
        task.add_done_callback(lambda _t, sid=it.stock_id: _entry_tasks.pop(sid, None))


def in_flight() -> int:
    return len([t for t in _live_tasks if not t.done()])


async def wait_pending(timeout: float = 45.0):
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


async def _place_with_recovery(it: OrderIntent, ctx: dict, token: str, qty: int) -> dict:
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


def _live_note(it: OrderIntent, msg: str):
    engine._log_runs_sync([it.run_id], msg)


def _live_record_success(it: OrderIntent, order_id: str, fill: dict, qty: int):
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
        engine._append_run_log(db, it.run_id, [
            f"[{engine._stamp()}] LIVE {it.kind} {it.side} {it.symbol} x{qty} @ MKT ✓ order_id={order_id} "
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


def _live_record_failure(it: OrderIntent, msg: str):
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
            line = (f"[{engine._stamp()}] LIVE ENTRY FAILED {it.side} {it.symbol} x{it.qty}: {msg} "
                    f"— NO position opened at the broker; trade cancelled for this client")
        else:
            stock.live_status = "EXIT_FAILED"
            line = (f"[{engine._stamp()}] ⚠ LIVE EXIT FAILED {it.side} {it.symbol} x{it.qty} [{it.reason}]: {msg} "
                    f"— POSITION MAY STILL BE OPEN AT THE BROKER, CLOSE IT MANUALLY")
        engine._append_run_log(db, it.run_id, [line])
        db.commit()
        print(f"[live] {line}")
    except Exception as e:
        print(f"[live] record_failure failed: {type(e).__name__}: {e}")
        db.rollback()
    finally:
        db.close()


async def _execute_live_order(it: OrderIntent):
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


async def _run_live_intent(it: OrderIntent, prior: Optional[asyncio.Task]):
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
