"""
Order execution: the ONLY part of the algo that differs between paper and live.

Everything else (Angel One data, prev close / gap scan, stock selection, entry trigger,
SL / target / trailing logic, P&L bookkeeping) is one shared engine — algo_engine.py —
run once for every client.

When the engine decides to ENTER or EXIT for a client it creates an OrderIntent and
calls dispatch(). Each intent goes down that client's route:

    mode == "paper"  →  paper.py   simulated: the engine's booked price is the fill
    mode == "live"   →  live.py    real Dhan orders through the client's static-IP proxy

Live entries do not wait for the trigger: as soon as the first-candle levels are known
(9:16) arm_entries() rests STOP_LOSS_MARKET orders at Dhan on the trigger level(s), so the
exchange fills them the instant price gets there. Paper ignores arming entirely.

A client's route is AlgoStrategy.paper_trading (the Paper / Live badge on the Clients page).
An exit always takes the route its entry took, so a position opened on paper is never
"closed" with a real order after the client is switched to live.
"""

from dataclasses import dataclass

PAPER = "paper"
LIVE  = "live"


@dataclass
class OrderIntent:
    mode: str                    # PAPER | LIVE
    kind: str                    # "ENTRY" | "EXIT" | "CANCEL" (drop resting entry orders)
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


def dispatch(intents: list[OrderIntent]):
    """Send each intent down its client's route. Never blocks: live orders run in background tasks."""
    from app.services.execution import paper, live
    for it in intents:
        if it.mode == LIVE:
            live.submit(it)
        else:
            paper.submit(it)


def arm_entries(stock_ids: list[str]):
    """First-candle levels are saved → live clients rest their entry stop orders at Dhan now.
    Paper has nothing to do here (its entry is booked when the trigger is crossed)."""
    from app.services.execution import live
    live.arm(stock_ids)


async def wait_pending(timeout: float = 45.0):
    """Before the day is finished: let in-flight live orders complete (paper has none)."""
    from app.services.execution import live
    await live.wait_pending(timeout)


def live_in_flight() -> int:
    from app.services.execution import live
    return live.in_flight()
