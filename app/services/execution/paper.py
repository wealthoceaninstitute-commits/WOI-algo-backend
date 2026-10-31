"""
PAPER route: simulated execution.

The shared engine has already booked the trade at its decision price (entry at LTP,
SL at LTP, TARGET at the target price, EOD at LTP) and written it to AlgoStock /
AlgoRun / DailyPnl. A paper fill IS that booked price, so nothing leaves the server.

Kept as its own file so paper and live are two clearly separate routes; any future
paper-only behaviour (e.g. simulated slippage or brokerage) goes here and nowhere else.
"""

from app.services.execution import OrderIntent


def submit(it: OrderIntent):
    return None
