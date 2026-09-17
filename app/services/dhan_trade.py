"""
Dhan Trading API Service — v2
Built from https://dhanhq.co/docs/v2/

All GET functions return { success, data, token_expired } so the router
can detect DH-906 and auto-refresh the token without any extra code here.
"""

import httpx
from typing import Optional

API_BASE = "https://api.dhan.co/v2"
TIMEOUT  = 15.0


# ── HTTP client ───────────────────────────────────────────────────────────────

def _client(
    access_token: str,
    proxy_host: Optional[str] = None,
    proxy_port: int = 443,
    proxy_user: Optional[str] = None,
    proxy_pass: Optional[str] = None,
    proxy_scheme: str = "https",      # ← add this
) -> httpx.AsyncClient:
    headers = {
        "access-token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    proxy = None
    if proxy_host:
        auth = f"{proxy_user}:{proxy_pass}@" if proxy_user and proxy_pass else ""
        proxy = f"{proxy_scheme}://{auth}{proxy_host}:{proxy_port}"
    return httpx.AsyncClient(
        headers=headers,
        proxies={"http://": proxy, "https://": proxy} if proxy else None,
        timeout=TIMEOUT,
        follow_redirects=True,
    )


def _parse(resp: httpx.Response):
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}


def _is_token_expired(resp: httpx.Response) -> bool:
    """Detect DH-906 Invalid Token — needs auto-refresh."""
    if resp.status_code != 400:
        return False
    try:
        return resp.json().get("errorCode") == "DH-906"
    except Exception:
        return False


# ── Generic GET wrapper ───────────────────────────────────────────────────────

async def _get(path: str, access_token: str, **proxy) -> dict:
    """
    Perform a GET to Dhan API.
    Returns:
      { success: True,  data: <list|dict>, token_expired: False }
      { success: False, token_expired: True,  message: "..." }   ← DH-906
      { success: False, token_expired: False, message: "..." }   ← other error
    """
    async with _client(access_token, **proxy) as c:
        r = await c.get(f"{API_BASE}{path}")
        if r.status_code == 200:
            return {"success": True, "data": _parse(r), "token_expired": False}
        if _is_token_expired(r):
            return {"success": False, "token_expired": True,
                    "message": f"Token expired (DH-906) — auto-refreshing"}
        return {"success": False, "token_expired": False,
                "message": f"Dhan {r.status_code}: {r.text[:300]}"}


# ── Read endpoints ────────────────────────────────────────────────────────────

async def get_order_book(access_token: str, **proxy) -> dict:
    return await _get("/orders", access_token, **proxy)

async def get_order_by_id(access_token: str, order_id: str, **proxy) -> dict:
    return await _get(f"/orders/{order_id}", access_token, **proxy)

async def get_trade_book(access_token: str, **proxy) -> dict:
    return await _get("/trades", access_token, **proxy)

async def get_trades_by_order(access_token: str, order_id: str, **proxy) -> dict:
    return await _get(f"/trades/{order_id}", access_token, **proxy)

async def get_positions(access_token: str, **proxy) -> dict:
    return await _get("/positions", access_token, **proxy)

async def get_holdings(access_token: str, **proxy) -> dict:
    return await _get("/holdings", access_token, **proxy)

async def get_fund_limit(access_token: str, **proxy) -> dict:
    result = await _get("/fundlimit", access_token, **proxy)
    if result["success"]:
        data = result["data"]
        result.update({
            "available_balance":    data.get("availabelBalance", 0),  # Dhan's typo
            "sod_limit":            data.get("sodLimit", 0),
            "utilized_amount":      data.get("utilizedAmount", 0),
            "withdrawable_balance": data.get("withdrawableBalance", 0),
            "collateral_amount":    data.get("collateralAmount", 0),
        })
    return result


# ── Write endpoints ───────────────────────────────────────────────────────────

async def place_order(
    access_token: str,
    dhan_client_id: str,
    transaction_type: str,
    exchange_segment: str,
    product_type: str,
    order_type: str,
    security_id: str,
    quantity: int,
    price: float = 0,
    trigger_price: float = 0,
    validity: str = "DAY",
    disclosed_quantity: int = 0,
    after_market_order: bool = False,
    correlation_id: str = "",
    **proxy,
) -> dict:
    body = {
        "dhanClientId":      dhan_client_id,
        "transactionType":   transaction_type,
        "exchangeSegment":   exchange_segment,
        "productType":       product_type,
        "orderType":         order_type,
        "validity":          validity,
        "securityId":        security_id,
        "quantity":          quantity,
        "price":             price,
        "triggerPrice":      trigger_price,
        "disclosedQuantity": disclosed_quantity,
        "afterMarketOrder":  after_market_order,
    }
    if correlation_id:
        body["correlationId"] = correlation_id[:30]

    async with _client(access_token, **proxy) as c:
        r = await c.post(f"{API_BASE}/orders", json=body)
        if r.status_code in (200, 201, 202):
            data = _parse(r)
            return {
                "success":      True,
                "order_id":     data.get("orderId"),
                "order_status": data.get("orderStatus"),
                "data":         data,
            }
        if _is_token_expired(r):
            return {"success": False, "token_expired": True,
                    "message": "Token expired — refresh and retry"}
        return {"success": False, "token_expired": False,
                "message": f"Dhan {r.status_code}: {r.text[:300]}"}


async def modify_order(
    access_token: str,
    dhan_client_id: str,
    order_id: str,
    order_type: str,
    quantity: int,
    price: float,
    trigger_price: float = 0,
    validity: str = "DAY",
    disclosed_quantity: int = 0,
    **proxy,
) -> dict:
    body = {
        "dhanClientId":      dhan_client_id,
        "orderId":           order_id,
        "orderType":         order_type,
        "quantity":          quantity,
        "price":             price,
        "triggerPrice":      trigger_price,
        "validity":          validity,
        "disclosedQuantity": disclosed_quantity,
    }
    async with _client(access_token, **proxy) as c:
        r = await c.put(f"{API_BASE}/orders/{order_id}", json=body)
        if r.status_code in (200, 202):
            data = _parse(r)
            return {
                "success":      True,
                "order_id":     data.get("orderId"),
                "order_status": data.get("orderStatus"),
            }
        if _is_token_expired(r):
            return {"success": False, "token_expired": True, "message": "Token expired"}
        return {"success": False, "token_expired": False,
                "message": f"Dhan {r.status_code}: {r.text[:300]}"}


async def cancel_order(access_token: str, order_id: str, **proxy) -> dict:
    async with _client(access_token, **proxy) as c:
        r = await c.delete(f"{API_BASE}/orders/{order_id}")
        if r.status_code in (200, 202):
            data = _parse(r)
            return {
                "success":      True,
                "order_id":     data.get("orderId"),
                "order_status": data.get("orderStatus"),
            }
        if _is_token_expired(r):
            return {"success": False, "token_expired": True, "message": "Token expired"}
        return {"success": False, "token_expired": False,
                "message": f"Dhan {r.status_code}: {r.text[:300]}"}


# ── Normalizers ───────────────────────────────────────────────────────────────

def _map_status(dhan_status: str) -> str:
    return {
        "TRANSIT":     "PENDING",
        "PENDING":     "PENDING",
        "PART_TRADED": "PENDING",
        "TRADED":      "EXECUTED",
        "REJECTED":    "REJECTED",
        "CANCELLED":   "CANCELLED",
        "EXPIRED":     "CANCELLED",
    }.get((dhan_status or "").upper(), "PENDING")


def normalize_order(o: dict) -> dict:
    return {
        "dhan_order_id":    o.get("orderId"),
        "symbol":           o.get("tradingSymbol", ""),
        "security_id":      o.get("securityId"),
        "exchange_segment": o.get("exchangeSegment"),
        "order_type":       o.get("transactionType", ""),  # BUY / SELL
        "quantity":         o.get("quantity", 0),
        "price":            o.get("price", 0.0),
        "trigger_price":    o.get("triggerPrice", 0.0),
        "executed_price":   o.get("averageTradedPrice", 0.0),
        "filled_qty":       o.get("filledQty", 0),
        "remaining_qty":    o.get("remainingQuantity", 0),
        "status":           _map_status(o.get("orderStatus", "")),
        "rejection_reason": o.get("omsErrorDescription"),
        "expiry":           o.get("drvExpiryDate"),
        "option_type":      o.get("drvOptionType"),        # CALL / PUT
        "strike_price":     o.get("drvStrikePrice", 0.0),
        "placed_at":        o.get("createTime"),
        "updated_at":       o.get("updateTime"),
        "product_type":     o.get("productType"),
        "order_sub_type":   o.get("orderType"),            # MARKET / LIMIT
        "correlation_id":   o.get("correlationId"),
    }


def normalize_position(p: dict) -> dict:
    net_qty    = p.get("netQty", 0)
    buy_avg    = p.get("buyAvg", 0.0)
    sell_avg   = p.get("sellAvg", 0.0)
    avg_cost   = p.get("costPrice") or buy_avg or sell_avg
    realized   = p.get("realizedProfit", 0.0)
    unrealized = p.get("unrealizedProfit", 0.0)
    pos_type   = p.get("positionType", "")

    return {
        "symbol":           p.get("tradingSymbol", ""),
        "security_id":      p.get("securityId"),
        "exchange_segment": p.get("exchangeSegment"),
        "quantity":         abs(net_qty),
        "avg_cost":         float(avg_cost),
        "buy_avg":          float(buy_avg),
        "sell_avg":         float(sell_avg),
        "net_qty":          net_qty,
        "realized_pnl":     float(realized),
        "unrealized_pnl":   float(unrealized),
        "total_pnl":        float(realized) + float(unrealized),
        "status":           "CLOSED" if pos_type == "CLOSED" or net_qty == 0 else "OPEN",
        "position_type":    pos_type,
        "expiry":           p.get("drvExpiryDate"),
        "option_type":      p.get("drvOptionType"),
        "strike_price":     p.get("drvStrikePrice", 0.0),
        "product_type":     p.get("productType"),
        "day_buy_qty":      p.get("dayBuyQty", 0),
        "day_sell_qty":     p.get("daySellQty", 0),
    }


def normalize_trade(t: dict) -> dict:
    return {
        "dhan_order_id":     t.get("orderId"),
        "exchange_order_id": t.get("exchangeOrderId"),
        "exchange_trade_id": t.get("exchangeTradeId"),
        "symbol":            t.get("tradingSymbol", ""),
        "security_id":       t.get("securityId"),
        "exchange_segment":  t.get("exchangeSegment"),
        "order_type":        t.get("transactionType"),
        "traded_quantity":   t.get("tradedQuantity", 0),
        "traded_price":      t.get("tradedPrice", 0.0),
        "expiry":            t.get("drvExpiryDate"),
        "option_type":       t.get("drvOptionType"),
        "strike_price":      t.get("drvStrikePrice", 0.0),
        "product_type":      t.get("productType"),
        "executed_at":       t.get("exchangeTime") or t.get("updateTime"),
    }
