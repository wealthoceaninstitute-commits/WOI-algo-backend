"""
Dhan API service — auto-generates access token using Client ID + PIN + TOTP.
No manual token entry needed. Token refreshed on every connect/test.

Auth endpoint: POST https://auth.dhan.co/app/generateAccessToken
               ?dhanClientId=...&pin=...&totp=...
"""
import time
import pyotp
import httpx
from datetime import datetime, timezone
from typing import Optional

from app.core.config import get_settings

settings = get_settings()
AUTH_BASE = "https://auth.dhan.co"
API_BASE  = "https://api.dhan.co/v2"
TIMEOUT   = 15.0


def _proxy_url(host, port, user, pwd):
    auth = f"{user}:{pwd}@" if user and pwd else ""
    return f"http://{auth}{host}:{port}"


def _api_client(access_token, proxy_host=None, proxy_port=443, proxy_user=None, proxy_pass=None):
    headers = {
        "access-token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    proxy = _proxy_url(proxy_host, proxy_port, proxy_user, proxy_pass) if proxy_host else None
    return httpx.AsyncClient(
        headers=headers,
        proxies={"http://": proxy, "https://": proxy} if proxy else None,
        timeout=TIMEOUT,
        follow_redirects=True,
    )


def _generate_totp(totp_secret: str) -> str:
    return pyotp.TOTP(totp_secret).now()


def _seconds_until_next_window() -> int:
    return 30 - (int(time.time()) % 30)


async def generate_access_token(
    dhan_client_id: str,
    pin: str,
    totp_secret: str,
    proxy_host=None,
    proxy_port=443,
    proxy_user=None,
    proxy_pass=None,
) -> dict:
    """
    Generate a fresh Dhan access token using Client ID + PIN + TOTP.
    Retries once if TOTP window expires mid-request.
    """
    checked_at = datetime.now(timezone.utc)
    proxy = _proxy_url(proxy_host, proxy_port, proxy_user, proxy_pass) if proxy_host else None

    for attempt in range(2):
        totp_code = _generate_totp(totp_secret)
        secs_left = _seconds_until_next_window()

        # Wait if in last 2 seconds of TOTP window
        if secs_left <= 2 and attempt == 0:
            time.sleep(secs_left + 1)
            totp_code = _generate_totp(totp_secret)

        url = (
            f"{AUTH_BASE}/app/generateAccessToken"
            f"?dhanClientId={dhan_client_id}&pin={pin}&totp={totp_code}"
        )

        try:
            async with httpx.AsyncClient(
                proxies={"http://": proxy, "https://": proxy} if proxy else None,
                timeout=TIMEOUT,
                follow_redirects=True,
            ) as client:
                resp = await client.post(url)

                if resp.status_code == 200:
                    data = resp.json()
                    token = data.get("accessToken") or data.get("access_token")
                    if token:
                        return {
                            "success": True,
                            "access_token": token,
                            "message": "Token generated successfully",
                            "checked_at": checked_at,
                        }
                    return {
                        "success": False,
                        "message": f"Token not in response: {data}",
                        "checked_at": checked_at,
                    }

                elif resp.status_code == 401:
                    if attempt == 0:
                        time.sleep(2)
                        continue
                    return {
                        "success": False,
                        "message": "Invalid Client ID, PIN or TOTP. Check your credentials.",
                        "checked_at": checked_at,
                    }

                else:
                    return {
                        "success": False,
                        "message": f"Auth server returned {resp.status_code}: {resp.text[:200]}",
                        "checked_at": checked_at,
                    }

        except httpx.ProxyError as e:
            return {"success": False, "message": f"Proxy error: {e}", "checked_at": checked_at}
        except httpx.ConnectTimeout:
            return {"success": False, "message": "Connection timed out.", "checked_at": checked_at}
        except httpx.RequestError as e:
            return {"success": False, "message": f"Network error: {e}", "checked_at": checked_at}

    return {"success": False, "message": "Token generation failed after retries.", "checked_at": checked_at}


async def test_dhan_connection(
    dhan_client_id: str,
    pin: str,
    totp_secret: str,
    proxy_host=None,
    proxy_port=443,
    proxy_user=None,
    proxy_pass=None,
) -> dict:
    """
    1. Generate fresh access token via TOTP
    2. Call Fund Limit API to verify token works
    """
    checked_at = datetime.now(timezone.utc)

    token_result = await generate_access_token(
        dhan_client_id, pin, totp_secret,
        proxy_host, proxy_port, proxy_user, proxy_pass,
    )

    if not token_result["success"]:
        return {
            "success": False,
            "message": token_result["message"],
            "checked_at": checked_at,
        }

    access_token = token_result["access_token"]

    try:
        async with _api_client(access_token, proxy_host, proxy_port, proxy_user, proxy_pass) as client:
            resp = await client.get(f"{API_BASE}/fundlimit")

            if resp.status_code == 200:
                data = resp.json()
                fund = (
                    data.get("availabelBalance")
                    or data.get("availableBalance")
                    or data.get("net")
                    or 0.0
                )
                return {
                    "success": True,
                    "message": "Connected to Dhan API. Token valid.",
                    "dhan_client_id": dhan_client_id,
                    "fund_limit": float(fund),
                    "access_token": access_token,
                    "checked_at": checked_at,
                }

            elif resp.status_code == 401:
                return {
                    "success": False,
                    "message": "Token generated but rejected by Dhan API. Try again.",
                    "checked_at": checked_at,
                }

            else:
                return {
                    "success": False,
                    "message": f"Fund API returned {resp.status_code}: {resp.text[:200]}",
                    "checked_at": checked_at,
                }

    except Exception as e:
        return {
            "success": False,
            "message": f"Fund check failed: {str(e)}",
            "checked_at": checked_at,
        }
