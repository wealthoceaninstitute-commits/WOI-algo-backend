"""
Dhan API service.
Handles connection testing using the stored access token.
Dhan API docs: https://dhanhq.co/docs/v2/
"""
import httpx
from datetime import datetime, timezone
from typing import Optional
from app.core.config import get_settings

settings = get_settings()

DHAN_BASE = settings.DHAN_BASE_URL
TIMEOUT = 10.0   # seconds


def _build_client(
    access_token: str,
    proxy_host: Optional[str] = None,
    proxy_port: int = 443,
    proxy_user: Optional[str] = None,
    proxy_pass: Optional[str] = None,
) -> httpx.AsyncClient:
    """Build an httpx async client, optionally routed through a proxy."""
    headers = {
        "access-token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    proxy_url = None
    if proxy_host:
        auth = f"{proxy_user}:{proxy_pass}@" if proxy_user and proxy_pass else ""
        proxy_url = f"http://{auth}{proxy_host}:{proxy_port}"

    return httpx.AsyncClient(
        headers=headers,
        proxies={"http://": proxy_url, "https://": proxy_url} if proxy_url else None,
        timeout=TIMEOUT,
        follow_redirects=True,
    )


async def test_dhan_connection(
    dhan_client_id: str,
    access_token: str,
    proxy_host: Optional[str] = None,
    proxy_port: int = 443,
    proxy_user: Optional[str] = None,
    proxy_pass: Optional[str] = None,
) -> dict:
    """
    Test the Dhan API connection by calling the Fund Limit endpoint.
    Returns: { success, message, fund_limit, checked_at }
    """
    checked_at = datetime.now(timezone.utc)

    try:
        async with _build_client(access_token, proxy_host, proxy_port, proxy_user, proxy_pass) as client:
            # Dhan Fund Limit API — requires valid access token
            resp = await client.get(f"{DHAN_BASE}/fundlimit")

            if resp.status_code == 200:
                data = resp.json()
                # Dhan returns availabelBalance (note their typo in API)
                fund_limit = (
                    data.get("availabelBalance")
                    or data.get("availableBalance")
                    or data.get("net")
                    or 0.0
                )
                return {
                    "success": True,
                    "message": "Connected to Dhan API successfully",
                    "dhan_client_id": dhan_client_id,
                    "fund_limit": float(fund_limit),
                    "checked_at": checked_at,
                }

            elif resp.status_code == 401:
                return {
                    "success": False,
                    "message": "Access token is invalid or expired. Please generate a new token from Dhan.",
                    "checked_at": checked_at,
                }

            elif resp.status_code == 429:
                return {
                    "success": False,
                    "message": "Dhan API rate limit hit. Wait a few seconds and try again.",
                    "checked_at": checked_at,
                }

            else:
                return {
                    "success": False,
                    "message": f"Dhan API returned status {resp.status_code}: {resp.text[:200]}",
                    "checked_at": checked_at,
                }

    except httpx.ConnectTimeout:
        return {
            "success": False,
            "message": "Connection timed out. Check your network or proxy settings.",
            "checked_at": checked_at,
        }
    except httpx.ProxyError as e:
        return {
            "success": False,
            "message": f"Proxy error: {str(e)}",
            "checked_at": checked_at,
        }
    except httpx.RequestError as e:
        return {
            "success": False,
            "message": f"Network error: {str(e)}",
            "checked_at": checked_at,
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Unexpected error: {str(e)}",
            "checked_at": checked_at,
        }
