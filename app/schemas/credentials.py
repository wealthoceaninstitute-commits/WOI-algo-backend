from pydantic import BaseModel
from typing import Optional
from datetime import datetime


# ── Dhan Credentials ──────────────────────────────────────────────────────────

class DhanCredentialRequest(BaseModel):
    dhan_client_id: str
    pin: str
    totp_secret: str
    access_token: str


class DhanCredentialResponse(BaseModel):
    id: str
    dhan_client_id: str          # returned unmasked for display
    is_active: bool
    last_verified: Optional[datetime]
    last_error: Optional[str]
    token_expires_at: Optional[datetime]

    class Config:
        from_attributes = True


# ── Connection Test ───────────────────────────────────────────────────────────

class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
    dhan_client_id: Optional[str] = None
    fund_limit: Optional[float] = None   # from Dhan fund API if test passes
    checked_at: datetime


# ── Proxy ─────────────────────────────────────────────────────────────────────

class ProxyRequest(BaseModel):
    host: str
    port: int = 443
    username: Optional[str] = None
    password: Optional[str] = None
    client_profile_id: Optional[str] = None  # master use only


class ProxyResponse(BaseModel):
    id: str
    host: str
    port: int
    username: Optional[str]
    is_active: bool
    set_by_master: bool

    class Config:
        from_attributes = True
