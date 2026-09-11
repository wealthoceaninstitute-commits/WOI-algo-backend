from pydantic import BaseModel, EmailStr, field_validator
from typing import Optional
from datetime import datetime


# ── Registration ──────────────────────────────────────────────────────────────

class ClientRegisterRequest(BaseModel):
    name: str
    email: EmailStr
    phone: Optional[str] = None
    password: str
    city: Optional[str] = None
    state: Optional[str] = None
    capital: float = 0.0

    @field_validator("password")
    @classmethod
    def password_min_length(cls, v: str) -> str:
        if len(v) < 6:
            raise ValueError("Password must be at least 6 characters")
        return v

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        if len(v.strip()) < 2:
            raise ValueError("Name must be at least 2 characters")
        return v.strip()


class MasterCreateClientRequest(ClientRegisterRequest):
    """Master creating a client — password is optional (defaults to client@123)"""
    password: str = "client@123"


# ── Login ─────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    user_id: str
    name: str


# ── Profile ───────────────────────────────────────────────────────────────────

class UpdateProfileRequest(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pan: Optional[str] = None
    active_index: Optional[str] = None
    paper_trading: Optional[bool] = None
    capital: Optional[float] = None


class UserResponse(BaseModel):
    id: str
    name: str
    email: str
    phone: Optional[str]
    role: str
    created_at: datetime

    class Config:
        from_attributes = True


class ClientProfileResponse(BaseModel):
    profile_id: str
    user_id: str
    name: str
    email: str
    phone: Optional[str]
    city: Optional[str]
    state: Optional[str]
    pan: Optional[str]
    active_index: str
    paper_trading: bool
    capital: float
    has_api_credentials: bool
    has_proxy: bool
    api_connected: bool
    today_pnl: float
    created_at: datetime
