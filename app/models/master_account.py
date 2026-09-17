"""
app/models/master_account.py

Master account — Angel One for ALL market data (free, circuit limits included).
One row in the entire system. Stores encrypted credentials.

Architecture:
  Master account (Angel One) → market data: LTP, OHLC, circuit limits, candles
  Client accounts (Dhan)     → order placement only
"""
import uuid
from sqlalchemy import Column, String, Boolean, DateTime, Text
from sqlalchemy.sql import func
from app.core.database import Base


class MasterDataAccount(Base):
    __tablename__ = "master_data_account"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    # ── Angel One credentials (market data) ───────────────────────────────────
    angel_client_id  = Column(String, nullable=True)   # Angel One Client ID
    angel_pin        = Column(String, nullable=True)   # Angel One MPIN (encrypted)
    angel_totp_secret= Column(String, nullable=True)   # Angel One TOTP secret (encrypted)
    angel_api_key    = Column(String, nullable=True)   # Angel One API key (encrypted)
    angel_jwt_token  = Column(Text,   nullable=True)   # Active JWT token (encrypted)
    angel_refresh_token = Column(Text, nullable=True)  # Refresh token (encrypted)
    angel_feed_token = Column(Text,   nullable=True)   # Feed token for WebSocket

    # ── Token status ──────────────────────────────────────────────────────────
    is_active        = Column(Boolean, default=False)
    last_verified    = Column(DateTime(timezone=True), nullable=True)
    token_expires_at = Column(DateTime(timezone=True), nullable=True)
    last_error       = Column(String, nullable=True)

    # ── Legacy Dhan fields (kept for migration, no longer used for data) ──────
    # These stay so existing rows don't break. Will be removed in future cleanup.
    dhan_client_id   = Column(String, nullable=True)
    pin              = Column(String, nullable=True)
    totp_secret      = Column(String, nullable=True)
    access_token     = Column(Text,   nullable=True, default="")

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
