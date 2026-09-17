"""
app/models/master_account.py

Master Dhan account — used exclusively for market data fetching.
One row in the entire system. Stores encrypted credentials.
"""
import uuid
from sqlalchemy import Column, String, Boolean, DateTime, Text
from sqlalchemy.sql import func
from app.core.database import Base


class MasterDataAccount(Base):
    __tablename__ = "master_data_account"

    id               = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    # Encrypted fields
    dhan_client_id   = Column(String, nullable=False)
    pin              = Column(String, nullable=False)
    totp_secret      = Column(String, nullable=False)
    access_token     = Column(Text, nullable=False, default="")

    is_active        = Column(Boolean, default=False)
    last_verified    = Column(DateTime(timezone=True), nullable=True)
    token_expires_at = Column(DateTime(timezone=True), nullable=True)
    last_error       = Column(String, nullable=True)
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), onupdate=func.now())
