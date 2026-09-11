"""
app/routers/master_account.py

Master data account management.
Only the MASTER user can view/edit these credentials.
Used by the algo engine for all market data fetching.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
from typing import Optional

from app.core.database import get_db
from app.core.security import require_master
from app.core.encryption import encrypt, decrypt
from app.models.user import User
from app.models.master_account import MasterDataAccount
from app.services.dhan import generate_access_token, test_dhan_connection

router = APIRouter(prefix="/api/master-account", tags=["master-account"])


class MasterAccountRequest(BaseModel):
    dhan_client_id: str
    pin: str
    totp_secret: str


class MasterAccountResponse(BaseModel):
    id: str
    dhan_client_id: str
    is_active: bool
    last_verified: Optional[datetime]
    last_error: Optional[str]
    token_expires_at: Optional[datetime]

    class Config:
        from_attributes = True


class TestResponse(BaseModel):
    success: bool
    message: str
    fund_limit: Optional[float] = None
    dhan_client_id: Optional[str] = None
    checked_at: datetime


def _get_or_none(db: Session) -> Optional[MasterDataAccount]:
    return db.query(MasterDataAccount).first()


@router.get("/", response_model=Optional[MasterAccountResponse])
def get_master_account(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Get current master data account config."""
    acc = _get_or_none(db)
    if not acc:
        return None
    return MasterAccountResponse(
        id=acc.id,
        dhan_client_id=decrypt(acc.dhan_client_id),
        is_active=acc.is_active,
        last_verified=acc.last_verified,
        last_error=acc.last_error,
        token_expires_at=acc.token_expires_at,
    )


@router.put("/", response_model=MasterAccountResponse)
def save_master_account(
    payload: MasterAccountRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Save/update master data account credentials."""
    acc = _get_or_none(db)
    if acc:
        acc.dhan_client_id = encrypt(payload.dhan_client_id)
        acc.pin            = encrypt(payload.pin)
        acc.totp_secret    = encrypt(payload.totp_secret)
        acc.access_token   = encrypt("")
        acc.is_active      = False
        acc.last_error     = None
    else:
        acc = MasterDataAccount(
            dhan_client_id=encrypt(payload.dhan_client_id),
            pin=encrypt(payload.pin),
            totp_secret=encrypt(payload.totp_secret),
            access_token=encrypt(""),
            is_active=False,
        )
        db.add(acc)

    db.commit()
    db.refresh(acc)
    return MasterAccountResponse(
        id=acc.id,
        dhan_client_id=payload.dhan_client_id,
        is_active=acc.is_active,
        last_verified=acc.last_verified,
        last_error=acc.last_error,
        token_expires_at=acc.token_expires_at,
    )


@router.post("/test", response_model=TestResponse)
async def test_master_account(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Generate fresh token via TOTP and verify with Dhan Fund Limit API."""
    acc = _get_or_none(db)
    if not acc:
        raise HTTPException(status_code=404, detail="No master data account configured")

    client_id   = decrypt(acc.dhan_client_id)
    pin         = decrypt(acc.pin)
    totp_secret = decrypt(acc.totp_secret)

    result = await test_dhan_connection(
        dhan_client_id=client_id,
        pin=pin,
        totp_secret=totp_secret,
        # No proxy — master account hits Dhan directly for data
    )

    now = datetime.now(timezone.utc)
    acc.is_active     = result["success"]
    acc.last_verified = now
    acc.last_error    = None if result["success"] else result["message"]

    if result["success"] and result.get("access_token"):
        acc.access_token     = encrypt(result["access_token"])
        acc.token_expires_at = now + timedelta(hours=24)

    db.commit()

    return TestResponse(
        success=result["success"],
        message=result["message"],
        fund_limit=result.get("fund_limit"),
        dhan_client_id=result.get("dhan_client_id"),
        checked_at=result["checked_at"],
    )


@router.delete("/", status_code=204)
def delete_master_account(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    acc = _get_or_none(db)
    if acc:
        db.delete(acc)
        db.commit()
