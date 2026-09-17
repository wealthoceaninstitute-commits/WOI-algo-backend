"""
app/routers/master_account.py
Angel One master data account management.
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
from app.services.master_token import refresh_master_token_now

router = APIRouter(prefix="/api/master-account", tags=["master-account"])


class MasterAccountRequest(BaseModel):
    angel_client_id:   str
    angel_pin:         str
    angel_totp_secret: str
    angel_api_key:     str


class MasterAccountResponse(BaseModel):
    id:              str
    angel_client_id: str
    is_active:       bool
    last_verified:   Optional[datetime]
    last_error:      Optional[str]
    token_expires_at:Optional[datetime]

    class Config:
        from_attributes = True


class TestResponse(BaseModel):
    success:      bool
    message:      str
    client_id:    Optional[str] = None
    checked_at:   datetime


def _get_or_none(db: Session) -> Optional[MasterDataAccount]:
    return db.query(MasterDataAccount).first()


@router.get("/", response_model=Optional[MasterAccountResponse])
def get_master_account(
    db: Session = Depends(get_db),
    _:  User    = Depends(require_master),
):
    acc = _get_or_none(db)
    if not acc:
        return None
    return MasterAccountResponse(
        id             = acc.id,
        angel_client_id= decrypt(acc.angel_client_id) if acc.angel_client_id else "",
        is_active      = acc.is_active,
        last_verified  = acc.last_verified,
        last_error     = acc.last_error,
        token_expires_at=acc.token_expires_at,
    )


@router.put("/", response_model=MasterAccountResponse)
def save_master_account(
    payload: MasterAccountRequest,
    db: Session = Depends(get_db),
    _:  User    = Depends(require_master),
):
    acc = _get_or_none(db)
    if acc:
        acc.angel_client_id   = encrypt(payload.angel_client_id)
        acc.angel_pin         = encrypt(payload.angel_pin)
        acc.angel_totp_secret = encrypt(payload.angel_totp_secret)
        acc.angel_api_key     = encrypt(payload.angel_api_key)
        acc.angel_jwt_token   = None
        acc.is_active         = False
        acc.last_error        = None
    else:
        acc = MasterDataAccount(
            angel_client_id  = encrypt(payload.angel_client_id),
            angel_pin        = encrypt(payload.angel_pin),
            angel_totp_secret= encrypt(payload.angel_totp_secret),
            angel_api_key    = encrypt(payload.angel_api_key),
            is_active        = False,
        )
        db.add(acc)

    db.commit()
    db.refresh(acc)
    return MasterAccountResponse(
        id             = acc.id,
        angel_client_id= payload.angel_client_id,
        is_active      = acc.is_active,
        last_verified  = acc.last_verified,
        last_error     = acc.last_error,
        token_expires_at=acc.token_expires_at,
    )


@router.post("/test", response_model=TestResponse)
async def test_master_account(
    db: Session = Depends(get_db),
    _:  User    = Depends(require_master),
):
    """Generate fresh Angel One token via TOTP and verify."""
    acc = _get_or_none(db)
    if not acc:
        raise HTTPException(404, "No master data account configured")

    try:
        jwt, api_key, client_id = await refresh_master_token_now(db)
        return TestResponse(
            success   = True,
            message   = "Angel One connected — token refreshed successfully",
            client_id = client_id,
            checked_at= datetime.now(timezone.utc),
        )
    except Exception as e:
        acc.is_active  = False
        acc.last_error = str(e)
        db.commit()
        return TestResponse(
            success   = False,
            message   = str(e),
            checked_at= datetime.now(timezone.utc),
        )


@router.delete("/", status_code=204)
def delete_master_account(
    db: Session = Depends(get_db),
    _:  User    = Depends(require_master),
):
    acc = _get_or_none(db)
    if acc:
        db.delete(acc)
        db.commit()
