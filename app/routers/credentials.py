from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timezone

from app.core.database import get_db
from app.core.security import get_current_user, require_master
from app.core.encryption import encrypt, decrypt
from app.models.user import User
from app.models.trading import ClientProfile, DhanCredential, ProxySetting
from app.schemas.credentials import (
    DhanCredentialRequest,
    DhanCredentialResponse,
    ConnectionTestResponse,
    ProxyRequest,
    ProxyResponse,
)
from app.services.dhan import test_dhan_connection

router = APIRouter(prefix="/api/credentials", tags=["credentials"])


def _get_profile(user: User, db: Session) -> ClientProfile:
    profile = db.query(ClientProfile).filter(ClientProfile.user_id == user.id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Client profile not found")
    return profile


def _get_profile_by_id(profile_id: str, db: Session) -> ClientProfile:
    profile = db.query(ClientProfile).filter(ClientProfile.id == profile_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Client not found")
    return profile


# ── Save / Update Dhan Credentials ───────────────────────────────────────────

@router.put("/dhan", response_model=DhanCredentialResponse)
def save_dhan_credentials(
    payload: DhanCredentialRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Client saves their own Dhan API credentials (all fields encrypted at rest)."""
    profile = _get_profile(current_user, db)

    cred = profile.dhan_cred
    if cred:
        cred.dhan_client_id = encrypt(payload.dhan_client_id)
        cred.pin = encrypt(payload.pin)
        cred.totp_secret = encrypt(payload.totp_secret)
        cred.access_token = encrypt(payload.access_token)
        cred.is_active = False   # reset — re-test required
        cred.last_error = None
    else:
        cred = DhanCredential(
            client_profile_id=profile.id,
            dhan_client_id=encrypt(payload.dhan_client_id),
            pin=encrypt(payload.pin),
            totp_secret=encrypt(payload.totp_secret),
            access_token=encrypt(payload.access_token),
            is_active=False,
        )
        db.add(cred)

    db.commit()
    db.refresh(cred)
    return DhanCredentialResponse(
        id=cred.id,
        dhan_client_id=payload.dhan_client_id,  # return plain for confirmation
        is_active=cred.is_active,
        last_verified=cred.last_verified,
        last_error=cred.last_error,
        token_expires_at=cred.token_expires_at,
    )


@router.get("/dhan", response_model=DhanCredentialResponse)
def get_dhan_credentials(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get current credential status (never returns raw secrets)."""
    profile = _get_profile(current_user, db)
    cred = profile.dhan_cred
    if not cred:
        raise HTTPException(status_code=404, detail="No API credentials saved yet")
    return DhanCredentialResponse(
        id=cred.id,
        dhan_client_id=decrypt(cred.dhan_client_id),
        is_active=cred.is_active,
        last_verified=cred.last_verified,
        last_error=cred.last_error,
        token_expires_at=cred.token_expires_at,
    )


# ── Test Connection ────────────────────────────────────────────────────────────

@router.post("/dhan/test", response_model=ConnectionTestResponse)
async def test_connection(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Test the saved Dhan API credentials by calling the live Fund Limit endpoint.
    Updates is_active + last_verified / last_error in DB.
    """
    profile = _get_profile(current_user, db)
    cred = profile.dhan_cred
    if not cred:
        raise HTTPException(status_code=400, detail="Save API credentials before testing")

    # Decrypt for use
    client_id = decrypt(cred.dhan_client_id)
    access_token = decrypt(cred.access_token)

    # Proxy settings if available
    proxy = profile.proxy_setting
    result = await test_dhan_connection(
        dhan_client_id=client_id,
        access_token=access_token,
        proxy_host=proxy.host if proxy and proxy.is_active else None,
        proxy_port=proxy.port if proxy and proxy.is_active else 443,
        proxy_user=proxy.username if proxy and proxy.is_active else None,
        proxy_pass=decrypt(proxy.password) if proxy and proxy.is_active and proxy.password else None,
    )

    # Persist result
    now = datetime.now(timezone.utc)
    cred.is_active = result["success"]
    cred.last_verified = now
    cred.last_error = None if result["success"] else result["message"]
    db.commit()

    return ConnectionTestResponse(**result)


# Master: test connection for any client
@router.post("/dhan/test/{client_profile_id}", response_model=ConnectionTestResponse)
async def master_test_connection(
    client_profile_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    """Master can test connection for any client."""
    profile = _get_profile_by_id(client_profile_id, db)
    cred = profile.dhan_cred
    if not cred:
        raise HTTPException(status_code=400, detail="Client has no API credentials saved")

    client_id = decrypt(cred.dhan_client_id)
    access_token = decrypt(cred.access_token)
    proxy = profile.proxy_setting
    result = await test_dhan_connection(
        dhan_client_id=client_id,
        access_token=access_token,
        proxy_host=proxy.host if proxy and proxy.is_active else None,
        proxy_port=proxy.port if proxy and proxy.is_active else 443,
        proxy_user=proxy.username if proxy and proxy.is_active else None,
        proxy_pass=decrypt(proxy.password) if proxy and proxy.is_active and proxy.password else None,
    )

    now = datetime.now(timezone.utc)
    cred.is_active = result["success"]
    cred.last_verified = now
    cred.last_error = None if result["success"] else result["message"]
    db.commit()
    return ConnectionTestResponse(**result)


# ── Proxy ─────────────────────────────────────────────────────────────────────

@router.put("/proxy", response_model=ProxyResponse)
def save_proxy(
    payload: ProxyRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Client saves own proxy, or master saves for a client (pass client_profile_id)."""
    is_master = current_user.role == "MASTER"

    if is_master and payload.client_profile_id:
        profile = _get_profile_by_id(payload.client_profile_id, db)
    else:
        profile = _get_profile(current_user, db)

    enc_password = encrypt(payload.password) if payload.password else None

    proxy = profile.proxy_setting
    if proxy:
        proxy.host = payload.host
        proxy.port = payload.port
        proxy.username = payload.username
        proxy.password = enc_password
        proxy.is_active = True
        proxy.set_by_master = is_master
    else:
        proxy = ProxySetting(
            client_profile_id=profile.id,
            host=payload.host,
            port=payload.port,
            username=payload.username,
            password=enc_password,
            is_active=True,
            set_by_master=is_master,
        )
        db.add(proxy)

    db.commit()
    db.refresh(proxy)
    return ProxyResponse(
        id=proxy.id,
        host=proxy.host,
        port=proxy.port,
        username=proxy.username,
        is_active=proxy.is_active,
        set_by_master=proxy.set_by_master,
    )


@router.get("/proxy", response_model=ProxyResponse)
def get_proxy(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    profile = _get_profile(current_user, db)
    if not profile.proxy_setting:
        raise HTTPException(status_code=404, detail="No proxy configured")
    p = profile.proxy_setting
    return ProxyResponse(id=p.id, host=p.host, port=p.port, username=p.username, is_active=p.is_active, set_by_master=p.set_by_master)


@router.delete("/proxy", status_code=204)
def delete_proxy(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    profile = _get_profile(current_user, db)
    if profile.proxy_setting:
        db.delete(profile.proxy_setting)
        db.commit()
