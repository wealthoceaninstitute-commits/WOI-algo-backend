from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, cast, Date
from datetime import date

from app.core.database import get_db
from app.core.security import hash_password, require_master, get_current_user
from app.models.user import User
from app.models.trading import ClientProfile, DhanCredential, ProxySetting, DailyPnl, Fund
from app.schemas.auth import MasterCreateClientRequest, ClientProfileResponse, UpdateProfileRequest

router = APIRouter(prefix="/api/clients", tags=["clients"])


def _build_client_response(user: User) -> ClientProfileResponse:
    p = user.client_profile
    today = date.today()
    today_pnl = 0.0
    if p and p.daily_pnl:
        for d in p.daily_pnl:
            if d.date == today:
                today_pnl = float(d.total_pnl)
                break
    return ClientProfileResponse(
        profile_id=p.id if p else "",
        user_id=user.id,
        name=user.name,
        email=user.email,
        phone=user.phone,
        city=p.city if p else None,
        state=p.state if p else None,
        pan=p.pan if p else None,
        active_index=p.active_index if p else "SENSEX",
        paper_trading=p.paper_trading if p else True,
        capital=float(p.capital) if p else 0.0,
        has_api_credentials=bool(p and p.dhan_cred),
        has_proxy=bool(p and p.proxy_setting and p.proxy_setting.is_active),
        api_connected=bool(p and p.dhan_cred and p.dhan_cred.is_active),
        today_pnl=today_pnl,
        created_at=user.created_at,
    )


@router.get("", response_model=list[ClientProfileResponse])
def list_clients(
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    users = (
        db.query(User)
        .filter(User.role == "CLIENT")
        .options(
            joinedload(User.client_profile).joinedload(ClientProfile.dhan_cred),
            joinedload(User.client_profile).joinedload(ClientProfile.proxy_setting),
            joinedload(User.client_profile).joinedload(ClientProfile.daily_pnl),
        )
        .order_by(User.created_at)
        .all()
    )
    return [_build_client_response(u) for u in users]


@router.post("", response_model=ClientProfileResponse, status_code=201)
def create_client(
    payload: MasterCreateClientRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    if db.query(User).filter(User.email == payload.email.lower()).first():
        raise HTTPException(status_code=409, detail="Email already registered")

    user = User(
        name=payload.name.strip(),
        email=payload.email.lower(),
        phone=payload.phone,
        password_hash=hash_password(payload.password),
        role="CLIENT",
    )
    db.add(user)
    db.flush()

    profile = ClientProfile(
        user_id=user.id,
        city=payload.city,
        state=payload.state,
        capital=payload.capital,
        paper_trading=True,
        active_index="SENSEX",
    )
    db.add(profile)
    db.flush()

    fund = Fund(client_profile_id=profile.id, available=payload.capital, total_balance=payload.capital)
    db.add(fund)
    db.commit()
    db.refresh(user)
    return _build_client_response(user)


@router.get("/me", response_model=ClientProfileResponse)
def my_profile(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Client's own profile."""
    db.refresh(current_user)
    return _build_client_response(current_user)


@router.patch("/me", response_model=ClientProfileResponse)
def update_my_profile(
    payload: UpdateProfileRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if payload.name:
        current_user.name = payload.name.strip()
    if payload.phone is not None:
        current_user.phone = payload.phone

    p = current_user.client_profile
    if p:
        for field in ("city", "state", "pan", "active_index", "paper_trading", "capital"):
            val = getattr(payload, field)
            if val is not None:
                setattr(p, field, val)

    db.commit()
    db.refresh(current_user)
    return _build_client_response(current_user)


@router.get("/{client_profile_id}", response_model=ClientProfileResponse)
def get_client(
    client_profile_id: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_master),
):
    profile = db.query(ClientProfile).filter(ClientProfile.id == client_profile_id).first()
    if not profile:
        raise HTTPException(status_code=404, detail="Client not found")
    return _build_client_response(profile.user)
