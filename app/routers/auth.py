from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import datetime, timezone

from app.core.database import get_db
from app.core.security import hash_password, verify_password, create_access_token, get_current_user
from app.models.user import User
from app.models.trading import ClientProfile, Fund
from app.schemas.auth import ClientRegisterRequest, LoginRequest, TokenResponse, UserResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _create_client_profile(db: Session, user: User, data: ClientRegisterRequest):
    """Create ClientProfile + Fund after user creation."""
    profile = ClientProfile(
        user_id=user.id,
        city=data.city,
        state=data.state,
        capital=data.capital,
        paper_trading=True,
        active_index="SENSEX",
    )
    db.add(profile)
    db.flush()  # get profile.id

    fund = Fund(
        client_profile_id=profile.id,
        available=data.capital,
        used_margin=0,
        total_balance=data.capital,
    )
    db.add(fund)


@router.post("/register", response_model=TokenResponse, status_code=201)
def register(payload: ClientRegisterRequest, db: Session = Depends(get_db)):
    """Self-registration for clients."""
    existing = db.query(User).filter(User.email == payload.email.lower()).first()
    if existing:
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
    _create_client_profile(db, user, payload)
    db.commit()
    db.refresh(user)

    token = create_access_token({"sub": user.id, "role": user.role})
    return TokenResponse(
        access_token=token,
        role=user.role,
        user_id=user.id,
        name=user.name,
    )


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == payload.email.lower()).first()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = create_access_token({"sub": user.id, "role": user.role})
    return TokenResponse(
        access_token=token,
        role=user.role,
        user_id=user.id,
        name=user.name,
    )


# OAuth2 form-compatible login (for Swagger UI "Authorize" button)
@router.post("/token", response_model=TokenResponse, include_in_schema=False)
def token_login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == form.username.lower()).first()
    if not user or not verify_password(form.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_access_token({"sub": user.id, "role": user.role})
    return TokenResponse(access_token=token, role=user.role, user_id=user.id, name=user.name)


@router.get("/me", response_model=UserResponse)
def me(current_user: User = Depends(get_current_user)):
    return current_user
