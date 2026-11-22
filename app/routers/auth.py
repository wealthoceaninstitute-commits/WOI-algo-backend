from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import datetime, timezone, timedelta
import secrets

from app.core.database import get_db
from app.core.security import (
    hash_password, verify_password, create_access_token,
    get_current_user, require_master,
)
from app.models.user import User
from app.models.trading import ClientProfile, Fund
from app.schemas.auth import (
    ClientRegisterRequest, LoginRequest, TokenResponse, UserResponse,
    ForgotPasswordRequest, ForgotPasswordResponse,
    ResetPasswordRequest, ChangePasswordRequest,
    MasterResetClientPasswordRequest,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])

# In-memory token store: {token: (user_id, expires_at)}
# For production swap with a DB table or Redis.
_reset_tokens: dict[str, tuple[str, datetime]] = {}

RESET_TOKEN_TTL_MINUTES = 60


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


# ── Forgot / Reset password (no email — token returned for Master to share) ───

@router.post("/forgot-password", response_model=ForgotPasswordResponse)
def forgot_password(payload: ForgotPasswordRequest, db: Session = Depends(get_db)):
    """
    Generate a password-reset token.
    Since no email server is configured, the token is returned in the response.
    Master should copy it and share with the client via WhatsApp / phone.
    Token expires in 60 minutes.
    """
    user = db.query(User).filter(User.email == payload.email.lower()).first()
    # Always return success to avoid user enumeration
    if not user:
        return ForgotPasswordResponse(
            message="If that email is registered, a reset token has been generated."
        )

    # Purge any existing token for this user
    expired_keys = [k for k, (uid, exp) in _reset_tokens.items()
                    if uid == user.id or exp < datetime.now(timezone.utc)]
    for k in expired_keys:
        _reset_tokens.pop(k, None)

    token = secrets.token_urlsafe(32)
    expires = datetime.now(timezone.utc) + timedelta(minutes=RESET_TOKEN_TTL_MINUTES)
    _reset_tokens[token] = (user.id, expires)

    return ForgotPasswordResponse(
        message=f"Reset token generated. It expires in {RESET_TOKEN_TTL_MINUTES} minutes. "
                "Share this token with the client via WhatsApp or phone.",
        reset_token=token,
    )


@router.post("/reset-password")
def reset_password(payload: ResetPasswordRequest, db: Session = Depends(get_db)):
    """
    Reset password using a token obtained from /forgot-password.
    """
    entry = _reset_tokens.get(payload.token)
    if not entry:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    user_id, expires = entry
    if datetime.now(timezone.utc) > expires:
        _reset_tokens.pop(payload.token, None)
        raise HTTPException(status_code=400, detail="Reset token has expired")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.password_hash = hash_password(payload.new_password)
    db.commit()
    _reset_tokens.pop(payload.token, None)

    return {"message": "Password reset successfully. You can now sign in."}


# ── Change password (authenticated) ──────────────────────────────────────────

@router.post("/change-password")
def change_password(
    payload: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Authenticated user changes their own password."""
    if not verify_password(payload.current_password, current_user.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    current_user.password_hash = hash_password(payload.new_password)
    db.commit()
    return {"message": "Password changed successfully"}


# ── Master: reset any client's password directly ─────────────────────────────

@router.post("/master/reset-client-password/{user_id}")
def master_reset_client_password(
    user_id: str,
    payload: MasterResetClientPasswordRequest,
    _master: User = Depends(require_master),
    db: Session = Depends(get_db),
):
    """Master resets a specific client's password directly (no token needed)."""
    user = db.query(User).filter(User.id == user_id, User.role == "CLIENT").first()
    if not user:
        raise HTTPException(status_code=404, detail="Client not found")

    user.password_hash = hash_password(payload.new_password)
    db.commit()
    return {"message": f"Password for {user.name} reset successfully"}
