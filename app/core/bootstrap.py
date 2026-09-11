"""
Auto-create the master user on startup if MASTER_EMAIL and MASTER_PASSWORD
are set in environment variables. Idempotent - runs safely on every deploy.
"""
from sqlalchemy.orm import Session
from app.core.config import get_settings
from app.core.database import SessionLocal
from app.core.security import hash_password
from app.models.user import User

settings = get_settings()


def create_master_if_needed():
    if not settings.MASTER_EMAIL or not settings.MASTER_PASSWORD:
        print("[bootstrap] MASTER_EMAIL / MASTER_PASSWORD not set - skipping master creation")
        return

    db: Session = SessionLocal()
    try:
        email = settings.MASTER_EMAIL.lower().strip()
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            print(f"[bootstrap] Master user already exists: {email}")
            return

        master = User(
            name=settings.MASTER_NAME,
            email=email,
            password_hash=hash_password(settings.MASTER_PASSWORD),
            role="MASTER",
        )
        db.add(master)
        db.commit()
        print(f"[bootstrap] Master user created: {email}")
    except Exception as e:
        print(f"[bootstrap] Failed to create master: {e}")
        db.rollback()
    finally:
        db.close()
