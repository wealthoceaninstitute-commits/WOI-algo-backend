"""
Which process is the trading engine?

After the Phase 1 split two Railway services run this same codebase:

    web     uvicorn app.main:app      -> API only
    engine  uvicorn app.worker:app    -> the trading engine

Only ONE of them may run the engine. Two engines would both see the same
`watching` rows and place duplicate live orders, so two independent guards
stop that:

  1. RUN_ENGINE — app.main starts engine tasks only when RUN_ENGINE=1, and it
     defaults to OFF. A misconfigured web service therefore trades nothing
     rather than trading twice.

  2. A Postgres advisory lock — whoever holds it IS the engine. Even if both
     services are told to run the engine, the second cannot take the lock and
     starts no tasks. The lock lives on a dedicated connection held for the
     life of the process and is released by Postgres when the process exits.
"""

import os

from sqlalchemy import text

# Arbitrary constant; it only has to be unique within this database.
ENGINE_LOCK_KEY = 81420261

_lock_conn = None          # held open for the process lifetime — never closed
_is_engine = False


def run_engine_requested() -> bool:
    """app.main honours RUN_ENGINE. app.worker ignores it — the worker IS the engine."""
    return os.getenv("RUN_ENGINE", "0").strip().lower() in ("1", "true", "yes", "on")


def engine_url() -> str:
    """Base URL of the engine worker, so the web service can forward a manual trigger."""
    return (os.getenv("ENGINE_URL") or "").rstrip("/")


def internal_token() -> str:
    """Shared secret for service-to-service calls. Both services already have SECRET_KEY."""
    from app.core.config import get_settings
    return get_settings().SECRET_KEY


def acquire_engine_lock() -> bool:
    """
    True  — this process is now the engine; the lock is held until it exits.
    False — another process holds it, so start no engine tasks here.

    Never raises: a database that cannot be reached returns False, and the
    caller simply runs without the engine.
    """
    global _lock_conn, _is_engine
    if _is_engine:
        return True
    try:
        from app.core.database import engine_engine
        if engine_engine.dialect.name != "postgresql":
            # Local sqlite development: single process, nothing to lock against.
            _is_engine = True
            return True
        conn = engine_engine.connect()
        got = conn.execute(
            text("SELECT pg_try_advisory_lock(:k)"), {"k": ENGINE_LOCK_KEY}
        ).scalar()
        if got:
            _lock_conn = conn
            _is_engine = True
            return True
        conn.close()
        return False
    except Exception as e:
        print(f"[engine-lock] could not take the engine lock: {type(e).__name__}: {e}")
        return False


def is_engine_process() -> bool:
    """Is the trading engine running inside THIS process?"""
    return _is_engine
