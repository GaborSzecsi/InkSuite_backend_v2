# SQLAlchemy session maker (later RDS). Placeholder until Postgres is connected.
from __future__ import annotations

from typing import Generator

# When RDS is ready: create_engine, sessionmaker, SessionLocal.
# For now, no DB session; auth can use Cognito only.


def get_db() -> Generator[None, None, None]:
    """Dependency: yield DB session. Placeholder."""
    yield
