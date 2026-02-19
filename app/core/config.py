# Env loading and settings.
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _env(key: str, default: str = "") -> str:
    return (os.environ.get(key) or default).strip()


class Settings:
    """Load from env vars (dotenv loaded in main)."""

    def __init__(self) -> None:
        self.cognito_user_pool_id = _env("COGNITO_USER_POOL_ID")
        self.cognito_client_id = _env("COGNITO_CLIENT_ID")
        # Prefer COGNITO_REGION so you can use us-east-1 for Cognito while AWS_REGION is us-east-2 for other services
        self.cognito_region = _env("COGNITO_REGION") or _env("AWS_REGION") or _env("AWS_DEFAULT_REGION") or "us-east-1"
        self.jwt_jwks_uri = _env("JWT_JWKS_URI")
        self.cognito_issuer = _env("COGNITO_ISSUER")  # optional; built from region + user_pool_id if not set
        self.jwks_url = _env("JWKS_URL")  # optional; built from issuer if not set
        self.cognito_client_secret = _env("COGNITO_CLIENT_SECRET")  # only if app client has a secret
        self.allow_origins = _env("ALLOW_ORIGINS")
        self.node_env = _env("NODE_ENV", "development")


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings

settings = get_settings()
