from __future__ import annotations

import os
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# --- Load .env early (before importing routers that may read env vars) ---
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ENV_PATH = PROJECT_ROOT / ".env"
if not ENV_PATH.exists():
    ENV_PATH = PROJECT_ROOT.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)

# ---------------------------------------------------------------------
# App
# ---------------------------------------------------------------------
app = FastAPI(title="InkSuite API", version="1.0.0")

# ---------------------------------------------------------------------
# Data roots (Uploads + Templates)
# ---------------------------------------------------------------------
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", "./data/uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Keep your original intent but make it overrideable so EC2/Windows don’t fight.
base_data_env = (os.environ.get("BASE_DATA_DIR") or "").strip()
BASE_DATA_DIR = Path(base_data_env) if base_data_env else (Path.home() / "Documents" / "marble_app" / "data")

templates_env = (os.environ.get("TEMPLATES_DIR") or "").strip()
TEMPLATES_DIR = Path(templates_env) if templates_env else (BASE_DATA_DIR / "Templates")
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

DRAFTS_DIR = BASE_DATA_DIR / "TempDraftContracts"
DRAFTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Auth + Tenants + Invites (app)
# ---------------------------------------------------------------------
from app.auth.router import router as auth_router  # noqa: E402
from app.tenants.router import router as tenants_router  # noqa: E402
from app.invites.router import router as invites_router  # noqa: E402

# ---------------------------------------------------------------------
# Import routers (AFTER env load) - routers package is at project root (routers/)
# ---------------------------------------------------------------------
# These module names must match files under routers/
from routers import books, royalty, uploads, banking, ingest  # noqa: E402
from routers import templates as contracts_templates  # noqa: E402
from routers import deal_memo_drafts  # noqa: E402
from routers.contract_docs import router as contract_docs_router  # noqa: E402
from routers.financials import router as financials_router  # noqa: E402
from routers import financialuploads  # noqa: E402
from routers import uploads_read  # noqa: E402

# ---------------------------------------------------------------------
# Routers (mount ONCE, consistently)
# ---------------------------------------------------------------------
# Auth router has prefix="/auth" inside router.py, so this yields /api/auth/login, /api/auth/me, etc.
app.include_router(auth_router, prefix="/api")
app.include_router(tenants_router, prefix="/api")
app.include_router(invites_router, prefix="/api")

# Uploads read router: mount ONCE.
# If uploads_read has absolute paths like "/api/uploads/book-assets" inside it, mount WITHOUT prefix.
# If it has relative paths like "/uploads/book-assets", mount WITH prefix="/api".
# Your earlier uploads_read patterns used absolute /api/... paths, so keep it unprefixed:
app.include_router(uploads_read.router)

# Core
app.include_router(royalty.router, prefix="/api", tags=["Royalty"])
app.include_router(books.router, prefix="/api", tags=["Books"])
app.include_router(uploads.router, prefix="/api", tags=["Uploads"])
app.include_router(banking.router, prefix="/api", tags=["Banking"])

# Contracts
app.include_router(contracts_templates.router, prefix="/api", tags=["Contracts"])
app.include_router(contracts_templates.onlyoffice_router, prefix="/api", tags=["ONLYOFFICE"])
app.include_router(deal_memo_drafts.router, prefix="/api", tags=["Contracts Drafts"])
app.include_router(contract_docs_router, prefix="/api", tags=["Contracts Docs"])

# Ingest
app.include_router(ingest.router, prefix="/api", tags=["Ingest"])

# Financials: mount under /api so it’s protected and consistent with your frontend URLs
app.include_router(financials_router, prefix="/api", tags=["Financials"])

# Upload endpoints under /api
app.include_router(financialuploads.router, prefix="/api", tags=["Financial Uploads"])

# ---------------------------------------------------------------------
# Static mounts
# ---------------------------------------------------------------------
app.mount("/static/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")
app.mount("/static/templates", StaticFiles(directory=str(TEMPLATES_DIR)), name="templates")

# ---------------------------------------------------------------------
# Deny-by-default auth: STRICT validation for /api/* (when REQUIRE_AUTH=1)
# ---------------------------------------------------------------------
REQUIRE_AUTH = os.environ.get("REQUIRE_AUTH", "").strip().lower() in ("1", "true", "yes")

def _bearer_token_from_header(auth_header: str | None) -> str | None:
    if not auth_header:
        return None
    s = auth_header.strip()
    if not s.lower().startswith("bearer "):
        return None
    token = s[7:].strip()
    return token or None

@app.middleware("http")
async def require_auth_middleware(request, call_next):
    if not REQUIRE_AUTH:
        return await call_next(request)

    path = request.url.path or ""

    # only protect /api/*
    if not path.startswith("/api/"):
        return await call_next(request)

    # allow auth and public invite endpoints
    if path.startswith("/api/auth") or path.startswith("/api/invites"):
        return await call_next(request)

    token = _bearer_token_from_header(request.headers.get("Authorization"))
    if not token:
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})

    # validate token (not just presence)
    try:
        from app.auth.service import get_current_user_from_token
        claims = get_current_user_from_token(token)
    except Exception:
        claims = None

    if not claims:
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})

    request.state.user_claims = claims

    # Restrict /api/royalty to tenant_admin (or superadmin) for the given X-Tenant
    if path.startswith("/api/royalty"):
        tenant_slug = (request.headers.get("X-Tenant") or "").strip()
        if not tenant_slug:
            return JSONResponse(status_code=403, content={"detail": "X-Tenant header required for royalty"})
        try:
            from app.auth.service import get_user_db_record_from_claims, get_memberships_for_user, is_superadmin
            from app.tenants.resolver import resolve_tenant
            user = get_user_db_record_from_claims(claims)
            if not user:
                return JSONResponse(status_code=403, content={"detail": "Forbidden"})
            if is_superadmin(user.get("platform_role")):
                pass  # allow
            else:
                tenant = resolve_tenant(tenant_slug)
                if not tenant:
                    return JSONResponse(status_code=403, content={"detail": "Tenant not found"})
                memberships = get_memberships_for_user(user["id"])
                role = next((m["role"] for m in memberships if (m.get("tenant_slug") or "").lower() == tenant_slug.lower()), None)
                if role != "tenant_admin":
                    return JSONResponse(status_code=403, content={"detail": "Royalty calculator is restricted to admins"})
        except Exception:
            return JSONResponse(status_code=403, content={"detail": "Forbidden"})

    return await call_next(request)

# ---------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------
allow_origins_env = os.environ.get("ALLOW_ORIGINS", "").strip()

if allow_origins_env:
    allow_origins = [o.strip().rstrip("/") for o in allow_origins_env.split(",") if o.strip()]
    _cors_info = ", ".join(allow_origins)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
else:
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"^https?://((localhost|127\.0\.0\.1)(:\d+)?|10\.\d+\.\d+\.\d+(:\d+)?|192\.168\.\d+\.\d+(:\d+)?|172\.(1[6-9]|2\d|3[0-1])\.\d+\.\d+(:\d+)?)$",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    _cors_info = "regex(localhost/127.0.0.1/RFC1918)"

_docservice = os.environ.get("ONLYOFFICE_DOCSERVICE_URL", "").strip() or "(not set)"

# ---------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True}

# ---------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def root():
    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>InkSuite API</title></head>
    <body style="font-family: Arial; margin: 40px;">
      <h2>InkSuite API</h2>
      <ul>
        <li><a href="/docs">/docs</a></li>
        <li><a href="/health">/health</a></li>
        <li><code>REQUIRE_AUTH={REQUIRE_AUTH}</code></li>
        <li><code>CORS={_cors_info}</code></li>
        <li><code>ONLYOFFICE={_docservice}</code></li>
      </ul>
    </body>
    </html>
    """
