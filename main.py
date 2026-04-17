# InkSuite_backend_v2 entry point.
# Start with: python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
# Do NOT use models.royalty:app (that app has no /api/tenants or /api/auth).
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import psycopg
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
# Log so you can confirm which .env is loaded (and that WOPI_PUBLIC_BASE / PUBLIC_BASE_URL are in that file)
logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__).info("Loaded env from %s (WOPI_PUBLIC_BASE=%s, PUBLIC_BASE_URL=%s)", ENV_PATH, os.getenv("WOPI_PUBLIC_BASE") or "(not set)", os.getenv("PUBLIC_BASE_URL") or "(not set)")

# ---------------------------------------------------------------------
# App
# ---------------------------------------------------------------------
app = FastAPI(title="InkSuite API", version="1.0.0")
logger = logging.getLogger(__name__)


@app.exception_handler(psycopg.Error)
async def db_error_handler(request, exc: psycopg.Error):
    """Return 503 on DB connection/query errors so clients get a clear response instead of 500 + traceback."""
    logger.warning("Database error: %s", exc)
    return JSONResponse(
        status_code=503,
        content={
            "detail": "Service temporarily unavailable. The database is unreachable—ensure PostgreSQL is running and DATABASE_URL is correct in the backend .env.",
            "error": str(exc),
        },
    )


# ---------------------------------------------------------------------
# Health check (diagnose DB from running server)
# ---------------------------------------------------------------------
@app.get("/api/health")
def health():
    """Returns 200 if DB is reachable, 503 with error detail otherwise. Use to verify DB from the running process."""
    try:
        from app.core.db import db_conn
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return {"status": "ok", "database": "ok"}
    except Exception as e:
        logger.warning("Health check DB error: %s", e)
        return JSONResponse(
            status_code=503,
            content={"status": "error", "database": "unreachable", "detail": str(e)},
        )


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
from app.admin.router import router as admin_router  # noqa: E402
from app.settings.router import router as settings_router


# ---------------------------------------------------------------------
# Import routers (AFTER env load) - routers package is at project root (routers/)
# ---------------------------------------------------------------------
# These module names must match files under routers/
from routers import books, royalty, uploads, banking, ingest  # noqa: E402
from routers import templates as contracts_templates  # noqa: E402
from routers import deal_memo_drafts  # noqa: E402
from routers.contract_docs import router as contract_docs_router, wopi_router  # noqa: E402
from routers.financials import router as financials_router  # noqa: E402
from routers import financialuploads  # noqa: E402
#from routers import uploads_read  # noqa: E402
from routers.contract_invites import router as contract_invites
from routers.catalog import router as catalog_router
from app.onix.router import router as onix_router
from routers.salesdata import router as salesdata_router
from routers.royalty_engine import router as royalty_engine_router

# ---------------------------------------------------------------------
# Routers (mount ONCE, consistently)
# ---------------------------------------------------------------------
# Auth router has prefix="/auth" inside router.py, so this yields /api/auth/login, /api/auth/me, etc.
app.include_router(auth_router, prefix="/api")
app.include_router(tenants_router, prefix="/api")
app.include_router(invites_router, prefix="/api")
app.include_router(admin_router, prefix="/api")
app.include_router(settings_router, prefix="/api")
app.include_router(contract_invites, prefix="/api")
app.include_router(catalog_router, prefix="/api")
app.include_router(onix_router, prefix="/api")

# Uploads read router: mount ONCE.
# If uploads_read has absolute paths like "/api/uploads/book-assets" inside it, mount WITHOUT prefix.
# If it has relative paths like "/uploads/book-assets", mount WITH prefix="/api".
# Your earlier uploads_read patterns used absolute /api/... paths, so keep it unprefixed:
#app.include_router(uploads_read.router)

# Core
app.include_router(royalty.router, prefix="/api", tags=["Royalty"])
app.include_router(royalty_engine_router, prefix="/api", tags=["Royalty Statements Engine"])
app.include_router(books.router, prefix="/api", tags=["Books"])
app.include_router(uploads.router, prefix="/api", tags=["Uploads"])
app.include_router(banking.router, prefix="/api", tags=["Banking"])

# Contracts
app.include_router(contracts_templates.router, prefix="/api", tags=["Contracts"])
app.include_router(deal_memo_drafts.router, prefix="/api", tags=["Contracts Drafts"])
app.include_router(contract_docs_router, prefix="/api", tags=["Contracts Docs"])
# WOPI host for Collabora Online (draft contracts + templates)
app.include_router(wopi_router, prefix="/api", tags=["WOPI"])
app.include_router(contracts_templates.wopi_templates_router, prefix="/api", tags=["WOPI Templates"])

# Ingest
app.include_router(ingest.router, prefix="/api", tags=["Ingest"])

# Financials: mount under /api so it’s protected and consistent with your frontend URLs
app.include_router(financials_router, prefix="/api", tags=["Financials"])
app.include_router(salesdata_router, prefix="/api")

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

for r in app.routes:
    methods = ",".join(sorted(getattr(r, "methods", []) or []))
    print(f"{methods:20s} {r.path}")

def _bearer_token_from_header(auth_header: str | None) -> str | None:
    if not auth_header:
        return None
    s = auth_header.strip()
    if not s.lower().startswith("bearer "):
        return None
    token = s[7:].strip()
    return token or None


def _token_from_cookie(cookie_header: str | None) -> str | None:
    """Parse access_token from Cookie header (e.g. when BFF forwards cookie or catch-all proxies)."""
    if not cookie_header:
        return None
    for part in cookie_header.split(";"):
        part = part.strip()
        if part.lower().startswith("access_token="):
            return part[13:].strip() or None
    return None


@app.middleware("http")
async def require_auth_middleware(request, call_next):
    path = request.url.path or ""

    # only care about /api/* for auth
    if not path.startswith("/api/"):
        return await call_next(request)

    # allow preflight (no credentials sent)
    if request.method.upper() == "OPTIONS":
        return await call_next(request)

    # allow auth and public invite endpoints (no token required)
    if path.startswith("/api/auth") or path.startswith("/api/invites"):
        return await call_next(request)
    # public: resolve contract invite by token (agent review link, no login)
    if path.startswith("/api/contracts/invites/") and request.method.upper() == "GET":
        return await call_next(request)

    # Always try to resolve token and set user_claims when valid (so routes like contract invites work even when REQUIRE_AUTH=0)
    token = _bearer_token_from_header(request.headers.get("Authorization"))
    if not token:
        token = _token_from_cookie(request.headers.get("Cookie"))
    if token:
        try:
            from app.auth.service import get_current_user_from_token
            claims = get_current_user_from_token(token)
            if claims:
                request.state.user_claims = claims
        except Exception:
            pass

    # When strict auth is on, require a valid token
    if REQUIRE_AUTH and not getattr(request.state, "user_claims", None):
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})

    # Restrict /api/royalty to tenant_admin (or superadmin) for the given X-Tenant
    if path.startswith("/api/royalty"):
        claims = getattr(request.state, "user_claims", None)
        tenant_slug = (request.headers.get("X-Tenant") or "").strip()
        if not tenant_slug:
            return JSONResponse(status_code=403, content={"detail": "X-Tenant header required for royalty"})
        try:
            from app.auth.service import get_user_db_record_from_claims, get_memberships_for_user, is_superadmin
            from app.tenants.resolver import resolve_tenant
            user = get_user_db_record_from_claims(claims) if claims else None
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
