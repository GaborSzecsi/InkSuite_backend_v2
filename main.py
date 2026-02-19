# InkSuite_backend_v2/main.py — run from project root: python -m uvicorn main:app
from __future__ import annotations

import os
import sys
from pathlib import Path

# Add project root so "routers", "models", "services", "app" are importable
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
ENV_PATH = PROJECT_ROOT / ".env"
if not ENV_PATH.exists():
    ENV_PATH = PROJECT_ROOT.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------
# App (CREATE ONCE)
# ---------------------------------------------------------------------
app = FastAPI(title="Marble App - Book Production Manager", version="1.0.0")

# ---------------------------------------------------------------------
# Data roots (Uploads + Templates)
# ---------------------------------------------------------------------
UPLOAD_DIR = Path("./data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# NOTE: This is a *Windows-ish* path. On your EC2 it becomes /home/ubuntu/Documents/...
# If you really store templates elsewhere on EC2, adjust this.
BASE_DATA_DIR = Path.home() / "Documents" / "marble_app" / "data"
TEMPLATES_DIR = BASE_DATA_DIR / "Templates"
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

DRAFTS_DIR = BASE_DATA_DIR / "TempDraftContracts"
DRAFTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# Auth router (app package). If it fails to load, register a fallback so /auth/login exists and returns 503.
# ---------------------------------------------------------------------
_auth_load_error: str | None = None
try:
    from app.auth.router import router as auth_router
    app.include_router(auth_router, tags=["Auth"])
except Exception as e:
    auth_router = None
    _auth_load_error = str(e)
    import traceback
    traceback.print_exc()
    # Fallback: so frontend gets 503 "Auth not available" instead of 404
    from fastapi import APIRouter
    _auth_fallback = APIRouter(prefix="/auth", tags=["Auth"])

    @_auth_fallback.post("/login")
    def _auth_fallback_login():
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"detail": "Auth module failed to load. Check backend startup logs. Error: " + (_auth_load_error or "unknown")},
        )

    @_auth_fallback.get("/me")
    def _auth_fallback_me():
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=503, content={"detail": "Auth not available"})

    @_auth_fallback.post("/logout")
    def _auth_fallback_logout():
        return {"ok": True}

    app.include_router(_auth_fallback)

# ---------------------------------------------------------------------
# Import routers (local packages: routers, models, services)
# ---------------------------------------------------------------------
from routers import books, royalty, uploads, banking, ingest
from routers import templates as contracts_templates
from routers import deal_memo_drafts
from routers.contract_docs import router as contract_docs_router
from routers.financials import router as financials_router
from routers import financialuploads
from routers import uploads_read

app.include_router(uploads_read.router)

# ---------------------------------------------------------------------
# Routers (mount ONCE, consistently)
# ---------------------------------------------------------------------
app.include_router(royalty.router, prefix="/api", tags=["Royalty"])
app.include_router(books.router, prefix="/api", tags=["Books"])
app.include_router(uploads.router, prefix="/api", tags=["Uploads"])
app.include_router(banking.router, prefix="/api", tags=["Banking"])
app.include_router(uploads_read.router)

# Contracts
app.include_router(contracts_templates.router, prefix="/api", tags=["Contracts"])
app.include_router(contracts_templates.onlyoffice_router, prefix="/api", tags=["ONLYOFFICE"])
app.include_router(deal_memo_drafts.router, prefix="/api", tags=["Contracts Drafts"])
app.include_router(contract_docs_router, prefix="/api", tags=["Contracts Docs"])

# Ingest
app.include_router(ingest.router, prefix="/api", tags=["Ingest"])

# Financials:
# - If your financials router defines routes like "/financials/..." then this makes them "/api/financials/..."
app.include_router(financials_router, tags=["Financials"])

# If financialuploads.router defines routes like "/financials/upload" (NO /api prefix),
# and you want them under /api too, mount it the same way:
app.include_router(financialuploads.router, prefix="/api", tags=["Financial Uploads"])

# ---------------------------------------------------------------------
# Static mounts
# ---------------------------------------------------------------------
app.mount("/static/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")
app.mount("/static/templates", StaticFiles(directory=str(TEMPLATES_DIR)), name="templates")

# ---------------------------------------------------------------------
# Deny-by-default auth: 401 for /api/* without valid token (when REQUIRE_AUTH=1)
# ---------------------------------------------------------------------
REQUIRE_AUTH = os.environ.get("REQUIRE_AUTH", "").strip().lower() in ("1", "true", "yes")

def _has_auth_header(request) -> bool:
    auth = request.headers.get("Authorization") or ""
    return auth.strip().lower().startswith("bearer ") and len(auth.strip()) > 7

@app.middleware("http")
async def require_auth_middleware(request, call_next):
    if not REQUIRE_AUTH:
        return await call_next(request)
    path = request.scope.get("path", "")
    if path.startswith("/api/") and not path.startswith("/api/auth") and path != "/api":
        if not _has_auth_header(request):
            return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
    return await call_next(request)

# ---------------------------------------------------------------------
# CORS (DROP-IN)
# ---------------------------------------------------------------------
# Recommended: set this in backend .env on EC2:
# ALLOW_ORIGINS=https://www.inksuite.io,https://inksuite.io,http://localhost:3000
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
    # Safe dev default: only localhost + RFC1918
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
# Root page
# ---------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def root():
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Marble App - Book Production Manager</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
            .container {{ max-width: 900px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #333; text-align: center; }}
            .links {{ display: flex; gap: 12px; justify-content: center; margin: 30px 0; flex-wrap: wrap; }}
            .link {{ padding: 12px 18px; background: #007bff; color: white; text-decoration: none; border-radius: 6px; }}
            .link:hover {{ background: #0056b3; }}
            .api-link {{ background: #28a745; }}
            .api-link:hover {{ background: #1e7e34; }}
            .info {{ background: #e9ecef; padding: 20px; border-radius: 6px; margin: 20px 0; }}
            code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 4px; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ border-bottom: 1px solid #ddd; padding: 8px; text-align: left; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📚 Marble App - Book Production Manager</h1>
            <div class="links">
                <a href="/docs" class="link api-link">📖 API Documentation</a>
                <a href="/health" class="link">🩺 Health</a>
            </div>

            <div class="info">
                <h3>🚀 Quick Start:</h3>
                <ul>
                    <li><strong>API Docs:</strong> <a href="/docs">/docs</a></li>
                    <li><strong>Books Data:</strong> <a href="/api/royalty/books">/api/royalty/books</a></li>
                    <li><strong>Financial KPIs:</strong> <code>/api/financials/book-kpis?... </code></li>
                    <li><strong>Financial Format Stats:</strong> <code>/api/financials/book-format-stats?... </code></li>
                    <li><strong>List Templates:</strong> <a href="/api/contracts/templates">/api/contracts/templates</a></li>
                </ul>
                <table>
                  <tr><th>Uploads dir</th><td><code>{UPLOAD_DIR.resolve()}</code></td></tr>
                  <tr><th>Templates dir</th><td><code>{TEMPLATES_DIR.resolve()}</code></td></tr>
                  <tr><th>CORS</th><td>{_cors_info}</td></tr>
                  <tr><th>ONLYOFFICE docServiceUrl</th><td><code>{_docservice}</code></td></tr>
                </table>
            </div>
        </div>
    </body>
    </html>
    """
