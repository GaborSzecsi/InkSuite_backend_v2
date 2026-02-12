# marble_app/main.py
from __future__ import annotations

import os
from pathlib import Path
from .routers import deal_memo_drafts
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from marble_app.routers import templates as contracts_templates
from marble_app.routers import deal_memo_drafts
from marble_app.routers.contract_docs import router as contract_docs_router
from marble_app.routers import ingest

# --- Load .env from the project root (the folder that contains "marble_app") ---
# This makes sure ONLYOFFICE_DOCSERVICE_URL and PUBLIC_BASE_URL are available
# even when uvicorn runs with a different working directory.
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # .../Documents/marble_app
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)

# Routers (import AFTER loading env so they can read os.environ if needed)
from marble_app.routers import books, royalty, uploads, banking, financialuploads

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(title="Marble App - Book Production Manager", version="1.0.0")

# -----------------------------------------------------------------------------
# Data roots (Uploads + Templates)
# -----------------------------------------------------------------------------
UPLOAD_DIR = Path("./data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
BASE_DATA_DIR = Path.home() / "Documents" / "marble_app" / "data"
TEMPLATES_DIR = BASE_DATA_DIR / "Templates"
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
DRAFTS_DIR = BASE_DATA_DIR / "TempDraftContracts"

# -----------------------------------------------------------------------------
# Routers (mounted ONCE under /api)
# -----------------------------------------------------------------------------
app.include_router(royalty.router,             prefix="/api", tags=["Royalty"])
app.include_router(books.router,               prefix="/api", tags=["Books"])
app.include_router(uploads.router,             prefix="/api", tags=["Uploads"])
app.include_router(banking.router,             prefix="/api", tags=["Banking"])
app.include_router(contracts_templates.router, prefix="/api", tags=["Contracts"])   # /api/contracts/...
# ONLYOFFICE endpoints: /api/onlyoffice/...
app.include_router(contracts_templates.onlyoffice_router, prefix="/api", tags=["ONLYOFFICE"])
app.include_router(deal_memo_drafts.router, prefix="/api", tags=["Contracts Drafts"])
app.include_router(deal_memo_drafts.router,    prefix="/api", tags=["Contracts"])
app.include_router(contract_docs_router, prefix="/api")
app.include_router(financialuploads.router)
app.include_router(ingest.router)

# -----------------------------------------------------------------------------
# Static mounts
# -----------------------------------------------------------------------------
app.mount("/static/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")
app.mount("/static/templates", StaticFiles(directory=str(TEMPLATES_DIR)), name="templates")

# -----------------------------------------------------------------------------
# CORS
# -----------------------------------------------------------------------------
allow_origins_env = os.environ.get("ALLOW_ORIGINS", "").strip()
if allow_origins_env:
    allow_origins = [o.strip() for o in allow_origins_env.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    _cors_info = ", ".join(allow_origins)
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

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"ok": True}

# -----------------------------------------------------------------------------
# Root page
# -----------------------------------------------------------------------------
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
            <p style="text-align: center; color: #666; font-size: 18px;">
                Welcome to your royalty calculator and book management system!
            </p>
            <div class="links">
                <a href="http://localhost:3000" class="link">🎨 Frontend App</a>
                <a href="/docs" class="link api-link">📖 API Documentation</a>
                <a href="/health" class="link">🩺 Health</a>
            </div>

            <div class="info">
                <h3>🚀 Quick Start:</h3>
                <ul>
                    <li><strong>Frontend:</strong> <a href="http://localhost:3000">http://localhost:3000</a></li>
                    <li><strong>API Docs:</strong> <a href="/docs">/docs</a></li>
                    <li><strong>Books Data:</strong> <a href="/api/royalty/books">/api/royalty/books</a></li>
                    <li><strong>Royalty Categories:</strong> <a href="/api/royalty/categories">/api/royalty/categories</a></li>
                    <li><strong>List Templates:</strong> <a href="/api/contracts/templates">/api/contracts/templates</a></li>
                    <li><strong>ONLYOFFICE Config (example):</strong> <code>/api/onlyoffice/config/&lt;id&gt;</code></li>
                    <li><strong>Templates folder (static):</strong> <a href="/static/templates">/static/templates</a></li>
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
