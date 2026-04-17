from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from psycopg.rows import dict_row

from app.core.db import db_conn
from services.royalty_statement_engine import (
    StatementValidationError,
    run_fetch_statement,
    run_generate_statement,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/royalty/statements-engine", tags=["Royalty Statements Engine"])


class GenerateStatementBody(BaseModel):
    work_id: str = Field(..., description="Work UUID")
    royalty_set_id: str = Field(..., description="Royalty set UUID")
    period_id: Optional[str] = Field(None, description="royalty_periods.id")
    period_start: Optional[str] = Field(
        None, description="ISO date; use with period_end if period_id is omitted"
    )
    period_end: Optional[str] = Field(None, description="ISO date")
    party: str = Field(..., pattern="^(author|illustrator)$")
    rebuild: bool = False
    status: str = Field(default="draft", description="draft | final")


def _require_tenant_id(request: Request) -> str:
    tenant_slug = request.headers.get("X-Tenant")
    if not tenant_slug:
        raise HTTPException(status_code=403, detail="X-Tenant header required for royalty")

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT id::text AS id FROM tenants WHERE slug = %s LIMIT 1",
                (tenant_slug,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")

    return str(row["id"])


@router.post("/generate")
def generate_statement_endpoint(body: GenerateStatementBody, request: Request) -> Dict[str, Any]:
    tenant_id = _require_tenant_id(request)

    logger.info(
        "POST /api/royalty/statements-engine/generate work_id=%s party=%s period_id=%s period_start=%s period_end=%s rebuild=%s",
        body.work_id,
        body.party,
        body.period_id,
        body.period_start,
        body.period_end,
        body.rebuild,
    )

    try:
        return run_generate_statement(
            tenant_id,
            body.work_id,
            body.royalty_set_id,
            body.party,
            period_id=body.period_id,
            period_start=body.period_start,
            period_end=body.period_end,
            rebuild=body.rebuild,
            status=body.status,
        )
    except StatementValidationError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/{statement_id}")
def get_statement_endpoint(statement_id: str, request: Request) -> Dict[str, Any]:
    _require_tenant_id(request)
    try:
        return run_fetch_statement(statement_id)
    except StatementValidationError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e