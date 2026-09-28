"""Authenticated, work-scoped royalty payment routing (existing schema)."""

from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.auth.dependencies import get_current_user
from app.core.db import db_conn

router = APIRouter(prefix="/payment-instructions", tags=["Payment instructions"])
Party = Literal["author", "illustrator"]


class Instruction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payee_mode: Literal["contributor_only", "agency_only", "split"]
    contributor_party_id: UUID
    agency_party_id: UUID | None = None
    contributor_percent: Decimal = Field(
        ge=0, le=100, decimal_places=4, allow_inf_nan=False
    )
    agency_percent: Decimal = Field(ge=0, le=100, decimal_places=4, allow_inf_nan=False)
    effective_start: date | None = None
    effective_end: date | None = None
    notes: str | None = Field(default=None, max_length=10000)

    @model_validator(mode="after")
    def validate_routing(self):
        if (
            self.effective_start
            and self.effective_end
            and self.effective_end < self.effective_start
        ):
            raise ValueError("Effective until cannot precede effective from.")
        if self.payee_mode == "contributor_only":
            self.agency_party_id = None
            self.contributor_percent, self.agency_percent = Decimal(100), Decimal(0)
        else:
            if not self.agency_party_id:
                raise ValueError(
                    "Select an agency before saving these payment instructions."
                )
            if self.payee_mode == "agency_only":
                self.contributor_percent, self.agency_percent = Decimal(0), Decimal(100)
            elif self.contributor_percent + self.agency_percent != Decimal(100):
                raise ValueError("Split percentages must total exactly 100%.")
        return self


def authorized_tenant(cur, work_id, claims):
    cur.execute(
        """SELECT w.tenant_id, m.role, m.module_permissions
        FROM public.works w JOIN public.memberships m ON m.tenant_id=w.tenant_id
        JOIN public.users u ON u.id=m.user_id
        WHERE w.id=%s AND u.cognito_sub=%s""",
        (work_id, claims.get("sub")),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Book not found or access unavailable.")
    permissions = row["module_permissions"] or {}
    if row["role"] in ("reader", "librarian") or not (
        row["role"] == "tenant_admin"
        or any(
            permissions.get(key) is True
            for key in (
                "books",
                "project_management",
                "contracts",
                "financials",
                "royalty",
            )
        )
    ):
        raise HTTPException(
            403, "Payment instructions require publisher module access."
        )
    return row["tenant_id"]


def current_instruction(cur, tenant_id, work_id, party):
    cur.execute(
        """SELECT id, payee_mode, contributor_party_id, agency_party_id,
        contributor_percent, agency_percent, effective_start, effective_end, notes, updated_at
        FROM public.royalty_payment_instructions
        WHERE tenant_id=%s AND work_id=%s AND party=%s::roy_party
        ORDER BY updated_at DESC, id LIMIT 2""",
        (tenant_id, work_id, party),
    )
    rows = cur.fetchall()
    if len(rows) > 1:
        raise HTTPException(
            409,
            "Multiple payment instructions exist for this book and role. Resolve them before editing.",
        )
    return rows[0] if rows else None


def reply(instruction):
    return JSONResponse(
        jsonable_encoder({"instruction": instruction}),
        headers={"Cache-Control": "private, no-store"},
    )


@router.get("/{work_id}/{party}")
def get_instruction(work_id: UUID, party: Party, claims=Depends(get_current_user)):
    with db_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        tenant_id = authorized_tenant(cur, work_id, claims)
        return reply(current_instruction(cur, tenant_id, work_id, party))


@router.put("/{work_id}/{party}")
def save_instruction(
    work_id: UUID, party: Party, payload: Instruction, claims=Depends(get_current_user)
):
    with db_conn() as conn, conn.transaction(), conn.cursor(
        row_factory=dict_row
    ) as cur:
        tenant_id = authorized_tenant(cur, work_id, claims)
        # The existing table has no unique work/party constraint. Serialize writes
        # on the existing work row so two initial saves cannot insert duplicates.
        cur.execute(
            "SELECT id FROM public.works WHERE id=%s AND tenant_id=%s FOR UPDATE",
            (work_id, tenant_id),
        )
        if not cur.fetchone():
            raise HTTPException(404, "Book not found.")
        for party_id, is_agency in [
            (payload.contributor_party_id, False),
            (payload.agency_party_id, True),
        ]:
            if not party_id:
                continue
            cur.execute(
                "SELECT party_type FROM public.parties WHERE id=%s AND tenant_id=%s",
                (party_id, tenant_id),
            )
            record = cur.fetchone()
            if not record or (is_agency and record["party_type"] != "org"):
                raise HTTPException(
                    422,
                    "The selected contributor or agency does not belong to this publisher.",
                )
        existing = current_instruction(cur, tenant_id, work_id, party)
        values = (
            payload.payee_mode,
            payload.contributor_party_id,
            payload.agency_party_id,
            payload.contributor_percent,
            payload.agency_percent,
            payload.effective_start,
            payload.effective_end,
            payload.notes,
        )
        if existing:
            cur.execute(
                """UPDATE public.royalty_payment_instructions SET payee_mode=%s,
                contributor_party_id=%s, agency_party_id=%s, contributor_percent=%s,
                agency_percent=%s, effective_start=%s, effective_end=%s, notes=%s, updated_at=now()
                WHERE id=%s AND tenant_id=%s""",
                (*values, existing["id"], tenant_id),
            )
        else:
            cur.execute(
                """INSERT INTO public.royalty_payment_instructions
                (payee_mode, contributor_party_id, agency_party_id, contributor_percent,
                 agency_percent, effective_start, effective_end, notes, tenant_id, work_id, party)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::roy_party)""",
                (*values, tenant_id, work_id, party),
            )
        return reply(current_instruction(cur, tenant_id, work_id, party))
