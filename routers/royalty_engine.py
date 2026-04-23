from __future__ import annotations

import base64
import logging
import requests
import os
from datetime import datetime
from typing import Any, Dict, Optional, List
import json
import smtplib
from email.message import EmailMessage

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from app.email.templates import render_royalty_statement_email

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from psycopg.rows import dict_row
from routers.storage_s3 import put_bytes, tenant_data_prefix

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


class BulkSaveBody(BaseModel):
    statement_ids: List[str]


class SendStatementBody(BaseModel):
    contributor_email: Optional[str] = None
    agent_email: Optional[str] = None
    send_to_contributor: bool = True
    send_to_agent: bool = False


class BulkSendItem(BaseModel):
    statement_id: str
    contributor_email: Optional[str] = None
    agent_email: Optional[str] = None
    send_to_contributor: bool = True
    send_to_agent: bool = False


class BulkSendBody(BaseModel):
    items: List[BulkSendItem]


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


def _money(v: Any) -> str:
    try:
        return f"${float(v or 0):,.2f}"
    except Exception:
        return "$0.00"


def _esc(v: Any) -> str:
    s = "" if v is None else str(v)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _logo_html() -> str:
    logo_url = os.getenv(
        "MARBLE_LOGO_URL",
        "https://inksuite-data.s3.us-east-2.amazonaws.com/tenants/marble-press/assets/logo+long2+NEW.png",
    )

    try:
        resp = requests.get(logo_url, timeout=5)
        resp.raise_for_status()

        encoded = base64.b64encode(resp.content).decode("utf-8")

        return f'<img src="data:image/png;base64,{encoded}" style="height:60px;" />'

    except Exception as e:
        print("⚠️ Failed to load logo from S3:", e)
        return "<div style='font-weight:bold;'>Marble Press</div>"


def _split_lines(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if value is None:
        return []
    txt = str(value).strip()
    if not txt:
        return []
    return [line.strip() for line in txt.splitlines() if line.strip()]


def _section_lines(
    lines: list[Dict[str, Any]],
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]]]:
    first_rights: list[Dict[str, Any]] = []
    canada_export: list[Dict[str, Any]] = []
    subrights: list[Dict[str, Any]] = []

    for ln in lines:
        section = str(ln.get("display_section") or "").strip().lower()
        label = str(ln.get("category_label") or "").strip().lower()
        line_type = str(ln.get("line_type") or "").strip().lower()

        if section == "canada_export" or label.startswith("canada -") or label.startswith("export -"):
            canada_export.append(ln)
        elif section == "subrights" or line_type == "subrights":
            subrights.append(ln)
        else:
            first_rights.append(ln)

    return first_rights, canada_export, subrights

def _load_smtp_secret(secret_id: str) -> tuple[str, str]:
    if not secret_id:
        raise ValueError("SMTP secret id is not configured.")

    client = boto3.client("secretsmanager")
    try:
        resp = client.get_secret_value(SecretId=secret_id)
    except (ClientError, BotoCoreError) as e:
        raise ValueError(f"Failed to read SMTP secret from Secrets Manager: {e}")

    raw = resp.get("SecretString")
    if not raw and "SecretBinary" in resp:
        raw = resp["SecretBinary"]
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")

    if not raw:
        raise ValueError("SMTP secret is empty.")

    try:
        data = json.loads(raw)
    except Exception:
        raise ValueError("SMTP secret is not valid JSON.")

    username = data.get("username") or data.get("user") or data.get("smtp_username") or ""
    password = data.get("password") or data.get("pass") or data.get("smtp_password") or ""

    if not username or not password:
        raise ValueError("SMTP secret must contain username and password.")

    return str(username), str(password)


def _load_tenant_email_settings_or_400(tenant_slug: str) -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT from_name, from_email,
                       smtp_host, smtp_port, tls_mode,
                       smtp_username, smtp_secret_id, is_enabled
                FROM tenant_email_settings
                WHERE tenant_slug = %s
                """,
                (tenant_slug,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=400, detail="Email settings not configured for this tenant")

    from_name = (row.get("from_name") or "").strip()
    from_email = (row.get("from_email") or "").strip()
    smtp_host = (row.get("smtp_host") or "").strip()
    smtp_port = row.get("smtp_port")
    tls_mode = (row.get("tls_mode") or "starttls").strip().lower()
    smtp_secret_id = (row.get("smtp_secret_id") or "").strip()
    is_enabled = bool(row.get("is_enabled"))

    if not is_enabled:
        raise HTTPException(status_code=400, detail="Email sending is disabled for this tenant")

    problems: list[str] = []
    if not from_email:
        problems.append("From email is not configured.")
    if not smtp_host:
        problems.append("SMTP host is not configured.")
    if not smtp_port:
        problems.append("SMTP port is not configured.")
    if not smtp_secret_id:
        problems.append("SMTP secret id is not configured.")
    if problems:
        raise HTTPException(status_code=400, detail=" ".join(problems))

    return {
        "from_name": from_name or from_email,
        "from_email": from_email,
        "smtp_host": smtp_host,
        "smtp_port": int(smtp_port or 587),
        "tls_mode": tls_mode,
        "smtp_secret_id": smtp_secret_id,
    }


def _send_email_smtp(
    *,
    smtp_host: str,
    smtp_port: int,
    tls_mode: str,
    username: str,
    password: str,
    from_email: str,
    from_name: str,
    to_email: str,
    cc_email: Optional[str],
    subject: str,
    body_text: str,
    attachment_filename: str,
    attachment_bytes: bytes,
) -> None:
    msg = EmailMessage()
    msg["To"] = to_email
    if cc_email:
        msg["Cc"] = cc_email
    msg["From"] = f"{from_name} <{from_email}>" if from_name else from_email
    msg["Subject"] = subject
    msg.set_content(body_text)
    msg.add_attachment(
        attachment_bytes,
        maintype="application",
        subtype="pdf",
        filename=attachment_filename,
    )

    if tls_mode == "ssl":
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as smtp:
            smtp.login(username, password)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(username, password)
            smtp.send_message(msg)


def _statement_recipients(cur, tenant_id: str, statement_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        WITH base AS (
            SELECT
                rs.id AS statement_id,
                rs.work_id,
                rs.period_id,
                rs.party::text AS party,
                rs.status,
                rs.pdf_saved_at,
                rs.pdf_s3_key,
                rs.sent_at,
                rs.sent_to_contributor_email,
                rs.sent_to_agent_email,
                w.title,
                w.subtitle,
                rp.period_code,
                rp.period_start,
                rp.period_end
            FROM royalty_statements rs
            JOIN works w
              ON w.id = rs.work_id
            JOIN royalty_periods rp
              ON rp.id = rs.period_id
            WHERE rs.tenant_id = %s::uuid
              AND rs.id = %s::uuid
            LIMIT 1
        ),
        contributor_candidates AS (
            SELECT
                b.statement_id,
                wc.party_id AS contributor_party_id,
                p.display_name AS contributor_name,
                p.email AS contributor_email,
                wc.sequence_number
            FROM base b
            JOIN work_contributors wc
              ON wc.work_id = b.work_id
            JOIN parties p
              ON p.id = wc.party_id
            WHERE (
                b.party = 'author'
                AND upper(COALESCE(wc.contributor_role, '')) = 'AUTHOR'
            ) OR (
                b.party = 'illustrator'
                AND upper(COALESCE(wc.contributor_role, '')) = 'ILLUSTRATOR'
            )
        ),
        picked_contributor AS (
            SELECT DISTINCT ON (statement_id)
                statement_id,
                contributor_party_id,
                contributor_name,
                contributor_email
            FROM contributor_candidates
            ORDER BY statement_id, sequence_number, contributor_party_id
        ),
        agent_candidates AS (
            SELECT
                b.statement_id,
                pr.id AS representation_id,
                pr.is_primary,
                pr.role_label,
                pr.created_at,
                pr.work_id AS representation_work_id,
                agent.id AS agent_party_id,
                agent.display_name AS agent_name,
                agent.email AS agent_email
            FROM base b
            JOIN picked_contributor pc
              ON pc.statement_id = b.statement_id
            LEFT JOIN party_representations pr
              ON pr.represented_party_id = pc.contributor_party_id
             AND (pr.work_id = b.work_id OR pr.work_id IS NULL)
            LEFT JOIN parties agent
              ON agent.id = pr.agent_party_id
        ),
        picked_agent AS (
            SELECT DISTINCT ON (statement_id)
                statement_id,
                representation_id,
                is_primary,
                role_label,
                agent_party_id,
                agent_name,
                agent_email
            FROM agent_candidates
            ORDER BY
                statement_id,
                is_primary DESC NULLS LAST,
                (representation_work_id IS NOT NULL) DESC,
                created_at DESC NULLS LAST,
                representation_id
        )
        SELECT
            b.statement_id::text AS statement_id,
            b.work_id::text AS work_id,
            b.title,
            b.subtitle,
            b.party,
            b.period_id::text AS period_id,
            b.period_code,
            b.period_start::text AS period_start,
            b.period_end::text AS period_end,
            b.status,
            b.pdf_saved_at,
            b.pdf_s3_key,
            b.sent_at,
            b.sent_to_contributor_email,
            b.sent_to_agent_email,
            pc.contributor_party_id::text AS contributor_party_id,
            pc.contributor_name,
            pc.contributor_email,
            pa.representation_id::text AS representation_id,
            pa.is_primary,
            pa.role_label,
            pa.agent_party_id::text AS agent_party_id,
            pa.agent_name,
            pa.agent_email
        FROM base b
        LEFT JOIN picked_contributor pc
          ON pc.statement_id = b.statement_id
        LEFT JOIN picked_agent pa
          ON pa.statement_id = b.statement_id
        LIMIT 1
        """,
        (tenant_id, statement_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Statement not found.")
    return dict(row)


def _first_rights_rows(rows: list[Dict[str, Any]]) -> str:
    if not rows:
        return """
        <tr>
          <td colspan="8" class="empty-cell">No first-rights activity this period.</td>
        </tr>
        """

    body: list[str] = []
    total = 0.0

    for ln in rows:
        royalty_amount = float(ln.get("royalty_amount") or 0)
        total += royalty_amount

        net_units_raw = ln.get("net_units")
        basis_amount = float(ln.get("basis_amount") or 0)
        basis_type = str(ln.get("basis_type") or ln.get("royalty_base") or "").strip().lower()
        is_net_receipts = basis_type == "net_receipts"

        unit_price = "Net receipts"
        try:
            net_units = float(net_units_raw) if net_units_raw is not None else 0.0
            if not is_net_receipts and net_units:
                unit_price = _money(basis_amount / net_units)
        except Exception:
            if not is_net_receipts:
                unit_price = "—"

        note = _esc(ln.get("rule_condition_text") or ln.get("rule_label") or "")

        body.append(
            f"""
            <tr>
              <td>
                <div class="main-cell">{_esc(ln.get("category_label", ""))}</div>
                {f'<div class="sub-cell">{note}</div>' if note else ''}
              </td>
              <td class="num">{_esc(ln.get("units_sold", ""))}</td>
              <td class="num">{_esc(ln.get("units_returned", ""))}</td>
              <td class="num">{_esc(ln.get("net_units", ""))}</td>
              <td class="num">{unit_price}</td>
              <td class="num">{_esc(ln.get("royalty_rate", ""))}</td>
              <td class="num">{_money(ln.get("basis_amount"))}</td>
              <td class="num strong">{_money(ln.get("royalty_amount"))}</td>
            </tr>
            """
        )

    body.append(
        f"""
        <tr class="total-row">
          <td colspan="7" class="num strong">Total</td>
          <td class="num strong">{_money(total)}</td>
        </tr>
        """
    )
    return "".join(body)


def _canada_export_rows(rows: list[Dict[str, Any]]) -> str:
    if not rows:
        return """
        <tr>
          <td colspan="5" class="empty-cell">No Canada or export activity this period.</td>
        </tr>
        """

    body: list[str] = []
    total = 0.0

    for ln in rows:
        royalty_amount = float(ln.get("royalty_amount") or 0)
        total += royalty_amount
        note = _esc(ln.get("source_us_condition_text") or ln.get("derived_rate_formula") or "")

        body.append(
            f"""
            <tr>
              <td>
                <div class="main-cell">{_esc(ln.get("category_label", ""))}</div>
                {f'<div class="sub-cell">{note}</div>' if note else ''}
              </td>
              <td class="num">{_money(ln.get("basis_amount"))}</td>
              <td class="num">{_esc(ln.get("source_us_rate_percent", ""))}</td>
              <td class="num">{_esc(ln.get("royalty_rate", ""))}</td>
              <td class="num strong">{_money(ln.get("royalty_amount"))}</td>
            </tr>
            """
        )

    body.append(
        f"""
        <tr class="total-row">
          <td colspan="4" class="num strong">Total</td>
          <td class="num strong">{_money(total)}</td>
        </tr>
        """
    )
    return "".join(body)


def _subrights_rows(rows: list[Dict[str, Any]]) -> str:
    if not rows:
        return """
        <tr>
          <td colspan="4" class="empty-cell">No subrights activity this period.</td>
        </tr>
        """

    body: list[str] = []
    total = 0.0

    for ln in rows:
        royalty_amount = float(ln.get("royalty_amount") or 0)
        total += royalty_amount

        body.append(
            f"""
            <tr>
              <td>{_esc(ln.get("category_label", ""))}</td>
              <td class="num">{_money(ln.get("basis_amount"))}</td>
              <td class="num">{_esc(ln.get("royalty_rate", ""))}</td>
              <td class="num strong">{_money(ln.get("royalty_amount"))}</td>
            </tr>
            """
        )

    body.append(
        f"""
        <tr class="total-row">
          <td colspan="3" class="num strong">Total</td>
          <td class="num strong">{_money(total)}</td>
        </tr>
        """
    )
    return "".join(body)


def _pdf_html(bundle: Dict[str, Any]) -> str:
    header = bundle.get("header") or {}
    lines = bundle.get("lines") or []

    agency_name = str(header.get("agency_name") or "").strip()
    agency_lines = _split_lines(header.get("agency_address_lines"))
    contributor_name = str(header.get("contributor_name") or "").strip()
    contributor_lines = _split_lines(header.get("contributor_address_lines"))
    isbns = _split_lines(header.get("isbns"))
    statement_date = str(header.get("statement_date") or "").strip()

    first_rights, canada_export, subrights = _section_lines(lines)

    format_order = {
        "hardcover": 0,
        "paperback": 1,
        "board book": 2,
        "boardbook": 2,
        "e-book": 3,
        "ebook": 3,
    }

    def _first_rights_sort_key(ln: Dict[str, Any]) -> tuple[int, str]:
        label = str(ln.get("category_label") or "").strip()
        norm = " ".join(label.lower().split())
        return (format_order.get(norm, 99), norm)

    first_rights = sorted(first_rights, key=_first_rights_sort_key)

    left_header_html = ""
    if agency_name:
        left_header_html += f'<div class="name-line">{_esc(agency_name)}</div>'
    if agency_lines:
        left_header_html += "".join(f'<div class="meta-line">{_esc(line)}</div>' for line in agency_lines)
    if contributor_name:
        left_header_html += f'<div class="name-line contributor-name">{_esc(contributor_name)}</div>'
    if contributor_lines:
        left_header_html += "".join(f'<div class="meta-line">{_esc(line)}</div>' for line in contributor_lines)

    right_header_html = f"""
      <div class="meta-row"><span class="meta-key">Royalty period</span><span class="meta-value">{_esc(header.get("period_label", ""))}</span></div>
      <div class="meta-row"><span class="meta-key">Statement date</span><span class="meta-value">{_esc(statement_date)}</span></div>
      <div class="meta-row"><span class="meta-key">ISBNs</span><span class="meta-value">{'<br/>'.join(_esc(isbn) for isbn in isbns) if isbns else '—'}</span></div>
    """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8" />
    <style>
        @page {{
        size: Letter;
        margin: 0.65in 0.7in 0.7in 0.7in;
        }}

        html, body {{
        background: #ffffff;
        color: #111827;
        font-family: Arial, Helvetica, sans-serif;
        font-size: 11px;
        line-height: 1.35;
        }}

        body {{
        margin: 0;
        padding: 0;
        }}

        .logo-wrap {{
        text-align: center;
        margin: 0 0 14px 0;
        }}

        .statement-title {{
        text-align: center;
        font-size: 20px;
        font-weight: 700;
        margin: 0 0 18px 0;
        letter-spacing: 0.02em;
        }}

        .header-grid {{
        display: table;
        width: 100%;
        table-layout: fixed;
        margin-bottom: 18px;
        }}

        .header-col {{
        display: table-cell;
        vertical-align: top;
        width: 50%;
        }}

        .header-col.left {{
        padding-right: 18px;
        }}

        .header-col.right {{
        padding-left: 18px;
        }}

        .section-label {{
        font-size: 10px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #6b7280;
        font-weight: 700;
        margin-bottom: 8px;
        }}

        .name-line {{
        font-size: 12px;
        font-weight: 700;
        margin: 0 0 4px 0;
        }}

        .contributor-name {{
        margin-top: 10px;
        }}

        .meta-line {{
        margin: 0 0 3px 0;
        }}

        .meta-row {{
        display: table;
        width: 100%;
        margin: 0 0 8px 0;
        }}

        .meta-key {{
        display: table-cell;
        width: 110px;
        font-weight: 700;
        vertical-align: top;
        }}

        .meta-value {{
        display: table-cell;
        vertical-align: top;
        }}

        .table-block {{
        margin-top: 18px;
        }}

        thead, tbody, tr {{
        width: 100%;
        }}

        .table-title {{
        font-size: 13px;
        font-weight: 700;
        margin: 0 0 7px 0;
        }}

        table {{
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
        box-sizing: border-box;
        }}

        th, td {{
        border: 1px solid #d1d5db;
        padding: 5px 6px;
        vertical-align: top;
        word-wrap: break-word;
        overflow-wrap: anywhere;
        box-sizing: border-box;
        }}

        th {{
        background: #f3f4f6;
        text-align: left;
        font-weight: 700;
        }}

        .num {{
        text-align: right;
        }}

        .strong {{
        font-weight: 700;
        }}

        .main-cell {{
        font-weight: 700;
        }}

        .sub-cell {{
        margin-top: 3px;
        color: #6b7280;
        font-size: 10px;
        font-weight: 400;
        }}

        .empty-cell {{
        text-align: center;
        color: #6b7280;
        padding: 10px;
        }}

        .total-row td {{
        background: #fafafa;
        }}

        .summary-wrap {{
        margin-top: 20px;
        display: flex;
        justify-content: flex-end;
        }}

        .summary-table {{
        width: 360px;
        border-collapse: collapse;
        }}

        .summary-table td {{
        border: none;
        padding: 4px 0;
        }}

        .summary-label {{
        font-weight: 700;
        padding-right: 14px;
        }}

        .summary-value {{
        text-align: right;
        }}
    </style>
    </head>
    <body>
    <div class="logo-wrap">{_logo_html()}</div>
    <div class="statement-title">Royalty Statement</div>

    <div class="header-grid">
        <div class="header-col left">
        <div class="section-label">Statement Information</div>
        {left_header_html or '<div class="meta-line">—</div>'}
        </div>

        <div class="header-col right">
        <div class="section-label">Payment Period</div>
        {right_header_html}
        </div>
    </div>

    <div class="table-block">
        <div class="table-title">First Rights</div>
        <table>
        <thead>
            <tr>
                <th style="width: 24%;">Format</th>
                <th style="width: 9%;" class="num">Units Sold</th>
                <th style="width: 9%;" class="num">Returns</th>
                <th style="width: 10%;" class="num">Net Units Sold</th>
                <th style="width: 12%;" class="num">Unit Price</th>
                <th style="width: 8%;" class="num">Rate %</th>
                <th style="width: 14%;" class="num">Value</th>
                <th style="width: 14%;" class="num">Royalty Amount</th>
            </tr>
        </thead>
        <tbody>
            {_first_rights_rows(first_rights)}
        </tbody>
        </table>
    </div>

    <div class="table-block">
        <div class="table-title">Canada &amp; Export</div>
        <table>
        <thead>
            <tr>
                <th style="width: 40%;">Right</th>
                <th style="width: 17%;" class="num">Net Receipts</th>
                <th style="width: 15%;" class="num">Prevailing U.S. Rate</th>
                <th style="width: 13%;" class="num">Derived Rate</th>
                <th style="width: 15%;" class="num">Royalty Amount</th>
            </tr>
        </thead>
        <tbody>
            {_canada_export_rows(canada_export)}
        </tbody>
        </table>
    </div>

    <div class="table-block">
        <div class="table-title">Subrights</div>
        <table>
        <thead>
            <tr>
            <th style="width: 45%;">Subright</th>
            <th style="width: 20%;" class="num">Period Receipts</th>
            <th style="width: 15%;" class="num">Rate %</th>
            <th style="width: 20%;" class="num">Royalty Amount</th>
            </tr>
        </thead>
        <tbody>
            {_subrights_rows(subrights)}
        </tbody>
        </table>
    </div>

    <div class="summary-wrap">
        <table class="summary-table">
        <tr>
            <td class="summary-label">Advance paid</td>
            <td class="summary-value">{_money(header.get("advance_paid_original"))}</td>
        </tr>
        <tr>
            <td class="summary-label">Total royalty this period</td>
            <td class="summary-value">{_money(header.get("earned_this_period"))}</td>
        </tr>
        <tr>
            <td class="summary-label">Earned to date</td>
            <td class="summary-value">{_money(header.get("earned_to_date"))}</td>
        </tr>
        <tr>
            <td class="summary-label">Remaining unrecouped balance</td>
            <td class="summary-value">{_money(header.get("closing_recoupment_balance"))}</td>
        </tr>
        <tr>
            <td class="summary-label">Amount payable</td>
            <td class="summary-value">{_money(header.get("payable_this_period"))}</td>
        </tr>
        </table>
    </div>
    </body>
    </html>
    """


def _fetch_distribution_items(cur, tenant_id: str, period_id: Optional[str] = None) -> list[Dict[str, Any]]:
    params: list[Any] = [tenant_id]
    period_filter_sql = ""
    if period_id:
        period_filter_sql = " AND rs.period_id = %s::uuid "
        params.append(period_id)

    sql = f"""
    WITH base AS (
        SELECT
            rs.id AS statement_id,
            rs.work_id,
            rs.period_id,
            rs.party::text AS party,
            rs.status,
            rs.pdf_saved_at,
            rs.pdf_s3_key,
            rs.sent_at,
            rs.sent_to_contributor_email,
            rs.sent_to_agent_email,
            w.title,
            w.subtitle,
            rp.period_code,
            rp.period_start,
            rp.period_end
        FROM royalty_statements rs
        JOIN works w
          ON w.id = rs.work_id
        JOIN royalty_periods rp
          ON rp.id = rs.period_id
        WHERE rs.tenant_id = %s::uuid
          AND rs.status = 'draft'
          {period_filter_sql}
    ),
    contributor_candidates AS (
        SELECT
            b.statement_id,
            wc.party_id AS contributor_party_id,
            p.display_name AS contributor_name,
            p.email AS contributor_email,
            wc.sequence_number
        FROM base b
        JOIN work_contributors wc
          ON wc.work_id = b.work_id
        JOIN parties p
          ON p.id = wc.party_id
        WHERE (
            b.party = 'author'
            AND upper(COALESCE(wc.contributor_role, '')) = 'AUTHOR'
        ) OR (
            b.party = 'illustrator'
            AND upper(COALESCE(wc.contributor_role, '')) = 'ILLUSTRATOR'
        )
    ),
    picked_contributor AS (
        SELECT DISTINCT ON (statement_id)
            statement_id,
            contributor_party_id,
            contributor_name,
            contributor_email
        FROM contributor_candidates
        ORDER BY statement_id, sequence_number, contributor_party_id
    ),
    agent_candidates AS (
        SELECT
            b.statement_id,
            pr.id AS representation_id,
            pr.is_primary,
            pr.role_label,
            pr.created_at,
            pr.work_id AS representation_work_id,
            agent.id AS agent_party_id,
            agent.display_name AS agent_name,
            agent.email AS agent_email
        FROM base b
        JOIN picked_contributor pc
          ON pc.statement_id = b.statement_id
        LEFT JOIN party_representations pr
          ON pr.represented_party_id = pc.contributor_party_id
         AND (pr.work_id = b.work_id OR pr.work_id IS NULL)
        LEFT JOIN parties agent
          ON agent.id = pr.agent_party_id
    ),
    picked_agent AS (
        SELECT DISTINCT ON (statement_id)
            statement_id,
            representation_id,
            is_primary,
            role_label,
            agent_party_id,
            agent_name,
            agent_email
        FROM agent_candidates
        ORDER BY
            statement_id,
            is_primary DESC NULLS LAST,
            (representation_work_id IS NOT NULL) DESC,
            created_at DESC NULLS LAST,
            representation_id
    )
    SELECT
        b.statement_id::text AS statement_id,
        b.work_id::text AS work_id,
        b.title,
        b.subtitle,
        b.party,
        b.period_id::text AS period_id,
        b.period_code,
        b.period_start::text AS period_start,
        b.period_end::text AS period_end,
        b.status,
        b.pdf_saved_at,
        b.pdf_s3_key,
        b.sent_at,
        b.sent_to_contributor_email,
        b.sent_to_agent_email,
        pc.contributor_party_id::text AS contributor_party_id,
        pc.contributor_name,
        pc.contributor_email,
        pa.representation_id::text AS representation_id,
        pa.is_primary,
        pa.role_label,
        pa.agent_party_id::text AS agent_party_id,
        pa.agent_name,
        pa.agent_email
    FROM base b
    LEFT JOIN picked_contributor pc
      ON pc.statement_id = b.statement_id
    LEFT JOIN picked_agent pa
      ON pa.statement_id = b.statement_id
    ORDER BY b.period_start DESC, b.title ASC, b.party ASC
    """
    cur.execute(sql, params)
    return [dict(r) for r in (cur.fetchall() or [])]


def _statement_pdf_bytes(statement_id: str, request: Request) -> tuple[bytes, Dict[str, Any]]:
    _require_tenant_id(request)

    try:
        bundle = run_fetch_statement(statement_id)
    except StatementValidationError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    bundle.setdefault("header", {})
    bundle["header"]["statement_date"] = datetime.now().strftime("%b %d, %Y")
    html = _pdf_html(bundle)

    try:
        from weasyprint import HTML, CSS

        pdf_bytes = HTML(string=html).write_pdf(
            stylesheets=[
                CSS(
                    string="""
                    @page { size: Letter; margin: 0.65in 0.7in 0.7in 0.7in; }
                    html, body { background: #ffffff !important; print-color-adjust: exact; }
                    """
                )
            ]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not generate PDF: {e}") from e

    return pdf_bytes, bundle


def _statement_file_meta(bundle: Dict[str, Any], statement_id: str) -> tuple[str, str]:
    work = bundle.get("work") or {}
    header = bundle.get("header") or {}

    book_key = str(work.get("uid") or work.get("id") or "").strip()
    if not book_key:
        raise HTTPException(status_code=500, detail="Statement is missing work uid/id for upload path.")

    title = str(work.get("title") or "royalty_statement").strip()
    safe_title = "".join(ch if ch.isalnum() or ch in (" ", "-", "_") else "_" for ch in title).strip()
    safe_title = "_".join(safe_title.split()) or "royalty_statement"

    party = str(header.get("party") or "statement").strip().lower() or "statement"
    period_code = str(header.get("period_code") or "").strip()
    period_suffix = f"_{period_code}" if period_code else ""
    filename = f"{safe_title}_{party}_royalty_statement{period_suffix}.pdf"

    return book_key, filename


def _save_statement_pdf_to_book_folder(
    cur,
    tenant_id: str,
    statement_id: str,
    pdf_bytes: bytes,
    bundle: Dict[str, Any],
) -> Dict[str, Any]:
    work = bundle.get("work") or {}
    header = bundle.get("header") or {}

    book_uid = str(work.get("uid") or "").strip()
    if not book_uid:
        raise HTTPException(status_code=500, detail="Statement is missing work uid for S3 upload path.")

    title = str(work.get("title") or "royalty_statement").strip()
    safe_title = "".join(ch if ch.isalnum() or ch in (" ", "-", "_") else "_" for ch in title).strip()
    safe_title = "_".join(safe_title.split()) or "royalty_statement"

    party = str(header.get("party") or "statement").strip().lower() or "statement"
    period_code = str(header.get("period_code") or "").strip()
    period_suffix = f"_{period_code}" if period_code else ""
    filename = f"{safe_title}_{party}_royalty_statement{period_suffix}.pdf"

    s3_key = f"{tenant_data_prefix('data', 'uploads')}/{book_uid}/{filename}"

    put_bytes(
        key=s3_key,
        data=pdf_bytes,
        content_type="application/pdf",
    )

    cur.execute(
        """
        UPDATE royalty_statements
        SET
            pdf_saved_at = now(),
            pdf_s3_key = %s,
            updated_at = now()
        WHERE tenant_id = %s::uuid
          AND id = %s::uuid
        """,
        (s3_key, tenant_id, statement_id),
    )
    if cur.rowcount != 1:
        raise HTTPException(status_code=404, detail="Statement not found for save.")

    return {
        "statement_id": statement_id,
        "book_key": book_uid,
        "filename": filename,
        "pdf_s3_key": s3_key,
        "saved": True,
    }


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


@router.get("/distribution-queue")
def distribution_queue_endpoint(request: Request, period_id: Optional[str] = None) -> Dict[str, Any]:
    tenant_id = _require_tenant_id(request)

    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            items = _fetch_distribution_items(cur, tenant_id, period_id=period_id)

    return {"items": items}


@router.post("/distribution-queue/bulk-save")
def bulk_save_distribution_endpoint(body: BulkSaveBody, request: Request) -> Dict[str, Any]:
    tenant_id = _require_tenant_id(request)
    if not body.statement_ids:
        return {"items": [], "saved_count": 0}

    results: list[Dict[str, Any]] = []

    with db_conn() as conn:
        prev_ac = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                for statement_id in body.statement_ids:
                    pdf_bytes, bundle = _statement_pdf_bytes(statement_id, request)
                    result = _save_statement_pdf_to_book_folder(
                        cur,
                        tenant_id=tenant_id,
                        statement_id=statement_id,
                        pdf_bytes=pdf_bytes,
                        bundle=bundle,
                    )
                    results.append(result)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = prev_ac

    return {"items": results, "saved_count": len(results)}


@router.post("/{statement_id}/save-pdf")
def save_statement_pdf_endpoint(statement_id: str, request: Request) -> Dict[str, Any]:
    tenant_id = _require_tenant_id(request)

    with db_conn() as conn:
        prev_ac = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                pdf_bytes, bundle = _statement_pdf_bytes(statement_id, request)
                result = _save_statement_pdf_to_book_folder(
                    cur,
                    tenant_id=tenant_id,
                    statement_id=statement_id,
                    pdf_bytes=pdf_bytes,
                    bundle=bundle,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = prev_ac

    return result


@router.post("/distribution-queue/bulk-send")
def bulk_send_distribution_endpoint(body: BulkSendBody, request: Request) -> Dict[str, Any]:
    if not body.items:
        return {"items": [], "sent_count": 0, "failed_count": 0}

    results: list[Dict[str, Any]] = []
    sent_count = 0
    failed_count = 0

    for item in body.items:
        try:
            result = send_statement_endpoint(
                statement_id=item.statement_id,
                body=SendStatementBody(
                    contributor_email=item.contributor_email,
                    agent_email=item.agent_email,
                    send_to_contributor=item.send_to_contributor,
                    send_to_agent=item.send_to_agent,
                ),
                request=request,
            )
            results.append(result)
            sent_count += 1
        except HTTPException as e:
            results.append({
                "ok": False,
                "statement_id": item.statement_id,
                "status_code": e.status_code,
                "detail": e.detail,
            })
            failed_count += 1
        except Exception as e:
            results.append({
                "ok": False,
                "statement_id": item.statement_id,
                "status_code": 500,
                "detail": str(e),
            })
            failed_count += 1

    return {
        "items": results,
        "sent_count": sent_count,
        "failed_count": failed_count,
    }


@router.post("/{statement_id}/send")
def send_statement_endpoint(statement_id: str, body: SendStatementBody, request: Request) -> Dict[str, Any]:
    tenant_id = _require_tenant_id(request)
    tenant_slug = (request.headers.get("X-Tenant") or "").strip()

    with db_conn() as conn:
        prev_ac = conn.autocommit
        conn.autocommit = False
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                recipients = _statement_recipients(cur, tenant_id, statement_id)
                pdf_bytes, bundle = _statement_pdf_bytes(statement_id, request)

                current_pdf_key = recipients.get("pdf_s3_key")
                if not current_pdf_key:
                    save_result = _save_statement_pdf_to_book_folder(
                        cur,
                        tenant_id=tenant_id,
                        statement_id=statement_id,
                        pdf_bytes=pdf_bytes,
                        bundle=bundle,
                    )
                    current_pdf_key = save_result["pdf_s3_key"]

                contributor_email = (body.contributor_email or recipients.get("contributor_email") or "").strip()
                agent_email = (body.agent_email or recipients.get("agent_email") or "").strip()

                to_email = contributor_email if body.send_to_contributor else ""
                cc_email = agent_email if body.send_to_agent else ""

                monitor_email = "gabor.szecsi@marblepress.com"
                if monitor_email and monitor_email != to_email:
                    if cc_email:
                        existing_ccs = [x.strip() for x in cc_email.split(",") if x.strip()]
                        if monitor_email not in existing_ccs:
                            existing_ccs.append(monitor_email)
                        cc_email = ", ".join(existing_ccs)
                    else:
                        cc_email = monitor_email

                if not to_email and not cc_email:
                    raise HTTPException(status_code=400, detail="Select at least one recipient email.")

                if not to_email and cc_email:
                    to_email = cc_email
                    cc_email = None


                if not to_email and not cc_email:
                    raise HTTPException(status_code=400, detail="Select at least one recipient email.")

                if not to_email and cc_email:
                    to_email = cc_email
                    cc_email = None

                settings = _load_tenant_email_settings_or_400(tenant_slug)
                username, password = _load_smtp_secret(settings["smtp_secret_id"])

                header = bundle.get("header") or {}
                work = bundle.get("work") or {}

                subject, body_text = render_royalty_statement_email(
                    contributor_name=str(recipients.get("contributor_name") or header.get("contributor_name") or "Contributor"),
                    title=str(work.get("title") or recipients.get("title") or "Royalty Statement"),
                    period=str(recipients.get("period_code") or header.get("period_label") or "Period"),
                    payable=float(header.get("payable_this_period") or 0),
                    signature=settings["from_name"],
                )

                _, attachment_filename = _statement_file_meta(bundle, statement_id)

                _send_email_smtp(
                    smtp_host=settings["smtp_host"],
                    smtp_port=settings["smtp_port"],
                    tls_mode=settings["tls_mode"],
                    username=username,
                    password=password,
                    from_email=settings["from_email"],
                    from_name=settings["from_name"],
                    to_email=to_email,
                    cc_email=cc_email,
                    subject=subject,
                    body_text=body_text,
                    attachment_filename=attachment_filename,
                    attachment_bytes=pdf_bytes,
                )

                cur.execute(
                    """
                    UPDATE royalty_statements
                    SET
                        sent_at = now(),
                        sent_to_contributor_email = %s,
                        sent_to_agent_email = %s,
                        status = 'final',
                        updated_at = now()
                    WHERE tenant_id = %s::uuid
                      AND id = %s::uuid
                    """,
                    (
                        contributor_email if body.send_to_contributor else None,
                        agent_email if body.send_to_agent else None,
                        tenant_id,
                        statement_id,
                    ),
                )
                if cur.rowcount != 1:
                    raise HTTPException(status_code=404, detail="Statement not found for send.")

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = prev_ac

    return {
        "ok": True,
        "statement_id": statement_id,
        "sent": True,
        "sent_to_contributor_email": contributor_email if body.send_to_contributor else None,
        "sent_to_agent_email": agent_email if body.send_to_agent else None,
        "pdf_s3_key": current_pdf_key,
    }


@router.get("/{statement_id}/pdf")
def get_statement_pdf_endpoint(statement_id: str, request: Request) -> Response:
    pdf_bytes, bundle = _statement_pdf_bytes(statement_id, request)

    header = bundle.get("header") or {}
    filename = f'royalty_statement_{header.get("party", "statement")}_{statement_id}.pdf'

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


@router.get("/{statement_id}")
def get_statement_endpoint(statement_id: str, request: Request) -> Dict[str, Any]:
    _require_tenant_id(request)
    try:
        return run_fetch_statement(statement_id)
    except StatementValidationError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e