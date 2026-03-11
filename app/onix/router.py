# app/onix/router.py
# ONIX Feed API: products list/detail, preview, export, recipients, export history.
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from app.core.db import db_conn
from app.onix import assembly, validation, xml_serializer, aws_helpers
from app.onix.models import ExportRequest, RecipientCreate, RecipientUpdate
from psycopg.rows import dict_row

router = APIRouter(prefix="/onix", tags=["ONIX Feed"])


def _tenant_id_from_request(cur, request: Request) -> str:
    slug = (request.query_params.get("tenant_slug") or request.headers.get("X-Tenant") or "").strip()
    if not slug:
        slug = "marble-press"
    cur.execute("SELECT id FROM tenants WHERE lower(slug) = lower(%s) LIMIT 1", (slug,))
    row = cur.fetchone()
    if not row:
        cur.execute("SELECT id FROM tenants ORDER BY id LIMIT 1")
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return str(row["id"])


def _user_id_from_request(request: Request) -> Optional[str]:
    claims = getattr(request.state, "user_claims", None)
    if not claims:
        return None
    sub = claims.get("sub")
    return str(sub) if sub else None


@router.get("/products")
def list_products(
    request: Request,
    tenant_slug: str = Query(""),
    q: str = Query(""),
    isbn: str = Query(""),
    title: str = Query(""),
    contributor: str = Query(""),
    format: str = Query(""),
    status: str = Query(""),
    validation_status: str = Query(""),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sort: str = Query("title"),
):
    slug = tenant_slug.strip() or "marble-press"
    result = assembly.list_exportable_products(
        tenant_slug=slug,
        q=q.strip() or None,
        isbn=isbn.strip() or None,
        title=title.strip() or None,
        contributor=contributor.strip() or None,
        format_filter=format.strip() or None,
        status_filter=status.strip() or None,
        page=page,
        page_size=page_size,
        sort=sort.strip() or "title",
    )
    items = result.get("items") or []
    tenant_id = _tenant_id_cur(slug)
    if validation_status.strip():
        filtered = []
        for it in items:
            pid = assembly.get_exportable_product_by_isbn(slug, it.get("isbn13") or "")
            if not pid:
                continue
            val = validation.validate_product(pid)
            if (val.get("status") or "").lower() == validation_status.strip().lower():
                it["validation_status"] = val.get("status")
                filtered.append(it)
        result["items"] = filtered
        result["total"] = len(filtered)
    else:
        for it in items:
            pid = assembly.build_onix_product_payload(tenant_id, it.get("edition_id") or "") if it.get("edition_id") else {}
            if pid:
                it["validation_status"] = validation.validate_product(pid).get("status", "")
    return result


def _tenant_id_cur(tenant_slug: str):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT id FROM tenants WHERE lower(slug) = lower(%s) LIMIT 1", (tenant_slug,))
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT id FROM tenants ORDER BY id LIMIT 1")
                row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Tenant not found")
            return str(row["id"])


@router.get("/products/{isbn}")
def get_product(
    isbn: str,
    tenant_slug: str = Query(""),
    include_raw: bool = Query(False, alias="include_raw"),
):
    slug = tenant_slug.strip() or "marble-press"
    product = assembly.get_exportable_product_by_isbn(slug, isbn)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    val = validation.validate_product(product)
    out = {
        "canonical": product,
        "validation": val,
        "latest_raw_reference": None,
    }
    if include_raw:
        tenant_id = _tenant_id_cur(slug)
        with db_conn() as conn2:
            with conn2.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT id, record_reference, isbn13, product_xml, created_at
                    FROM onix_raw_products
                    WHERE tenant_id = %s AND normalize_isbn(isbn13) = normalize_isbn(%s)
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (tenant_id, isbn.strip()),
                )
                row = cur.fetchone()
                if row:
                    out["latest_raw_reference"] = {
                        "id": str(row["id"]),
                        "record_reference": row.get("record_reference"),
                        "isbn13": row.get("isbn13"),
                        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
                    }
                    out["latest_raw_xml"] = row.get("product_xml")
    return out


@router.get("/products/{isbn}/preview")
def preview_product_xml(
    isbn: str,
    tenant_slug: str = Query(""),
    raw: bool = Query(False),
    pretty: bool = Query(True),
):
    slug = tenant_slug.strip() or "marble-press"
    product = assembly.get_exportable_product_by_isbn(slug, isbn)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    xml_str = xml_serializer.message_to_xml({"release": "3.0", "products": [product]}, pretty=pretty)
    return Response(content=xml_str, media_type="application/xml; charset=utf-8")


@router.post("/export")
def create_export(request: Request, body: ExportRequest):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            user_id = _user_id_from_request(request)

            edition_ids: List[str] = list(body.edition_ids or [])
            if body.isbns and not edition_ids:
                for isbn in body.isbns:
                    cur.execute(
                        "SELECT id FROM editions WHERE tenant_id = %s AND normalize_isbn(isbn13) = normalize_isbn(%s) LIMIT 1",
                        (tenant_id, (isbn or "").strip()),
                    )
                    row = cur.fetchone()
                    if row:
                        edition_ids.append(str(row["id"]))

            if not edition_ids:
                raise HTTPException(status_code=400, detail="No edition_ids or isbns provided")

            export_mode = body.export_mode or "preview"
            recipient_id = None
            if body.recipient_id:
                try:
                    recipient_id = str(uuid.UUID(body.recipient_id))
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid recipient_id")

            isbns_list = []
            for ed_id in edition_ids:
                cur.execute("SELECT isbn13 FROM editions WHERE tenant_id = %s AND id = %s", (tenant_id, ed_id))
                row = cur.fetchone()
                if row and row.get("isbn13"):
                    isbns_list.append((row.get("isbn13") or "").strip())
            job_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO onix_export_jobs
                (id, tenant_id, requested_by_user_id, recipient_id, export_mode, export_scope, product_count,
                 selected_isbns, selected_edition_ids, filters_json, status, started_at)
                VALUES (%s, %s, %s, %s, %s, 'selected', %s, %s::jsonb, %s::jsonb, %s::jsonb, 'pending', now())
                """,
                (
                    job_id,
                    tenant_id,
                    user_id,
                    recipient_id,
                    export_mode,
                    len(edition_ids),
                    json.dumps(isbns_list),
                    json.dumps(edition_ids),
                    json.dumps(body.filters_json or {}),
                ),
            )

            message = assembly.build_onix_message_payload(tenant_id, edition_ids, cur=cur)
            xml_str = xml_serializer.message_to_xml(message, pretty=True)
            checksum = aws_helpers.xml_checksum_sha256(xml_str)

            s3_key = ""
            if body.save_to_s3:
                s3_key, _ = aws_helpers.upload_xml_to_s3(
                    xml_str, tenant_id=tenant_id, job_id=job_id
                )
            cur.execute(
                "UPDATE onix_export_jobs SET xml_checksum = %s, xml_s3_key = %s, status = 'generated', completed_at = now() WHERE id = %s",
                (checksum, s3_key, job_id),
            )

            for ed_id in edition_ids:
                cur.execute(
                    "SELECT e.id, e.work_id, e.isbn13, e.record_reference, w.title FROM editions e JOIN works w ON w.id = e.work_id WHERE e.tenant_id = %s AND e.id = %s",
                    (tenant_id, ed_id),
                )
                row = cur.fetchone()
                if row:
                    prod = assembly.build_onix_product_payload(tenant_id, ed_id, cur=cur)
                    val = validation.validate_product(prod)
                    cur.execute(
                        """
                        INSERT INTO onix_export_job_items
                        (export_job_id, tenant_id, work_id, edition_id, isbn13, record_reference, title, product_form, status, validation_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'included', %s::jsonb)
                        """,
                        (
                            job_id,
                            tenant_id,
                            str(row["work_id"]),
                            str(row["id"]),
                            (row.get("isbn13") or "").strip(),
                            (row.get("record_reference") or "").strip(),
                            (row.get("title") or "").strip(),
                            (prod.get("product_form") or "").strip(),
                            json.dumps(val),
                        ),
                    )

            if export_mode == "transfer" and recipient_id:
                cur.execute(
                    "SELECT name, host, port, username, auth_type, remote_path, filename_pattern, secret_arn FROM onix_recipients WHERE id = %s AND tenant_id = %s AND is_active = true",
                    (recipient_id, tenant_id),
                )
                rec = cur.fetchone()
                if not rec:
                    cur.execute(
                        "UPDATE onix_export_jobs SET transfer_status = 'failed', transfer_error = %s WHERE id = %s",
                        ("Recipient not found or inactive", job_id),
                    )
                else:
                    filename = aws_helpers.interpolate_filename_pattern(
                        rec.get("filename_pattern") or "onix_{date}.xml",
                        tenant=tenant_id,
                        count=len(edition_ids),
                        job_id=job_id,
                    )
                    ok, err = aws_helpers.sftp_upload(
                        host=rec.get("host") or "",
                        port=int(rec.get("port") or 22),
                        username=rec.get("username") or "",
                        auth_type=rec.get("auth_type") or "password",
                        secret_arn=rec.get("secret_arn") or "",
                        remote_path=rec.get("remote_path") or "",
                        local_content=xml_str,
                        filename=filename,
                    )
                    cur.execute(
                        "UPDATE onix_export_jobs SET transfer_status = %s, transfer_error = %s WHERE id = %s",
                        ("success" if ok else "failed", err or "", job_id),
                    )

            conn.commit()

            response = {"job_id": job_id, "status": "generated", "product_count": len(edition_ids), "checksum": checksum}
            if export_mode == "preview":
                response["xml"] = xml_str
            if export_mode == "download":
                response["xml"] = xml_str
                response["content_type"] = "application/xml; charset=utf-8"
            return response


@router.get("/export-history")
def export_history(
    request: Request,
    tenant_slug: str = Query(""),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            offset = (page - 1) * page_size
            cur.execute(
                """
                SELECT j.id, j.export_mode, j.export_scope, j.product_count, j.status, j.xml_s3_key, j.xml_checksum,
                       j.transfer_status, j.transfer_error, j.started_at, j.completed_at, j.created_at,
                       r.name AS recipient_name
                FROM onix_export_jobs j
                LEFT JOIN onix_recipients r ON r.id = j.recipient_id
                WHERE j.tenant_id = %s
                ORDER BY j.created_at DESC
                LIMIT %s OFFSET %s
                """,
                (tenant_id, page_size, offset),
            )
            rows = cur.fetchall() or []
            cur.execute("SELECT COUNT(*) AS n FROM onix_export_jobs WHERE tenant_id = %s", (tenant_id,))
            total = int((cur.fetchone() or {}).get("n") or 0)
            items = []
            for r in rows:
                items.append({
                    "id": str(r["id"]),
                    "export_mode": r.get("export_mode"),
                    "export_scope": r.get("export_scope"),
                    "product_count": r.get("product_count"),
                    "status": r.get("status"),
                    "xml_s3_key": r.get("xml_s3_key"),
                    "xml_checksum": r.get("xml_checksum"),
                    "transfer_status": r.get("transfer_status"),
                    "transfer_error": r.get("transfer_error"),
                    "started_at": r.get("started_at").isoformat() if r.get("started_at") else None,
                    "completed_at": r.get("completed_at").isoformat() if r.get("completed_at") else None,
                    "created_at": r.get("created_at").isoformat() if r.get("created_at") else None,
                    "recipient_name": r.get("recipient_name"),
                })
            return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/export-history/{job_id}")
def export_job_detail(job_id: str, request: Request, tenant_slug: str = Query("")):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            cur.execute(
                "SELECT * FROM onix_export_jobs WHERE id = %s AND tenant_id = %s LIMIT 1",
                (job_id, tenant_id),
            )
            job = cur.fetchone()
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            cur.execute(
                "SELECT id, edition_id, work_id, isbn13, record_reference, title, product_form, status, validation_json, created_at FROM onix_export_job_items WHERE export_job_id = %s ORDER BY created_at",
                (job_id,),
            )
            item_rows = cur.fetchall() or []
            items = []
            for r in item_rows:
                items.append({
                    "id": str(r["id"]),
                    "edition_id": str(r["edition_id"]) if r.get("edition_id") else None,
                    "work_id": str(r["work_id"]) if r.get("work_id") else None,
                    "isbn13": r.get("isbn13"),
                    "record_reference": r.get("record_reference"),
                    "title": r.get("title"),
                    "product_form": r.get("product_form"),
                    "status": r.get("status"),
                    "validation": r.get("validation_json"),
                    "created_at": r.get("created_at").isoformat() if r.get("created_at") else None,
                })
            return {
                "id": str(job["id"]),
                "tenant_id": str(job["tenant_id"]),
                "export_mode": job.get("export_mode"),
                "export_scope": job.get("export_scope"),
                "product_count": job.get("product_count"),
                "selected_isbns": job.get("selected_isbns"),
                "selected_edition_ids": job.get("selected_edition_ids"),
                "status": job.get("status"),
                "xml_s3_key": job.get("xml_s3_key"),
                "xml_checksum": job.get("xml_checksum"),
                "transfer_status": job.get("transfer_status"),
                "transfer_error": job.get("transfer_error"),
                "started_at": job.get("started_at").isoformat() if job.get("started_at") else None,
                "completed_at": job.get("completed_at").isoformat() if job.get("completed_at") else None,
                "created_at": job.get("created_at").isoformat() if job.get("created_at") else None,
                "items": items,
            }


@router.get("/recipients")
def list_recipients(request: Request, tenant_slug: str = Query("")):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            cur.execute(
                "SELECT id, name, protocol, host, port, username, auth_type, remote_path, filename_pattern, is_active, created_at, updated_at FROM onix_recipients WHERE tenant_id = %s ORDER BY name",
                (tenant_id,),
            )
            rows = cur.fetchall() or []
            return {"items": [{"id": str(r["id"]), "name": r.get("name"), "protocol": r.get("protocol"), "host": r.get("host"), "port": r.get("port"), "username": r.get("username"), "auth_type": r.get("auth_type"), "remote_path": r.get("remote_path"), "filename_pattern": r.get("filename_pattern"), "is_active": r.get("is_active"), "created_at": r.get("created_at").isoformat() if r.get("created_at") else None, "updated_at": r.get("updated_at").isoformat() if r.get("updated_at") else None} for r in rows]}


@router.post("/recipients")
def create_recipient(request: Request, body: RecipientCreate):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            user_id = _user_id_from_request(request)
            secret_arn = (body.secret_arn or "").strip()
            if body.password is not None and body.auth_type == "password":
                if not secret_arn:
                    import boto3
                    name = f"onix-sftp-{tenant_id[:8]}-{uuid.uuid4().hex[:8]}"
                    client = boto3.client("secretsmanager")
                    r = client.create_secret(Name=name, SecretString=json.dumps({"password": body.password}))
                    secret_arn = r.get("ARN") or ""
                else:
                    aws_helpers.put_secret_password(secret_arn, body.password)
            if (body.private_key is not None or body.passphrase is not None) and body.auth_type == "ssh_key":
                if not secret_arn:
                    import boto3
                    name = f"onix-sftp-key-{tenant_id[:8]}-{uuid.uuid4().hex[:8]}"
                    client = boto3.client("secretsmanager")
                    key = body.private_key or ""
                    pp = body.passphrase
                    r = client.create_secret(Name=name, SecretString=json.dumps({"privateKey": key, "passphrase": pp or ""}))
                    secret_arn = r.get("ARN") or ""
                else:
                    aws_helpers.put_secret_ssh_key(secret_arn, body.private_key or "", body.passphrase)
            rec_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO onix_recipients (id, tenant_id, name, protocol, host, port, username, auth_type, remote_path, filename_pattern, secret_arn, is_active, created_by_user_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (rec_id, tenant_id, body.name, body.protocol, body.host, body.port, body.username, body.auth_type, body.remote_path, body.filename_pattern or "", secret_arn, body.is_active, user_id),
            )
            conn.commit()
            return {"id": rec_id, "name": body.name, "secret_arn": secret_arn}


@router.patch("/recipients/{recipient_id}")
def update_recipient(recipient_id: str, request: Request, body: RecipientUpdate):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            cur.execute("SELECT id, secret_arn FROM onix_recipients WHERE id = %s AND tenant_id = %s", (recipient_id, tenant_id))
            rec = cur.fetchone()
            if not rec:
                raise HTTPException(status_code=404, detail="Recipient not found")
            secret_arn = rec.get("secret_arn") or ""
            if body.password is not None and body.auth_type != "ssh_key":
                if secret_arn:
                    aws_helpers.put_secret_password(secret_arn, body.password)
            if (body.private_key is not None or body.passphrase is not None) and body.auth_type == "ssh_key":
                if secret_arn:
                    aws_helpers.put_secret_ssh_key(secret_arn, body.private_key or "", body.passphrase)
            updates = []
            params = []
            for k, v in body.model_dump(exclude_unset=True).items():
                if k in ("password", "private_key", "passphrase"):
                    continue
                if v is not None:
                    updates.append(f"{k} = %s")
                    params.append(v)
            if updates:
                params.extend([recipient_id, tenant_id])
                cur.execute(f"UPDATE onix_recipients SET {', '.join(updates)}, updated_at = now() WHERE id = %s AND tenant_id = %s", params)
            conn.commit()
            return {"id": recipient_id, "updated": True}


@router.post("/recipients/{recipient_id}/test")
def test_recipient(recipient_id: str, request: Request):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            cur.execute(
                "SELECT host, port, username, auth_type, secret_arn FROM onix_recipients WHERE id = %s AND tenant_id = %s AND is_active = true",
                (recipient_id, tenant_id),
            )
            rec = cur.fetchone()
            if not rec:
                raise HTTPException(status_code=404, detail="Recipient not found")
            ok, err = aws_helpers.sftp_upload(
                host=rec.get("host") or "",
                port=int(rec.get("port") or 22),
                username=rec.get("username") or "",
                auth_type=rec.get("auth_type") or "password",
                secret_arn=rec.get("secret_arn") or "",
                remote_path="",
                local_content="test",
                filename=".inksuite_test",
            )
            if ok:
                return {"ok": True, "message": "Connection successful"}
            return {"ok": False, "message": err or "Connection failed"}


@router.get("/recipients/{recipient_id}")
def get_recipient(recipient_id: str, request: Request):
    with db_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            cur.execute(
                "SELECT id, name, protocol, host, port, username, auth_type, remote_path, filename_pattern, is_active, created_at, updated_at FROM onix_recipients WHERE id = %s AND tenant_id = %s",
                (recipient_id, tenant_id),
            )
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Recipient not found")
            return {"id": str(r["id"]), "name": r.get("name"), "protocol": r.get("protocol"), "host": r.get("host"), "port": r.get("port"), "username": r.get("username"), "auth_type": r.get("auth_type"), "remote_path": r.get("remote_path"), "filename_pattern": r.get("filename_pattern"), "is_active": r.get("is_active"), "created_at": r.get("created_at").isoformat() if r.get("created_at") else None, "updated_at": r.get("updated_at").isoformat() if r.get("updated_at") else None}


@router.delete("/recipients/{recipient_id}")
def delete_recipient(recipient_id: str, request: Request):
    with db_conn() as conn:
        with conn.cursor() as cur:
            tenant_id = _tenant_id_from_request(cur, request)
            cur.execute("UPDATE onix_recipients SET is_active = false, updated_at = now() WHERE id = %s AND tenant_id = %s", (recipient_id, tenant_id))
            conn.commit()
            return {"id": recipient_id, "deleted": True}
