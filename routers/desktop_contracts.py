"""Tenant-scoped desktop contract queue and immutable review artifacts."""
import hashlib
import json
import os
import re

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

from app.auth.service import get_memberships_for_user
from app.tenants.workspace import workspace
from routers import storage_s3
from routers.contract_docs import _find_draft

router = APIRouter(prefix="/contracts/desktop", tags=["Desktop contracts"])
DIGEST = re.compile(r"[a-f0-9]{64}")


def contract_access():
    selected = workspace()
    if selected.role != "tenant_admin":
        membership = next(
            (item for item in get_memberships_for_user(selected.user_id)
             if str(item["tenant_id"]) == selected.tenant_id), {}
        )
        if (membership.get("module_permissions") or {}).get("contracts") is not True:
            raise HTTPException(403, "Contract Management access is required.")
    return selected


def scope(*parts):
    workspace()
    return storage_s3.tenant_data_prefix("desktop-contracts", *parts)


def missing(exc):
    return exc.response["Error"]["Code"] in {"NoSuchKey", "NotFound", "404"}


def object_with_etag(key):
    result = storage_s3.s3().get_object(Bucket=storage_s3._bucket(), Key=key)
    with result["Body"] as content:
        return content.read(), result["ETag"]


async def limited_body(request, limit):
    result = bytearray()
    async for chunk in request.stream():
        if len(result) + len(chunk) > limit:
            raise HTTPException(413, "Desktop payload is too large.")
        result.extend(chunk)
    return bytes(result)


@router.get("/queue", dependencies=[Depends(contract_access)])
def queue():
    try:
        body, etag = object_with_etag(scope("queue.json"))
    except ClientError as exc:
        if not missing(exc):
            raise
        body, etag = b'{"cases":[],"inbox":[],"playbook":"","defaultModel":"qwen3.5:2b"}', '"new"'
    return Response(body, media_type="application/json", headers={"ETag": etag})


@router.put("/queue")
async def update_queue(request: Request, if_match: str = Header(), selected=Depends(contract_access)):
    body = await limited_body(request, 8_000_000)
    try:
        data = json.loads(body)
        if not (isinstance(data, dict) and isinstance(data.get("cases"), list)
                and isinstance(data.get("inbox"), list) and isinstance(data.get("playbook"), str)):
            raise ValueError()
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(422, "Invalid desktop queue.") from exc
    if selected.role != "tenant_admin":
        try:
            existing, _ = object_with_etag(scope("queue.json"))
            saved = json.loads(existing)
        except ClientError as exc:
            if not missing(exc):
                raise
            saved = {"defaultModel": "qwen3.5:2b"}
        if any(data.get(field) != saved.get(field) for field in ("settings", "defaultModel")):
            raise HTTPException(403, "Only a publisher administrator can change the mailbox and integration settings.")
    data["updatedBy"] = selected.user_id
    condition = {"IfNoneMatch": "*"} if if_match == '"new"' else {"IfMatch": if_match}
    try:
        result = storage_s3.s3().put_object(
            Bucket=storage_s3._bucket(), Key=scope("queue.json"),
            Body=json.dumps(data, ensure_ascii=False).encode(), ContentType="application/json",
            **condition,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"412", "PreconditionFailed", "ConditionalRequestConflict"}:
            raise HTTPException(409, "The queue changed on another device. Refresh and review the latest version.") from exc
        raise
    return Response(status_code=204, headers={"ETag": result["ETag"]})


@router.put("/files/{digest}", dependencies=[Depends(contract_access)])
async def upload_file(digest: str, request: Request):
    data = await limited_body(request, 24_000_000)
    if not DIGEST.fullmatch(digest) or hashlib.sha256(data).hexdigest() != digest:
        raise HTTPException(422, "The file checksum did not match.")
    storage_s3.put_bytes(scope("files", digest), data)
    return Response(status_code=204)


def artifact(digest):
    if not DIGEST.fullmatch(digest):
        raise HTTPException(404, "File not found.")
    try:
        return storage_s3.get_bytes(scope("files", digest))
    except ClientError as exc:
        if not missing(exc):
            raise
        raise HTTPException(404, "File not found.") from exc


@router.get("/files/{digest}", dependencies=[Depends(contract_access)])
def download_file(digest: str):
    return Response(artifact(digest), media_type="application/octet-stream")


@router.put("/signed/{contract_id}/{digest}", dependencies=[Depends(contract_access)])
def file_signed_contract(contract_id: str, digest: str):
    if not _find_draft(contract_id):
        raise HTTPException(404, "Original InkSuite contract not found.")
    contents = artifact(digest)
    if not contents.startswith(b"%PDF-"):
        raise HTTPException(422, "Expected the provider's signed PDF.")
    record = hashlib.sha256(contract_id.encode()).hexdigest()
    key = scope("signed", record, digest + ".pdf")
    storage_s3.put_bytes(key, contents, "application/pdf")
    storage_s3.put_bytes(scope("signed", record, "latest.txt"), digest.encode(), "text/plain")
    return {"digest": digest}


@router.get("/signed/{contract_id}", dependencies=[Depends(contract_access)])
def signed_contract(contract_id: str):
    if not _find_draft(contract_id):
        raise HTTPException(404, "Original InkSuite contract not found.")
    record = hashlib.sha256(contract_id.encode()).hexdigest()
    try:
        digest = storage_s3.get_bytes(scope("signed", record, "latest.txt")).decode()
        if not DIGEST.fullmatch(digest):
            raise HTTPException(404, "Signed agreement not found.")
        contents = storage_s3.get_bytes(scope("signed", record, digest + ".pdf"))
    except ClientError as exc:
        if not missing(exc):
            raise
        raise HTTPException(404, "Signed agreement not found.") from exc
    return Response(contents, media_type="application/pdf",
                    headers={"Content-Disposition": 'attachment; filename="signed-agreement.pdf"'})


@router.get("/guidance", dependencies=[Depends(contract_access)])
def shared_guidance():
    key = os.getenv("INKSUITE_DESKTOP_SHARED_GUIDANCE_KEY")
    if not key:
        return {"guidance": ""}
    return {"guidance": storage_s3.get_bytes(key).decode()}

DESKTOP_RELEASE = "0.1.1"
INSTALLERS = {
    "windows": f"InkSuite-Desktop-{DESKTOP_RELEASE}-x64.exe",
    "macos": f"InkSuite-Desktop-{DESKTOP_RELEASE}-universal.dmg",
}


@router.get("/installers/{platform}", dependencies=[Depends(workspace)])
def download_installer(platform: str):
    filename = INSTALLERS.get(platform)
    if filename is None:
        raise HTTPException(404, "Installer not found.")
    key = f"releases/inksuite-desktop/{DESKTOP_RELEASE}/{filename}"
    try:
        storage_s3.s3().head_object(Bucket=storage_s3._bucket(), Key=key)
    except ClientError as exc:
        if not missing(exc):
            raise
        raise HTTPException(404, "Installer pending release.") from exc
    return RedirectResponse(
        storage_s3.presign_get(key, expires=900),
        status_code=302,
        headers={"Cache-Control": "private, no-store"},
    )
