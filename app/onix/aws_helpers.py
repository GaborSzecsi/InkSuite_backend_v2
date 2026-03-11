# app/onix/aws_helpers.py
# Secrets Manager fetch, S3 store, SFTP transfer. No secrets in Postgres or logs.
from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime
from typing import Any, Dict, Optional

_SECRET_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_MAX = 50


def get_secret(secret_arn: str, use_cache: bool = True) -> Dict[str, Any]:
    """Fetch secret from AWS Secrets Manager. Returns dict (e.g. password, privateKey, passphrase)."""
    if not secret_arn or not secret_arn.strip():
        return {}
    arn = secret_arn.strip()
    if use_cache and arn in _SECRET_CACHE:
        return dict(_SECRET_CACHE[arn])
    try:
        import boto3
        import json
        client = boto3.client("secretsmanager")
        resp = client.get_secret_value(SecretId=arn)
        raw = resp.get("SecretString") or ""
        if not raw:
            return {}
        data = json.loads(raw) if isinstance(raw, str) else raw
        if use_cache and len(_SECRET_CACHE) < _CACHE_MAX:
            _SECRET_CACHE[arn] = dict(data)
        return dict(data)
    except Exception:
        return {}


def put_secret_password(secret_arn: str, password: str) -> str:
    """Store password in Secrets Manager; create or update. Returns ARN. Do not log password."""
    import boto3
    import json
    client = boto3.client("secretsmanager")
    body = {"password": password}
    try:
        client.put_secret_value(SecretId=secret_arn, SecretString=json.dumps(body))
    except client.exceptions.ResourceNotFoundException:
        name = secret_arn.split(":")[-1] if ":" in secret_arn else f"onix-sftp-{hash(secret_arn) % 10**8}"
        r = client.create_secret(Name=name, SecretString=json.dumps(body))
        return r.get("ARN") or secret_arn
    if secret_arn in _SECRET_CACHE:
        del _SECRET_CACHE[secret_arn]
    return secret_arn


def put_secret_ssh_key(secret_arn: str, private_key: str, passphrase: Optional[str] = None) -> str:
    """Store SSH key (+ optional passphrase) in Secrets Manager. Do not log key."""
    import boto3
    import json
    client = boto3.client("secretsmanager")
    body = {"privateKey": private_key}
    if passphrase is not None:
        body["passphrase"] = passphrase
    try:
        client.put_secret_value(SecretId=secret_arn, SecretString=json.dumps(body))
    except client.exceptions.ResourceNotFoundException:
        name = secret_arn.split(":")[-1] if ":" in secret_arn else f"onix-sftp-key-{hash(secret_arn) % 10**8}"
        r = client.create_secret(Name=name, SecretString=json.dumps(body))
        return r.get("ARN") or secret_arn
    if secret_arn in _SECRET_CACHE:
        del _SECRET_CACHE[secret_arn]
    return secret_arn


def xml_checksum_sha256(xml_text: str) -> str:
    return hashlib.sha256(xml_text.encode("utf-8")).hexdigest()


def upload_xml_to_s3(
    xml_content: str,
    bucket: Optional[str] = None,
    key_prefix: str = "onix-exports",
    tenant_id: str = "",
    job_id: str = "",
) -> tuple[str, str]:
    """Upload XML to S3. Returns (s3_key, checksum)."""
    bucket = bucket or os.environ.get("ONIX_EXPORT_BUCKET", "").strip()
    if not bucket:
        return "", xml_checksum_sha256(xml_content)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    safe_tenant = re.sub(r"[^a-zA-Z0-9_-]", "_", tenant_id)[:64]
    safe_job = (job_id or "").replace("-", "")[:32]
    key = f"{key_prefix}/{safe_tenant}/{ts}_{safe_job}.xml"
    try:
        import boto3
        client = boto3.client("s3")
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=xml_content.encode("utf-8"),
            ContentType="application/xml; charset=utf-8",
        )
        return key, xml_checksum_sha256(xml_content)
    except Exception:
        return "", xml_checksum_sha256(xml_content)


def sftp_upload(
    host: str,
    port: int,
    username: str,
    auth_type: str,
    secret_arn: str,
    remote_path: str,
    local_content: str,
    filename: str,
) -> tuple[bool, str]:
    """Upload string content to SFTP. Returns (success, error_message)."""
    if not host or not username:
        return False, "Missing host or username"
    try:
        import paramiko
    except ImportError:
        return False, "paramiko not installed"
    secret = get_secret(secret_arn, use_cache=False)
    transport = None
    try:
        transport = paramiko.Transport((host, port))
        if auth_type == "ssh_key":
            key = secret.get("privateKey") or ""
            passphrase = secret.get("passphrase") or None
            pkey = paramiko.RSAKey.from_private_key(
                __import__("io").StringIO(key),
                password=passphrase,
            )
            transport.connect(username=username, pkey=pkey)
        else:
            password = secret.get("password") or ""
            transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        path = (remote_path or "").strip().rstrip("/")
        if path:
            try:
                sftp.stat(path)
            except FileNotFoundError:
                parts = path.split("/")
                for i in range(1, len(parts) + 1):
                    p = "/".join(parts[:i])
                    if p:
                        try:
                            sftp.mkdir(p)
                        except OSError:
                            pass
        full = f"{path}/{filename}" if path else filename
        from io import BytesIO
        sftp.putfo(BytesIO(local_content.encode("utf-8")), full)
        sftp.close()
        return True, ""
    except Exception as e:
        return False, str(e)
    finally:
        if transport:
            try:
                transport.close()
            except Exception:
                pass


def interpolate_filename_pattern(
    pattern: str,
    date: Optional[str] = None,
    timestamp: Optional[str] = None,
    tenant: str = "",
    count: int = 0,
    job_id: str = "",
) -> str:
    """Replace {date}, {timestamp}, {tenant}, {count}, {job_id} in pattern."""
    now = datetime.utcnow()
    date = date or now.strftime("%Y-%m-%d")
    timestamp = timestamp or now.strftime("%Y%m%d_%H%M%S")
    s = (pattern or "onix_export_{date}.xml")
    s = s.replace("{date}", date).replace("{timestamp}", timestamp)
    s = s.replace("{tenant}", (tenant or "").strip()[:32])
    s = s.replace("{count}", str(count))
    s = s.replace("{job_id}", (job_id or "")[:16])
    return s or "onix_export.xml"
