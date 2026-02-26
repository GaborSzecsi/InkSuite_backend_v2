# app/email/ses_mailer.py  (DROP-IN UPDATE: supports invite_link)
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError


@dataclass
class EmailSendResult:
    ok: bool
    message_id: Optional[str] = None
    error: Optional[str] = None


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _get_ses_client():
    region = _env("AWS_REGION") or _env("AWS_DEFAULT_REGION") or "us-east-2"
    return boto3.client("ses", region_name=region)


def send_invite_email(
    *,
    to_email: str,
    invite_link: str,
    tenant_slug: Optional[str],
    role: str,
    invited_by_email: Optional[str],
) -> EmailSendResult:
    """
    Sends the invite email through Amazon SES.
    Requires:
      - INVITE_FROM_EMAIL=no-reply@inksuite.io (verified identity)
      - AWS_REGION matches your SES region
    """
    from_email = _env("INVITE_FROM_EMAIL", "no-reply@inksuite.io")
    if not from_email:
        return EmailSendResult(ok=False, error="INVITE_FROM_EMAIL is not set")

    link = (invite_link or "").strip()
    if not link:
        return EmailSendResult(ok=False, error="invite_link is required")

    subject = "You're invited to InkSuite"

    lines = [
        "You’ve been invited to InkSuite.",
        "",
        f"Accept your invite: {link}",
        "",
    ]
    if tenant_slug:
        lines.append(f"Tenant: {tenant_slug}")
    if role:
        lines.append(f"Role: {role}")
    if invited_by_email:
        lines.append(f"Invited by: {invited_by_email}")
    lines.append("")
    lines.append("If you did not expect this invitation, you can ignore this email.")
    text_body = "\n".join(lines)

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; line-height: 1.4;">
        <h2>You’ve been invited to InkSuite</h2>
        <p>
          Click to accept your invite:<br/>
          <a href="{link}">{link}</a>
        </p>
        <p>
          <b>Tenant:</b> {tenant_slug or "-"}<br/>
          <b>Role:</b> {role or "-"}<br/>
          <b>Invited by:</b> {invited_by_email or "-"}<br/>
        </p>
        <p style="color:#666;">If you did not expect this invitation, you can ignore this email.</p>
      </body>
    </html>
    """.strip()

    try:
        ses = _get_ses_client()
        resp = ses.send_email(
            Source=from_email,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
        )
        return EmailSendResult(ok=True, message_id=resp.get("MessageId"))
    except (ClientError, BotoCoreError) as e:
        return EmailSendResult(ok=False, error=str(e))
    except Exception as e:
        return EmailSendResult(ok=False, error=str(e))