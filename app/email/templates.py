"""
Email templates for InkSuite.

Design:
- Global templates shared across tenants
- Tenant branding (from_name / from_email) comes from DB
- Templates remain simple text for maximum deliverability
"""

from datetime import datetime


INVITE_AGENT_SUBJECT = "Action Required: Review Draft Contract"

def render_royalty_statement_email(
    contributor_name: str,
    title: str,
    period: str,
    payable: float,
    signature: str = "Marble Press",
) -> tuple[str, str]:
    subject = f"Royalty Statement – {title} ({period})"

    body = f"""Dear {contributor_name},

Please find attached your royalty statement for:

Title: {title}
Period: {period}

Amount payable: ${payable:,.2f}

If you have any questions, please feel free to reach out.

Best regards,
{signature}
"""

    return subject, body


def render_invite_agent_email(
    reviewer_name: str,
    review_link: str,
    expires_at: datetime,
    signature: str,
) -> tuple[str, str]:
    """
    Render the invite agent email.

    Returns:
        subject, body_text
    """

    expires_str = expires_at.strftime("%B %d, %Y")

    body = f"""Hello {reviewer_name},

Please review the draft contract at the link below:

{review_link}

You can add comments and suggest edits directly in the document.

This link expires on {expires_str}. Please do not forward it.

{signature}
"""

    return INVITE_AGENT_SUBJECT, body