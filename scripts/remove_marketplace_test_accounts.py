"""Remove ONLY the two explicitly retired Marketplace test identities.
Run from the same authenticated AWS environment used to start the backend.
No password, token, or credential values are printed.
"""

from pathlib import Path
import sys
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
from botocore.exceptions import BotoCoreError, ClientError
from app.auth.service import _cognito
from app.core.config import settings
from app.core.db import db_conn

EMAILS = ("szecsiwork@gmail.com", "klizsu9@gmail.com")


def main():
    client = _cognito()
    for email in EMAILS:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE lower(email)=%s", (email,))
            if cur.fetchone():
                print(
                    email
                    + ": a linked database user exists; stopped for relationship review."
                )
                return 2
        try:
            user = client.admin_get_user(
                UserPoolId=settings.cognito_user_pool_id, Username=email
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "UserNotFoundException":
                print(email + ": no Cognito account exists.")
                continue
            raise
        attributes = {a["Name"]: a["Value"] for a in user.get("UserAttributes", [])}
        if attributes.get("email", "").lower() != email:
            raise RuntimeError(
                "The Cognito email does not match the requested identity."
            )
        username = user["Username"]
        client.admin_user_global_sign_out(
            UserPoolId=settings.cognito_user_pool_id, Username=username
        )
        client.admin_delete_user(
            UserPoolId=settings.cognito_user_pool_id, Username=username
        )
        print(email + ": Cognito test identity removed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (BotoCoreError, ClientError) as exc:
        print(
            "Identity cleanup could not run: "
            + type(exc).__name__
            + ". Use the AWS environment that runs the backend."
        )
        raise SystemExit(2)
