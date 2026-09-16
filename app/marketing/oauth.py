"""Server-only OAuth credentials. Keys must come from deployment secret configuration."""

import hashlib
import json
import os
import secrets

from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from . import service as s
from .providers import request, graph, ProviderError


def cipher():
    from cryptography.fernet import Fernet

    key = os.getenv("MARKETING_TOKEN_KEY", "")
    if not key:
        raise ProviderError(
            "configuration",
            "Configure the Marketing encryption key before connecting accounts.",
        )
    return Fernet(key.encode())


def encrypt(token, tenant):
    return cipher().encrypt(
        json.dumps(
            {
                "tenant": str(tenant),
                "token": token,
            }
        ).encode()
    ).decode()


def decrypt(value, tenant):
    try:
        data = json.loads(cipher().decrypt(value.encode()))
    except Exception:
        raise ProviderError(
            "credentials",
            "Stored social credentials cannot be decrypted.",
        ) from None

    if data["tenant"] != str(tenant):
        raise ProviderError(
            "credentials",
            "Credential tenant mismatch.",
        )

    return data["token"]


def config(provider):
    # Facebook uses the Meta app credentials. Instagram uses the separate
    # Instagram app credentials created by the Instagram API use case.
    family = "META" if provider == "facebook" else provider.upper()

    client = os.getenv("MARKETING_" + family + "_CLIENT_ID", "")
    secret = os.getenv("MARKETING_" + family + "_CLIENT_SECRET", "")

    # Instagram Business Login and TikTok Login Kit require HTTPS redirect URIs.
    # Allow each provider to use a separate callback base (for example, an HTTPS
    # localhost tunnel) without changing the default callback base used by
    # Facebook and Pinterest.
    if provider in ("instagram", "tiktok"):
        provider_base_env = (
            "MARKETING_INSTAGRAM_PUBLIC_BASE_URL"
            if provider == "instagram"
            else "MARKETING_TIKTOK_PUBLIC_BASE_URL"
        )
        base = (
            os.getenv(provider_base_env, "")
            or os.getenv("MARKETING_PUBLIC_BASE_URL", "")
        ).rstrip("/")
        base_setting = provider_base_env + " or MARKETING_PUBLIC_BASE_URL"
    else:
        base = os.getenv("MARKETING_PUBLIC_BASE_URL", "").rstrip("/")
        base_setting = "MARKETING_PUBLIC_BASE_URL"

    if not client or not secret or not base:
        raise ProviderError(
            "configuration",
            f"Configure {family} OAuth credentials and {base_setting} first.",
        )

    return (
        client,
        secret,
        base + "/app/project_management/marketing/oauth/" + provider,
    )


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def begin(cur, ctx, provider):
    client, secret, redirect = config(provider)
    cipher()

    state = secrets.token_urlsafe(40)

    cur.execute(
        """
        INSERT INTO marketing_oauth_states(
            state_hash,
            tenant_id,
            user_id,
            provider,
            expires_at
        )
        VALUES(
            %s,
            %s,
            %s,
            %s,
            now() + interval '10 minutes'
        )
        """,
        (
            digest(state),
            ctx["tenant"]["id"],
            ctx["user"]["id"],
            provider,
        ),
    )

    params = {
        "client_id": client,
        "redirect_uri": redirect,
        "state": state,
        "response_type": "code",
    }

    if provider == "facebook":
        params["scope"] = ",".join(
            [
                "pages_show_list",
                "pages_read_engagement",
                "pages_manage_posts",
                "business_management",
            ]
        )

        # Force Meta to reconsider permissions instead of silently reusing
        # an older authorization grant.
        params["auth_type"] = "rerequest"

        url = (
            "https://www.facebook.com/"
            + graph().rsplit("/", 1)[-1]
            + "/dialog/oauth"
        )

    elif provider == "instagram":
        params["scope"] = ",".join(
            [
                "instagram_business_basic",
                "instagram_business_content_publish",
                "instagram_business_manage_comments",
                "instagram_business_manage_messages",
            ]
        )

        # Use Instagram Business Login directly. This does not require the
        # Instagram professional account to be linked to a Facebook Page.
        params["enable_fb_login"] = "0"
        params["force_authentication"] = "1"
        url = "https://www.instagram.com/oauth/authorize"

    elif provider == "pinterest":
        params["scope"] = (
            "boards:read,"
            "pins:read,"
            "pins:write,"
            "user_accounts:read"
        )
        url = "https://www.pinterest.com/oauth/"

    elif provider == "tiktok":
        params.pop("client_id")
        params["client_key"] = client
        params["scope"] = "user.info.basic,video.publish"
        url = "https://www.tiktok.com/v2/auth/authorize/"

    else:
        raise ProviderError(
            "unsupported_provider",
            "Unsupported provider.",
        )

    return {
        "url": url + "?" + urlencode(params),
    }


def token_exchange(provider, body):
    client, secret, redirect = config(provider)

    if provider == "instagram":
        return request(
            "POST",
            "https://api.instagram.com/oauth/access_token",
            data={
                **body,
                "client_id": client,
                "client_secret": secret,
            },
        )

    if provider == "pinterest":
        return request(
            "POST",
            "https://api.pinterest.com/v5/oauth/token",
            auth=(client, secret),
            data=body,
        )

    if provider == "tiktok":
        return request(
            "POST",
            "https://open.tiktokapis.com/v2/oauth/token/",
            data={
                **body,
                "client_key": client,
                "client_secret": secret,
            },
        )

    return request(
        "GET",
        graph() + "/oauth/access_token",
        params={
            **body,
            "client_id": client,
            "client_secret": secret,
        },
    )


def consume_state(cur, ctx, provider, state):
    tenant = ctx["tenant"]["id"]
    user = ctx["user"]["id"]

    entry = s.one(
        cur,
        """
        UPDATE marketing_oauth_states
        SET used_at = now()
        WHERE state_hash = %s
          AND tenant_id = %s
          AND user_id = %s
          AND provider = %s
          AND used_at IS NULL
          AND expires_at > now()
        RETURNING state_hash
        """,
        (
            digest(state),
            tenant,
            user,
            provider,
        ),
    )

    if not entry:
        raise HTTPException(
            400,
            "OAuth state is invalid, expired or already used.",
        )


def finish(cur, ctx, provider, code):
    tenant = ctx["tenant"]["id"]
    user = ctx["user"]["id"]

    client, secret, redirect = config(provider)

    tokens = token_exchange(
        provider,
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect,
        },
    )

    result = []

    if provider == "facebook":
        tokens = token_exchange(
            provider,
            {
                "grant_type": "fb_exchange_token",
                "fb_exchange_token": tokens["access_token"],
            },
        )

        pages = request(
            "GET",
            graph() + "/me/accounts",
            tokens["access_token"],
            params={
                "fields": (
                    "id,"
                    "name,"
                    "tasks,"
                    "access_token,"
                    "instagram_business_account{"
                    "id,"
                    "username,"
                    "name,"
                    "profile_picture_url"
                    "}"
                ),
                "limit": 100,
            },
        )

        all_pages = list(pages.get("data", []))
        seen_cursors = set()
        while pages.get("paging", {}).get("next"):
            after = pages.get("paging", {}).get("cursors", {}).get("after")
            if not after or after in seen_cursors or len(seen_cursors) >= 20:
                raise HTTPException(
                    422,
                    "Could not retrieve all Pages. Please reconnect and try again.",
                )
            seen_cursors.add(after)
            pages = request(
                "GET",
                graph() + "/me/accounts",
                tokens["access_token"],
                params={
                    "fields": (
                        "id,name,tasks,access_token,"
                        "instagram_business_account{"
                        "id,username,name,profile_picture_url}"
                    ),
                    "limit": 100,
                    "after": after,
                },
            )
            all_pages.extend(pages.get("data", []))

        # Temporary safe Meta diagnostics. Never log tokens, authorization
        # codes, client secrets, or provider payloads containing credentials.
        permissions = request(
            "GET",
            graph() + "/me/permissions",
            tokens["access_token"],
        )

        print(
            "META PERMISSIONS:",
            [
                {
                    "permission": p.get("permission"),
                    "status": p.get("status"),
                }
                for p in permissions.get("data", [])
            ],
            flush=True,
        )

        print(
            "META FACEBOOK PAGES:",
            [
                {
                    "id": page.get("id"),
                    "name": page.get("name"),
                    "tasks": page.get("tasks"),
                }
                for page in all_pages
            ],
            flush=True,
        )

        for page in all_pages:
            result.append(
                (
                    page,
                    {
                        "access_token": page["access_token"],
                    },
                    page["id"],
                )
            )

    elif provider == "instagram":
        # Instagram first returns a short-lived token. Exchange it for a
        # long-lived Instagram token before persisting the account.
        short_token = tokens["access_token"]

        long_token = request(
            "GET",
            "https://graph.instagram.com/access_token",
            params={
                "grant_type": "ig_exchange_token",
                "client_secret": secret,
                "access_token": short_token,
            },
        )

        access_token = long_token["access_token"]

        profile = request(
            "GET",
            "https://graph.instagram.com/me",
            access_token,
            params={
                "fields": (
                    "id,username,name,account_type,"
                    "profile_picture_url"
                ),
            },
        )

        account_tokens = {
            "access_token": access_token,
            "expires_in": long_token.get("expires_in"),
            "scope": ",".join(
                [
                    "instagram_business_basic",
                    "instagram_business_content_publish",
                    "instagram_business_manage_comments",
                    "instagram_business_manage_messages",
                ]
            ),
        }

        print(
            "INSTAGRAM DESTINATION:",
            {
                "id": profile.get("id"),
                "username": profile.get("username"),
                "name": profile.get("name"),
                "account_type": profile.get("account_type"),
            },
            flush=True,
        )

        result = [
            (
                {
                    "id": profile["id"],
                    "name": profile.get("name")
                    or profile.get("username")
                    or "Instagram",
                    "username": profile.get("username"),
                    "profile_picture_url": profile.get("profile_picture_url"),
                },
                account_tokens,
                None,
            )
        ]

    elif provider == "pinterest":
        item = request(
            "GET",
            "https://api.pinterest.com/v5/user_account",
            tokens["access_token"],
        )

        result = [
            (
                {
                    "id": item.get("id") or item["username"],
                    "name": item["username"],
                    "username": item["username"],
                },
                tokens,
                None,
            )
        ]

    else:
        item = request(
            "GET",
            "https://open.tiktokapis.com/v2/user/info/",
            tokens["access_token"],
            params={
                "fields": "open_id,display_name,avatar_url",
            },
        )["data"]["user"]

        result = [
            (
                {
                    "id": item["open_id"],
                    "name": item["display_name"],
                    "profile_picture_url": item.get("avatar_url"),
                },
                tokens,
                None,
            )
        ]

    if not result:
        raise HTTPException(
            422,
            "No eligible social destinations were returned. "
            "Check account type and granted permissions.",
        )

    if provider == "facebook":
        return prepare_selection(cur, ctx, provider, result)
    return persist_accounts(cur, ctx, provider, result)


def prepare_selection(cur, ctx, provider, result):
    # The browser receives only an encrypted, expiring envelope. The nonce in
    # the existing OAuth table makes confirmation single-use across workers.
    result = list({str(item[0]["id"]):item for item in result}.values())
    nonce = secrets.token_urlsafe(40)
    cur.execute(
        """INSERT INTO marketing_oauth_states
        (state_hash,tenant_id,user_id,provider,expires_at)
        VALUES (%s,%s,%s,%s,now() + interval '10 minutes')""",
        (digest(nonce),ctx["tenant"]["id"],ctx["user"]["id"],provider),
    )
    payload = {"purpose":"account_selection", "nonce":nonce,
        "tenant":str(ctx["tenant"]["id"]), "user":str(ctx["user"]["id"]),
        "provider":provider, "accounts":result}
    ticket = cipher().encrypt(json.dumps(payload).encode()).decode()
    return {"selection_required":True, "selection_token":ticket, "expires_in":600,
        "accounts":[{"id":str(item["id"]),
            "display_name":item.get("name") or item.get("username") or provider,
            "username":item.get("username"), "provider":provider}
            for item, _, _ in result]}


def confirm_selection(cur, ctx, provider, ticket, account_ids):
    try:
        payload = json.loads(cipher().decrypt(ticket.encode(), ttl=600))
    except Exception:
        raise HTTPException(400, "Page selection expired or is invalid. Connect again.") from None
    if (payload.get("purpose") != "account_selection"
        or payload.get("tenant") != str(ctx["tenant"]["id"])
        or payload.get("user") != str(ctx["user"]["id"])
        or payload.get("provider") != provider):
        raise HTTPException(400, "Page selection does not belong to this user and workspace.")
    available = {str(item[0]["id"]):item for item in payload["accounts"]}
    if not account_ids or len(set(account_ids)) != len(account_ids) or any(i not in available for i in account_ids):
        raise HTTPException(422, "Choose one or more of the available accounts.")
    consume_state(cur, ctx, provider, payload["nonce"])
    return persist_accounts(cur, ctx, provider, [available[i] for i in account_ids])


def persist_accounts(cur, ctx, provider, result):
    tenant = ctx["tenant"]["id"]
    user = ctx["user"]["id"]
    for item, account_tokens, parent in result:
        expires = (
            datetime.now(timezone.utc)
            + timedelta(seconds=int(account_tokens["expires_in"]))
            if account_tokens.get("expires_in")
            else None
        )

        refresh_expires = (
            datetime.now(timezone.utc)
            + timedelta(
                seconds=int(account_tokens["refresh_expires_in"])
            )
            if account_tokens.get("refresh_expires_in")
            else None
        )

        row = s.one(
            cur,
            """
            INSERT INTO social_accounts(
                id,
                tenant_id,
                provider,
                provider_account_id,
                provider_parent_account_id,
                display_name,
                username,
                account_type,
                profile_image_url,
                access_token_encrypted,
                refresh_token_encrypted,
                token_expires_at,
                refresh_token_expires_at,
                scopes,
                connected_by
            )
            VALUES(
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s
            )
            ON CONFLICT(
                tenant_id,
                provider,
                provider_account_id
            )
            DO UPDATE SET
                display_name = EXCLUDED.display_name,
                access_token_encrypted = EXCLUDED.access_token_encrypted,
                refresh_token_encrypted = EXCLUDED.refresh_token_encrypted,
                token_expires_at = EXCLUDED.token_expires_at,
                refresh_token_expires_at =
                    EXCLUDED.refresh_token_expires_at,
                scopes = EXCLUDED.scopes,
                status = 'connected',
                disconnected_at = NULL,
                updated_at = now(),
                connected_by = EXCLUDED.connected_by
            RETURNING id
            """,
            (
                s.uid(),
                tenant,
                provider,
                str(item["id"]),
                parent,
                item.get("name")
                or item.get("username")
                or provider,
                item.get("username"),
                "page"
                if provider == "facebook"
                else "professional",
                item.get("profile_picture_url"),
                encrypt(
                    account_tokens["access_token"],
                    tenant,
                ),
                encrypt(
                    account_tokens["refresh_token"],
                    tenant,
                )
                if account_tokens.get("refresh_token")
                else None,
                expires,
                refresh_expires,
                Jsonb(
                    account_tokens.get(
                        "scope",
                        "",
                    ).split(",")
                ),
                user,
            ),
        )

        s.audit(
            cur,
            ctx,
            row["id"],
            "social_account_connected",
        )

    return {
        "connected": len(result),
    }


def refresh(account):
    if account["status"] != "connected":
        raise ProviderError(
            "disconnected",
            "Reconnect this social account.",
        )

    expiry = account.get("token_expires_at")

    if (
        not expiry
        or expiry
        > datetime.now(timezone.utc)
        + timedelta(minutes=5)
    ):
        return (
            decrypt(
                account["access_token_encrypted"],
                account["tenant_id"],
            ),
            None,
        )

    if account["provider"] == "instagram":
        current_token = decrypt(
            account["access_token_encrypted"],
            account["tenant_id"],
        )
        data = request(
            "GET",
            "https://graph.instagram.com/refresh_access_token",
            params={
                "grant_type": "ig_refresh_token",
                "access_token": current_token,
            },
        )
        return data["access_token"], data

    if not account.get("refresh_token_encrypted"):
        raise ProviderError(
            "expired",
            "Reconnect this social account to renew authorization.",
        )

    refresh_expiry = account.get("refresh_token_expires_at")

    if (
        refresh_expiry
        and refresh_expiry <= datetime.now(timezone.utc)
    ):
        raise ProviderError(
            "expired",
            "Reconnect this social account to renew authorization.",
        )

    data = token_exchange(
        account["provider"],
        {
            "grant_type": "refresh_token",
            "refresh_token": decrypt(
                account["refresh_token_encrypted"],
                account["tenant_id"],
            ),
        },
    )

    return data["access_token"], data