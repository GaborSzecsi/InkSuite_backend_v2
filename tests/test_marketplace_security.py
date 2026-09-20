"""Focused authorization tests. No DB, AWS credentials, or provider calls needed."""

from uuid import uuid4
from unittest.mock import patch
import pytest
from fastapi import HTTPException
from app.marketplace import core
from app.marketplace.schemas import Post, Profile, Message


def test_personal_actor_cannot_be_impersonated():
    user = {"id": uuid4()}
    with patch.object(
        core,
        "one",
        return_value={"user_id": uuid4(), "organization_id": None, "status": "active"},
    ):
        with pytest.raises(HTTPException) as exc:
            core.actor(None, user, uuid4())
        assert exc.value.status_code == 403


@pytest.mark.parametrize(
    "permissions, messaging, expected",
    [
        ({}, False, False),
        ({"marketplace": True}, False, True),
        ({"marketplace": True}, True, False),
        ({"marketplace_messages": True}, True, False),
        ({"marketplace": True, "marketplace_messages": True}, True, True),
    ],
)
def test_organization_permissions(permissions, messaging, expected):
    org = {"id": uuid4(), "tenant_id": uuid4()}
    member = {"role": "editor", "module_permissions": permissions}
    with patch.object(core, "one", side_effect=[org, member]):
        if expected:
            assert (
                core.organization_permission(
                    None, {"id": uuid4()}, org["id"], messaging=messaging
                )
                == org
            )
        else:
            with pytest.raises(HTTPException):
                core.organization_permission(
                    None, {"id": uuid4()}, org["id"], messaging=messaging
                )


def test_missing_membership_denies_existing_session():
    with patch.object(core, "one", side_effect=[{"tenant_id": uuid4()}, None]):
        with pytest.raises(HTTPException):
            core.organization_permission(None, {"id": uuid4()}, uuid4())


@pytest.mark.parametrize(
    "preference,blocked,connected,expected",
    [
        ("anyone", False, False, True),
        ("anyone", True, True, False),
        ("connections_only", False, False, False),
        ("connections_only", False, True, True),
        ("nobody", False, True, False),
    ],
)
def test_messaging_preferences_and_blocks(preference, blocked, connected, expected):
    recipient = {
        "organization_id": None,
        "status": "active",
        "messaging_preference": preference,
    }
    with patch.object(core, "one", return_value=recipient), patch.object(
        core, "blocked", return_value=blocked
    ), patch.object(core, "connected", return_value=connected):
        if expected:
            core.message_allowed(None, uuid4(), uuid4())
        else:
            with pytest.raises(HTTPException):
                core.message_allowed(None, uuid4(), uuid4())


def test_private_profile_projection_hides_identity_fields():
    record = {
        "id": uuid4(),
        "user_id": uuid4(),
        "organization_id": None,
        "status": "active",
        "profile_visibility": "private",
        "organization_status": None,
        "display_name": "Private Name",
        "name": None,
        "username": "private",
        "slug": None,
        "avatar_asset_ref": None,
        "logo_asset_ref": None,
        "bio": "Private biography",
        "description": None,
    }
    with patch.object(core, "one", return_value=record):
        result = core.public_actor(None, record["id"])
    assert (
        result["name"] == "Private profile"
        and result["href"] is None
        and result["bio"] == ""
    )
    assert "user_id" not in result


@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(1)",
        "data:image/svg+xml,a",
        "http://example.com/x",
        "https://user:password@example.com/a",
    ],
)
def test_unsafe_image_links_rejected(url):
    assert core.safe_url(url) == ""


def test_untrusted_fields_cannot_set_identity_or_audit_values():
    with pytest.raises(ValueError):
        Post(actor_id=uuid4(), body="hi", created_by_user_id=uuid4())
    with pytest.raises(ValueError):
        Message(actor_id=uuid4(), body="   ")
    with pytest.raises(ValueError):
        Profile(username="Invalid Name", display_name="Test")


def test_image_reference_cannot_cross_personal_accounts():
    from app.marketplace.media import validated_reference, personal_prefix

    u = {"id": uuid4()}
    with pytest.raises(HTTPException) as exc:
        validated_reference(
            None,
            u,
            {"user_id": u["id"], "organization_id": None},
            key=personal_prefix(uuid4()) + str(uuid4()) + ".jpg",
        )
    assert exc.value.status_code == 403


def test_image_reference_rejects_ambiguous_sources():
    from app.marketplace.media import validated_reference

    with pytest.raises(HTTPException) as exc:
        validated_reference(None, {}, {}, asset_id=uuid4(), key="somewhere")
    assert exc.value.status_code == 422


def test_reader_login_uses_existing_cognito_without_tenant_creation():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from app.marketplace import identity

    provider = Mock()
    provider.initiate_auth.return_value = {
        "AuthenticationResult": {"AccessToken": "verified-test-token"}
    }
    request = SimpleNamespace(client=SimpleNamespace(host="login-test"))
    with patch.object(identity.auth, "_cognito", return_value=provider), patch.object(
        identity.auth,
        "get_current_user_from_token",
        return_value={"sub": "existing-person"},
    ), patch.object(
        identity.auth, "get_user_db_record_from_claims", return_value={"id": uuid4()}
    ) as account:
        result = identity.login(
            identity.Credentials(email="reader@example.com", password="test-password"),
            request,
        )
    assert result["access_token"] == "verified-test-token"
    account.assert_called_once_with({"sub": "existing-person"})
    assert provider.initiate_auth.call_args.kwargs["AuthFlow"] == "USER_PASSWORD_AUTH"


def test_anonymous_writes_require_authentication():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.marketplace.routes import router

    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        result = client.post(
            "/marketplace/posts",
            json={"actor_id": str(uuid4()), "body": "Cannot post anonymously"},
        )
    assert result.status_code == 401


def test_public_registration_is_closed_without_contacting_cognito():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.marketplace import identity
    from app.marketplace.routes import router

    app = FastAPI()
    app.include_router(router)
    with patch.object(identity.auth, "_cognito") as cognito, TestClient(app) as client:
        result = client.post("/marketplace/auth/register", json={
            "email": "reader@example.com", "password": "test-password"
        })
    assert result.status_code == 403
    assert "coming soon" in result.json()["detail"]
    cognito.assert_not_called()
