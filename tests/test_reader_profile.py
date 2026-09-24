from contextlib import contextmanager
from unittest.mock import Mock, patch
import pytest
from fastapi import HTTPException
from app.marketplace.schemas import ReaderContact
from app.marketplace.identity_service import public_contact_details
from app.marketplace import identity_routes as routes


def test_contact_details_are_private_by_default():
    contact = ReaderContact(email="reader@example.com", phone="123", address="Private home")
    assert public_contact_details(contact.model_dump()) == {}


def test_public_projection_only_includes_explicit_fields():
    assert public_contact_details({"email":"reader@example.com", "email_public":True, "phone":"123", "phone_public":False, "address":"Private", "address_public":"true", "secret":"x"}) == {"email":"reader@example.com"}


def test_invalid_email_rejected():
    with pytest.raises(ValueError): ReaderContact(email="not-an-email")


def test_email_update_requires_verification_configuration():
    client=Mock()
    client.describe_user_pool.return_value={"UserPool":{}}
    with patch.object(routes.auth,"_cognito",return_value=client):
        with pytest.raises(HTTPException) as exc:
            routes.change_email(routes.EmailChange(email="new@example.com"),Mock(),{"id":"owner"})
    assert exc.value.status_code == 409
    client.update_user_attributes.assert_not_called()


def test_email_change_uses_current_users_token():
    client=Mock()
    client.describe_user_pool.return_value={"UserPool":{"UserAttributeUpdateSettings":{"AttributesRequireVerificationBeforeUpdate":["email"]}}}
    request=Mock(headers={"authorization":"Bearer current-user-token"})
    with patch.object(routes.auth,"_cognito",return_value=client):
        routes.change_email(routes.EmailChange(email="new@example.com"),request,{"id":"owner"})
    client.update_user_attributes.assert_called_once_with(AccessToken="current-user-token",UserAttributes=[{"Name":"email","Value":"new@example.com"}])


def test_verified_email_is_read_from_cognito_not_client_input():
    client=Mock()
    client.get_user.return_value={"UserAttributes":[{"Name":"email","Value":"verified@example.com"},{"Name":"email_verified","Value":"true"}]}
    cur=Mock()
    @contextmanager
    def transaction(): yield cur
    with patch.object(routes.auth,"_cognito",return_value=client), patch.object(routes.service,"transaction",transaction), patch.object(routes.service._repository,"one",return_value={"ready":1}):
        result=routes.verify_email(routes.EmailVerification(code="123456"),Mock(headers={"authorization":"Bearer token"}),{"id":"owner"})
    assert result["email"]=="verified@example.com"
    assert cur.execute.call_args_list[0].args[1]==("verified@example.com","owner")


def test_unverified_email_cannot_be_saved():
    client=Mock()
    client.get_user.return_value={"UserAttributes":[{"Name":"email_verified","Value":"false"}]}
    with patch.object(routes.auth,"_cognito",return_value=client), patch.object(routes.service,"transaction") as transaction:
        with pytest.raises(HTTPException): routes.verify_email(routes.EmailVerification(code="123456"),Mock(headers={"authorization":"Bearer token"}),{"id":"owner"})
    transaction.assert_not_called()


@pytest.mark.parametrize("name,expected",[("Gabor Szecsi","gabor_szecsi"),("Gábor Szécsi","gabor_szecsi"),(" A ","a_reader"),("","reader")])
def test_readable_username(name,expected):
    from app.marketplace.usernames import name_username
    assert name_username(name)==expected


def test_username_collision_gets_number_and_excludes_owner():
    from app.marketplace import usernames
    with patch.object(usernames,"one",side_effect=[{"taken":1},None]) as query:
        assert usernames.available_username(Mock(),"gabor_szecsi","owner",lock=True)=="gabor_szecsi_2"
        assert query.call_args.args[2]==("gabor_szecsi_2","owner","owner")


def test_reserved_routes_are_not_used_as_profile_handles():
    from app.marketplace import usernames
    with patch.object(usernames,"one",return_value=None):
        assert usernames.available_username(Mock(),"marketplace")=="marketplace_2"


def test_long_duplicate_username_stays_within_limit():
    from app.marketplace import usernames
    with patch.object(usernames,"one",side_effect=[{"taken":1},None]):
        value=usernames.available_username(Mock(),"a"*30)
    assert len(value)==30 and value.endswith("_2")
