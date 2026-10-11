from unittest.mock import Mock, patch
from botocore.exceptions import ClientError
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.auth import service, router


def test_refresh_supports_rotation_without_password_or_database():
    client = Mock()
    client.get_tokens_from_refresh_token.return_value = {'AuthenticationResult': {
        'AccessToken': 'new-access', 'RefreshToken': 'rotated-refresh', 'ExpiresIn': 900}}
    with patch.object(service, '_cognito', return_value=client):
        result = service.refresh_session('old-refresh')
    assert result == dict(access_token='new-access', refresh_token='rotated-refresh', expires_in=900)
    assert client.get_tokens_from_refresh_token.call_args.kwargs['RefreshToken'] == 'old-refresh'


def test_expired_refresh_is_401_but_provider_outage_is_503():
    app = FastAPI()
    app.include_router(router.router, prefix='/api')
    with TestClient(app) as client:
        with patch.object(router, 'refresh_session', return_value=None):
            assert client.post('/api/auth/refresh', json={'refresh_token': 'expired'}).status_code == 401
        with patch.object(router, 'refresh_session', side_effect=RuntimeError('outage')):
            assert client.post('/api/auth/refresh', json={'refresh_token': 'token'}).status_code == 503


def test_invalid_cognito_refresh_returns_none():
    client = Mock()
    client.get_tokens_from_refresh_token.side_effect = ClientError(
        {'Error': {'Code': 'NotAuthorizedException'}}, 'GetTokensFromRefreshToken')
    with patch.object(service, '_cognito', return_value=client):
        assert service.refresh_session('expired') is None
