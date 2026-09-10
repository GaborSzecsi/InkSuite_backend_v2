from contextlib import contextmanager
from unittest.mock import Mock

import pytest
from app.meetings import routes as r
from app.meetings.providers import CalendarProvider


@pytest.fixture
def photo_db(monkeypatch):
    cur=Mock()
    @contextmanager
    def cursor(**kwargs):yield cur
    conn=Mock();conn.cursor=cursor
    @contextmanager
    def database():yield conn
    monkeypatch.setattr(r,'db_conn',database)
    query=Mock(return_value={'account_id':'google-user'})
    monkeypatch.setattr(r.s,'one',query)
    return query


@pytest.mark.parametrize('picture,expected',[
    ('https://lh3.googleusercontent.com/photo','https://lh3.googleusercontent.com/photo'),
    ('https://googleusercontent.com.evil.example/photo',None),
    ('http://lh3.googleusercontent.com/photo',None),
    ('',None),
])
def test_google_photo_is_owner_scoped_and_validated(monkeypatch,photo_db,picture,expected):
    provider=Mock();provider.identity.return_value={'id':'google-user','picture':picture}
    monkeypatch.setattr(r.s,'adapter',Mock(return_value=provider))
    result=r.profile_photo({'tenant':{'id':'tenant'},'user':{'id':'user','email':'me@example.com'}})
    assert result=={'picture':expected}
    assert photo_db.call_args.args[2]==('tenant','user','me@example.com')
    assert "provider='google' AND status='connected'" in photo_db.call_args.args[1]


def test_no_google_account_falls_back_without_provider_request(monkeypatch,photo_db):
    photo_db.return_value=None
    adapter=Mock();monkeypatch.setattr(r.s,'adapter',adapter)
    assert r.profile_photo({'tenant':{'id':'tenant'},'user':{'id':'user'}})=={'picture':None}
    adapter.assert_not_called()


def test_google_identity_preserves_picture():
    provider=CalendarProvider('google',{})
    provider.api=Mock(return_value={'sub':'google-user','email':'me@example.com','email_verified':True,'picture':'https://lh3.googleusercontent.com/photo'})
    assert provider.identity()['picture']=='https://lh3.googleusercontent.com/photo'
