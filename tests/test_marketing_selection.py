import json
import time
from unittest.mock import Mock

import pytest
from cryptography.fernet import Fernet
from fastapi import HTTPException
from app.marketing import oauth


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setenv('MARKETING_TOKEN_KEY', Fernet.generate_key().decode())
    ctx = {'tenant': {'id': 'tenant-a'}, 'user': {'id': 'user-a'}}
    candidates = [({'id': str(i), 'name': name}, {'access_token': 'secret-page-'+str(i)}, str(i))
                  for i, name in [(1, 'Marble Press'), (2, 'Susan Szecsi ART'), (3, 'Susan Szecsi')]]
    return ctx, candidates


def test_callback_does_not_connect_pages(setup, monkeypatch):
    ctx, candidates = setup
    monkeypatch.setattr(oauth, 'config', lambda _: ('client', 'secret', 'redirect'))
    monkeypatch.setattr(oauth, 'token_exchange', lambda *a: {'access_token': 'user-token'})
    monkeypatch.setattr(oauth, 'graph', lambda: 'https://graph.facebook.com/v25.0')
    monkeypatch.setattr(oauth, 'request', lambda *a, **k: {'data': [dict(item, **tokens) for item, tokens, _ in candidates]})
    persist = Mock()
    monkeypatch.setattr(oauth, 'persist_accounts', persist)
    cur = Mock()
    result = oauth.finish(cur, ctx, 'facebook', 'code')
    assert result['selection_required']
    assert [a['id'] for a in result['accounts']] == ['1', '2', '3']
    assert 'secret-page' not in json.dumps(result)
    persist.assert_not_called()
    assert 'marketing_oauth_states' in cur.execute.call_args.args[0]


@pytest.mark.parametrize('chosen', [['1'], ['1', '2'], ['1', '2', '3']])
def test_only_selected_pages_persist(setup, monkeypatch, chosen):
    ctx, candidates = setup
    ticket = oauth.prepare_selection(Mock(), ctx, 'facebook', candidates)['selection_token']
    consume = Mock()
    persist = Mock(return_value={'connected': len(chosen)})
    monkeypatch.setattr(oauth, 'consume_state', consume)
    monkeypatch.setattr(oauth, 'persist_accounts', persist)
    assert oauth.confirm_selection(Mock(), ctx, 'facebook', ticket, chosen)['connected'] == len(chosen)
    assert [a[0]['id'] for a in persist.call_args.args[3]] == chosen
    consume.assert_called_once()


@pytest.mark.parametrize('chosen', [[], ['missing'], ['1', '1'], ['1', 'missing']])
def test_invalid_selection_writes_nothing(setup, monkeypatch, chosen):
    ctx, candidates = setup
    ticket = oauth.prepare_selection(Mock(), ctx, 'facebook', candidates)['selection_token']
    persist = Mock(); consume = Mock()
    monkeypatch.setattr(oauth, 'persist_accounts', persist)
    monkeypatch.setattr(oauth, 'consume_state', consume)
    with pytest.raises(HTTPException): oauth.confirm_selection(Mock(), ctx, 'facebook', ticket, chosen)
    persist.assert_not_called(); consume.assert_not_called()


@pytest.mark.parametrize('change', ['tenant', 'user', 'provider', 'expired', 'tampered'])
def test_ticket_binding_and_expiration(setup, monkeypatch, change):
    ctx, candidates = setup
    ticket = oauth.prepare_selection(Mock(), ctx, 'facebook', candidates)['selection_token']
    provider = 'facebook'
    if change in ('tenant', 'user'): ctx = {**ctx, change: {'id': 'other'}}
    if change == 'provider': provider = 'instagram'
    if change == 'expired':
        raw = oauth.cipher().decrypt(ticket.encode())
        ticket = oauth.cipher().encrypt_at_time(raw, int(time.time())-700).decode()
    if change == 'tampered': ticket = 'invalid'+ticket
    persist = Mock(); monkeypatch.setattr(oauth, 'persist_accounts', persist)
    with pytest.raises(HTTPException): oauth.confirm_selection(Mock(), ctx, provider, ticket, ['1'])
    persist.assert_not_called()


def test_used_nonce_cannot_persist_again(setup, monkeypatch):
    ctx, candidates = setup
    ticket = oauth.prepare_selection(Mock(), ctx, 'facebook', candidates)['selection_token']
    cur = Mock(); cur.fetchone.return_value = None
    persist = Mock(); monkeypatch.setattr(oauth, 'persist_accounts', persist)
    with pytest.raises(HTTPException): oauth.confirm_selection(cur, ctx, 'facebook', ticket, ['1'])
    assert 'used_at IS NULL' in cur.execute.call_args.args[0]
    persist.assert_not_called()
