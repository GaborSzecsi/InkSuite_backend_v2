from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.marketing import publishing, schemas
from app.marketing.providers import TikTokPublisher, ProviderError


@pytest.fixture
def post():
    return {'id': uuid4(), 'title': 'Book review', 'content_text': 'Our latest release',
            'timezone': 'UTC', 'status': 'draft'}


@pytest.fixture
def video():
    return [{'public_url': 'https://media.example/video.mp4', 'media_type': 'video',
             'mime_type': 'video/mp4', 'width': 1080, 'height': 1920,
             'filesize_bytes': 100000, 'duration_seconds': 20}]


@pytest.mark.parametrize('own,branded,allowed', [(False, False, True), (True, False, True),
                                               (False, True, False), (True, True, False)])
def test_private_disclosure_combinations(post, video, own, branded, allowed):
    target = {'provider_payload': {'privacy_level': 'SELF_ONLY', 'consent': True,
        'brand_organic_toggle': own, 'brand_content_toggle': branded}}
    if allowed:
        TikTokPublisher().validate_post(post, target, video)
    else:
        with pytest.raises(ProviderError, match='Paid partnership'):
            TikTokPublisher().validate_post(post, target, video)


@pytest.mark.parametrize('own,branded', [(True, False), (False, True), (True, True)])
def test_public_disclosures_allowed(post, video, own, branded):
    target = {'provider_payload': {'privacy_level': 'PUBLIC_TO_EVERYONE', 'consent': True,
        'commercial_content_disclosure': True, 'brand_organic_toggle': own, 'brand_content_toggle': branded}}
    TikTokPublisher().validate_post(post, target, video)


def test_enabled_disclosure_needs_a_category(post, video):
    with pytest.raises(ProviderError, match='Indicate whether'):
        TikTokPublisher().validate_post(post, {'provider_payload': {'privacy_level': 'SELF_ONLY',
            'consent': True, 'commercial_content_disclosure': True}}, video)


@pytest.mark.parametrize('options', [[], ['SELF_ONLY'], None])
def test_creator_permission_checked_before_initializing(monkeypatch, post, video, options):
    adapter = TikTokPublisher()
    monkeypatch.setattr(adapter, 'creator', Mock(return_value={'privacy_level_options': options}))
    send = Mock()
    monkeypatch.setattr('app.marketing.providers.request', send)
    target = {'provider_state': {}, 'provider_payload': {'privacy_level': 'PUBLIC_TO_EVERYONE', 'consent': True}}
    with pytest.raises(ProviderError, match='visibility options changed'):
        adapter.advance({}, 'token', post, target, video)
    send.assert_not_called()


def test_private_paid_partnership_never_initializes_delivery(monkeypatch, post, video):
    send = Mock()
    monkeypatch.setattr('app.marketing.providers.request', send)
    with pytest.raises(ProviderError, match='Paid partnership'):
        TikTokPublisher().advance({}, 'token', post, {'provider_state': {}, 'provider_payload': {
            'privacy_level': 'SELF_ONLY', 'consent': True, 'brand_content_toggle': True}}, video)
    send.assert_not_called()


def test_valid_disclosures_keep_existing_video_payload_and_disabled_interactions(monkeypatch, post, video):
    adapter = TikTokPublisher()
    monkeypatch.setattr(adapter, 'creator', lambda token: {'privacy_level_options': ['PUBLIC_TO_EVERYONE'],
        'max_video_post_duration_sec': 60, 'comment_disabled': True, 'duet_disabled': True, 'stitch_disabled': True})
    monkeypatch.setenv('MARKETING_TIKTOK_VERIFIED_URL_PREFIXES', 'https://media.example')
    send = Mock(return_value={'data': {'publish_id': 'submission'}})
    monkeypatch.setattr('app.marketing.providers.request', send)
    target = {'provider_state': {}, 'provider_payload': {'privacy_level': 'PUBLIC_TO_EVERYONE',
        'consent': True, 'brand_content_toggle': True, 'brand_organic_toggle': True, 'is_aigc': True,
        'allow_comment': True, 'allow_duet': True, 'allow_stitch': True}}
    assert adapter.advance({}, 'token', post, target, video).state['publish_id'] == 'submission'
    body = send.call_args.kwargs['json']
    assert body['source_info'] == {'source': 'PULL_FROM_URL', 'video_url': video[0]['public_url']}
    assert body['post_info']['brand_content_toggle'] and body['post_info']['brand_organic_toggle']
    assert body['post_info']['is_aigc']
    assert all(body['post_info'][key] for key in ['disable_comment', 'disable_duet', 'disable_stitch'])


@pytest.mark.parametrize('visibility,allowed', [('PUBLIC_TO_EVERYONE', False), ('SELF_ONLY', True)])
def test_schedule_validates_creator_visibility_before_writing_jobs(monkeypatch, post, video, visibility, allowed):
    target = {'id': uuid4(), 'provider': 'tiktok', 'social_account_id': uuid4(),
              'provider_status': 'pending', 'provider_payload': {'privacy_level': visibility, 'consent': True, 'brand_organic_toggle': True}}
    post['targets'] = [target]
    adapter = TikTokPublisher()
    monkeypatch.setattr(adapter, 'creator', Mock(return_value={'privacy_level_options': ['SELF_ONLY']}))
    monkeypatch.setattr(publishing, 'publisher', lambda name: adapter)
    monkeypatch.setattr(publishing, 'credentials', lambda *args: 'token')
    monkeypatch.setattr(publishing, 'media_for_target', lambda *args: video)
    monkeypatch.setattr(publishing.s, 'post_view', lambda *args: post)
    monkeypatch.setattr(publishing.s, 'owned', lambda cur, table, *args: post if table == 'social_posts' else {'status': 'connected'})
    cur = Mock()
    ctx = {'tenant': {'id': uuid4()}, 'user': {'id': uuid4()}}
    if allowed:
        assert publishing.schedule(cur, ctx, post['id']) == post
        assert any('INSERT INTO social_publish_jobs' in call.args[0] for call in cur.execute.call_args_list)
    else:
        with pytest.raises(HTTPException) as error:
            publishing.schedule(cur, ctx, post['id'])
        assert error.value.status_code == 422
        assert 'visibility options changed' in error.value.detail
        cur.execute.assert_not_called()


@pytest.mark.parametrize('value', ['true', 1, None])
def test_disclosure_setting_must_be_boolean(value):
    with pytest.raises(ValidationError):
        schemas.Target(social_account_id=uuid4(), provider_payload={'commercial_content_disclosure': value})
