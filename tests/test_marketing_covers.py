from unittest.mock import Mock
import pytest
from app.marketing.providers import InstagramPublisher, ProviderError, media_for_target, reel_cover


def fixture():
    video=dict(id='video',media_type='video',mime_type='video/mp4',public_url='https://example.com/video.mp4',width=1080,height=1920,filesize_bytes=10000,duration_seconds=5)
    cover=dict(id='cover',role='cover',media_type='image',mime_type='image/jpeg',public_url='https://example.com/cover.jpg',width=1080,height=1920,filesize_bytes=10000)
    return dict(content_text='Test',assets=[video,cover]),dict(provider_payload={'cover_asset_id':'cover'},provider_state={})


def test_cover_is_not_carousel_media(monkeypatch):
    post,target=fixture()
    media=media_for_target(post,target)
    assert len(media)==1
    publisher=InstagramPublisher()
    publisher.validate_post(post,target,media)
    request=Mock(return_value={'id':'container'})
    monkeypatch.setattr('app.marketing.providers.request',request)
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    publisher.advance({'provider_account_id':'account'},'token',post,target,media)
    body=request.call_args.kwargs['data']
    assert body['media_type']=='REELS'
    assert body['cover_url']=='https://example.com/cover.jpg'
    assert 'image_url' not in body


def test_default_has_no_custom_cover(monkeypatch):
    post,target=fixture();target['provider_payload']={}
    request=Mock(return_value={'id':'container'})
    monkeypatch.setattr('app.marketing.providers.request',request)
    monkeypatch.setenv('MARKETING_META_API_VERSION','v25.0')
    InstagramPublisher().advance({'provider_account_id':'account'},'token',post,target,media_for_target(post,target))
    assert 'cover_url' not in request.call_args.kwargs['data']


@pytest.mark.parametrize('change',[{'id':'other'},{'mime_type':'image/png'},{'public_url':'http://example.com/a.jpg'},{'filesize_bytes':9000000}])
def test_invalid_cover_rejected(change):
    post,target=fixture();post['assets'][1].update(change)
    with pytest.raises(ProviderError): reel_cover(post,target)


def test_cover_cannot_be_selected_as_content():
    post,target=fixture();target['provider_payload']['asset_ids']=['cover']
    with pytest.raises(ProviderError): media_for_target(post,target)


def test_cover_rejects_carousel():
    post,target=fixture()
    with pytest.raises(ProviderError,match='single Reel'):
        InstagramPublisher().validate_post(post,target,[post['assets'][0],post['assets'][0]])
