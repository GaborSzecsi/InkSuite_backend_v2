from unittest.mock import Mock

import pytest

from app.marketing.providers import TikTokPublisher, ProviderError, tiktok_upload_chunks

MIB = 1024 * 1024


@pytest.mark.parametrize('total', [1, 5*MIB-1, 5*MIB, 10*MIB, 12_800_000,
                                  int(12.8*MIB), 25*MIB, 64*MIB-1, 64*MIB])
def test_small_and_medium_videos_declare_the_whole_file(total):
    assert tiktok_upload_chunks(total) == (total, 1)


@pytest.mark.parametrize('total', [64*MIB+1, 65*MIB, 70*MIB, 70*MIB+1, 500*MIB])
def test_large_videos_merge_remainder_into_the_final_chunk(total):
    chunk, count = tiktok_upload_chunks(total)
    assert chunk == 10*MIB
    assert count == total // chunk
    assert 2 <= count <= 1000
    lengths = [chunk]*(count-1) + [total-(count-1)*chunk]
    assert sum(lengths) == total
    assert all(5*MIB <= length <= 64*MIB for length in lengths[:-1])
    assert chunk <= lengths[-1] <= 128*MIB


@pytest.mark.parametrize('total', [0, -1, True, '12800000', 12.8, 10010*MIB])
def test_invalid_sizes_rejected(total):
    with pytest.raises(ProviderError):
        tiktok_upload_chunks(total)


@pytest.mark.parametrize('total', [12_800_000, int(12.8*MIB), 65*MIB, 70*MIB+1])
def test_init_and_sequential_transfer_agree(monkeypatch, total):
    from app.marketing import oauth
    import routers.storage_s3 as storage

    adapter = TikTokPublisher()
    monkeypatch.setattr(adapter, 'creator', lambda token: {
        'privacy_level_options': ['SELF_ONLY'], 'max_video_post_duration_sec': 60})
    monkeypatch.delenv('MARKETING_TIKTOK_VERIFIED_URL_PREFIXES', raising=False)
    monkeypatch.setattr(oauth, 'encrypt', lambda *args: 'encrypted-upload-url')
    monkeypatch.setattr(oauth, 'decrypt', lambda *args: 'https://upload.tiktokapis.com/video/')
    initialize = Mock(return_value={'data': {'publish_id': 'publish-id',
        'upload_url': 'https://upload.tiktokapis.com/video/'}})
    monkeypatch.setattr('app.marketing.providers.request', initialize)
    post = {'title': 'Book review', 'content_text': 'Review'}
    target = {'provider_payload': {'privacy_level': 'SELF_ONLY', 'consent': True}, 'provider_state': {}}
    media = [{'public_url': 'https://s3.example/video.mp4', 's3_bucket': 'bucket', 's3_key': 'video.mp4',
              'media_type': 'video', 'mime_type': 'video/mp4', 'duration_seconds': 30,
              'width': 1080, 'height': 1920, 'filesize_bytes': total}]
    account = {'tenant_id': 'tenant'}
    state = adapter.advance(account, 'token', post, target, media).state
    chunk, count = tiktok_upload_chunks(total)
    assert initialize.call_args.kwargs['json']['source_info'] == {
        'source': 'FILE_UPLOAD', 'video_size': total, 'chunk_size': chunk, 'total_chunk_count': count}
    assert (state['chunk_size'], state['total_chunk_count']) == (chunk, count)

    s3 = Mock()
    streams = []
    expected_start = 0

    def get_object(**kwargs):
        nonlocal expected_start
        start, end = map(int, kwargs['Range'].removeprefix('bytes=').split('-'))
        assert start == expected_start
        assert end < total
        expected_start = end + 1
        body = Mock()
        body.read.return_value = bytes(end-start+1)
        streams.append(body)
        return {'Body': body}

    s3.get_object.side_effect = get_object
    monkeypatch.setattr(storage, 's3', lambda: s3)
    sent = []

    def put(url, *, data, headers, **kwargs):
        start = sum(sent)
        size = len(data)
        assert headers['Content-Length'] == str(size)
        assert headers['Content-Range'] == f'bytes {start}-{start+size-1}/{total}'
        sent.append(size)
        return Mock(status_code=201 if start+size == total else 206)

    monkeypatch.setattr('app.marketing.providers.requests.put', put)
    for index in range(count):
        target['provider_state'] = state
        state = adapter.advance(account, 'token', post, target, media).state
        assert state['upload_offset'] == sum(sent)
        assert ('upload_url_encrypted' in state) == (index < count-1)
    assert len(sent) == count
    assert sum(sent) == expected_start == total
    assert all(body.close.call_count == 1 for body in streams)
    # Initialization is never repeated during chunk delivery.
    assert initialize.call_count == 1
