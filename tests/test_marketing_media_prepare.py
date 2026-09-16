import io
import subprocess
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from PIL import Image, ImageDraw
from app.marketing import media_prepare as m


def artwork():
    image=Image.new('RGB',(320,180),'green')
    draw=ImageDraw.Draw(image);draw.rectangle((0,0,25,179),fill='red');draw.rectangle((294,0,319,179),fill='blue')
    stream=io.BytesIO();image.save(stream,'PNG');return stream.getvalue()


@pytest.mark.parametrize('size',[(1080,1350),(1080,1080),(1080,566)])
def test_fit_keeps_both_edges(size):
    original=artwork();output=m.image_export(original,*size,'fit','solid')
    with Image.open(io.BytesIO(output)) as image:
        assert image.size==size and image.format=='JPEG'
        assert image.getpixel((8,size[1]//2))[0]>230
        assert image.getpixel((size[0]-9,size[1]//2))[2]>230
    assert len(output)<8*1024*1024 and original==artwork()


def test_fill_is_explicit_crop():
    output=m.image_export(artwork(),1080,1350,'fill','solid')
    with Image.open(io.BytesIO(output)) as image:
        assert image.getpixel((10,675))[1]>90
        assert image.getpixel((1070,675))[1]>90


def test_blur_and_white_backgrounds():
    with Image.open(io.BytesIO(m.image_export(artwork(),1080,1350,'fit','solid'))) as image:
        assert min(image.getpixel((540,10)))>245
    with Image.open(io.BytesIO(m.image_export(artwork(),1080,1350,'fit','blur'))) as image:
        assert image.getpixel((540,10))[1]>90 and image.getpixel((540,10))[0]<50


def test_title_picker_includes_generated_assets(monkeypatch):
    from app.marketing import routes
    from contextlib import contextmanager
    tenant,work,campaign=uuid4(),uuid4(),uuid4()
    ctx={'tenant':{'id':tenant}}
    cur=Mock();cur.__enter__=Mock(return_value=cur);cur.__exit__=Mock(return_value=False)
    conn=Mock();conn.cursor.return_value=cur
    @contextmanager
    def connection(): yield conn
    monkeypatch.setattr(routes,'db_conn',connection)
    monkeypatch.setattr(routes.assets,'discover',lambda *args:[{'filename':'original'}])
    monkeypatch.setattr(routes.s,'owned',lambda *args:{})
    rows=Mock(return_value=[{'filename':'generated','id':'derived'}]);monkeypatch.setattr(routes.s,'rows',rows)
    result=routes.title_assets(work,campaign,ctx)
    assert [a['filename'] for a in result]==['generated','original']
    assert rows.call_args.args[2]==(tenant,work,campaign)
    assert 'campaign_id IS NULL OR campaign_id=%s' in rows.call_args.args[1]


@pytest.fixture
def video(tmp_path):
    if not Path(m.binary('ffmpeg')).is_file(): pytest.skip('Local FFmpeg not installed')
    png=tmp_path/'source.png';png.write_bytes(artwork());video=tmp_path/'source.mp4'
    subprocess.run([m.binary('ffmpeg'),'-v','error','-y','-loop','1','-i',str(png),
        '-f','lavfi','-i','sine=frequency=440:sample_rate=44100','-t','3.2','-r','25',
        '-c:v','libx264','-pix_fmt','yuv420p','-c:a','aac',str(video)],check=True,timeout=30)
    return video


@pytest.mark.parametrize('mode,background',[('fit','blur'),('fit','solid'),('fill','blur')])
def test_real_reel_conversion(video,tmp_path,mode,background):
    original=video.read_bytes();output=tmp_path/'reel.mp4'
    info=m.video_export(video,output,mode,background)
    assert info and m.reel_ready(info)
    assert (info['width'],info['height'],info['fps'])==(1080,1920,30)
    assert info['audio_codec']=='aac' and info['audio_rate']==48000
    assert abs(info['duration']-3.2)<.15
    assert video.read_bytes()==original
    frame=tmp_path/'frame.png'
    subprocess.run([m.binary('ffmpeg'),'-v','error','-y','-i',str(output),'-frames:v','1',str(frame)],check=True,timeout=30)
    with Image.open(frame) as image:
        if mode=='fit':
            assert image.getpixel((10,960))[0]>200
            assert image.getpixel((1070,960))[2]>200
        else:
            assert image.getpixel((10,960))[1]>80
    assert m.video_export(output,tmp_path/'not-created.mp4','original') is None
    assert not (tmp_path/'not-created.mp4').exists()


def test_original_rejects_incompatible_video(video,tmp_path):
    with pytest.raises(HTTPException) as error: m.video_export(video,tmp_path/'output.mp4','original')
    assert 'not Reel-ready' in error.value.detail
    assert not (tmp_path/'output.mp4').exists()
