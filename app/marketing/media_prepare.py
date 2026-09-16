"""Non-destructive Instagram exports. Never crop unless fill is requested."""
import io
import json
import os
import subprocess
from fractions import Fraction
from pathlib import Path

from fastapi import HTTPException
from PIL import Image, ImageFilter, ImageOps


def binary(name):
    configured=os.getenv(name.upper()+'_BINARY')
    if configured: return configured
    local=Path(__file__).resolve().parents[2]/'.tools'/'ffmpeg'/f'{name}.exe'
    return str(local) if os.name=='nt' and local.exists() else name


def image_export(data, width, height, mode='fit', background='blur'):
    with Image.open(io.BytesIO(data)) as original:
        original = ImageOps.exif_transpose(original).convert('RGBA')
        image = Image.new('RGBA', original.size, 'white')
        image.alpha_composite(original)
        image = image.convert('RGB')
        if mode == 'fill':
            canvas = ImageOps.fit(image, (width,height), method=Image.Resampling.LANCZOS)
        else:
            canvas = (ImageOps.fit(image, (width,height), method=Image.Resampling.LANCZOS)
                .filter(ImageFilter.GaussianBlur(35)) if background=='blur'
                else Image.new('RGB',(width,height),'white'))
            fitted = ImageOps.contain(image,(width,height),method=Image.Resampling.LANCZOS)
            canvas.paste(fitted,((width-fitted.width)//2,(height-fitted.height)//2))
        for quality in (92,85,75,65):
            output=io.BytesIO();canvas.save(output,'JPEG',quality=quality,optimize=True)
            if output.tell() <= 8*1024*1024: return output.getvalue()
    raise HTTPException(422,'Prepared JPEG exceeds 8 MB. Choose a smaller canvas.')


def probe(path):
    try:
        result=subprocess.run([binary('ffprobe'),'-v','error','-protocol_whitelist','file,pipe','-format_whitelist','mov,matroska,webm',
            '-show_streams','-show_format','-of','json',str(path)],capture_output=True,text=True,timeout=40,check=True)
        data=json.loads(result.stdout)
        video=next(s for s in data['streams'] if s.get('codec_type')=='video')
        audio=next((s for s in data['streams'] if s.get('codec_type')=='audio'),None)
        rotation=abs(float(next((s['rotation'] for s in video.get('side_data_list',[]) if 'rotation' in s),video.get('tags',{}).get('rotate',0))))%180
        width,height=int(video['width']),int(video['height'])
        if rotation==90: width,height=height,width
        fps=float(Fraction(video.get('avg_frame_rate','0/1')))
        return {'width':width,'height':height,'duration':float(data['format'].get('duration') or video.get('duration') or 0),
            'fps':fps,'codec':video.get('codec_name'),'pixel_format':video.get('pix_fmt'),
            'sar':video.get('sample_aspect_ratio','1:1'),'rotation':rotation,
            'audio_codec':audio.get('codec_name') if audio else None,
            'audio_rate':int(audio.get('sample_rate',0)) if audio else 0,
            'audio_channels':int(audio.get('channels',0)) if audio else 0,
            'container':data['format'].get('format_name',''),'bytes':Path(path).stat().st_size}
    except FileNotFoundError:
        raise HTTPException(422,'Video preparation requires FFPROBE_BINARY and FFMPEG_BINARY on the server.') from None
    except (ValueError,KeyError,StopIteration,ZeroDivisionError,subprocess.SubprocessError):
        raise HTTPException(422,'The video could not be inspected. Choose a readable MP4, MOV or WebM.') from None


def reel_ready(info):
    return (info['width']>0 and info['height']>0 and abs(info['width']/info['height']-9/16)<.001
        and info['width']<=1920 and 3<=info['duration']<=900 and 23<=info['fps']<=60
        and info['bytes']<=1024**3 and 'mp4' in info['container']
        and info['codec'] in ('h264','hevc') and info['pixel_format']=='yuv420p'
        and info['sar'] in ('1:1','0:1','N/A') and not info['rotation']
        and (info['audio_codec'] is None or (info['audio_codec']=='aac'
             and info['audio_rate']==48000 and info['audio_channels']<=2)))


def video_export(source, destination, mode='fit', background='blur'):
    info=probe(source)
    if not 3<=info['duration']<=900:
        raise HTTPException(422,'Reels must be 3 seconds to 15 minutes. Trim the source first; InkSuite will not trim it automatically.')
    if reel_ready(info): return None
    if mode=='original':
        raise HTTPException(422,'This original is not Reel-ready. Choose Fit or Fill to prepare it.')
    # Normalize non-square pixels before scaling. FFmpeg autorotates by default.
    base='scale=trunc(iw*sar/2)*2:ih,setsar=1,fps=30'
    contain='scale=1080:1920:force_original_aspect_ratio=decrease:force_divisible_by=2'
    cover='scale=1080:1920:force_original_aspect_ratio=increase:force_divisible_by=2,crop=1080:1920'
    if mode=='fill': filters=f'[0:v:0]{base},{cover},setsar=1,format=yuv420p[v]'
    elif background=='solid': filters=f'[0:v:0]{base},{contain},pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=white,setsar=1,format=yuv420p[v]'
    else: filters=f'[0:v:0]{base},split=2[bg][fg];[bg]{cover},gblur=sigma=30[blur];[fg]{contain}[fit];[blur][fit]overlay=(W-w)/2:(H-h)/2:shortest=1,setsar=1,format=yuv420p[v]'
    command=[binary('ffmpeg'),'-nostdin','-hide_banner','-loglevel','error','-y',
        '-protocol_whitelist','file,pipe','-format_whitelist','mov,matroska,webm','-i',str(source),'-filter_complex_threads','1','-filter_complex',filters,
        '-map','[v]','-map','0:a:0?','-c:v','libx264','-preset','fast','-crf','21',
        '-maxrate','8M','-bufsize','16M','-threads','2','-c:a','aac','-ar','48000','-ac','2','-b:a','128k',
        '-map_metadata','-1','-movflags','+faststart',str(destination)]
    try:
        subprocess.run(command,capture_output=True,timeout=240,check=True)
    except FileNotFoundError:
        raise HTTPException(422,'Configure FFMPEG_BINARY on the server to prepare Reels.') from None
    except subprocess.TimeoutExpired:
        raise HTTPException(422,'Video preparation exceeded four minutes. Try a shorter or smaller source video.') from None
    except subprocess.CalledProcessError:
        raise HTTPException(422,'Video conversion failed. Try another source video.') from None
    output=probe(destination)
    if not reel_ready(output) or abs(output['duration']-info['duration'])>1:
        raise HTTPException(422,'The prepared video did not pass Reel validation. The original was preserved.')
    return output
