"""Bounded, sequential Pillow compositor + FFmpeg decoding/encoding. Invoked by worker only."""
import json, math, os, subprocess, time
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageEnhance, ImageFilter, ImageOps
from .schema import Document
from .composition import evaluate,dimensions
from app.marketing.media_prepare import binary

FONT=Path(__file__).parent/'fonts'/'NunitoSans.ttf'
FLAGS={'creationflags':subprocess.CREATE_NO_WINDOW} if os.name=='nt' else {}
class Cancelled(Exception): pass

def probe(path):
    result=subprocess.run([binary('ffprobe'),'-v','error','-protocol_whitelist','file,pipe','-format_whitelist','mov,matroska,webm,mp3,wav,aac,ogg,flac','-show_streams','-show_format','-of','json',str(path)],capture_output=True,timeout=40,check=True,**FLAGS)
    return json.loads(result.stdout)

def text_image(c,w,h,fraction=1):
    font=ImageFont.truetype(str(FONT),max(8,round(c.font_size*h)))
    axes=font.get_variation_axes();font.set_variation_by_axes([c.font_weight if axis['name']==b'Weight' else 112 if axis['name']==b'Width' and c.font=='Nunito Sans Wide' else axis['default'] for axis in axes])
    lines=[]
    for paragraph in c.text[:math.ceil(len(c.text)*fraction)].split('\n'):
        line=''
        for word in paragraph.split(' '):
            candidate=(line+' '+word).strip()
            if font.getlength(candidate)>w*.85 and line: lines.append(line);line=word
            else: line=candidate
        lines.append(line)
    padding=max(4,round(h*.012));line_h=round(c.font_size*h*1.3);tw=min(w*2,max([font.getlength(line) for line in lines]+[1])+padding*2+c.outline*2)
    image=Image.new('RGBA',(max(1,math.ceil(tw)),max(1,line_h*len(lines)+padding*2)))
    draw=ImageDraw.Draw(image)
    if c.background_opacity: draw.rectangle((0,0,image.width,image.height),fill=c.text_background+f'{round(c.background_opacity*255):02x}')
    for i,line in enumerate(lines):
        width=font.getlength(line);x=padding if c.align=='left' else image.width-padding-width if c.align=='right' else (image.width-width)/2
        y=padding+i*line_h
        if c.shadow: draw.text((x+2,y+2),line,font=font,fill='#000000a0',anchor='lt',stroke_width=c.outline,stroke_fill='black')
        draw.text((x,y),line,font=font,fill=c.color,anchor='lt',stroke_width=c.outline,stroke_fill='black')
    return image

class Reader:
    def __init__(self,path,c,info,w,h,folder):
        video=next(s for s in info['streams'] if s['codec_type']=='video');iw,ih=video['width'],video['height']
        rotation=abs(float(next((s['rotation'] for s in video.get('side_data_list',[]) if 'rotation' in s),video.get('tags',{}).get('rotate',0))))%180
        if rotation==90: iw,ih=ih,iw
        size=min(1,max(w,h)/max(iw,ih));self.w=max(2,round(iw*size/2)*2);self.h=max(2,round(ih*size/2)*2);self.last=None
        self.errors=open(Path(folder)/(c.id+'.decode.log'),'wb')
        self.proc=subprocess.Popen([binary('ffmpeg'),'-nostdin','-v','error','-threads','1','-protocol_whitelist','file,pipe','-format_whitelist','mov,matroska,webm,mp3,wav,aac,ogg,flac',*(['-stream_loop','-1'] if c.loop else []),'-ss',str(c.trim),'-i',str(path),'-an','-vf',f'setpts=(PTS-STARTPTS)/{c.speed},fps=30,scale={self.w}:{self.h},setsar=1','-threads','1','-f','rawvideo','-pix_fmt','rgb24','pipe:1'],stdout=subprocess.PIPE,stderr=self.errors,**FLAGS)
    def frame(self):
        size=self.w*self.h*3;data=bytearray()
        while len(data)<size:
            chunk=self.proc.stdout.read(size-len(data))
            if not chunk: break
            data.extend(chunk)
        if len(data)==size: self.last=Image.frombytes('RGB',(self.w,self.h),bytes(data))
        elif self.last is None: raise ValueError('A video source could not be decoded.')
        return self.last.copy().convert('RGBA')
    def close(self):
        if self.proc.poll() is None: self.proc.kill()
        self.proc.wait();self.proc.stdout.close();self.errors.close()

def frame(document,time,w,h,source):
    canvas=Image.new('RGBA',(w,h),document.canvas.background)
    if document.canvas.background_asset_id:
        bg=ImageOps.fit(source(str(document.canvas.background_asset_id),None),(w,h))
        if document.canvas.background_blur: bg=bg.filter(ImageFilter.GaussianBlur(25*w/1080))
        canvas.alpha_composite(bg.convert('RGBA'))
    tracks={t.id:t for t in document.tracks}
    for track in document.tracks:
        if track.hidden or track.kind=='audio': continue
        for c in document.clips:
            if c.track_id!=track.id or c.kind=='audio' or not c.start<=time<c.start+c.duration: continue
            state=evaluate(c,time)
            image=text_image(c,w,h,state['text_fraction']) if c.kind=='text' else source(str(c.asset_id),c)
            if c.kind!='text':
                crop=c.crop;image=image.crop((round(crop.x*image.width),round(crop.y*image.height),max(1,round((crop.x+crop.width)*image.width)),max(1,round((crop.y+crop.height)*image.height))))
                image=ImageEnhance.Brightness(image).enhance(c.brightness);image=ImageEnhance.Contrast(image).enhance(c.contrast);image=ImageEnhance.Color(image).enhance(c.saturation)
                image=ImageOps.fit(image,(w,h)) if c.fit=='cover' else ImageOps.contain(image,(w,h))
            scale=state['scale'];image=image.resize((max(1,round(image.width*scale)),max(1,round(image.height*scale))),Image.Resampling.BICUBIC)
            if state['wipe']<1: image.putalpha(image.getchannel('A').point(lambda a:a));mask=Image.new('L',image.size);ImageDraw.Draw(mask).rectangle((0,0,image.width*state['wipe'],image.height),fill=255);from PIL import ImageChops;image.putalpha(ImageChops.multiply(image.getchannel('A'),mask))
            image.putalpha(image.getchannel('A').point(lambda a:round(a*state['opacity'])))
            image=image.rotate(-state['rotation'],resample=Image.Resampling.BICUBIC,expand=True)
            canvas.alpha_composite(image,(round(state['x']*w-image.width/2),round(state['y']*h-image.height/2)))
    return canvas.convert('RGB')

def render(document:Document,paths:dict,output,resolution=1080,progress=lambda p:None):
    """Source paths are trusted local downloads, never project-provided URLs or paths."""
    w,h=dimensions(document.canvas.ratio,resolution);duration=max((c.start+c.duration for c in document.clips),default=0)
    if not duration: raise ValueError('Add a clip before exporting.')
    output=Path(output);folder=output.parent;silent=folder/'silent.mp4';readers={};images={};infos={};started=time.monotonic()
    for asset,path in paths.items():
        try:
            with Image.open(path) as im: images[asset]=ImageOps.exif_transpose(im).convert('RGBA')
        except Exception: infos[asset]=probe(path)
    def source(asset,c):
        if asset in images: return images[asset].copy()
        if c is None: raise ValueError('Canvas background must be a photograph.')
        if c.id not in readers: readers[c.id]=Reader(paths[asset],c,infos[asset],w,h,folder)
        return readers[c.id].frame()
    with open(folder/'encode.log','wb') as errors:
        process=subprocess.Popen([binary('ffmpeg'),'-nostdin','-v','error','-y','-f','rawvideo','-pix_fmt','rgb24','-s',f'{w}x{h}','-r','30','-i','pipe:0','-an','-c:v','libx264','-preset','fast','-crf','20','-threads','2','-pix_fmt','yuv420p','-movflags','+faststart',str(silent)],stdin=subprocess.PIPE,stderr=errors,**FLAGS)
        try:
            frames=math.ceil(duration*30)
            for n in range(frames):
                if n%30==0:
                    progress(min(90,int(n/frames*90)))
                    if time.monotonic()-started>1800: raise ValueError('Export exceeded the 30-minute render limit.')
                process.stdin.write(frame(document,n/30,w,h,source).tobytes())
                for c in document.clips:
                    if c.id in readers and (n+1)/30>=c.start+c.duration: readers.pop(c.id).close()
            process.stdin.close()
            if process.wait(timeout=120): raise ValueError('Video encoder failed.')
        finally:
            if process.poll() is None: process.kill();process.wait()
            for reader in readers.values(): reader.close()
    progress(92)
    tracks={t.id:t for t in document.tracks};audio=[c for c in document.clips if c.kind in ('audio','video') and not c.muted and not tracks[c.track_id].muted and c.volume and any(s['codec_type']=='audio' for s in infos.get(str(c.asset_id),{}).get('streams',[]))]
    if not audio: silent.replace(output);progress(100);return
    command=[binary('ffmpeg'),'-nostdin','-v','error','-y','-i',str(silent)];filters=[];labels=[]
    for i,c in enumerate(audio,1):
        command+=['-protocol_whitelist','file,pipe','-format_whitelist','mov,matroska,webm,mp3,wav,aac,ogg,flac',*(['-stream_loop','-1'] if c.loop else []),'-ss',str(c.trim),'-t',str(c.duration*c.speed),'-i',str(paths[str(c.asset_id)])]
        tempo=c.speed;factors=[]
        while tempo>2: factors.append(2);tempo/=2
        while tempo<.5: factors.append(.5);tempo/=.5
        factors.append(tempo)
        chain=','.join(f'atempo={f}' for f in factors)
        fade_in=f'min(1,(t+{c.animation_offset})/{c.fade_in})' if c.fade_in else '1'
        fade_out=f'max(0,min(1,({c.animation_length or c.duration}-t-{c.animation_offset})/{c.fade_out}))' if c.fade_out else '1'
        gain=f'{c.volume}*{fade_in}*{fade_out}'
        filters.append(f"[{i}:a:0]asetpts=PTS-STARTPTS,{chain},atrim=duration={c.duration},volume='{gain}':eval=frame,adelay={round(c.start*1000)}:all=1[a{i}]");labels.append(f'[a{i}]')
    filters.append(''.join(labels)+f'amix=inputs={len(audio)}:duration=longest:normalize=0,alimiter=level=false[mix]')
    command+=['-filter_complex_threads','1','-filter_complex',';'.join(filters),'-map','0:v:0','-map','[mix]','-c:v','copy','-c:a','aac','-b:a','192k','-ar','48000','-ac','2','-t',str(frames/30),'-movflags','+faststart',str(output)]
    with open(folder/'audio.log','wb') as errors:
        proc=subprocess.Popen(command,stdout=subprocess.DEVNULL,stderr=errors,**FLAGS)
        try:
            while proc.poll() is None:
                progress(95)
                try: proc.wait(timeout=1)
                except subprocess.TimeoutExpired: pass
                if time.monotonic()-started>1800: raise ValueError('Export timed out.')
            if proc.returncode: raise ValueError('Audio mixing failed. Check the imported audio files.')
        finally:
            if proc.poll() is None: proc.kill();proc.wait()
    progress(100)


def normalize(source,destination,kind,progress):
    """Editing copy preserving source aspect ratio and orientation, without altering originals."""
    info=probe(source);seconds=float(info['format'].get('duration') or 0)
    if not 0<seconds<=300: raise ValueError('Editing copies support sources up to five minutes.')
    command=[binary('ffmpeg'),'-nostdin','-v','error','-y','-protocol_whitelist','file,pipe','-format_whitelist','mov,matroska,webm,mp3,wav,aac,ogg,flac','-i',str(source)]
    if kind=='video': command+=['-map','0:v:0','-map','0:a:0?','-vf',"scale=w='min(1920,iw)':h='min(1920,ih)':force_original_aspect_ratio=decrease:force_divisible_by=2,setsar=1,fps=30",'-c:v','libx264','-crf','20','-preset','fast','-pix_fmt','yuv420p']
    else: command+=['-vn']
    command+=['-threads','2','-c:a','aac','-b:a','192k','-ar','48000','-ac','2','-map_metadata','-1','-movflags','+faststart',str(destination)]
    with open(Path(destination).with_suffix('.log'),'wb') as errors:
        process=subprocess.Popen(command,stdout=subprocess.DEVNULL,stderr=errors,**FLAGS);started=time.monotonic()
        try:
            while process.poll() is None:
                progress(min(95,int((time.monotonic()-started)/max(seconds,1)*30)))
                try: process.wait(timeout=1)
                except subprocess.TimeoutExpired: pass
                if time.monotonic()-started>1800: raise ValueError('Editing copy timed out.')
            if process.returncode: raise ValueError('This source could not be normalized. Use another media file.')
        finally:
            if process.poll() is None: process.kill();process.wait()
    progress(99)
