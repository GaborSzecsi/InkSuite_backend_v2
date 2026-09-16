import io, mimetypes, os, uuid, json, subprocess, tempfile
from urllib.parse import quote
from fastapi import HTTPException
from PIL import Image, ImageOps
from routers.storage_s3 import s3
from .domain import public_key, tenant_root, upload_key
from . import service as s

def bucket(): return (os.getenv('S3_BUCKET') or os.getenv('TENANT_BUCKET') or 'inksuite-data').strip()
def public_url(key):
    region=os.getenv('AWS_REGION') or 'us-east-2'
    return f'https://{bucket()}.s3.{region}.amazonaws.com/{quote(key,safe="/")}'

def inspect_media(data,filename):
    if len(data)>500*1024*1024: raise HTTPException(422,'Media exceeds the 500 MB upload limit.')
    guessed=mimetypes.guess_type(filename)[0] or ''
    if guessed.startswith('video/'):
        path=None
        try:
            with tempfile.NamedTemporaryFile(suffix='.media',delete=False) as tmp: tmp.write(data);path=tmp.name
            from .media_prepare import binary
            result=subprocess.run([binary('ffprobe'),'-protocol_whitelist','file,pipe','-format_whitelist','mov,matroska,webm','-v','error','-show_streams','-show_format','-of','json',path],capture_output=True,text=True,timeout=40,check=True)
            info=json.loads(result.stdout);stream=next(x for x in info['streams'] if x.get('codec_type')=='video')
            duration=float(info['format'].get('duration') or stream.get('duration') or 0)
            if duration<=0: raise ValueError('Missing duration')
            formats=info['format'].get('format_name','')
            mime='video/mp4' if 'mp4' in formats or 'mov' in formats else 'video/webm' if 'webm' in formats else None
            if not mime: raise ValueError('Unsupported container')
            return mime,'video',stream['width'],stream['height'],duration
        except FileNotFoundError: raise HTTPException(422,'Configure FFPROBE_BINARY before adding video media.') from None
        except (ValueError,KeyError,StopIteration,subprocess.SubprocessError): raise HTTPException(422,'Use a valid MP4 or WebM video with a readable duration.') from None
        finally:
            if path: os.unlink(path)
    if len(data)>25*1024*1024: raise HTTPException(422,'Image exceeds 25 MB.')
    try:
        with Image.open(io.BytesIO(data)) as image: image.verify()
        with Image.open(io.BytesIO(data)) as image:
            mime=Image.MIME.get(image.format);width,height=image.size
        if mime not in ('image/jpeg','image/png','image/webp'): raise ValueError()
        return mime,'image',width,height,None
    except Exception: raise HTTPException(422,'Use a valid JPEG, PNG, WebP, MP4 or WebM file.') from None

def discover(cur,ctx,work_id):
    work=s.owned(cur,'works',work_id,ctx['tenant']['id'])
    # Match uploads._resolve_work: works.uid is the existing upload folder identity.
    folder=str(work.get('uid') or work['id'])
    uuid.UUID(folder)
    prefix=f"{tenant_root(ctx['tenant']['slug'])}data/uploads/{folder}/public/"
    found=[]
    for page in s3().get_paginator('list_objects_v2').paginate(Bucket=bucket(),Prefix=prefix):
        for item in page.get('Contents',[]):
            key=item['Key']
            if key.endswith('/'): continue
            public_key(ctx['tenant']['slug'],key,title_uid=folder)
            mime=mimetypes.guess_type(key)[0] or 'application/octet-stream'
            found.append({'s3_key':key,'s3_bucket':bucket(),'public_url':public_url(key),'filename':key.rsplit('/',1)[-1],
                          'work_id':str(work['id']),'source_type':'title_public_asset','mime_type':mime,
                          'media_type':mime.split('/')[0], 'filesize_bytes':item['Size']})
    return found

def reference(cur,ctx,body):
    assets=discover(cur,ctx,body.work_id)
    item=next((a for a in assets if a['s3_key']==body.s3_key),None)
    if not item: raise HTTPException(404,'Selected public title asset was not found.')
    existing=s.one(cur,'SELECT * FROM social_media_assets WHERE tenant_id=%s AND s3_bucket=%s AND s3_key=%s',(ctx['tenant']['id'],bucket(),body.s3_key))
    if existing: return existing
    width=height=duration=None
    if item['media_type'] in ('image','video'):
        limit=(25 if item['media_type']=='image' else 500)*1024*1024
        if item['filesize_bytes']>limit: raise HTTPException(422,'This media exceeds the supported size limit.')
        stream=s3().get_object(Bucket=bucket(),Key=body.s3_key)['Body']
        try:
            item['mime_type'],item['media_type'],width,height,duration=inspect_media(stream.read(limit+1),item['filename'])
        finally: stream.close()
    # Reference only: no S3 copy, upload, ACL or object mutation.
    return s.one(cur,'''INSERT INTO social_media_assets(id,tenant_id,work_id,source_type,s3_bucket,s3_key,public_url,filename,mime_type,media_type,filesize_bytes,width,height,duration_seconds,created_by)
      VALUES(%s,%s,%s,'title_public_asset',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
      (s.uid(),ctx['tenant']['id'],body.work_id,bucket(),body.s3_key,item['public_url'],item['filename'],item['mime_type'],item['media_type'],item['filesize_bytes'],width,height,duration,ctx['user']['id']))

def upload(cur,ctx,filename,data,campaign_id,category):
    if campaign_id: s.owned(cur,'marketing_campaigns',campaign_id,ctx['tenant']['id'])
    if not data: raise HTTPException(422,'Choose a media file.')
    mime,media_type,width,height,duration=inspect_media(data,filename)
    asset_id=s.uid(); key=upload_key(ctx['tenant']['slug'],str(asset_id),filename,str(campaign_id) if campaign_id else None,category)
    from .storage import require_space
    require_space(cur,ctx,len(data))
    s3().put_object(Bucket=bucket(),Key=key,Body=data,ContentType=mime)
    try:
        return s.one(cur,'''INSERT INTO social_media_assets(id,tenant_id,campaign_id,source_type,s3_bucket,s3_key,public_url,filename,mime_type,media_type,width,height,duration_seconds,filesize_bytes,created_by)
          VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
          (asset_id,ctx['tenant']['id'],campaign_id,'campaign_asset' if campaign_id else 'publisher_asset',bucket(),key,public_url(key),filename,mime,media_type,width,height,duration,len(data),ctx['user']['id']))
    except Exception:
        s3().delete_object(Bucket=bucket(),Key=key)
        raise

def save_derivative(cur,ctx,asset_id,source,body,data,width,height,duration=None):
    from pathlib import Path
    video=duration is not None
    mime,media_type,extension=('video/mp4','video','mp4') if video else ('image/jpeg','image','jpg')
    identity=s.uid();filename=f'{identity}_{width}x{height}.{extension}'
    folder=f'campaigns/{body.campaign_id}/generated/' if body.campaign_id else 'library/general/generated/'
    key=public_key(ctx['tenant']['slug'],tenant_root(ctx['tenant']['slug'])+'assets/marketing/public/'+folder+filename)
    size=Path(data).stat().st_size if video else len(data)
    from .storage import require_space
    require_space(cur,ctx,size)
    if video:
        with open(data,'rb') as stream: s3().put_object(Bucket=bucket(),Key=key,Body=stream,ContentType=mime)
    else: s3().put_object(Bucket=bucket(),Key=key,Body=data,ContentType=mime)
    name=f"{source.get('display_name') or source.get('filename') or 'Media'} - {width}x{height} {body.mode}"
    try:
        return s.one(cur,'''INSERT INTO social_media_assets(id,tenant_id,campaign_id,work_id,source_type,source_asset_id,s3_bucket,s3_key,public_url,filename,mime_type,media_type,width,height,filesize_bytes,created_by,duration_seconds,display_name)
          VALUES(%s,%s,%s,%s,'generated_derivative',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
          (identity,ctx['tenant']['id'],body.campaign_id,source['work_id'],asset_id,bucket(),key,public_url(key),filename,mime,media_type,width,height,size,ctx['user']['id'],duration,name))
    except Exception:
        s3().delete_object(Bucket=bucket(),Key=key)
        raise


def derivative(cur,ctx,asset_id,body):
    from .media_prepare import image_export, video_export
    source=s.owned(cur,'social_media_assets',asset_id,ctx['tenant']['id'])
    if source['media_type'] not in ('image','video'): raise HTTPException(422,'Select an image or video source.')
    if body.campaign_id: s.owned(cur,'marketing_campaigns',body.campaign_id,ctx['tenant']['id'])
    if source['campaign_id'] and source['campaign_id']!=body.campaign_id: raise HTTPException(422,'Use the source campaign for this derivative.')
    if source['source_type']=='title_public_asset':
        work=s.owned(cur,'works',source['work_id'],ctx['tenant']['id'])
        public_key(ctx['tenant']['slug'],source['s3_key'],title_uid=str(work.get('uid') or work['id']))
    else: public_key(ctx['tenant']['slug'],source['s3_key'])
    if source['media_type']=='image' and body.mode=='original':
        if (source['mime_type']=='image/jpeg' and source.get('width') and source.get('height')
            and .8<=source['width']/source['height']<=1.91 and (source.get('filesize_bytes') or 0)<=8*1024*1024): return source
        raise HTTPException(422,'This original is not Instagram-ready. Choose Fit or Fill to create a JPEG.')
    limit=(500 if source['media_type']=='video' else 25)*1024*1024
    stream=s3().get_object(Bucket=source['s3_bucket'],Key=source['s3_key'])['Body']
    if source['media_type']=='video':
        with tempfile.TemporaryDirectory(prefix='inksuite-reel-') as folder:
            from pathlib import Path
            original=Path(folder)/'source.media';output=Path(folder)/'reel.mp4'
            try:
                with original.open('wb') as f:
                    total=0
                    while True:
                        chunk=stream.read(1024*1024)
                        if not chunk: break
                        total+=len(chunk)
                        if total>limit: raise HTTPException(422,'Source video exceeds 500 MB.')
                        f.write(chunk)
            finally: stream.close()
            info=video_export(original,output,body.mode,body.background)
            if info is None: return source
            return save_derivative(cur,ctx,asset_id,source,body,output,1080,1920,info['duration'])
    try: data=stream.read(limit+1)
    finally: stream.close()
    if len(data)>limit: raise HTTPException(422,'Source image exceeds 25 MB.')
    try: data=image_export(data,body.width,body.height,body.mode,body.background)
    except (OSError,ValueError): raise HTTPException(422,'Could not prepare this image. Choose a readable image source.') from None
    return save_derivative(cur,ctx,asset_id,source,body,data,body.width,body.height)
