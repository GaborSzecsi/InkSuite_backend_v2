"""Direct private ARC uploads bypass frontend request-body limits.
Signed S3 metadata binds staging objects to the authorized user and edition.
"""
import time
from uuid import UUID, uuid4
from fastapi import HTTPException
from .core import transaction, required, organization_permission
from . import arc_service as arc

MAX_UPLOAD = arc.arc_epub.MAX_EXPANDED
LIFETIME = 900


def authorize(edition_id,user):
    with transaction() as cur:
        arc.check_ready(cur)
        e=required(arc.repo.edition(cur,edition_id))
        organization_permission(cur,user,e['organization_id'],admin=True)
        if e['organization_status']!='active':
            raise HTTPException(403,'Publisher is not active.')
        return e


def prefix(e,user):
    return f"tenants/{e['slug']}/data/uploads/{e['uid']}/arc/staging/{user['id']}/{e['id']}/"


def initiate(edition_id,user,filename,size):
    e=authorize(edition_id,user)
    if not filename.lower().endswith('.epub'):
        raise HTTPException(422,'Choose an EPUB file.')
    if size<1 or size>MAX_UPLOAD:
        raise HTTPException(413,'Choose an EPUB of 160 MB or smaller.')
    key=prefix(e,user)+str(uuid4())+'.epub'
    fields={'Content-Type':'application/epub+zip','x-amz-server-side-encryption':'AES256',
            'x-amz-meta-upload-user':str(user['id']),'x-amz-meta-edition-id':str(edition_id),
            'x-amz-meta-upload-expires':str(int(time.time())+LIFETIME),'x-amz-meta-upload-size':str(size)}
    client,bucket=arc.storage()
    signed=client.generate_presigned_post(Bucket=bucket,Key=key,Fields=fields,
      Conditions=[{name:value} for name,value in fields.items()]+[['content-length-range',size,size]],ExpiresIn=LIFETIME)
    return {'key':key,'upload':signed,'expires_in':LIFETIME}


def complete(edition_id,user,body):
    e=authorize(edition_id,user)
    root=prefix(e,user)
    key=body.key
    suffix=key.removeprefix(root)
    try:
        valid=key.startswith(root) and suffix.endswith('.epub') and str(UUID(suffix[:-5]))==suffix[:-5]
    except ValueError:
        valid=False
    if not valid:
        raise HTTPException(403,'This upload does not belong to this edition and user.')
    client,bucket=arc.storage()
    try:
        try:
            response=client.get_object(Bucket=bucket,Key=key)
        except client.exceptions.NoSuchKey:
            raise HTTPException(404,'The uploaded EPUB could not be found. Upload it again.') from None
        stream=response['Body']
        try:
            meta=response.get('Metadata',{})
            try:
                expires=int(meta.get('upload-expires','0'));size=int(meta.get('upload-size','0'))
            except ValueError:
                raise HTTPException(403,'Invalid upload metadata.') from None
            if meta.get('upload-user')!=str(user['id']) or meta.get('edition-id')!=str(edition_id):
                raise HTTPException(403,'This uploaded file belongs to another identity.')
            if expires<int(time.time()):
                raise HTTPException(410,'The upload expired. Choose the file and upload it again.')
            if size<1 or size>MAX_UPLOAD or response.get('ContentLength')!=size:
                raise HTTPException(413,'The uploaded EPUB size does not match the authorized upload.')
            data=stream.read(MAX_UPLOAD+1)
            if len(data)!=size:
                raise HTTPException(422,'The EPUB transfer was incomplete. Please upload it again.')
        finally:
            stream.close()
        # Reuse validation, authorization, replacement rules, transactions and private
        # final-object persistence. No staged file is ever exposed to readers.
        return arc.upload(edition_id,user,data,body.filename,'application/epub+zip',body.replace,
                          body.available_from,body.available_until,body.request_enabled)
    finally:
        arc._delete_uploaded_object(client,bucket,key)
