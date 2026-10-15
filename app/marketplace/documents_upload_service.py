"""Signed, private staging uploads for public PDFs; binary bytes bypass Amplify."""
import time
from uuid import UUID, uuid4
from fastapi import HTTPException
from . import documents_service as documents

LIFETIME=900


def authorize(edition_id,org_id,user):
    with documents.transaction() as cur:
        return documents.upload_context(cur,edition_id,org_id,user)


def prefix(context,user):
    # Keep unvalidated uploads outside public assets and every /public/ folder.
    return f"tenants/{context['slug']}/data/uploads/document-staging/{user['id']}/{context['kind']}/{context['target']}/"


def initiate(edition_id,org_id,user,filename,size):
    context=authorize(edition_id,org_id,user)
    if not filename.lower().endswith('.pdf'):
        raise HTTPException(422,'Choose a PDF file.')
    if size<1 or size>documents.MAX_PDF:
        raise HTTPException(413,'Each PDF must be 50 MB or smaller.')
    key=prefix(context,user)+str(uuid4())+'.pdf'
    fields={'Content-Type':'application/pdf','x-amz-server-side-encryption':'AES256',
            'x-amz-meta-upload-user':str(user['id']),'x-amz-meta-upload-target':context['target'],
            'x-amz-meta-upload-kind':context['kind'],'x-amz-meta-upload-size':str(size),
            'x-amz-meta-upload-expires':str(int(time.time())+LIFETIME)}
    client,bucket=documents.storage()
    signed=client.generate_presigned_post(Bucket=bucket,Key=key,Fields=fields,
      Conditions=[{name:value} for name,value in fields.items()]+[['content-length-range',size,size]],ExpiresIn=LIFETIME)
    return {'key':key,'upload':signed,'expires_in':LIFETIME}


def complete(edition_id,org_id,user,body):
    context=authorize(edition_id,org_id,user)
    key=body.key;root=prefix(context,user);suffix=key.removeprefix(root)
    try:
        valid=key.startswith(root) and suffix.endswith('.pdf') and str(UUID(suffix[:-4]))==suffix[:-4]
    except ValueError:
        valid=False
    if not valid:
        raise HTTPException(403,'This upload belongs to another publisher, title or user.')
    client,bucket=documents.storage()
    try:
        try:
            response=client.get_object(Bucket=bucket,Key=key)
        except client.exceptions.NoSuchKey:
            raise HTTPException(404,'The uploaded PDF could not be found. Upload it again.') from None
        stream=response['Body']
        try:
            metadata=response.get('Metadata',{})
            try:
                expires=int(metadata.get('upload-expires','0'));size=int(metadata.get('upload-size','0'))
            except ValueError:
                raise HTTPException(403,'Invalid upload metadata.') from None
            if metadata.get('upload-user')!=str(user['id']) or metadata.get('upload-target')!=context['target'] or metadata.get('upload-kind')!=context['kind']:
                raise HTTPException(403,'This uploaded PDF belongs to another identity or destination.')
            if expires<int(time.time()):
                raise HTTPException(410,'The upload expired. Choose the PDF and upload it again.')
            if size<1 or size>documents.MAX_PDF or response.get('ContentLength')!=size:
                raise HTTPException(413,'The uploaded PDF size does not match the authorized upload.')
            data=stream.read(documents.MAX_PDF+1)
            if len(data)!=size:
                raise HTTPException(422,'The PDF transfer was incomplete. Please upload it again.')
        finally:
            stream.close()
        # Publish only after validating the actual bytes and rechecking permissions.
        return documents.upload(edition_id,org_id,user,body.filename,data)
    finally:
        try:
            client.delete_object(Bucket=bucket,Key=key)
        except Exception:
            pass
