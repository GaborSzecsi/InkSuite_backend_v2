"""Public document uploads; existing publishing assets, permissions and storage."""
import re
from pathlib import PurePosixPath
from urllib.parse import quote
from uuid import uuid4
from fastapi import HTTPException
from . import documents_repository as repo
from .core import transaction, required, one, organization_permission
from .media_service import storage

MAX_PDF = 50 * 1024 * 1024


def validate_pdf(filename, data):
    name = PurePosixPath(filename.replace('\\','/')).name
    if not name.lower().endswith('.pdf') or not data.startswith(b'%PDF-') or b'%%EOF' not in data[-65536:]:
        raise HTTPException(422,'Choose a PDF file.')
    if len(data)>MAX_PDF:
        raise HTTPException(413,'Each PDF must be 50 MB or smaller.')
    return name[:-4][:196]+".pdf"


def view(asset):
    return {'id':asset['id'],'name':asset['asset_name'],'size':asset['file_size_bytes'],
            'url':f"/api/marketplace/documents/{asset['id']}/download"}


def edition_materials(edition_id,user):
    with transaction() as cur:
        e=required(repo.edition(cur,edition_id))
        organization_permission(cur,user,e['organization_id'],admin=True)
        return {'items':[view(a) for a in repo.assets(cur,e['tenant_id'],e['work_id'])]}


def catalogs(org_id,user):
    with transaction() as cur:
        organization_permission(cur,user,org_id,admin=True)
        o=required(repo.organization(cur,org_id))
        return {'items':[view(a) for a in repo.assets(cur,o['tenant_id'])]}


def upload_context(cur,edition_id,org_id,user):
    if edition_id is not None:
        e=required(repo.edition(cur,edition_id))
        organization_permission(cur,user,e['organization_id'],admin=True)
        tenant_id=e['tenant_id']; work_id=e['work_id']; slug=e['slug']
        kind='marketing_material'
        folder=f"tenants/{slug}/data/uploads/{e['uid'] or e['work_id']}/public/marketing/"
    else:
        organization_permission(cur,user,org_id,admin=True)
        o=required(repo.organization(cur,org_id))
        nullable=one(cur,"""SELECT is_nullable FROM information_schema.columns WHERE table_schema='public'
          AND table_name='publishing_assets' AND column_name='work_id'""")
        if not nullable or nullable['is_nullable']!='YES':
            raise HTTPException(503,'Catalog uploads are awaiting migration 036_marketplace_public_documents.sql.')
        tenant_id=o['tenant_id'];work_id=None;slug=o['tenant_slug'];kind='catalog'
        folder=f'tenants/{slug}/assets/marketing/public/catalogs/'
    return {'tenant_id':tenant_id,'work_id':work_id,'slug':slug,'kind':kind,'folder':folder,
            'target':str(edition_id if edition_id is not None else org_id)}


def upload(edition_id,org_id,user,filename,data):
    client=bucket=key=None
    try:
        with transaction() as cur:
            context=upload_context(cur,edition_id,org_id,user)
            tenant_id=context['tenant_id'];work_id=context['work_id'];kind=context['kind'];folder=context['folder']
            name=validate_pdf(filename,data)
            identity=uuid4();key=folder+str(identity)+'.pdf'
            client,bucket=storage()
            client.put_object(Bucket=bucket,Key=key,Body=data,ContentType='application/pdf',
              ContentDisposition="attachment; filename*=UTF-8''"+quote(name,safe=''),ServerSideEncryption='AES256')
            # Keep stable public-folder metadata; downloads use server-generated links
            # so they also work when the bucket itself disallows anonymous reads.
            region=client.meta.region_name or 'us-east-2'
            url=f'https://{bucket}.s3.{region}.amazonaws.com/{quote(key,safe="/")}'
            asset=repo.save(cur,identity,tenant_id,work_id,kind,name,key,url,len(data))
        return view(asset)
    except Exception:
        if client and key:
            # Delete only this request's newly-created UUID object after DB failure.
            try:
                client.delete_object(Bucket=bucket,Key=key)
            except Exception:
                pass
        raise


def book_materials(slug):
    with transaction() as cur:
        b=required(repo.public_book(cur,slug))
        return {'items':[view(a) for a in repo.assets(cur,b['tenant_id'],b['work_id'])]}


def download(asset_id):
    with transaction() as cur:
        a=required(repo.public_asset(cur,asset_id))
        key=a['storage_key'];slug=a['slug']
        if not key.startswith(f'tenants/{slug}/') or any(p in ('.','..','') for p in key.split('/')):
            raise HTTPException(404,'File unavailable.')
        if a['asset_type']=='marketing_material':
            # Resolve the title folder from canonical SQL, not caller-supplied keys.
            w=required(one(cur,'SELECT COALESCE(uid,id) AS uid FROM works WHERE id=%s AND tenant_id=%s',(a['work_id'],a['tenant_id'])))
            prefix=f"tenants/{slug}/data/uploads/{w['uid']}/public/"
        else:
            prefix=f'tenants/{slug}/assets/marketing/public/'
        if not key.startswith(prefix):
            raise HTTPException(404,'File unavailable.')
        client,bucket=storage()
        name=re.sub(r'[\r\n]','',a['asset_name'])
        return client.generate_presigned_url('get_object',Params={'Bucket':bucket,'Key':key,
          'ResponseContentType':'application/pdf','ResponseContentDisposition':"attachment; filename*=UTF-8''"+quote(name,safe='')},ExpiresIn=900)
