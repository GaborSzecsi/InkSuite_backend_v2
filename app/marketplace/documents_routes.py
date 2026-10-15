from uuid import UUID
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import RedirectResponse
from starlette.concurrency import run_in_threadpool
from .core import current_user
from . import documents_service as service

router=APIRouter()

async def upload_pdf(file,edition_id,org_id,user):
    data=await file.read(service.MAX_PDF+1)
    if len(data)>service.MAX_PDF:
        raise HTTPException(413,'Each PDF must be 50 MB or smaller.')
    return await run_in_threadpool(service.upload,edition_id,org_id,user,file.filename or '',data)

@router.get('/editions/{edition_id}/marketing-materials')
def edition_materials(edition_id: UUID,user=Depends(current_user)):
    return service.edition_materials(edition_id,user)

@router.post('/editions/{edition_id}/marketing-materials')
async def upload_marketing(edition_id: UUID,file: UploadFile=File(...),user=Depends(current_user)):
    return await upload_pdf(file,edition_id,None,user)

@router.get('/organizations/{org_id}/catalogs')
def catalogs(org_id: UUID,user=Depends(current_user)):
    return service.catalogs(org_id,user)

@router.post('/organizations/{org_id}/catalogs')
async def upload_catalog(org_id: UUID,file: UploadFile=File(...),user=Depends(current_user)):
    return await upload_pdf(file,None,org_id,user)

@router.get('/books/{slug}/marketing-materials')
def book_materials(slug: str):
    return service.book_materials(slug)

@router.get('/documents/{asset_id}/download')
def download(asset_id: UUID):
    return RedirectResponse(service.download(asset_id),status_code=302,headers={'Cache-Control':'private, no-store','Referrer-Policy':'no-referrer'})
