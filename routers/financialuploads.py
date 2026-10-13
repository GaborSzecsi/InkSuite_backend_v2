"""Retired local CSV endpoints. Financial records are maintained in SQL."""
from fastapi import APIRouter, HTTPException

router = APIRouter()

@router.post("/financials/upload")
async def upload_financial_file():
    # No multipart parsing, file writes, or ingestion subprocesses.
    raise HTTPException(410, "Manual CSV uploads have been retired. Use the configured distributor integration.")

@router.get("/financials/uploads")
async def list_financial_uploads():
    raise HTTPException(410, "The legacy CSV upload history has been retired. Financial records are stored in the database.")
