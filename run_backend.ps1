# Run the correct FastAPI app (main.py at project root).
# Do NOT use: uvicorn models.royalty:app (that app has no /api/tenants routes).
# From: C:\Users\szecs\Documents\InkSuite_backend_v2
Set-Location $PSScriptRoot
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
