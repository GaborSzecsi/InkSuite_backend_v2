import os, json, pathlib
from typing import Any, Dict
from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter()
DATA_DIR = os.environ.get("DATA_DIR", "./data")
BANK_FILE = os.path.join(DATA_DIR, "banking.json")
pathlib.Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
if not os.path.exists(BANK_FILE):
    with open(BANK_FILE, "w", encoding="utf-8") as f:
        json.dump({}, f)

class USBankInfo(BaseModel):
    routing: str
    accountNumber: str | None = None
    accountType: str | None = "checking"
    bankName: str | None = None
    bankAddress: str | None = None
    validated: bool | None = None

class ForeignBankInfo(BaseModel):
    bankName: str | None = None
    bankAddress: str | None = None
    country: str | None = None
    swiftBic: str | None = None
    iban: str | None = None
    accountNumber: str | None = None
    currency: str | None = None

class BankingParty(BaseModel):
    isForeign: bool = False
    us: USBankInfo | None = None
    foreign: ForeignBankInfo | None = None

class BankingEnvelope(BaseModel):
    author: BankingParty = Field(default_factory=BankingParty)
    illustrator: BankingParty = Field(default_factory=BankingParty)

def _load() -> Dict[str, Any]:
    with open(BANK_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def _save(payload: Dict[str, Any]) -> None:
    tmp = BANK_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, BANK_FILE)

@router.get("/{book_key}", response_model=BankingEnvelope)
def get_banking(book_key: str):
    db = _load()
    return db.get(book_key, BankingEnvelope().model_dump())

@router.put("/{book_key}", response_model=BankingEnvelope)
def put_banking(book_key: str, payload: BankingEnvelope):
    db = _load()
    db[book_key] = payload.model_dump()
    _save(db)
    return payload
