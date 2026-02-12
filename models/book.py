from pydantic import BaseModel, Field, Extra, validator
from typing import Optional, Dict, Any, List

US_STATE_MAP = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada",
    "NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota",
    "OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
    "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
    "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia",
}

class Address(BaseModel):
    street: str = ""
    city: str = ""
    state: str = ""   # full state name for US
    zip: str = ""
    country: str = ""

    @validator("state", pre=True)
    def expand_state(cls, v):
        s = (v or "").strip()
        if len(s) == 2 and s.upper() in US_STATE_MAP:
            return US_STATE_MAP[s.upper()]
        return s

class AgencyInfo(BaseModel):
    name: Optional[str] = None
    agency: Optional[str] = None
    email: Optional[str] = None
    address: Optional[Address] = Address()

class IllustratorInfo(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    address: Optional[Address] = Address()
    agent: Optional[AgencyInfo] = None

class Book(BaseModel):
    # ... your existing fields ...
    author_address: Optional[Address] = Address()
    author_agent: Optional[AgencyInfo] = None
    illustrator: Optional[IllustratorInfo] = None

    class Config:
        extra = Extra.allow
        allow_population_by_field_name = True
