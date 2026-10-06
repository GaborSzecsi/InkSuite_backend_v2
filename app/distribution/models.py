from typing import Literal
from uuid import UUID
from pydantic import BaseModel, Field, ConfigDict, model_validator

class StrictModel(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)

class ConnectionIn(StrictModel):
    display_name: str = Field(min_length=1,max_length=120)
    adapter: Literal['hachette_exporteo'] = 'hachette_exporteo'
    safety_buffer: int = Field(default=5,ge=0,le=100000)

class CredentialsIn(StrictModel):
    username: str = Field(min_length=1,max_length=255)
    password: str = Field(min_length=1,max_length=4096)

class ShippingAddress(StrictModel):
    name: str = Field(min_length=1,max_length=200)
    company: str = Field(default='',max_length=200)
    address_1: str = Field(min_length=1,max_length=250)
    address_2: str = Field(default='',max_length=250)
    city: str = Field(min_length=1,max_length=100)
    state_region: str = Field(default='',max_length=100)
    postal_code: str = Field(min_length=1,max_length=30)
    country: str = Field(pattern=r'^[A-Z]{2}$')
    phone: str = Field(default='',max_length=40)
    email: str = Field(default='',max_length=254)

class OrderItem(StrictModel):
    edition_id: UUID
    quantity: int = Field(gt=0,le=100000)

class OrderIn(StrictModel):
    connection_id: UUID
    source: Literal['SHOPIFY','MARKETPLACE']
    source_account: str = Field(min_length=1,max_length=255)
    external_order_id: str = Field(min_length=1,max_length=255)
    reference: str = Field(min_length=1,max_length=100)
    recipient: ShippingAddress | None = None
    shipping_method: str = Field(min_length=1,max_length=100)
    delivery_instructions: str = Field(default='',max_length=1000)
    items: list[OrderItem] = Field(min_length=1,max_length=100)

    @model_validator(mode='after')
    def marketplace_address_required(self):
        if self.source == 'MARKETPLACE' and self.recipient is None:
            raise ValueError('A shipping address is required for Marketplace orders.')
        return self

class ListingIn(StrictModel):
    enabled: bool

class MappingIn(StrictModel):
    edition_id: UUID
    connection_id: UUID

class TransportConfig(StrictModel):
    transport: Literal['https','sftp','ftps','ftp']
    host: str = Field(min_length=1,max_length=253,pattern=r'^[a-zA-Z0-9.-]+$')
    port: int = Field(default=22,ge=1,le=65535)
    directory: str = Field(default='/',max_length=500)
    host_key: str = Field(default='',max_length=2000)

class ConfigurationIn(StrictModel):
    orders: TransportConfig | None = None
    inventory: TransportConfig | None = None
    tracking: TransportConfig | None = None
    shipping_methods: dict[str,str] = Field(default_factory=dict,max_length=100)
    safety_buffer: int = Field(default=5,ge=0,le=100000)
