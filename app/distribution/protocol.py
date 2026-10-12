"""InkSuite distributor contract v1. Buyer delivery details are transmitted, never archived."""
import json
from datetime import datetime, timezone, timedelta
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, AwareDatetime, model_validator
from .adapters import Capabilities, ConfigurationRequired
from .transports import HTTPS

class Record(BaseModel):
    model_config = ConfigDict(extra='forbid')

class Stock(Record):
    isbn: str = Field(pattern=r'^97[89][0-9]{10}$')
    on_hand: int = Field(strict=True, ge=0, le=100000000)

class Snapshot(Record):
    version: Literal[1] = 1
    as_of: AwareDatetime
    complete: bool
    items: list[Stock] = Field(max_length=100000)
    # Shipped order UUIDs already deducted from on_hand (never merely accepted orders).
    reflected_order_keys: list[UUID] = Field(default_factory=list, max_length=100000)

    @model_validator(mode='after')
    def validate_snapshot(self):
        now = datetime.now(timezone.utc)
        if self.as_of > now + timedelta(minutes=5) or self.as_of < now - timedelta(hours=24):
            raise ValueError('Inventory snapshot is stale or in the future.')
        if len({i.isbn for i in self.items}) != len(self.items):
            raise ValueError('Duplicate inventory ISBN.')
        if len(set(self.reflected_order_keys)) != len(self.reflected_order_keys):
            raise ValueError('Duplicate reflected order key.')
        return self

class Receipt(Record):
    order_key: UUID
    distributor_order_id: str = Field(pattern=r'^[A-Za-z0-9_-]{1,100}$')
    status: Literal['SUBMITTED','ACCEPTED','SHIPPED','REJECTED']

class StandardJSONAdapter:
    """HTTPS gateway contract, usable by any distributor implementing v1."""
    capabilities = Capabilities(inventory=True, submit_order=True, order_status=True)
    idempotent_submission = True
    simulated = False

    def __init__(self, configuration=None, credentials=None, **kwargs):
        self.configuration = configuration or {}
        self.credentials = credentials or {}

    def client(self):
        url = self.configuration.get('api_url')
        if not url or not self.credentials:
            raise ConfigurationRequired('Configure the distributor HTTPS endpoint and credentials.')
        return HTTPS(url, self.credentials)

    def test_connection(self):
        data = json.loads(self.client().request('GET','health'))
        if data != {'protocol':'inksuite-distributor','version':1,'idempotent_orders':True}:
            raise ConfigurationRequired('Endpoint must support the InkSuite distributor v1 contract.')

    def fetch_inventory(self):
        return Snapshot.model_validate_json(self.client().request('GET','inventory'))

    def submit_order(self, order, items):
        key = str(order['id'])
        # Stable path and identity across retries. Receiver MUST deduplicate this key.
        payload = {'version':1, 'order_key':key, 'reference':key,
                   'recipient':order['recipient'], 'shipping_method':order['shipping_method'],
                   'items':[{'isbn':i['isbn'],'quantity':i['quantity']} for i in items]}
        return Receipt.model_validate_json(self.client().request('PUT','orders/'+key,json.dumps(payload).encode()))

    def get_order_status(self, order_key):
        key=str(UUID(str(order_key)))
        data=json.loads(self.client().request('GET','orders/'+key))
        if data=={'order_key':key,'status':'NOT_FOUND'}: return None
        return Receipt.model_validate(data)

class SimulatedAdapter:
    """Persistent test distributor stores only ISBNs, quantities and opaque order keys."""
    capabilities = Capabilities(inventory=True, submit_order=True, order_status=True)
    idempotent_submission = True
    simulated = True

    def __init__(self, configuration=None, cur=None, connection=None, **kwargs):
        self.configuration=configuration or {}; self.cur=cur; self.connection=connection

    def test_connection(self):
        for isbn, quantity in self.configuration.get('simulated_stock',{}).items():
            Stock(isbn=isbn,on_hand=quantity)

    def fetch_inventory(self):
        available = dict(self.configuration.get('simulated_stock',{}))
        self.cur.execute("SELECT order_id,items FROM distribution_simulated_orders WHERE tenant_id=%s AND connection_id=%s AND status='SHIPPED'",
                         (self.connection['tenant_id'],self.connection['id']))
        rows=self.cur.fetchall()
        for row in rows:
            for item in row['items']:
                available[item['isbn']] = max(0,available.get(item['isbn'],0)-item['quantity'])
        return Snapshot(as_of=datetime.now(timezone.utc),complete=True,
                        items=[Stock(isbn=k,on_hand=v) for k,v in available.items()],
                        reflected_order_keys=[r['order_id'] for r in rows])

    def submit_order(self, order, items):
        from . import service as s
        from psycopg.types.json import Jsonb
        tenant=self.connection['tenant_id']; connection=self.connection['id']
        existing=s.one(self.cur,'SELECT status FROM distribution_simulated_orders WHERE tenant_id=%s AND connection_id=%s AND order_id=%s',(tenant,connection,order['id']))
        if existing:
            return Receipt(order_key=order['id'],distributor_order_id='sim-'+str(order['id']),status=existing['status'])
        available={i.isbn:i.on_hand for i in self.fetch_inventory().items}
        self.cur.execute("SELECT items FROM distribution_simulated_orders WHERE tenant_id=%s AND connection_id=%s AND status='ACCEPTED'",(tenant,connection))
        for pending in self.cur.fetchall():
            for item in pending['items']:
                available[item['isbn']]=max(0,available.get(item['isbn'],0)-item['quantity'])
        if any(available.get(i['isbn'],0)<i['quantity'] for i in items):
            return Receipt(order_key=order['id'],distributor_order_id='sim-'+str(order['id']),status='REJECTED')
        minimal=[{'isbn':i['isbn'],'quantity':i['quantity']} for i in items]
        self.cur.execute('INSERT INTO distribution_simulated_orders(tenant_id,connection_id,order_id,items) VALUES(%s,%s,%s,%s)',(tenant,connection,order['id'],Jsonb(minimal)))
        return Receipt(order_key=order['id'],distributor_order_id='sim-'+str(order['id']),status='ACCEPTED')

    def get_order_status(self, order_key):
        from . import service as s
        row=s.one(self.cur,'SELECT status FROM distribution_simulated_orders WHERE tenant_id=%s AND connection_id=%s AND order_id=%s',(self.connection['tenant_id'],self.connection['id'],order_key))
        if not row: return None
        return Receipt(order_key=order_key,distributor_order_id='sim-'+str(order_key),status=row['status'])

