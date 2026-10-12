"""Distributor-specific wire format, transcribed from the existing Exporteo template.
Hachette uses the verified Exporteo order format; other adapters implement the common contract.
"""
from dataclasses import dataclass
from typing import Protocol

class ConfigurationRequired(Exception):
    pass

@dataclass(frozen=True)
class Capabilities:
    inventory: bool=False
    submit_order: bool=False
    order_status: bool=False
    tracking: bool=False
    cancellation: bool=False
    order_preview: bool=True

class DistributorAdapter(Protocol):
    capabilities: Capabilities
    def test_connection(self): ...
    def fetch_inventory(self): ...
    def submit_order(self,order,items): ...
    def get_order_status(self,distributor_order_id): ...
    def get_tracking(self,distributor_order_id): ...


def field(value):
    text=str(value or '')
    if any(c in text for c in ('\t','\r','\n','\x00')):
        raise ConfigurationRequired('Order fields cannot contain tabs or line breaks in this distributor format.')
    return text


class HachetteExporteoAdapter:
    capabilities=Capabilities()
    idempotent_submission=False
    simulated=False
    def __init__(self,configuration=None,credentials=None):
        self.configuration=configuration or {}
        self.credentials=credentials or {}
        self.capabilities=Capabilities(submit_order=bool(self.configuration.get('orders',{}).get('host')))
    def render_order(self,order,items):
        mappings=self.configuration.get('shipping_methods',{})
        method=mappings.get(order['shipping_method'])
        if not method: raise ConfigurationRequired('Map this shipping method to a verified distributor service code.')
        recipient=order['recipient']
        if not recipient.get('address_1'): raise ConfigurationRequired('A delivery address is required.')
        if recipient.get('company') or recipient.get('phone'):
            raise ConfigurationRequired('Company/phone field placement needs distributor confirmation; this export template has no mapping.')
        ref=order['reference']
        header=['HDR',ref,ref,recipient['name'],recipient['address_1'],recipient.get('address_2',''),'','',recipient['city'],recipient.get('state_region',''),recipient['postal_code'],recipient['country'],'',recipient.get('email',''),method,'','','','','',order.get('delivery_instructions','')]
        records=[header]+[['DTL',ref,index,item['isbn'],item['quantity']] for index,item in enumerate(items,1)]
        return ('\r\n'.join('\t'.join(field(v) for v in row) for row in records)+'\r\n').encode('utf-8')
    def test_connection(self):
        from .transports import sftp
        config=self.configuration.get('orders',{})
        if config.get('transport')!='sftp' or not self.credentials:
            raise ConfigurationRequired('Credentials and verified SFTP configuration are required.')
        with sftp(config,self.credentials) as client: client.stat(config['directory'])
    def fetch_inventory(self): raise ConfigurationRequired('Hachette inventory feed specification is required.')
    def submit_order(self,order,items):
        from .transports import sftp,sftp_upload
        from .protocol import Receipt
        from uuid import UUID
        config=self.configuration.get('orders',{})
        if config.get('transport')!='sftp' or not self.credentials:
            raise ConfigurationRequired('Configure SFTP and credentials before forwarding orders.')
        key=str(UUID(str(order['id'])))
        content=self.render_order({**order,'reference':key},items)
        with sftp(config,self.credentials) as client:
            sftp_upload(client,config['directory'],'inksuite-'+key+'.txt',content)
        return Receipt(order_key=key,distributor_order_id=key,status='SUBMITTED')
    def get_order_status(self,distributor_order_id): raise ConfigurationRequired('Hachette acknowledgement specification is required.')
    def get_tracking(self,distributor_order_id): raise ConfigurationRequired('Hachette tracking specification is required.')

ADAPTERS={'hachette_exporteo':HachetteExporteoAdapter}
def adapter_for(connection, cur=None, credentials=None):
    from .protocol import StandardJSONAdapter, SimulatedAdapter
    if connection['adapter'] in ('standard_json','simulator'):
        cls = StandardJSONAdapter if connection['adapter']=='standard_json' else SimulatedAdapter
        return cls(connection['configuration'], cur=cur, connection=connection, credentials=credentials)
    cls=ADAPTERS.get(connection['adapter'])
    if not cls: raise ConfigurationRequired('Distributor adapter is not installed.')
    return cls(connection['configuration'],credentials=credentials)
