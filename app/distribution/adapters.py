"""Distributor-specific wire format, transcribed from the existing Exporteo template.
No guessed inventory or tracking interface. Hachette activation awaits its specification.
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
    def __init__(self,configuration=None): self.configuration=configuration or {}
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
    def test_connection(self): raise ConfigurationRequired('Credentials and verified SFTP host key are required before testing.')
    def fetch_inventory(self): raise ConfigurationRequired('Hachette inventory feed specification is required.')
    def submit_order(self,order,items): raise ConfigurationRequired('Live submission is disabled while Exporteo remains active.')
    def get_order_status(self,distributor_order_id): raise ConfigurationRequired('Hachette acknowledgement specification is required.')
    def get_tracking(self,distributor_order_id): raise ConfigurationRequired('Hachette tracking specification is required.')

ADAPTERS={'hachette_exporteo':HachetteExporteoAdapter}
def adapter_for(connection):
    cls=ADAPTERS.get(connection['adapter'])
    if not cls: raise ConfigurationRequired('Distributor adapter is not installed.')
    return cls(connection['configuration'])
