"""Distribution business rules and credentials-safe transport checks. No network calls."""
import base64,hashlib,hmac,unittest
from unittest.mock import patch
from starlette.datastructures import QueryParams
from fastapi import HTTPException
from app.distribution.adapters import HachetteExporteoAdapter,ConfigurationRequired
from app.distribution.shopify import verify_webhook,verify_callback,shop_domain
from app.distribution.transports import approved_address,remote_path,TransportError

class DistributionTests(unittest.TestCase):
 def setUp(self):
  self.order={'reference':'#1001','recipient':{'name':'Test Reader','address_1':'1 Test Road','address_2':'','city':'Boston','state_region':'MA','postal_code':'02110','country':'US','email':'reader@example.test'},'shipping_method':'Ground','delivery_instructions':'Gift'}
  self.adapter=HachetteExporteoAdapter({'shipping_methods':{'Ground':'PG'}})
 def test_observed_wire_format(self):
  data=self.adapter.render_order(self.order,[{'isbn':'9780000000002','quantity':2}])
  rows=data.decode().split('\r\n')
  header=rows[0].split('\t')
  self.assertEqual(len(header),21);self.assertEqual(header[14],'PG');self.assertEqual(header[20],'Gift')
  self.assertEqual(rows[1],'DTL\t#1001\t1\t9780000000002\t2')
 def test_unknown_shipping_blocks(self):
  self.order['shipping_method']='Unknown'
  with self.assertRaises(ConfigurationRequired): self.adapter.render_order(self.order,[])
 def test_delimiter_injection_blocks(self):
  self.order['recipient']['name']='Reader\tOTHER'
  with self.assertRaises(ConfigurationRequired): self.adapter.render_order(self.order,[])
 def test_unsupported_company_not_silently_lost(self):
  self.order['recipient']['company']='Test School'
  with self.assertRaises(ConfigurationRequired): self.adapter.render_order(self.order,[])
 def test_live_send_disabled(self):
  self.assertFalse(self.adapter.capabilities.submit_order)
  with self.assertRaises(ConfigurationRequired): self.adapter.submit_order(self.order,[])
 def test_signed_webhook(self):
  raw=b'{"id":123}';key='test-only'
  signature=base64.b64encode(hmac.new(key.encode(),raw,hashlib.sha256).digest()).decode()
  self.assertTrue(verify_webhook(raw,signature,key));self.assertFalse(verify_webhook(raw+b' ',signature,key));self.assertFalse(verify_webhook(raw,'',key))
 def test_oauth_hmac(self):
  query='code=123&shop=example.myshopify.com&state=nonce&timestamp=100'
  signature=hmac.new(b'test',query.encode(),hashlib.sha256).hexdigest()
  self.assertTrue(verify_callback(QueryParams(query+'&hmac='+signature),'test'))
  self.assertFalse(verify_callback(QueryParams(query+'&state=other&hmac='+signature),'test'))
 def test_shop_domain_no_ssrf(self):
  self.assertEqual(shop_domain('EXAMPLE.myshopify.com'),'example.myshopify.com')
  for value in ['example.myshopify.com.evil.test','127.0.0.1','example.myshopify.com@evil.test','https://example.myshopify.com']:
   with self.assertRaises(HTTPException):shop_domain(value)
 def test_remote_path(self):
  self.assertEqual(remote_path('/queue','order.tsv'),'/queue/order.tsv')
  with self.assertRaises(TransportError):remote_path('/queue','../order.tsv')
 def test_host_allowlist(self):
  with patch.dict('os.environ',{'DISTRIBUTION_ALLOWED_HOSTS':'approved.test'}):
   with self.assertRaises(TransportError):approved_address('evil.test',443)
   with patch('socket.getaddrinfo',return_value=[(2,1,6,'',('127.0.0.1',443))]):
    with self.assertRaises(TransportError):approved_address('approved.test',443)
if __name__=='__main__':unittest.main()
