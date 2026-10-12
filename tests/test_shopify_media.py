import unittest
from unittest.mock import patch
from app.distribution import media as m

class MediaTests(unittest.TestCase):
 def test_existing_cover_only_adds_interior(self):
  with patch.object(m,'fingerprint',return_value=None):
   self.assertEqual(m.missing_images(['https://example.com/cover.jpg','https://example.com/interior.jpg'],[{'status':'READY','image':{'url':'https://cdn.shopify.com/cover.jpg?v=3'}}]),['https://example.com/interior.jpg'])
 def test_identical_artwork_across_format_filenames(self):
  with patch.object(m,'fingerprint',side_effect=['same','same','interior']):
   self.assertEqual(m.missing_images(['https://example.com/hc.jpg','https://example.com/pb.jpg','https://example.com/interior.jpg'],[]),['https://example.com/hc.jpg','https://example.com/interior.jpg'])
 def test_same_artwork_under_new_name_is_not_uploaded(self):
  with patch.object(m,'fingerprint',return_value='identical'):
   self.assertEqual(m.missing_images(['https://example.com/new.jpg'],[{'image':{'url':'https://cdn.shopify.com/old.jpg'}}]),[])
 def test_no_mutation_when_all_images_exist(self):
  with patch.object(m.sh,'graphql',return_value={'product':{'media':{'nodes':[{'mediaContentType':'IMAGE','image':{'url':'https://cdn.shopify.com/cover.jpg'}}],'pageInfo':{'hasNextPage':False}}}}) as call,patch.object(m,'fingerprint',return_value=None):
   self.assertEqual(m.sync_images(None,{},'product',['https://example.com/cover.jpg'],'Book'),0)
   self.assertEqual(call.call_count,1)
 def test_no_untrusted_download(self):
  with patch.object(m.requests,'get') as call:
   for u in ['https://127.0.0.1/a','https://example.com/a','http://cdn.shopify.com/a','https://cdn.shopify.com:8443/a']:
    self.assertIsNone(m.fingerprint(u))
   call.assert_not_called()
 def test_existing_product_update_only_adds_media(self):
  replies=[{'product':{'media':{'nodes':[],'pageInfo':{'hasNextPage':False}}}},{'productUpdate':{'product':{'id':'product'},'userErrors':[]}}]
  with patch.object(m.sh,'graphql',side_effect=replies) as call,patch.object(m,'fingerprint',return_value=None):
   m.sync_images(None,{},'product',['https://example.com/interior.jpg'],'Book')
   self.assertEqual(call.call_args.args[3]['product'],{'id':'product'})
   self.assertEqual(len(call.call_args.args[3]['media']),1)
