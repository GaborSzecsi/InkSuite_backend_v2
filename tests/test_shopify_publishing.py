import unittest
from unittest.mock import patch
from app.distribution import publishing as p

class CatalogPublishingTests(unittest.TestCase):
 def edition(self,isbn='9780306406157',format='Hardcover'):
  return dict(id='edition',title='One Book',isbn=isbn,format=format,price='19.99',description='A book.',images=['https://images.example.com/cover.jpg'],issues=[])
 def variant(self,isbn='9780306406157',pid='p1',title='One Book'):
  return dict(id='v1',title='Hardcover',barcode=isbn,sku=isbn,product={'id':pid,'title':title})
 def test_recognizes_existing_isbn_not_title_only(self):
  e=self.edition();matched,missing,pid=p.plan([e],[self.variant(title='Changed title')])
  self.assertEqual(len(matched),1);self.assertEqual(missing,[]);self.assertEqual(pid,'p1')
 def test_title_groups_missing_paperback(self):
  e=self.edition('9780140328721','Paperback')
  matched,missing,pid=p.plan([e],[self.variant()]);self.assertFalse(matched);self.assertEqual(missing,[e]);self.assertEqual(pid,'p1')
 def test_duplicate_and_conflicting_isbns_block(self):
  for variants in ([self.variant(),self.variant(pid='p2')],[dict(self.variant(),sku='9780140328721')]):
   with self.assertRaises(ValueError):p.plan([self.edition()],variants)
 def test_multiple_title_products_block(self):
  with self.assertRaises(ValueError):p.plan([self.edition('9780140328721')],[self.variant(pid='p1'),self.variant(pid='p2')])
 def test_new_product_is_draft_selected_formats_only(self):
  e=self.edition();v=self.variant()
  with patch.object(p,'live_variants',return_value=[]),patch.object(p.sh,'graphql',return_value={'productSet':{'product':{'variants':{'nodes':[v]}},'userErrors':[]}}) as call:
   self.assertEqual(len(p.create_or_add(None,{},[e])),1)
   data=call.call_args.args[3]['input']
   self.assertEqual(data['status'],'DRAFT');self.assertEqual(len(data['variants']),1)
   self.assertEqual(data['variants'][0]['barcode'],e['isbn']);self.assertEqual(data['files'][0]['originalSource'],e['images'][0])
 def test_existing_match_makes_no_mutation(self):
  with patch.object(p,'live_variants',return_value=[self.variant()]),patch.object(p.sh,'graphql') as call:
   p.create_or_add(None,{},[self.edition()]);call.assert_not_called()
 def test_add_variant_preserves_existing_content(self):
  e=self.edition('9780140328721','Paperback');v=self.variant(e['isbn'])
  responses=[{'product':{'status':'ACTIVE','options':[{'name':'Format','optionValues':[{'name':'Hardcover'}]}]}},{'productVariantsBulkCreate':{'productVariants':[v],'userErrors':[]}}]
  with patch.object(p,'live_variants',return_value=[self.variant()]),patch.object(p.sh,'graphql',side_effect=responses) as call:
   p.create_or_add(None,{},[e]);payload=call.call_args.args[3]
   self.assertEqual(set(payload),{'id','variants'});self.assertEqual(payload['variants'][0]['optionValues'][0]['name'],'Paperback')
 def test_private_or_signed_images_not_sent(self):
  for url in ('http://example.com/a.jpg','https://127.0.0.1/a','https://example.com/a?token=secret','https://user:pass@example.com/a'):
   self.assertIsNone(p.image_url(url))
  self.assertEqual(p.image_url('https://example.com/cover.jpg'),'https://example.com/cover.jpg')
 def test_missing_metadata_blocks_creation(self):
  with patch.object(p,'live_variants',return_value=[]),patch.object(p.sh,'graphql') as call:
   with self.assertRaises(ValueError):p.create_or_add(None,{},[dict(self.edition(),issues=['No price'])])
   call.assert_not_called()

if __name__=='__main__':unittest.main()
