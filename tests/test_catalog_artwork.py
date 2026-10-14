import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from app.catalog_artwork import artwork_resolver


class ArtworkTests(unittest.TestCase):
    def setUp(self):
        self.cur = MagicMock()
        self.cur.fetchone.return_value = {'slug': 'demo'}
        self.client = MagicMock()
        self.client.generate_presigned_url.return_value = 'https://signed.example/cover.jpg?signature=test'
        self.module = patch.dict(sys.modules, {'routers.uploads': SimpleNamespace(_s3_client=lambda: self.client)})
        self.module.start()
        self.env = patch.dict('os.environ', {'UPLOADS_S3_BUCKET':'inksuite-data', 'AWS_REGION':'us-east-2'})
        self.env.start()
        self.resolve = artwork_resolver(self.cur, 'tenant-demo')
        self.key = 'tenants/demo/data/uploads/book/public/cover.jpg'
    def tearDown(self):
        self.module.stop()
        self.env.stop()
    def test_key_and_stable_url_sign_the_same_object(self):
        for value in (self.key, 'https://inksuite-data.s3.us-east-2.amazonaws.com/'+self.key):
            self.assertIn('signature=test', self.resolve(value))
            self.assertEqual(self.client.generate_presigned_url.call_args.kwargs['Params']['Key'], self.key)
        self.cur.execute.assert_called_once_with('SELECT slug FROM tenants WHERE id=%s', ('tenant-demo',))
    def test_private_cross_tenant_and_traversal_are_never_signed(self):
        for key in (self.key.replace('/demo/', '/other/'), self.key.replace('/public/', '/contracts/'), self.key.replace('/book/', '/../'), self.key.replace('cover.jpg','contract.pdf')):
            self.assertEqual(self.resolve(key), '')
        self.client.generate_presigned_url.assert_not_called()
    def test_external_image_does_not_query_or_sign(self):
        self.assertEqual(self.resolve('https://images.example/cover.jpg'), 'https://images.example/cover.jpg')
        self.cur.execute.assert_not_called()
        self.client.generate_presigned_url.assert_not_called()
    def test_same_image_is_signed_once_per_response(self):
        self.resolve(self.key)
        self.resolve(self.key)
        self.client.generate_presigned_url.assert_called_once()


class CatalogIsbnTests(unittest.TestCase):
    def test_saved_invalid_isbn_is_distinguished_from_missing(self):
        from app.distribution.publishing import entries
        for isbn, expected in [('9784567891111','Invalid ISBN'), ('','Missing ISBN')]:
            cur=MagicMock()
            cur.fetchall.side_effect=[[
                dict(id='edition',work_id='work',title='Demo',isbn13=isbn,product_form='Hardcover',description='Demo',cover_image_link='https://example.com/cover.jpg')],
                [dict(edition_id='edition',price_amount=10)], []]
            result=entries(cur,'tenant','USD')[0]
            self.assertEqual(result['isbn13'],isbn)
            self.assertTrue(any(issue.startswith(expected) for issue in result['issues']))
    def test_uploaded_cover_is_resolved_in_shopify_catalog(self):
        from app.distribution.publishing import entries
        cur=MagicMock()
        cur.fetchall.side_effect=[[
            dict(id='edition',work_id='work',title='Demo',isbn13='9780306406157',product_form='Hardcover',description='Demo',cover_image_link='tenants/demo/data/uploads/book/public/cover.jpg')],
            [dict(edition_id='edition',price_amount=10)], []]
        with patch('app.distribution.publishing.artwork_resolver',return_value=lambda value:'https://signed.example/cover.jpg?signature=test' if value else ''):
            result=entries(cur,'tenant','USD')[0]
        self.assertEqual(result['issues'],[])
        self.assertIn('signature=test',result['images'][0])
