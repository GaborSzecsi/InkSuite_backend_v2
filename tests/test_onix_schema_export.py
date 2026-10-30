import copy
import json
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET
from app.onix import xml_serializer as serializer, schema_validation

class SchemaExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture=json.loads((Path(__file__).parent/'fixtures/onix_schema/rich-products.json').read_text(encoding='utf8'))

    def test_rich_records_pass_both_schemas_without_mutating_input(self):
        for release in ('3.0','3.1'):
            for item in self.fixture['products']:
                with self.subTest(release=release,isbn=item['isbn']):
                    before=copy.deepcopy(item['product'])
                    xml=serializer.message_to_xml({'release':release,'header':self.fixture['header'],'products':[item['product']]},False)
                    result=schema_validation.validate_xml(xml,release)
                    self.assertTrue(result['valid'],result['errors'])
                    self.assertEqual(before,item['product'])
                    root=ET.fromstring(xml)
                    self.assertEqual(root.tag,'{http://ns.editeur.org/onix/'+release+'/reference}ONIXMessage')

    def test_resource_caption_and_credit_are_preserved_as_standard_features(self):
        root=ET.Element('CollateralDetail')
        serializer._write_supporting_resource(root,{'resource_content_type':'01','content_audience':'00','resource_mode':'03','caption':'Cover caption','credit':'Artist credit','versions':[{'resource_form':'02','resource_link':'https://example.org/cover.jpg'}]})
        self.assertEqual(root.findtext('.//ResourceFeature[ResourceFeatureType="02"]/FeatureNote'),'Cover caption')
        self.assertEqual(root.findtext('.//ResourceFeature[ResourceFeatureType="01"]/FeatureNote'),'Artist credit')
        self.assertIsNone(root.find('.//Caption'))

    def test_placeholder_price_is_not_exported_as_free(self):
        item=copy.deepcopy(self.fixture['products'][0]['product'])
        item['product_supply'][0]['prices'][0]['price_amount']=0
        xml=serializer.message_to_xml({'release':'3.1','header':self.fixture['header'],'products':[item]},False)
        root=ET.fromstring(xml)
        self.assertFalse(any(float(n.text)==0 for n in root.findall('.//{*}PriceAmount')))

    def test_pending_price_is_explicit_when_no_positive_price_exists(self):
        root=ET.Element('Product')
        serializer._write_product_supply(root,{'product_supply':[{'supplier_name':'Publisher','supplier_role':'01','product_availability':'99','prices':[{'price_type_code':'02','price_amount':0,'currency_code':'USD'}]}]})
        self.assertEqual(root.findtext('.//UnpricedItemType'),'02')
        self.assertIsNone(root.find('.//PriceAmount'))

    def test_market_date_role_without_a_date_does_not_create_an_empty_composite(self):
        product=copy.deepcopy(self.fixture['products'][0]['product'])
        product['publishing_detail']['market_date']={'market_date_role':'01','market_date_text':''}
        xml=serializer.message_to_xml({'release':'3.1','header':self.fixture['header'],'products':[product]},False)
        self.assertFalse(ET.fromstring(xml).findall('.//{*}MarketDate'))
        self.assertTrue(schema_validation.validate_xml(xml,'3.1')['valid'])

    def test_schema_rejects_wrong_namespace_and_missing_required_fields(self):
        invalid='<ONIXMessage xmlns="http://ns.editeur.org/onix/3.0/reference" release="3.1"/>'
        self.assertFalse(schema_validation.validate_xml(invalid,'3.1')['valid'])

    def test_schema_gate_precedes_export_job_and_external_upload(self):
        from unittest.mock import MagicMock, patch
        from uuid import uuid4
        from fastapi import HTTPException
        from app.onix import router
        from app.onix.models import ExportRequest
        edition=str(uuid4());cursor=MagicMock();cursor.fetchall.return_value=[{'id':edition}];cursor.fetchone.return_value={'isbn13':'9781958325148'}
        connection=MagicMock();connection.cursor.return_value.__enter__.return_value=cursor
        db=MagicMock();db.return_value.__enter__.return_value=connection
        with patch.object(router,'db_conn',db), patch.object(router,'_tenant_id_from_request',return_value=str(uuid4())), patch.object(router,'_user_id_from_request',return_value=None), patch.object(router,'_require_complete_onix_header'), patch.object(router.assembly,'build_onix_message_payload',return_value={'release':'3.0','header':self.fixture['header'],'products':[]}), patch.object(router.aws_helpers,'upload_xml_to_s3') as upload:
            with self.assertRaises(HTTPException) as error:
                router.create_export(MagicMock(),ExportRequest(edition_ids=[edition],export_mode='download'))
            self.assertEqual(error.exception.status_code,422)
            upload.assert_not_called()
            self.assertFalse(any('INSERT INTO onix_export_jobs' in str(call.args[0]) for call in cursor.execute.call_args_list))

    def test_external_entity_is_not_loaded(self):
        xml='<!DOCTYPE ONIXMessage [<!ENTITY x SYSTEM "file:///nonexistent-secret">]><ONIXMessage release="3.0">&x;</ONIXMessage>'
        self.assertFalse(schema_validation.validate_xml(xml,'3.0')['valid'])

if __name__=='__main__':unittest.main()
