import io
import unittest
import zipfile
from contextlib import contextmanager
from unittest.mock import patch
from xml.etree import ElementTree as ET
from app.onix import importer as imp, xml_serializer as serializer


def feed(reference='same', isbn='9781958325025'):
    return f'''<ONIXMessage release="3.1"><Product><RecordReference>{reference}</RecordReference><ProductIdentifier><ProductIDType>15</ProductIDType><IDValue>{isbn}</IDValue></ProductIdentifier><DescriptiveDetail><ProductForm>EA</ProductForm><PrimaryContentType>10</PrimaryContentType><ProductContentType>11</ProductContentType><EpubTechnicalProtection>01</EpubTechnicalProtection><Contributor><SequenceNumber>1</SequenceNumber><ContributorRole>A01</ContributorRole><ContributorRole>B06</ContributorRole><NamesBeforeKey>Jane</NamesBeforeKey><KeyNames>Example</KeyNames><NameIdentifier><NameIDType>16</NameIDType><IDValue>ISNI123</IDValue></NameIdentifier><AlternativeName><NameType>01</NameType><PersonName>J. Example</PersonName></AlternativeName><FromLanguage><LanguageCode>fra</LanguageCode></FromLanguage><ToLanguage><LanguageCode>eng</LanguageCode></ToLanguage><ContributorDate><ContributorDateRole>50</ContributorDateRole><Date>19800102</Date></ContributorDate><ProfessionalAffiliation><ProfessionalPosition>Professor</ProfessionalPosition><Affiliation>University</Affiliation></ProfessionalAffiliation><BiographicalNote textformat="05"><p>A <em>writer</em>.</p></BiographicalNote></Contributor><EpubUsageConstraint><EpubUsageType>02</EpubUsageType><EpubUsageStatus>02</EpubUsageStatus><EpubUsageLimit><Quantity>10</Quantity><EpubUsageUnit>03</EpubUsageUnit></EpubUsageLimit></EpubUsageConstraint></DescriptiveDetail><ProductSupply><SupplyDetail><Supplier><SupplierName>Supplier A</SupplierName></Supplier><Price><PriceType>02</PriceType><PriceQualifier>05</PriceQualifier><DiscountCoded><DiscountCodeType>01</DiscountCodeType><DiscountCode>AA</DiscountCode></DiscountCoded><PriceAmount>12</PriceAmount><Tax><TaxRateCode>S</TaxRateCode><TaxRatePercent>20</TaxRatePercent><TaxableAmount>10</TaxableAmount><TaxAmount>2</TaxAmount></Tax><CurrencyCode>GBP</CurrencyCode></Price></SupplyDetail></ProductSupply><ProductSupply><SupplyDetail><Supplier><SupplierName>Supplier B</SupplierName></Supplier></SupplyDetail></ProductSupply></Product></ONIXMessage>'''.encode()


class OnixMigrationTests(unittest.TestCase):
    def product(self):
        return imp._products(feed())[1][0]

    def test_all_suppliers_and_price_metadata(self):
        p=imp._parse_product(self.product())
        self.assertEqual([r['supplier_name'] for r in p['supply']['supply_details']],['Supplier A','Supplier B'])
        price=p['supply']['supply_details'][0]['prices'][0]
        self.assertEqual((price['price_qualifier'],price['discount_code_type'],price['discount_code']),('05','01','AA'))
        self.assertEqual(price['tax_amount'],'2')

    def test_contributor_roles_and_profile(self):
        rows=imp._parse_product(self.product())['contributors']
        self.assertEqual([r['contributor_role_code'] for r in rows],['A01','B06'])
        r=rows[0]
        self.assertEqual(r['name'],'Jane Example')
        self.assertEqual(r['name_identifiers'][0]['id_value'],'ISNI123')
        self.assertEqual(r['contributor_dates'][0]['date_value'],'1980-01-02')
        self.assertEqual(r['from_languages'],['fra'])
        self.assertEqual(r['professional_affiliations'][0]['affiliation'],'University')
        self.assertIn('<em>',r['biographical_note'])

    def test_ebook_content_and_constraints(self):
        p=imp._parse_product(self.product())
        self.assertEqual(p['product_details']['product_content_types'],['10','11'])
        self.assertEqual(p['product_details']['technical_protection'],'01')
        self.assertEqual(p['rights']['usage_constraints'][0]['quantity'],'10')

    def test_shared_reference_different_formats_are_distinct(self):
        first=self.product()
        second=imp._products(feed(isbn='9781958325032'))[1][0]
        root=ET.Element('ONIXMessage',release='3.1');root.extend([first,second])
        products=imp._products(ET.tostring(root))[1]
        self.assertNotEqual(imp._source_key(products[0]),imp._source_key(products[1]))
        self.assertIs(imp._product_by_key(products,'same','9781958325032'),products[1])
        with self.assertRaisesRegex(ValueError,'ambiguous'):
            imp._product_by_key(products,'same','')

    def test_duplicate_same_product_is_rejected(self):
        root=ET.Element('ONIXMessage',release='3.1');root.extend([self.product(),self.product()])
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            imp._products(ET.tostring(root))

    def test_archive_reads_all_xml_files(self):
        data=io.BytesIO()
        with zipfile.ZipFile(data,'w') as z:
            z.writestr('one.xml',feed());z.writestr('two.xml',feed(isbn='9781958325032'))
        self.assertEqual(len(imp._products(imp._read_upload(data.getvalue(),'feed.zip'))[1]),2)

    def test_unsupported_format_and_entity_are_explicit(self):
        for source in [b'<ONIXMessage release="2.1"/>',b'<ONIXmessage release="3.0"><product/></ONIXmessage>',b'<!DOCTYPE ONIXMessage><ONIXMessage/>']:
            with self.assertRaises(ValueError):imp._products(source)

    def test_unknown_fields_reported_before_import(self):
        p=self.product();ET.SubElement(p,'UnmappedPublisherValue').text='Keep me'
        warnings=imp._parse_product(p)['import_warnings']
        self.assertIn('Not mapped into editable metadata: /Product/UnmappedPublisherValue',warnings)

    def test_xhtml_headings_preserved_and_active_markup_rejected(self):
        root=ET.Element('CollateralDetail')
        serializer._write_text_content(root,{'text_type':'03','text_format':'05','text_value':'<h2>Heading</h2><p><span>Body</span></p>'})
        self.assertEqual(root.findtext('.//h2'),'Heading')
        with self.assertRaises(ValueError):
            serializer._write_text_content(ET.Element('CollateralDetail'),{'text_format':'05','text_value':'<script>bad()</script>'})

    def test_corporate_contributor_exports_as_corporate(self):
        root=ET.Element('DescriptiveDetail')
        serializer._write_contributor(root,{'role':'A01','party':{'party_type':'organization','display_name':'Publisher Team','corporate_name':'Publisher Team'}})
        self.assertEqual(root.findtext('.//CorporateName'),'Publisher Team')
        self.assertIsNone(root.find('.//PersonName'))

    def test_series_name_is_not_a_book_identity(self):
        p=self.product()
        dd=imp._child(p,'DescriptiveDetail')
        td=ET.SubElement(dd,'TitleDetail');ET.SubElement(td,'TitleType').text='01'
        te=ET.SubElement(td,'TitleElement');ET.SubElement(te,'TitleElementLevel').text='01';ET.SubElement(te,'TitleText').text='Series Volume Two'
        self.assertIsNone(imp._best_work_match(p,[{'work_id':'one','title':'Series','subtitle':'','author_names':[]}]))
        self.assertIsNone(imp._best_work_match(p,[{'work_id':'one','title':'Series Volume Two','subtitle':'','author_names':['Different Author']}]))

    def test_profile_repeats_survive_later_formats(self):
        one=imp._parse_product(self.product());two=imp._parse_product(self.product())
        one['contributors'][0]['websites']=[{'website_role':'06','website_link':'https://author.example'}]
        two['contributors'][0]['websites']=[{'website_role':'02','website_link':'https://book.example'}]
        imp._batch_profiles([one,two])
        self.assertEqual({r['website_link'] for r in two['contributors'][0]['websites']},{'https://author.example','https://book.example'})

    def test_edition_type_survives_parser_and_export(self):
        p=self.product();ET.SubElement(imp._child(p,'DescriptiveDetail'),'EditionType').text='REV'
        parsed=imp._parse_product(p)
        self.assertEqual(parsed['product_details']['edition_types'],['REV'])
        xml=serializer.product_to_xml_string({'record_reference':'r','descriptive_detail':{'product_form':'EA','edition_types':[{'edition_type_code':'REV'}]}},pretty=False)
        self.assertIn('<EditionType>REV</EditionType>',xml)

    def test_mixed_biography_ampersands_escape_once(self):
        root=ET.Element('DescriptiveDetail')
        serializer._write_contributor(root,{'role':'A01','party':{'display_name':'Writer','short_bio':'A & B wrote <i>Dragons & Dreams</i> for AT&amp;T.'}})
        xml=ET.tostring(root,encoding='unicode')
        self.assertIn('A &amp; B',xml)
        self.assertIn('Dragons &amp; Dreams',xml)
        self.assertIn('AT&amp;T.',xml)
        self.assertNotIn('&amp;amp;',xml)

    def test_safe_links_and_plain_comparisons_in_xhtml(self):
        node=ET.Element('Text')
        serializer._append_onix_xhtml(node,'<p>1 < 2. <a href="https://example.com?a=1&amp;b=2">Read</a> <a href="javascript:alert(1)">Unsafe</a></p>')
        self.assertIn('1 < 2.',node.find('p').text)
        self.assertEqual(node.findall('.//a')[0].get('href'),'https://example.com?a=1&b=2')
        self.assertIsNone(node.findall('.//a')[1].get('href'))

    def test_import_opts_into_transaction_before_writing(self):
        class Connection:
            autocommit=True
            def cursor(inner,**kw):
                self.assertFalse(inner.autocommit)
                raise RuntimeError('stop before writes')
        @contextmanager
        def connection():yield Connection()
        with patch.object(imp,'db_conn',connection):
            with self.assertRaisesRegex(RuntimeError,'stop before writes'):
                imp.apply_onix_import_batch(raw=feed(),filename='feed.xml',tenant_slug='demo',plans=[])

if __name__=='__main__':unittest.main()
