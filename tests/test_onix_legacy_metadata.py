import unittest
from unittest.mock import patch
from xml.etree import ElementTree as ET
from app.onix import assembly, xml_serializer, validation


class LegacyMetadataTests(unittest.TestCase):
    def texts(self, rows, legacy):
        def fetch(cur, table, **kwargs):
            self.assertEqual(kwargs['tenant_id'], 'tenant')
            return [dict(r) for r in rows] if table == 'edition_texts' else []
        with patch.object(assembly, '_fetch_rows', side_effect=fetch):
            return assembly._build_texts(None, 'tenant', 'edition', legacy)

    def test_teaser_does_not_hide_full_legacy_description(self):
        rows = self.texts([{'id':'one','text_type':'02','text_value':'Teaser'}], 'Full description')
        self.assertEqual([(r['text_type'], r['text_value']) for r in rows],
                         [('02','Teaser'), ('03','Full description')])

    def test_no_onix_rows_exports_legacy_description(self):
        rows = self.texts([], 'Full description with & punctuation')
        root = ET.Element('CollateralDetail')
        for row in rows:
            xml_serializer._write_text_content(root, row)
        self.assertEqual(root.findtext('./TextContent/Text'), 'Full description with & punctuation')
        self.assertEqual(root.findtext('./TextContent/TextType'), '03')
        self.assertEqual(root.findtext('./TextContent/ContentAudience'), '00')

    def test_nonblank_onix_description_always_wins_even_if_shorter(self):
        source = [{'id':'one','text_type':'03','text_value':'Approved','language_code':'fra','text_format':'05'}]
        rows = self.texts(source, 'Much longer legacy description')
        self.assertEqual(len(rows), 1)
        for k,v in source[0].items(): self.assertEqual(rows[0][k], v)

    def test_blank_full_text_falls_back_without_duplicate_empty_composite(self):
        rows = self.texts([{'text_type':'03','text_value':'  '}], 'Legacy')
        self.assertEqual([r['text_value'] for r in rows], ['Legacy'])

    def test_no_source_no_invented_text(self):
        self.assertEqual(self.texts([], ''), [])

    def test_contributor_role_aliases_preserve_codes_and_missing_roles(self):
        for source, expected in [('AUTHOR','A01'), (' illustrator ','A12'), ('B06','B06'), ('',None)]:
            with self.subTest(source=source):
                contributor = {'role':source, 'party':{'display_name':'Jane','short_bio':'Biography'}}
                root = ET.Element('DescriptiveDetail')
                xml_serializer._write_contributor(root, contributor)
                self.assertEqual(root.findtext('./Contributor/ContributorRole'), expected)
                self.assertEqual(contributor['role'], source)
                self.assertEqual(contributor['party']['short_bio'], 'Biography')

    def test_missing_role_is_reported_for_source_review(self):
        result = validation.validate_product({'descriptive_detail':{'contributors':[{'role':''}]}})
        self.assertTrue(any('source evidence required' in error for error in result['errors']))


if __name__ == '__main__': unittest.main()
