"""Contract identity tags, tested without importing storage/network initialization."""
import ast
import io
import re
import unittest
from pathlib import Path
from docx import Document
from docx.shared import RGBColor, Pt

SOURCE = Path(__file__).resolve().parents[1] / 'routers' / 'contract_docs.py'
tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
ns = {'re': re, 'RGBColor': RGBColor, 'TOKEN_RE': re.compile(r'\{\{\s*([^{}]+?)\s*\}\}')}
for name in ('_dig', '_first_non_empty', '_normalize_token_name', '_get_value_from_memo',
             '_default_mapping', '_replace_inline_tokens', '_iter_contract_paragraphs'):
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), 'exec'), ns)

class IdentityFields(unittest.TestCase):
    def test_palette_tags_and_legacy_author_tags(self):
        memo = {'illustrator_name': 'Iris Artist', 'illustrator_email': 'iris@example.test',
                'illustrator_phone_number': '555-0100', 'illustrator_address': {
                    'street': '1 Art Lane', 'city': 'Boston', 'state': 'MA', 'zip': '02101'},
                'agent_name': 'Selected Agent', 'agent_email': 'agent@example.test',
                'agency_name': 'Art Agency', 'agency_street': '2 Agency Road',
                'agency_phone_number': '555-0200', 'agency_email': 'agency@example.test'}
        expected = {'Illustrator_Name': 'Iris Artist', 'Illustrator_Email': 'iris@example.test',
                    'Illustrator_Phone': '555-0100', 'Illustrator_Street_Address': '1 Art Lane',
                    'Illustrator_City': 'Boston', 'Illustrator_State': 'MA', 'Illustrator_Zip': '02101',
                    'Agent_Name': 'Selected Agent', 'Agent_Email': 'agent@example.test',
                    'Agency_Name': 'Art Agency', 'Agency_Address': '2 Agency Road',
                    'Agency_Phone': '555-0200', 'Agency_Email': 'agency@example.test',
                    'Author_Name': 'Iris Artist', 'Author_Email': 'iris@example.test'}
        mapping = ns['_default_mapping']('illustrator')
        values = {token: ns['_get_value_from_memo'](memo, field) for token, field in mapping.items()}
        doc = Document()
        for token, value in expected.items():
            p = doc.add_paragraph('Before ')
            p.add_run('{{' + token[:5]).font.name = 'Garamond'
            p.add_run(token[5:] + '}}').font.name = 'Garamond'
            p.add_run(' after.').italic = True
            ns['_replace_inline_tokens'](p, values)
            self.assertEqual(p.text, 'Before ' + value + ' after.')
            self.assertTrue(p.runs[-1].italic)
        self.assertEqual(ns['_default_mapping']('author')['Author_Name'], 'author')
        self.assertEqual(ns['_get_value_from_memo']({'illustrator': {'display_name': 'Nested Artist'}}, 'illustrator_name'), 'Nested Artist')

    def test_headers_footers_and_nested_tables_round_trip(self):
        doc = Document()
        doc.add_paragraph('{{Illustrator_Name}}')
        section = doc.sections[0]
        for story in (section.header, section.first_page_header, section.even_page_header,
                      section.footer, section.first_page_footer, section.even_page_footer):
            story.paragraphs[0].text = '{{Illustrator_Email}}'
        cell = doc.add_table(rows=1, cols=1).cell(0, 0)
        cell.add_table(rows=1, cols=1).cell(0, 0).text = '{{Agency_Name}}'
        doc.add_section()  # linked stories must not duplicate or create extra definitions
        paragraphs = list(ns['_iter_contract_paragraphs'](doc))
        self.assertEqual(len(paragraphs), len({p._p for p in paragraphs}))
        values = {'Illustrator_Name': 'Iris Artist', 'Illustrator_Email': 'iris@example.test', 'Agency_Name': 'Art Agency'}
        for p in paragraphs:
            ns['_replace_inline_tokens'](p, values)
        output = io.BytesIO()
        doc.save(output)
        output.seek(0)
        result = Document(output)
        text = '\n'.join(p.text for p in ns['_iter_contract_paragraphs'](result))
        self.assertNotIn('{{', text)
        self.assertEqual(text.count('iris@example.test'), 6)
        self.assertIn('Iris Artist', text)
        self.assertIn('Art Agency', text)
        self.assertTrue(result.sections[1].header.is_linked_to_previous)

if __name__ == '__main__':
    unittest.main()
