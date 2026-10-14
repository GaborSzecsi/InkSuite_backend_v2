import ast
import sqlite3
import unittest
from pathlib import Path


class Cursor:
    def __init__(self, db): self.cur = db.cursor()
    def __enter__(self): return self
    def __exit__(self, *args): self.cur.close()
    def execute(self, sql, params=()):
        self.cur.execute(sql.replace('::uuid', '').replace('::text', '').replace('%s', '?').replace('now()', 'CURRENT_TIMESTAMP'), params)
    def fetchone(self):
        row = self.cur.fetchone()
        return dict(row) if row else None


class Connection:
    def __init__(self, db): self.db = db
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def cursor(self, **kwargs): return Cursor(self.db)
    def commit(self): self.db.commit()


class UploadResourceTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
        CREATE TABLE edition_supporting_resources (
          id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
          tenant_id TEXT, edition_id TEXT, resource_content_type TEXT,
          content_audience TEXT, territory_countries TEXT, caption TEXT,
          credit TEXT, resource_mode TEXT, cloned_from_resource_id TEXT,
          item_order INTEGER, updated_at TEXT);
        CREATE TABLE edition_supporting_resource_versions (
          id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
          tenant_id TEXT, resource_id TEXT, resource_form TEXT, resource_link TEXT,
          storage_key TEXT, filename TEXT, file_format TEXT, file_size_bytes INTEGER,
          width_pixels INTEGER, height_pixels INTEGER, item_order INTEGER, updated_at TEXT);
        ''')
        source = Path(__file__).resolve().parents[1] / 'routers' / 'uploads.py'
        tree = ast.parse(source.read_text(encoding='utf-8-sig'))
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_upsert_supporting_resource')
        namespace = {'db_conn': lambda: Connection(self.db), 'dict_row': None}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), 'exec'), namespace)
        self.save = namespace['_upsert_supporting_resource']
        self.args = dict(tenant_id='demo', edition_id='hardcover', resource_content_type='01', resource_mode='03', resource_form='02', storage_key='cover.jpg', resource_link='https://example.test/cover.jpg', filename='cover.jpg', mime='image/jpeg', size_bytes=100, width_pixels=600, height_pixels=900)
    def tearDown(self): self.db.close()
    def test_new_cover_is_registered(self):
        resource, version = self.save(**self.args)
        self.assertTrue(resource and version)
        row = self.db.execute('SELECT tenant_id, edition_id FROM edition_supporting_resources').fetchone()
        self.assertEqual(tuple(row), ('demo', 'hardcover'))
    def test_repeated_upload_updates_without_duplicate(self):
        first = self.save(**self.args)
        self.args.update(size_bytes=200, caption='Updated cover')
        self.assertEqual(self.save(**self.args), first)
        self.assertEqual(self.db.execute('SELECT count(*) FROM edition_supporting_resources').fetchone()[0], 1)
        self.assertEqual(self.db.execute('SELECT file_size_bytes FROM edition_supporting_resource_versions').fetchone()[0], 200)
    def test_same_key_in_other_tenant_or_edition_is_separate(self):
        first = self.save(**self.args)
        other_tenant = self.save(**dict(self.args, tenant_id='other'))
        other_edition = self.save(**dict(self.args, edition_id='paperback'))
        self.assertEqual(len({first[0], other_tenant[0], other_edition[0]}), 3)
        self.assertEqual(self.save(**self.args), first)


if __name__ == '__main__':
    unittest.main()
