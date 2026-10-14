import sqlite3
import unittest
from app.catalog_content import description_for_work, save_shared_description, DESCRIPTION_SQL

class Cursor:
    def __init__(self, db): self.cur = db.cursor()
    def execute(self, sql, params=()):
        self.cur.execute(sql.replace("%s", "?").replace("now()", "CURRENT_TIMESTAMP"), params)
    def fetchone(self):
        row = self.cur.fetchone()
        return dict(row) if row else None

class ContentTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
          CREATE TABLE works(id TEXT, tenant_id TEXT, main_description TEXT);
          CREATE TABLE editions(id TEXT, tenant_id TEXT, work_id TEXT);
          CREATE TABLE edition_texts(id TEXT, tenant_id TEXT, edition_id TEXT, text_type TEXT, text_value TEXT, text_format TEXT, updated_at TEXT, created_at TEXT, item_order INT);
          INSERT INTO works VALUES ('book','demo','Old description'),('other','marble','Private description');
          INSERT INTO editions VALUES ('hc','demo','book'),('pb','demo','book'),('private','marble','other');
          INSERT INTO edition_texts VALUES ('text','demo','hc','03','Saved ONIX description','06','2026-10-14','2026-10-14',1),('toc','demo','hc','04','Chapter One','06','2026-10-14','2026-10-14',2),('secret','marble','private','03','Private ONIX','06','2026-10-15','2026-10-15',1);
        """)
        self.cur = Cursor(self.db)
    def tearDown(self): self.db.close()
    def test_existing_onix_description_overrides_stale_legacy(self):
        self.assertEqual(description_for_work(self.cur,'demo','book'),'Saved ONIX description')
    def test_both_shopify_formats_share_same_projection(self):
        self.cur.execute(f"SELECT e.id, {DESCRIPTION_SQL} AS description FROM editions e JOIN works w ON w.id=e.work_id AND w.tenant_id=e.tenant_id WHERE e.tenant_id=%s", ('demo',))
        self.assertEqual([tuple(r) for r in self.cur.cur.fetchall()], [('hc','Saved ONIX description'),('pb','Saved ONIX description')])
    def test_book_editor_update_changes_onix_and_mirror_not_toc_or_other_tenant(self):
        save_shared_description(self.cur,'demo','book','Updated through Book Management')
        self.assertEqual(description_for_work(self.cur,'demo','book'),'Updated through Book Management')
        self.assertEqual(self.db.execute("SELECT main_description FROM works WHERE id='book'").fetchone()[0],'Updated through Book Management')
        self.assertEqual(self.db.execute("SELECT text_value FROM edition_texts WHERE id='toc'").fetchone()[0],'Chapter One')
        self.assertEqual(description_for_work(self.cur,'marble','other'),'Private ONIX')
    def test_legacy_and_empty_description(self):
        self.cur.execute("DELETE FROM edition_texts WHERE tenant_id=%s AND text_type='03'",('demo',))
        self.assertEqual(description_for_work(self.cur,'demo','book'),'Old description')
        save_shared_description(self.cur,'demo','book','')
        self.assertEqual(description_for_work(self.cur,'demo','book'),'')
    def test_cross_tenant_lookup_and_write_cannot_touch_title(self):
        self.assertEqual(description_for_work(self.cur,'marble','book'),'')
        save_shared_description(self.cur,'marble','book','Wrong')
        self.assertEqual(description_for_work(self.cur,'demo','book'),'Saved ONIX description')
