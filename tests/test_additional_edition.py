import ast
import re
import unittest
from pathlib import Path
from typing import Dict, Any

source = Path(__file__).parents[1] / "routers/catalog_write.py"
tree = ast.parse(source.read_text(encoding="utf-8"))
node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "create_additional_work_edition")
namespace = {"re": re, "Dict": Dict, "Any": Any}
exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), namespace)
create = namespace["create_additional_work_edition"]

class Cursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.calls = []
    def execute(self, sql, params):
        self.calls.append((sql, params))
    def fetchone(self):
        return next(self.rows)

class AdditionalEditionTests(unittest.TestCase):
    def test_persists_only_new_edition_with_tenant_and_work(self):
        cur = Cursor([{"id": "work"}, None, {"id": "edition"}])
        result = create(cur, "demo", "work", {"isbn13": "978-0744311570", "format": "Hardcover"})
        self.assertEqual(result["edition_id"], "edition")
        self.assertIn("FOR UPDATE", cur.calls[0][0])
        self.assertEqual(cur.calls[-1][1][:4], ("demo", "work", "9780744311570", "Hardcover"))
        self.assertTrue(all("DELETE" not in sql and "UPDATE editions" not in sql for sql, _ in cur.calls))
    def test_missing_work_cannot_create(self):
        cur = Cursor([None])
        with self.assertRaisesRegex(ValueError, "Existing work not found"):
            create(cur, "other", "work", {"isbn13": "9780744311570", "format": "Hardcover"})
        self.assertEqual(len(cur.calls), 1)
    def test_duplicate_is_rejected_without_insert(self):
        cur = Cursor([{"id": "work"}, {"id": "existing"}])
        with self.assertRaisesRegex(ValueError, "already exists"):
            create(cur, "demo", "work", {"isbn13": "9780744311570", "format": "Paperback"})
        self.assertEqual(len(cur.calls), 2)
    def test_incomplete_isbn_rejected_before_database(self):
        cur = Cursor([])
        with self.assertRaisesRegex(ValueError, "13-digit"):
            create(cur, "demo", "work", {"isbn13": "123456789", "format": "Paperback"})
        self.assertEqual(cur.calls, [])
