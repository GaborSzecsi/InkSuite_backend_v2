import ast
import re
import unittest
import uuid
from datetime import date
from pathlib import Path
from routers.catalog_shared import _safe_str, _parse_date_or_none, _to_int_or_none

source = Path(__file__).parents[1] / "routers/catalog_write.py"
node = next(n for n in ast.parse(source.read_text(encoding="utf-8")).body if isinstance(n, ast.FunctionDef) and n.name == "update_edition_publishing_dates")
namespace = dict(re=re, uuid=uuid, _safe_str=_safe_str, _parse_date_or_none=_parse_date_or_none, _to_int_or_none=_to_int_or_none)
exec(compile(ast.fix_missing_locations(ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])), str(source), "exec"), namespace)

class Cursor:
    def __init__(self): self.inserts = []
    def execute(self, sql, params):
        if "INSERT INTO edition_publishing_dates" in sql: self.inserts.append(params)
    def fetchone(self): return {"id": "edition"}

class PublicationDateTests(unittest.TestCase):
    def test_full_date_is_saved_as_a_date_and_duplicate_alias_is_collapsed(self):
        cur=Cursor()
        row={"date_role":"01", "date_text":"20271012", "date_format":"00"}
        result=namespace["update_edition_publishing_dates"](cur,"tenant","work",str(uuid.uuid4()),{"publishing_dates":[row,row]})
        self.assertEqual(result["publication_date"], "2027-10-12")
        self.assertEqual(len(cur.inserts),1)
        self.assertEqual(cur.inserts[0][3],date(2027,10,12))
    def test_partial_date_preserves_precision(self):
        cur=Cursor()
        result=namespace["update_edition_publishing_dates"](cur,"tenant","work",str(uuid.uuid4()),{"publishing_dates":[{"date_role":"01","date_text":"202710","date_format":"01"}]})
        self.assertEqual(result["publication_date"],"")
        self.assertIsNone(cur.inserts[0][3])
        self.assertEqual(cur.inserts[0][4],"202710")

if __name__ == "__main__": unittest.main()
