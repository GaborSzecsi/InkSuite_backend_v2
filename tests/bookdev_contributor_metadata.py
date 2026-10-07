"""Exercise the request metadata writer against disposable PostgreSQL."""
import ast
import contextlib
import uuid
from datetime import datetime
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from pydantic import BaseModel, validator
from fastapi import HTTPException

root = Path(__file__).resolve().parent.parent
source = ast.parse((root / "routers/bookdev_email.py").read_text(encoding="utf-8"))
names = {"ContributorWebsiteIn", "ContributorInfoSubmitIn", "_bookdev_update_request_metadata", "_bookdev_update_contributor_core", "_load_contributor_prefill", "_execute_savepoint", "_required_savepoint", "_safe", "_ensure_work_contributor", "_fetch_one_savepoint", "_bookdev_get_or_create_agency_party", "_bookdev_replace_agency_address", "_bookdev_replace_party_representation"}
namespace = dict(uuid=uuid, HTTPException=HTTPException, datetime=datetime, dict_row=None, BaseModel=BaseModel, validator=validator, Optional=Optional, List=List, Dict=Dict, Any=Any)
exec(compile(ast.Module(body=[node for node in source.body if getattr(node, "name", "") in names or (isinstance(node, ast.Assign) and any(getattr(target,"id", "").startswith("BOOKDEV_CONTRIBUTOR_") for target in node.targets))], type_ignores=[]), "<request metadata>", "exec"), namespace)
proc = subprocess.Popen(["node", "--preserve-symlinks", "--preserve-symlinks-main", str(root / "tests/banking_pg.cjs")], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, encoding="utf-8")
assert json.loads(proc.stdout.readline())["ready"]
def rpc(sql, params=(), execute=False):
    proc.stdin.write(json.dumps({"sql": sql, "params": params, "exec": execute}) + "\n")
    proc.stdin.flush()
    result = json.loads(proc.stdout.readline())
    assert "error" not in result, result
    return result["result"]
class Cursor:
    def execute(self, sql, params=()):
        for index in range(sql.count("%s")):
            sql = sql.replace("%s", f"${index+1}", 1)
        self.rows = rpc(sql, params)["rows"]
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows
    def __enter__(self): return self
    def __exit__(self, *args): pass
class Connection:
    def cursor(self, **kwargs): return Cursor()
@contextlib.contextmanager
def connection(): yield Connection()
namespace.update(db_conn=connection, _sp_name=lambda: "request_test", _optional_uuid=lambda value,*args:value,
    _fetch_socials=lambda *args:{}, _fetch_agency_agent_prefill=lambda *args,**kwargs:{})
try:
    rpc("CREATE TABLE parties(id uuid, tenant_id uuid, language_code text, updated_at timestamptz); CREATE TABLE party_websites(id serial, tenant_id uuid, party_id uuid, website_role text, website_description text, website_link text, item_order integer);", execute=True)
    tenant, other_tenant, party = map(str, [uuid4(), uuid4(), uuid4()])
    rpc("INSERT INTO parties VALUES ($1,$2,'',now()),($1,$3,'fra',now())", [party,tenant,other_tenant])
    rpc("INSERT INTO party_websites(tenant_id,party_id,website_role,website_link) VALUES ($1,$2,'41','https://other.example.com')", [other_tenant,party])
    scalar = namespace["BOOKDEV_CONTRIBUTOR_IDENTITY_FIELDS"]
    rpc("ALTER TABLE parties ADD COLUMN party_type text, ADD COLUMN display_name text, ADD COLUMN email text, ADD COLUMN website text, ADD COLUMN phone_country_code text, ADD COLUMN phone_number text, ADD COLUMN birth_city text, ADD COLUMN birth_country text, ADD COLUMN birth_date date, ADD COLUMN citizenship text, ADD COLUMN short_bio text, ADD COLUMN long_bio text;" + " ".join(f"ALTER TABLE parties ADD COLUMN {field} text;" for field in scalar), execute=True)
    rpc("CREATE TABLE party_addresses(tenant_id uuid,party_id uuid,label text,street text,city text,state text,zip text,country text,is_non_us boolean); CREATE TABLE work_contributors(id uuid DEFAULT gen_random_uuid(),tenant_id uuid,work_id uuid,party_id uuid,contributor_role text,sequence_number integer,from_language_codes text[],to_language_codes text[],contributor_description text);", execute=True)
    for field,(table,columns) in namespace["BOOKDEV_CONTRIBUTOR_REPEAT_FIELDS"].items():
        rpc(f"CREATE TABLE {table}(id serial,tenant_id uuid,party_id uuid," + ",".join(column + (" date" if column=="date_value" else " text") for column in columns) + ",item_order integer)", execute=True)
    work = str(uuid4())
    rpc("INSERT INTO work_contributors(tenant_id,work_id,party_id,contributor_role,sequence_number,from_language_codes,to_language_codes,contributor_description) VALUES($1,$2,$3,'A01',1,ARRAY[]::text[],ARRAY[]::text[],'')", [tenant,work,party])
    payload = namespace["ContributorInfoSubmitIn"](language_code="eng", websites=[{"website_role":"06","website_link":"https://www.brainmonsters.com/"},{"website_role":"41","website_description":"Susan on social media","website_link":"https://social.example.com/susan"}])
    save = namespace["_bookdev_update_request_metadata"]
    details = {field: "Saved " + field for field in scalar}
    details.update(contributor_type="person",from_languages=["eng"],to_languages=["fra"],contributor_description="Assignment description",contributor_phone_country_code="+1",contributor_phone_number="5551234",contributor_address_street="Main Street",contributor_address_city="Test City",contributor_address_state="CA",contributor_address_zip="12345",contributor_address_country="US",contributor_website="https://www.brainmonsters.com/",contributor_birth_date="1996-02-29",short_bio="Short bio",long_bio="Long bio")
    for field,(_,columns) in namespace["BOOKDEV_CONTRIBUTOR_REPEAT_FIELDS"].items():
        details[field] = [{column:("1996-02-29" if column=="date_value" else "50" if column=="contributor_date_role" else "Value " + column) for column in columns}]
    raw = payload.model_dump() if hasattr(payload,"model_dump") else payload.dict()
    payload = namespace["ContributorInfoSubmitIn"](**{**raw,**details})
    rpc("BEGIN")
    namespace["_bookdev_update_contributor_core"](Cursor(),tenant,party,payload,"Susan Szecsi","susan@example.com")
    namespace["_ensure_work_contributor"](Cursor(),tenant,work,party,"A12",2)
    save(Cursor(), tenant, party, payload, work)
    rpc("COMMIT")
    for field,(_,columns) in namespace["BOOKDEV_CONTRIBUTOR_REPEAT_FIELDS"].items():
        table = namespace["BOOKDEV_CONTRIBUTOR_REPEAT_FIELDS"][field][0]
        stored = rpc(f"SELECT * FROM {table} WHERE tenant_id=$1 AND party_id=$2",[tenant,party])["rows"]
        assert len(stored)==1,field
        for column in columns:
            assert (stored[0][column][:10] if column=="date_value" else stored[0][column])==details[field][0][column], (field,column,stored)
    party_row = rpc("SELECT * FROM parties WHERE tenant_id=$1",[tenant])["rows"][0]
    for field in scalar: assert party_row[field]==details[field],field
    assert party_row["phone_number"]=="5551234" and party_row["short_bio"]=="Short bio" and party_row["long_bio"]=="Long bio"
    address = rpc("SELECT * FROM party_addresses WHERE tenant_id=$1",[tenant])["rows"][0]
    assert [address[field] for field in ("street","city","state","zip","country")] == ["Main Street","Test City","CA","12345","US"]
    namespace["_fetch_party_address"] = lambda cur,tenant,party: address
    prefill = namespace["_load_contributor_prefill"](tenant_id=tenant,work_id=work,contributor_party_id=party,contributor_role_code="A12")["contributor"]
    assert prefill["role_code"]=="A12" and prefill["sequence_number"]==2
    for field in scalar: assert prefill[field]==details[field],field
    for field in namespace["BOOKDEV_CONTRIBUTOR_REPEAT_FIELDS"]: assert prefill[field],field
    assert prefill["websites"][1]["website_description"]=="Susan on social media"
    assert prefill["language_code"]=="eng" and prefill["from_languages"]==["eng"] and prefill["to_languages"]==["fra"]
    assert rpc("SELECT language_code FROM parties WHERE tenant_id=$1", [tenant])["rows"][0]["language_code"] == "eng"
    rows = rpc("SELECT website_role,website_link,website_description FROM party_websites WHERE tenant_id=$1 ORDER BY item_order", [tenant])["rows"]
    assert [row["website_role"] for row in rows] == ["06","41"]
    assert rows[1]["website_description"] == "Susan on social media"
    assert rows[1]["website_link"] == "https://social.example.com/susan"
    save(Cursor(), tenant, party, namespace["ContributorInfoSubmitIn"]())
    assert len(rpc("SELECT * FROM party_websites WHERE tenant_id=$1", [tenant])["rows"]) == 2
    assert rpc("SELECT language_code FROM parties WHERE tenant_id=$1", [other_tenant])["rows"][0]["language_code"] == "fra"
    assert len(rpc("SELECT * FROM party_websites WHERE tenant_id=$1", [other_tenant])["rows"]) == 1
    # Exercise optional agency/agent fields using the same transaction writer.
    rpc("CREATE TABLE party_representations(tenant_id uuid,represented_party_id uuid,agent_party_id uuid,work_id uuid,is_primary boolean,role_label text,notes text); CREATE TABLE agency_agent_links(tenant_id uuid,agency_party_id uuid,agent_party_id uuid,is_primary boolean,role_label text,updated_at timestamptz,UNIQUE(agency_party_id,agent_party_id));", execute=True)
    agent = str(uuid4())
    def get_agent(cur, tenant, name, email, party_type):
        cur.execute("INSERT INTO parties(id,tenant_id,display_name,email,party_type) VALUES(%s::uuid,%s::uuid,%s,%s,%s)", (agent,tenant,name,email,party_type))
        return agent
    namespace["_get_or_create_party"] = get_agent
    agency_payload = namespace["ContributorInfoSubmitIn"](has_agent=True,agency_name="Test Agency",agency_email="agency@example.com",agency_website="https://agency.example.com",agency_street="Agency Street",agency_city="Agency City",agency_state="Agency State",agency_zip="54321",agency_country="GB",agent_name="Test Agent",agent_email="agent@example.com",agent_phone_country_code="+44",agent_phone_number="123456")
    rpc("BEGIN")
    namespace["_bookdev_replace_party_representation"](Cursor(),tenant,party,work,agency_payload)
    rpc("COMMIT")
    agency = rpc("SELECT * FROM parties WHERE display_name='Test Agency'")["rows"][0]
    assert agency["email"]=="agency@example.com" and agency["website"]=="https://agency.example.com"
    agent_row = rpc("SELECT * FROM parties WHERE id=$1",[agent])["rows"][0]
    assert [agent_row[field] for field in ("display_name","email","phone_country_code","phone_number")] == ["Test Agent","agent@example.com","+44","123456"]
    agency_address = rpc("SELECT * FROM party_addresses WHERE party_id=$1",[agency["id"]])["rows"][0]
    assert [agency_address[field] for field in ("street","city","state","zip","country")] == ["Agency Street","Agency City","Agency State","54321","GB"]
    assert rpc("SELECT * FROM party_representations WHERE represented_party_id=$1",[party])["rows"][0]["agent_party_id"]==agent
    assert rpc("SELECT * FROM agency_agent_links WHERE agency_party_id=$1",[agency["id"]])["rows"][0]["agent_party_id"]==agent
    # Required failures must surface rather than silently completing the request.
    original_execute = namespace["_execute_savepoint"]
    namespace["_execute_savepoint"] = lambda *args: False
    try:
        namespace["_required_savepoint"](Cursor(), "unused")
        raise AssertionError("Required write failure was ignored")
    except HTTPException as error:
        assert error.status_code==500
    finally:
        namespace["_execute_savepoint"] = original_execute
    print("PASS: identity, names, websites/descriptions, places, dates, affiliations, language, contact/address, biography/notes saved and prefilled; role/order and agency/agent saved; required failures surfaced; omitted fields preserve data; other tenants unchanged.")
finally:
    proc.terminate()
    proc.wait(timeout=10)
