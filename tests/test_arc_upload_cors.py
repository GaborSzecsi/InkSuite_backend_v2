import importlib.util
from pathlib import Path

spec=importlib.util.spec_from_file_location('arc_cors',Path(__file__).parents[1]/'scripts/configure_arc_upload_cors.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


def test_cors_merge_preserves_existing_rules_and_is_idempotent():
    original=[{'AllowedOrigins':['https://other.example'],'AllowedMethods':['GET']}]
    result=module.merge_rules(original,'https://www.inksuite.io')
    assert len(result)==2 and result[0]==original[0] and len(original)==1
    assert result[1]['AllowedMethods']==['POST']
    assert module.merge_rules(result,'https://www.inksuite.io')==result
