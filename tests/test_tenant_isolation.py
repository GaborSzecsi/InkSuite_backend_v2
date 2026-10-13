"""Exercise the production HTTP boundary without loading .env or touching AWS."""
import ast
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from app.tenants import workspace as scope

ROOT = Path(__file__).resolve().parents[1]


def production_middleware():
    tree = ast.parse((ROOT / 'main.py').read_text(encoding='utf-8-sig'))
    names = {'_bearer_token_from_header', '_token_from_cookie', 'require_auth_middleware'}
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    for node in nodes:
        node.decorator_list = []
    namespace = {'HTTPException': HTTPException, 'JSONResponse': JSONResponse, 'REQUIRE_AUTH': False}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'main.py', 'exec'), namespace)
    return namespace['require_auth_middleware']


class TenantBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.middleware('http')(production_middleware())
        self.entered = []

        @self.app.api_route('/api/{path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
        async def record(request: Request, path: str):
            self.entered.append(path)
            if path.startswith('banking/public/'):
                return {'recipient_handler': True}
            current = scope.workspace()
            await asyncio.sleep(0)
            return {'tenant_id': current.tenant_id, 'slug': scope.tenant_slug(), 'query': request.query_params.get('tenant_slug')}

        self.client = TestClient(self.app)
        self.patches = [
            patch('app.auth.service.get_current_user_from_token', return_value={'sub': 'test-user'}),
            patch('app.auth.service.get_user_db_record_from_claims', return_value={'id': 'user-1', 'platform_role': 'superadmin'}),
            patch('app.auth.service.is_superadmin', return_value=True),
            patch.object(scope, 'get_user_db_record_from_claims', return_value={'id': 'user-1', 'platform_role': 'superadmin'}),
            patch.object(scope, 'get_memberships_for_user', return_value=[
                {'tenant_id': 'tenant-a', 'tenant_slug': 'marble-press', 'role': 'tenant_admin'},
                {'tenant_id': 'tenant-b', 'tenant_slug': 'inksuite-demo', 'role': 'tenant_admin'},
            ]),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def headers(self, slug='inksuite-demo'):
        return {'Authorization': 'Bearer test', 'X-Workspace': slug, 'X-Tenant': slug}

    def test_all_publisher_families_require_auth_even_when_require_auth_is_off(self):
        for prefix in scope.PREFIXES:
            with self.subTest(prefix=prefix):
                self.assertEqual(self.client.get(prefix).status_code, 401)
        self.assertEqual(self.entered, [])

    def test_missing_or_conflicting_context_never_reaches_a_handler(self):
        for headers, query in [
            ({'Authorization': 'Bearer test'}, ''),
            (self.headers(), '?tenant_slug=marble-press'),
            (self.headers(), '?tenant_slug=inksuite-demo&tenant_slug=marble-press'),
            ({**self.headers(), 'Cookie': 'workspace_company=marble-press'}, ''),
        ]:
            self.assertEqual(self.client.get('/api/catalog/works' + query, headers=headers).status_code, 403)
        self.assertEqual(self.entered, [])

    def test_platform_admin_does_not_bypass_membership(self):
        response = self.client.get('/api/catalog/works', headers=self.headers('unknown-publisher'))
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.entered, [])

    def test_workspace_database_lookup_does_not_block_event_loop(self):
        original = scope.select_workspace
        def checked_lookup(request, claims):
            with self.assertRaises(RuntimeError):
                asyncio.get_running_loop()
            return original(request, claims)
        with patch.object(scope, 'select_workspace', side_effect=checked_lookup) as lookup:
            response = self.client.get('/api/catalog/works', headers=self.headers())
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['tenant_id'], 'tenant-b')
        lookup.assert_called_once()

    def test_reads_and_writes_bind_query_defaults_to_active_workspace(self):
        for method in ['GET', 'POST', 'PUT', 'DELETE']:
            for slug, tenant_id in [('inksuite-demo', 'tenant-b'), ('marble-press', 'tenant-a')]:
                response = self.client.request(method, '/api/catalog/works', headers=self.headers(slug))
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json(), {'tenant_id': tenant_id, 'slug': slug, 'query': slug})
                self.assertEqual(response.headers['cache-control'], 'private, no-store')
        self.assertIsNone(scope.current_workspace.get())

    def test_url_tenant_must_match_active_workspace(self):
        response = self.client.get('/api/tenants/marble-press/distribution', headers=self.headers())
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.entered, [])

    def test_shared_legacy_files_are_not_available_to_new_tenants(self):
        for path in ['/api/contracts/templates', '/api/contracts/drafts', '/api/books', '/api/financials/uploads', '/api/financials/upload', '/api/royalty/statements/author/Test', '/api/royalty/render']:
            response = self.client.get(path, headers=self.headers())
            self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self.entered, [])

    def test_recipient_token_exceptions_are_exact_and_method_specific(self):
        for method, path in [('GET', 'request'), ('POST', 'otp'), ('POST', 'verify'), ('PUT', 'bank-account')]:
            response = self.client.request(method, '/api/banking/public/' + path)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['cache-control'], 'private, no-store')
        self.entered.clear()
        for method, path in [('DELETE', 'request'), ('GET', 'request/extra'), ('POST', 'bank-account')]:
            self.assertEqual(self.client.request(method, '/api/banking/public/' + path).status_code, 401)
        self.assertEqual(self.entered, [])

    def test_scoped_modules_accept_demo_without_shared_storage(self):
        for path in ['/api/uploads/book-assets', '/api/financials/book-kpis', '/api/salesdata/overview', '/api/banking/status']:
            response = self.client.get(path, headers=self.headers())
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['tenant_id'], 'tenant-b')

    def test_modified_sources_parse(self):
        for relative in ['main.py', 'app/tenants/workspace.py', 'routers/uploads.py', 'routers/catalog.py', 'routers/banking.py', 'routers/financials.py', 'routers/royalty.py', 'routers/payment_instructions.py', 'app/onix/router.py', 'routers/contract_docs.py', 'services/royalty_calculator.py', 'routers/templates.py', 'routers/contract_invites.py', 'app/tenants/contract_storage.py', 'app/wopi/workspace.py', 'app/wopi/tokens.py']:
            ast.parse((ROOT / relative).read_text(encoding='utf-8-sig'), filename=relative)


class ContractStorageTests(unittest.TestCase):
    def setUp(self):
        self.marker = scope.current_workspace.set(scope.Workspace('demo-id', 'inksuite-demo', 'tenant_admin', 'user'))
        self.addCleanup(scope.current_workspace.reset, self.marker)

    def test_document_tokens_bind_publisher_kind_and_write_permission(self):
        from app.wopi.tokens import make_wopi_token, verify_wopi_token
        from app.wopi.workspace import token_workspace
        token = make_wopi_token('same-id', 'reviewer', 60, 'test-only-secret', read_only=True)
        payload = verify_wopi_token(token, 'test-only-secret')
        selected = token_workspace(payload, file_id='same-id', file_kind='draft', method='GET')
        self.assertEqual(selected.slug, 'inksuite-demo')
        for kwargs in [
            {'file_id': 'same-id', 'file_kind': 'draft', 'method': 'POST'},
            {'file_id': 'other-id', 'file_kind': 'draft', 'method': 'GET'},
            {'file_id': 'same-id', 'file_kind': 'template', 'method': 'GET'},
        ]:
            with self.assertRaises(HTTPException):
                token_workspace(payload, **kwargs)
        with self.assertRaises(HTTPException):
            verify_wopi_token(token, 'wrong-secret')
        with self.assertRaises(HTTPException):
            token_workspace({'file_id': 'same-id'}, file_id='same-id', file_kind='draft', method='GET')

    def test_contract_key_and_path_escape_is_rejected(self):
        from app.tenants.contract_storage import checked_key, checked_local_path, filename, draft_prefix
        self.assertEqual(checked_key(draft_prefix() + 'contract.docx', draft_prefix()), draft_prefix() + 'contract.docx')
        for key in ['tenants/marble-press/data/TempDraftContracts/contract.docx', draft_prefix() + '../contract.docx']:
            with self.assertRaises(HTTPException):
                checked_key(key, draft_prefix())
        for value in ['../contract.docx', 'folder\\contract.docx', '*']:
            with self.assertRaises(HTTPException):
                filename(value)
        with TemporaryDirectory() as directory:
            root = Path(directory) / 'demo'
            with self.assertRaises(HTTPException):
                checked_local_path(root / '..' / 'marble' / 'file.docx', root)

    def test_real_draft_index_functions_use_separate_s3_keys(self):
        from typing import List
        from app.tenants import contract_storage as storage
        tree = ast.parse((ROOT / 'routers/contract_docs.py').read_text(encoding='utf-8-sig'))
        names = {'draft_index_key', 'draft_index_path', '_load_index', '_save_index'}
        nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
        objects = {}
        def put(key, data, **kwargs):
            objects[key] = data
        namespace = {'List': List, 'json': json, '_S3_AVAILABLE': True, 'draft_prefix': storage.draft_prefix,
                     'draft_directory': storage.draft_directory, 'get_bytes': lambda key: objects[key], 'put_bytes': put}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'contract_docs.py', 'exec'), namespace)
        namespace['_save_index']([{'id': 'same-id', 'title': 'Demo contract'}])
        demo_key = namespace['draft_index_key']()
        marker = scope.current_workspace.set(scope.Workspace('marble-id', 'marble-press', 'tenant_admin', 'user'))
        try:
            namespace['_save_index']([{'id': 'same-id', 'title': 'Marble contract'}])
            self.assertNotEqual(namespace['draft_index_key'](), demo_key)
            self.assertEqual(namespace['_load_index']()[0]['title'], 'Marble contract')
        finally:
            scope.current_workspace.reset(marker)
        self.assertEqual(namespace['_load_index']()[0]['title'], 'Demo contract')
        namespace['_save_index']([])
        self.assertIn(b'Marble contract', objects['tenants/marble-press/data/TempDraftContracts/index.json'])


class StatementStorageTests(unittest.TestCase):
    def test_shared_calculator_resolves_workspace_on_each_operation(self):
        from services.royalty_calculator import RoyaltyCalculator
        with TemporaryDirectory() as directory:
            root = Path(directory)
            calc = RoyaltyCalculator()
            calc._legacy_data_dir = root
            records = {'Same Name': [{'period_start': '2025-01-01', 'period_end': '2025-06-30', 'balance': 88}]}
            legacy = root / 'author_royalties.json'
            legacy.write_text(json.dumps(records), encoding='utf-8')
            original = legacy.read_bytes()
            selected = scope.current_workspace.set(scope.Workspace('demo-id', 'inksuite-demo', 'tenant_admin', 'user'))
            try:
                self.assertEqual(calc.get_person_statements('Same Name', 'author'), [])
                self.assertFalse(calc.delete_statement('Same Name', 'author', '2025-01-01', '2025-06-30'))
                calc._save_person_statement(calc.author_royalty_file, 'Same Name', '2025-01-01', '2025-06-30', {'balance': 2})
                self.assertEqual(calc.get_person_statements('Same Name', 'author')[0]['balance'], 2)
                self.assertEqual(legacy.read_bytes(), original)
                self.assertTrue(calc.delete_statement('Same Name', 'author', '2025-01-01', '2025-06-30'))
                self.assertEqual(legacy.read_bytes(), original)
            finally:
                scope.current_workspace.reset(selected)
            selected = scope.current_workspace.set(scope.Workspace('marble-id', 'marble-press', 'tenant_admin', 'user'))
            try:
                self.assertEqual(calc.get_person_statements('Same Name', 'author')[0]['balance'], 88)
            finally:
                scope.current_workspace.reset(selected)

    def test_statement_files_require_explicit_context(self):
        from services.royalty_calculator import RoyaltyCalculator
        with self.assertRaises(HTTPException):
            RoyaltyCalculator().get_person_statements('Same Name', 'author')

    def test_concurrent_contexts_do_not_share_storage(self):
        async def run():
            async def path_for(slug):
                token = scope.current_workspace.set(scope.Workspace(slug, slug, 'tenant_admin', 'user'))
                try:
                    await asyncio.sleep(0)
                    return str(scope.storage_directory(Path('data')))
                finally:
                    scope.current_workspace.reset(token)
            return await asyncio.gather(path_for('inksuite-demo'), path_for('other-publisher'))
        demo, other = asyncio.run(run())
        self.assertNotEqual(demo, other)
        self.assertTrue(demo.endswith('inksuite-demo'))
        self.assertTrue(other.endswith('other-publisher'))


if __name__ == '__main__':
    unittest.main()
