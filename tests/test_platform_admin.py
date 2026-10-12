"""HTTP authorization checks: mocks database access, never reads application DB credentials."""
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.admin import platform
from app.auth.dependencies import get_current_user


class PlatformAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(platform.router)
        self.client = TestClient(self.app)
        self.actor = str(uuid4())
        self.tenant = str(uuid4())
        self.app.dependency_overrides[get_current_user] = lambda: {"sub": "test-user"}

    def test_publisher_cannot_read_or_modify_platform(self):
        user = {"id": self.actor, "platform_role": "user"}
        with patch('app.auth.dependencies.get_user_db_record_from_claims', return_value=user), patch.object(platform, 'db_conn') as db:
            for method, path, body in [
                ('GET', '/tenants', None), ('GET', '/activity', None),
                ('GET', '/support-sessions', None),
                ('POST', '/tenants', {'name': 'Demo', 'slug': 'demo'}),
                ('DELETE', '/tenants/'+self.tenant, {'confirmation': 'demo'}),
                ('POST', '/support-sessions', {'tenant_id':self.tenant,'reason':'Review reported issue'}),
            ]:
                response=self.client.request(method,'/platform-admin'+path,json=body)
                self.assertEqual(response.status_code,403,response.text)
            db.assert_not_called()

    def test_unauthenticated_access_denied(self):
        self.app.dependency_overrides.clear()
        with patch.object(platform,'db_conn') as db:
            self.assertEqual(self.client.get('/platform-admin/tenants').status_code,401)
            db.assert_not_called()

    def test_activity_cannot_be_attributed_to_another_tenant(self):
        with patch.object(platform,'get_user_db_record_from_claims',return_value={'id':self.actor,'platform_role':'user'}), patch.object(platform,'get_memberships_for_user',return_value=[{'tenant_id':self.tenant,'tenant_slug':'mine'}]), patch.object(platform,'db_conn') as db:
            response=self.client.post('/platform-admin/activity',json={'module':'books','tenant_slug':'other'})
            self.assertEqual(response.status_code,403)
            db.assert_not_called()

    def test_activity_does_not_accept_paths_or_payload_as_module(self):
        with patch.object(platform,'db_conn') as db:
            response=self.client.post('/platform-admin/activity',json={'module':'books?secret=123','tenant_slug':'mine'})
            self.assertEqual(response.status_code,400)
            db.assert_not_called()

    def test_expired_or_other_admin_support_session_cannot_show_members(self):
        cursor=MagicMock()
        cursor.fetchone.side_effect=[{'activity':'platform_activity','support':'platform_support_sessions'},None]
        conn=MagicMock()
        conn.cursor.return_value.__enter__.return_value=cursor
        @contextmanager
        def fake_db():
            yield conn
        with patch('app.auth.dependencies.get_user_db_record_from_claims',return_value={'id':self.actor,'platform_role':'superadmin'}),patch.object(platform,'db_conn',fake_db):
            response=self.client.get('/platform-admin/support-sessions/'+str(uuid4()))
            self.assertEqual(response.status_code,404)
            self.assertEqual(cursor.execute.call_count,2)
            query,params=cursor.execute.call_args.args
            self.assertIn('actor_id=%s',query)
            self.assertIn('expires_at>now()',query)
            self.assertEqual(str(params[1]),self.actor)

    def test_whitespace_support_reason_rejected_before_database(self):
        with patch('app.auth.dependencies.get_user_db_record_from_claims',return_value={'id':self.actor,'platform_role':'superadmin'}),patch.object(platform,'db_conn') as db:
            response=self.client.post('/platform-admin/support-sessions',json={'tenant_id':self.tenant,'reason':' '*15})
            self.assertEqual(response.status_code,400)
            db.assert_not_called()


if __name__ == '__main__':
    unittest.main()
