import time,unittest
from unittest.mock import patch
from jose import jwt
from starlette.requests import Request
from fastapi import HTTPException
from app.distribution import embedded as em

class ShopifySessionTests(unittest.TestCase):
 def claims(self):
  return {'aud':'app-id','iss':'https://demo.myshopify.com/admin','dest':'https://demo.myshopify.com','sub':'42','exp':int(time.time())+60,'nbf':int(time.time())-1,'iat':int(time.time())}
 def validate(self,claims,key='secret',algorithm='HS256'):
  token=jwt.encode(claims,key,algorithm=algorithm)
  request=Request({'type':'http','headers':[(b'authorization',('Bearer '+token).encode())]})
  with patch.object(em.sh,'app_credentials',return_value={'client_id':'app-id','client_secret':'secret'}):return em.session(request)
 def test_valid(self):self.assertEqual(self.validate(self.claims())['shop'],'demo.myshopify.com')
 def test_invalid_claims(self):
  for change in ({'aud':'another-app'},{'exp':0},{'nbf':int(time.time())+60},{'iss':'https://other.myshopify.com/admin'},{'dest':'https://attacker.test'},{'sub':''}):
   with self.subTest(change=change),self.assertRaises(HTTPException) as err:self.validate({**self.claims(),**change})
   self.assertEqual(err.exception.status_code,401)
 def test_wrong_signature_and_algorithm(self):
  for key,algorithm in [('wrong','HS256'),('secret','HS384')]:
   with self.assertRaises(HTTPException):self.validate(self.claims(),key,algorithm)
 def test_required_claims(self):
  for field in ('aud','exp','nbf','iat','iss','dest','sub'):
   claims=self.claims();del claims[field]
   with self.assertRaises(HTTPException):self.validate(claims)
