# Cognito auth setup

To use AWS Cognito for login (no stub / no "any password"):

## 1. Backend env vars

In `.env` (or your environment) set:

```env
# Required for Cognito login
COGNITO_USER_POOL_ID=us-east-1_KRsVd8ubP
COGNITO_CLIENT_ID=<your App Client ID>
COGNITO_REGION=us-east-1
```

**App Client ID:** In AWS Console → Cognito → User pools → **User pool - s-maap** → **App integration** → **App clients and analytics** → open your app client → copy **Client ID**.  
The app client must have:
- **Authentication flows:** `ALLOW_USER_PASSWORD_AUTH` and `ALLOW_REFRESH_TOKEN_AUTH` enabled.
- **Client secret:** optional; if you set one, also set `COGNITO_CLIENT_SECRET` in `.env`.

**Optional:**  
- `COGNITO_ISSUER` – defaults to `https://cognito-idp.{region}.amazonaws.com/{user_pool_id}`  
- `JWKS_URL` – defaults to `{COGNITO_ISSUER}/.well-known/jwks.json`

## 2. Frontend: disable dev stub

So the frontend does **not** accept any password when the backend is unreachable, set in **marble-frontend** `.env.local`:

```env
NEXT_PUBLIC_USE_COGNITO=1
```

(or `NEXT_PUBLIC_DISABLE_DEV_STUB_AUTH=1`)

Then restart `npm run dev`. Login will always go to the backend; wrong password → "Invalid email or password".

## 3. Backend must be running and reachable

- Start the backend from **InkSuite_backend_v2** so the `app` package (auth router) is loaded:
  ```bash
  uvicorn main:app --reload --host 127.0.0.1 --port 8000
  ```
- Frontend must call the backend: set `NEXT_PUBLIC_API_BASE=http://127.0.0.1:8000` in `.env.local` if needed.

## 4. Test

- **Correct:** `szecsi.gabor@gmail.com` + your Cognito user password → redirect to `/app`.
- **Wrong password** → "Invalid email or password" (no stub login).
