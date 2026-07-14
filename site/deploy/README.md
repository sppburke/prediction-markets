# Deploying pe-analytics-site to the VPS (#398 WS3, step 21)

Single-email (Google SSO) gated Next.js dashboard, served behind nginx + TLS next to pe-service
on the VPS (82.22.32.225). All non-static routes require sign-in as the allowlisted email
(`site/lib/auth.ts`).

## One-time prerequisites (operator)

1. **GCP OAuth client** — GCP console → APIs & Services → Credentials → Create OAuth client ID,
   type **Web application**. Authorized redirect URI: `https://<your-domain>/api/auth/callback/google`.
   Note the client ID + secret.
2. **`.env.production`** in `site/` on the VPS (`chmod 600`, never committed):
   ```
   NEXT_PUBLIC_SUPABASE_URL=...
   NEXT_PUBLIC_SUPABASE_ANON_KEY=...
   AUTH_SECRET=$(openssl rand -base64 32)
   AUTH_GOOGLE_CLIENT_ID=...
   AUTH_GOOGLE_CLIENT_SECRET=...
   AUTH_URL=https://<your-domain>
   SUPABASE_SERVICE_ROLE_KEY=...   # server-only; bypasses RLS for the admin write
   ```

## Deploy

```sh
cd site
npm ci
npm run build
# systemd unit (adjust User/WorkingDirectory/domain in the files first):
sudo cp deploy/pe-site.service /etc/systemd/system/pe-site.service
sudo systemctl daemon-reload && sudo systemctl enable --now pe-site
# nginx + TLS:
sudo cp deploy/nginx-pe-site.conf /etc/nginx/sites-available/pe-site
sudo ln -sf /etc/nginx/sites-available/pe-site /etc/nginx/sites-enabled/pe-site
sudo nginx -t && sudo systemctl reload nginx
sudo certbot --nginx -d <your-domain>
```

## Verify (WS3 AC)

- Visiting any route while signed out → redirected to Google sign-in.
- Signing in with a non-allowlisted Google account → AccessDenied (nothing renders).
- Signing in as the allowlisted email → dashboard + `/admin` load.
- `/admin` edits a `service_config` row → row's `updated_by`/`updated_at` stamped (check via psql);
  pe-service polls it within ≤30 s and applies it after validation.
- A PATCH to `/api/config` from any other session → 403.

## Lockout recovery (risk #9)

There is no auth bypass. The allowed email is the compile-time constant `ALLOWED_EMAIL` in
`site/lib/auth.ts` (NOT stored in the database), so the only recovery is to edit that constant and
redeploy the site. (A `psql` edit to `service_config` changes trader knobs, not the auth gate.)
