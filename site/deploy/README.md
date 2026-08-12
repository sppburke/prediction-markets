# Deploying pe-analytics-site to the VPS (#398 WS3, #508 Phase C)

Google-SSO-gated Next.js dashboard, served behind nginx + TLS next to pe-service on the VPS
(82.22.32.225). The Paper section admits any authenticated session; account-scoped Live data is
bound per request to `accounts.login_email`. Only the compile-time admin in `site/lib/auth.ts` can
use account controls.

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
   SUPABASE_SERVICE_ROLE_KEY=...   # server-only; account authz/reads + admin RPCs
   PE_AGE_RECIPIENT=age1...        # public recipient only; never the private identity
   ```

## Credential recipient configuration (#508 Decision 9)

- **C0 — provision one service-scoped recipient.** Generate an X25519 age identity outside the
  site host (for example, `age-keygen -o pe-live-identity.txt`), store the private identity with
  the execution service's restricted secrets, and put only its `age1...` recipient string in the
  site's `PE_AGE_RECIPIENT`. The site must not have a decrypt identity. If the env var is absent or
  invalid, credential rotation fails before any RPC or plaintext write.
- **C1 — rotate the recipient safely.** Provision the new private identity to the execution service
  first, update `PE_AGE_RECIPIENT` and restart the site, then rotate every account bundle from
  `/admin/accounts`. Keep the prior private identity available until every account's displayed
  `key_id` and incremented `bundle_version` confirm rotation. The UI and API expose metadata only;
  there is no credential readback path.

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
- Signing in with an email absent from `accounts.login_email` → AccessDenied.
- Signing in as an account viewer → Paper pages + only that account's `/live` data; admin routes
  return 403/404.
- Signing in as the compile-time admin → Paper, `/live`, `/admin`, and `/admin/accounts` load.
- Clear a viewer's `accounts.login_email` through `/admin/accounts`; their next Live request is
  denied while their existing session can still use Paper until expiry.
- `/admin` edits a `service_config` row → row's `updated_by`/`updated_at` stamped (check via psql);
  pe-service polls it within ≤30 s and applies it after validation.
- Every non-admin account/config API request → 403.
- Credential rotation increments `bundle_version` and displays only `key_id` + fingerprint; no
  endpoint returns `sealed_bundle` or plaintext.

## Lockout recovery (risk #9)

There is no auth or database bypass for admin identity. The sole admin is the compile-time constant
`ALLOWED_EMAIL` in `site/lib/auth.ts`; no `accounts` row, role-shaped field, or login-email grant can
confer admin. Loss of that Google account is recovered only by editing `ALLOWED_EMAIL`, rebuilding,
and redeploying the site. Database edits can grant/revoke viewer access but cannot recover admin.
