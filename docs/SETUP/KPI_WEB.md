# Cortex KPI web (browser UI + read/write API)

Browser UI over the same registry (`config/my-metrics.yaml`) and
`kpi_service` / SQLite store used by the CLI. **Not** a second source of truth.
Catalog CRUD reuses `src.metrics_registry_write` (same YAML shape as
`cortex kpi add|edit|delete`).

## Run locally

```bash
# Dev auth (no Google OAuth) — actor must be catalog_admin or a lead in kpi_owners.yaml
bin/kpi-web --dev-user marc.schriftman@leandna.com --skip-s3

# Or:
CORTEX_KPI_WEB_ALLOW_DEV_AUTH=true \
CORTEX_KPI_WEB_DEV_USER=marc.schriftman@leandna.com \
CORTEX_KPI_WEB_SKIP_S3=true \
python3 -m src.kpi_web
```

Open http://127.0.0.1:8080 — use **Dev login**, then filter by owner/tag and open a KPI detail.

Stored values need a local KPI SQLite DB (from `cortex kpi-snapshot`). With
`--skip-s3`, the app does not pull from `CORTEX_KPI_STORE_S3_URI`. Live mode runs
registry generators (same as `cortex kpi --mode live`) and needs integration
credentials for those generators.

## Maintain (add / edit / delete)

Permissions match CLI ownership rules (enforced server-side via
`assert_actor_may_mutate_metric`):

| Role | Read | Write |
|------|------|-------|
| Catalog admin (Marc) | all | full catalog |
| Lead | all | **edit-own** only; may set `metric-generator` / `metric-id`; topic packs apply |

### In the UI

1. Sign in (Google SSO or Dev login).
2. **Add KPI** — toolbar button opens a form. Default owner is you. Leads cannot
   assign another owner. Check **Dry-run** to preview the change JSON without
   writing YAML; uncheck to persist.
3. Select a KPI you may edit → **Edit** / **Delete** in the detail pane.
4. Delete opens a confirmation dialog, dry-runs, then writes the YAML delete.

Leads see Edit/Delete only on KPIs they own; other owners are view-only.

### Via API (CLI parity)

```bash
# Dry-run add
curl -s -X POST 'http://127.0.0.1:8080/api/kpis?dry_run=1' \
  -H 'Content-Type: application/json' \
  -b 'cortex_kpi_session=...' \
  -d '{"name":"Example","tags":["engineering"],"owner":"me"}'

# Persist edit
curl -s -X PATCH 'http://127.0.0.1:8080/api/kpis/Example' \
  -H 'Content-Type: application/json' \
  -b 'cortex_kpi_session=...' \
  -d '{"description":"Updated","metric_generator":"get_open_help"}'

# Delete
curl -s -X DELETE 'http://127.0.0.1:8080/api/kpis/Example?dry_run=1' \
  -b 'cortex_kpi_session=...'
```

Unauthorized mutations return **403** with an explicit error (YAML unchanged).
Successful writes log `kpi_catalog_change action=… actor=…` and return
`change.actor` in the JSON body (minimal audit).

## Google Workspace SSO

1. In Google Cloud Console, create an OAuth **Web** client for the LeanDNA Workspace project.
2. Authorized redirect URI: `{CORTEX_KPI_WEB_BASE_URL}/auth/callback`
   (e.g. `http://127.0.0.1:8080/auth/callback` for local, or your HTTPS origin).
3. Set env:

```bash
CORTEX_KPI_WEB_BASE_URL=https://kpi.example.com
CORTEX_KPI_WEB_GOOGLE_CLIENT_ID=....apps.googleusercontent.com
CORTEX_KPI_WEB_GOOGLE_CLIENT_SECRET=...
CORTEX_KPI_WEB_SESSION_SECRET=<long random string>
CORTEX_KPI_WEB_ALLOWED_DOMAINS=leandna.com
# CORTEX_KPI_WEB_COOKIE_SECURE=true   # auto when base_url is https
```

4. Run `bin/kpi-web` (do **not** set `CORTEX_KPI_WEB_ALLOW_DEV_AUTH` in production).
5. Users sign in at `/auth/login`. Only emails listed as `catalog_admin` or
   `leads` in `config/kpi_owners.yaml` may access the catalog.

## AWS (ECS + CloudFront)

Terraform (`enable_kpi_web`, default on) runs the same container as a Fargate
service: ALB in `us-east-2` (CloudFront origin only) and HTTPS at the CloudFront
URL. Dev login is refused on ECS (`scripts/run_kpi_web.sh`).

```bash
export AWS_PROFILE=aws-schrifty-login AWS_REGION=us-east-2
cd infra/terraform
terraform apply
terraform output kpi_web_url
terraform output kpi_web_oauth_redirect_uri
```

1. Add the redirect URI to the Google OAuth **Web** client.
2. Merge Google client id/secret into `cortex/prod/kpi-web` (keep
   `CORTEX_KPI_WEB_SESSION_SECRET` from the first apply):

```bash
# Download current JSON, add CORTEX_KPI_WEB_GOOGLE_CLIENT_ID and
# CORTEX_KPI_WEB_GOOGLE_CLIENT_SECRET, then:
aws secretsmanager put-secret-value \
  --secret-id cortex/prod/kpi-web \
  --secret-string file://kpi-web-secret.json
aws ecs update-service --cluster cortex --service cortex-kpi-web --force-new-deployment
```

Image must include `scripts/run_kpi_web.sh` (`scripts/push_ecr_image.sh --tag latest`).
Catalog YAML writes stay on the container filesystem (ephemeral); stored KPI
values come from EFS (and S3 when `kpi_store_s3_uri` is set).

Outside-domain accounts and unknown Workspace users get **403** with an explicit
error (no empty fake catalog).

## API (authenticated)

| Method | Path | Notes |
|--------|------|--------|
| GET | `/api/health` | Unauthenticated liveness |
| GET | `/auth/status` | Whether a session exists; SSO/dev flags |
| GET | `/api/me` | Actor + permissions |
| GET | `/api/meta` | Owners, tags, topic packs |
| GET | `/api/kpis?owner=&tag=&mode=stored\|live\|leandna&values=0\|1` | List/filter; `values=1` resolves |
| GET | `/api/kpis/{name}?mode=&history=12` | Detail + observation + history |
| POST | `/api/kpis` | Add KPI (`?dry_run=1` or body `dry_run`) |
| PATCH | `/api/kpis/{name}` | Edit / rename (`new_name`, field clears, dry-run) |
| DELETE | `/api/kpis/{name}` | Delete (`?dry_run=1`) |

Errors and empty observations are returned as-is (`error` / `warnings` /
`value_status`) — the UI does not invent KPI numbers.

**Out of scope:** authoring new Python generators in-browser; LeanDNA metric
creation UI.

## Env reference

| Variable | Purpose |
|----------|---------|
| `CORTEX_KPI_WEB_BASE_URL` | Public origin (OAuth redirect + CORS) |
| `CORTEX_KPI_WEB_HOST` / `PORT` | Bind address (default `127.0.0.1:8080`) |
| `CORTEX_KPI_WEB_GOOGLE_CLIENT_ID` / `SECRET` | Google OAuth web client |
| `CORTEX_KPI_WEB_SESSION_SECRET` | HS256 cookie signing key |
| `CORTEX_KPI_WEB_ALLOWED_DOMAINS` | Comma-separated Workspace domains |
| `CORTEX_KPI_WEB_COOKIE_SECURE` | Force Secure cookie (auto when base URL is https) |
| `CORTEX_KPI_WEB_SESSION_TTL_SECONDS` | Session TTL (default 86400) |
| `CORTEX_KPI_WEB_ALLOW_DEV_AUTH` | Explicit local/dev login (logged; not for prod) |
| `CORTEX_KPI_WEB_DEV_USER` | Default email for `/auth/dev-login` |
| `CORTEX_KPI_WEB_REGISTRY` / `OWNERS` | Alternate YAML paths (tests/dev) |
| `CORTEX_KPI_WEB_SKIP_S3` | Skip S3 pull for KPI store reads |

## Rollback & release

Bad catalog writes: **git revert** `config/my-metrics.yaml` (CLI/web both write
that file). Prefer dry-run before persist. Full ship gate, owner onboarding,
test matrix, and env checklist: **[`KPI_RELEASE.md`](./KPI_RELEASE.md)**.

Production: SSO env vars set; never enable `CORTEX_KPI_WEB_ALLOW_DEV_AUTH`.
Salesforce boundary: KPIs ≠ customer SoR.
