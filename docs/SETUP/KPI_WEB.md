# Cortex KPI web view (browser UI + read API)

Local read-only UI over the same registry (`config/my-metrics.yaml`) and
`kpi_service` / SQLite store used by the CLI. **Not** a second source of truth.
Catalog CRUD forms are workstream D (not in this app yet).

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
   `leads` in `config/kpi_owners.yaml` may access the catalog. Leads get
   **read-all** (view); edit-own is enforced later in web maintain (D).

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

Errors and empty observations are returned as-is (`error` / `warnings` /
`value_status`) — the UI does not invent KPI numbers.

## Env reference

| Variable | Purpose |
|----------|---------|
| `CORTEX_KPI_WEB_BASE_URL` | Public origin (OAuth redirect + CORS) |
| `CORTEX_KPI_WEB_HOST` / `PORT` | Bind address (default `127.0.0.1:8080`) |
| `CORTEX_KPI_WEB_GOOGLE_CLIENT_ID` / `SECRET` | Google OAuth web client |
| `CORTEX_KPI_WEB_SESSION_SECRET` | HS256 cookie signing key |
| `CORTEX_KPI_WEB_ALLOWED_DOMAINS` | Comma-separated Workspace domains |
| `CORTEX_KPI_WEB_ALLOW_DEV_AUTH` | Explicit local/dev login (logged; not for prod) |
| `CORTEX_KPI_WEB_DEV_USER` | Default email for `/auth/dev-login` |
| `CORTEX_KPI_WEB_REGISTRY` / `OWNERS` | Alternate YAML paths (tests/dev) |
| `CORTEX_KPI_WEB_SKIP_S3` | Skip S3 pull for KPI store reads |

## Notes for web maintain (workstream D)

- Reuse `src.metrics_registry_write` and `assert_actor_may_mutate_metric` — do not
  fork write rules in the browser layer.
- Leads: edit-own only; Marc: full catalog. Topic-pack validation already lives
  in `validate_entry_ownership`.
- Prefer `POST/PATCH/DELETE /api/kpis…` with dry-run query flag mirroring CLI
  `--dry-run`; keep this view app’s read routes unchanged.
