# KPI release checklist (ship gate)

Marc’s “can we ship?” checklist for Cortex KPI surfaces (CLI + web + snapshot +
morning digest). Detail lives in sibling docs; this file is the sign-off list.

| Topic | Detail |
|-------|--------|
| Web UI / SSO / CRUD | [`KPI_WEB.md`](./KPI_WEB.md) |
| Snapshot / digest / SES / S3 | [`KPI_OPS.md`](./KPI_OPS.md) |
| Ownership config | `config/kpi_owners.yaml` |
| Registry (SoT) | `config/my-metrics.yaml` |
| Env names | `.env.example` (KPI sections) + § Env vars below |

**Salesforce boundary:** KPIs are **not** customer inventory or churn SoR.
Salesforce remains the system of record for who is a customer and contract
status. KPI ownership (`kpi_owners.yaml` / registry `owner`) is independent of
Salesforce Accounts and of LeanDNA `Metric.ownerId` (`cortex kpi mine` is
enrichment only).

**Fail loud:** empty observations, authz denials, missing digest From/To, and
missing SSO config return explicit errors — do **not** ship with fake KPI
values, empty fake catalogs, or silent digest “success.”

---

## Pre-ship checklist (tick before “ship”)

### Ownership & config

- [ ] `config/kpi_owners.yaml`: `catalog_admin` is Marc; **`leads: []` populated**
      with real lead emails + topic packs (or consciously left empty for
      Marc-only v1)
- [ ] Topic packs match how leads will tag KPIs (`engineering` / `support` /
      `akkr` or add packs intentionally)
- [ ] Every row in `config/my-metrics.yaml` has an `owner` email
- [ ] Permissions understood: Marc = full catalog; leads = **edit-own, read-all**;
      leads may set `metric-generator` / `metric-id`; topic packs constrain lead tags

### CLI

- [ ] `cortex kpi --help` / `KPI_HELP` matches how owners work day-to-day
- [ ] Actor: `--as-user` / `CORTEX_KPI_ACTOR` (default catalog_admin locally)
- [ ] List/filter: `cortex kpi --owner me` / `--me` / tags; `cortex kpi list`
- [ ] CRUD: `cortex kpi add|edit|delete|show` — unauthorized edit **fails loud**
- [ ] Values: `--mode live|stored|leandna`; stored needs snapshot DB / S3
- [ ] Optional enrichment: `cortex kpi mine` (LeanDNA) — not Cortex ownership

### Web view + maintain

- [ ] Prod: Google Workspace SSO only — **`CORTEX_KPI_WEB_ALLOW_DEV_AUTH` unset/false**
- [ ] OAuth web client + redirect `{BASE_URL}/auth/callback`; session secret set
- [ ] Allowed domains = LeanDNA Workspace (`leandna.com` unless changed)
- [ ] Unknown / out-of-domain users get **403** (no fake catalog)
- [ ] View: filter owner/tag; modes live|stored|leandna; errors/empty shown as-is
- [ ] Maintain: add/edit/delete; dry-run available; leads cannot mutate others’ KPIs
- [ ] Server-side authz only (UI may hide buttons; API enforces)

### Snapshot schedule

- [ ] Job `kpi-snapshot` persists (not dry-run) — see `config/jobs/kpi-snapshot.yaml`
- [ ] EventBridge `cortex-kpi-snapshot` **enabled** (07:15 UTC) after terraform apply
- [ ] Smoke: `cortex kpi --mode stored` (or `./bin/metrics-by-tag --all --mode stored`)
      returns expected rows after a run
- [ ] Optional: `CORTEX_KPI_STORE_S3_URI` / Terraform `kpi_store_s3_uri` — if unset,
      local/EFS only + explicit `s3=skipped_no_uri` warning (not a silent invent)

### Morning digest / SES (**open ops gate**)

Morning digest is **in v1** by product decision, but **SES DKIM is still Marc’s
ops item** — do **not** pretend this is done until the boxes below are real.

- [ ] SES From identity verified in `us-east-1`; **DKIM/DNS for send domain complete**
- [ ] Sandbox: recipients verified (or production access requested)
- [ ] Secrets: `CORTEX_METRICS_DIGEST_TO`, `CORTEX_METRICS_DIGEST_FROM` set
- [ ] Terraform: `scheduled_jobs.morning-report.enabled = true` + apply
      (rule stays **DISABLED** until then — intentional)
- [ ] Smoke: `./bin/metrics-digest --dry-run`; one real send fails loud if From/To missing

Until SES DKIM + enable: ship may include CLI + web + snapshot; **digest email is
not production-ready**. Document that gap in any “shipped” note.

### Rollback (bad registry edits)

- [ ] Owners know: registry SoT is git-tracked YAML — prefer **git revert** of
      `config/my-metrics.yaml` (and commit) over hand-editing under pressure
- [ ] Web/CLI dry-run used before risky writes
- [ ] After revert: redeploy / sync config so ECS and local match
- [ ] Observation history (SQLite) is separate — reverting YAML does not delete
      past observations; re-snapshot if needed

### Env vars

- [ ] All **required** KPI env vars for the surfaces you enable are set (see § Env
      vars) — no undocumented surprises
- [ ] Prod secrets do **not** include `CORTEX_KPI_WEB_ALLOW_DEV_AUTH=true`

### Tests / smoke matrix

- [ ] Unit suite for KPI surfaces green (commands in § Test matrix)
- [ ] Manual smoke: CLI list + one CRUD dry-run; web login + view + one dry-run add;
      snapshot write + stored read; digest dry-run (send only if SES ready)

### Security review (auth) — short notes

- [ ] Prod auth = Google Workspace SSO only; session cookie HS256-signed
- [ ] Mutate paths call `assert_actor_may_mutate_metric` (same as CLI) — not UI-only
- [ ] View requires actor in `catalog_admin` or `leads` — unknown Workspace email → 403
- [ ] Dev auth is explicit opt-in and logged — never on prod task defs / secrets
- [ ] OAuth client secret + session secret only in Secrets Manager / local env —
      never committed

### Sign-off

- [ ] Salesforce boundary acknowledged (KPIs ≠ customer SoR)
- [ ] Marc-only blockers listed (SES, OAuth client, terraform apply, `leads[]`, …)
      are either done or explicitly deferred with owner
- [ ] **Ship** decision recorded (date / who)

---

## Owner onboarding (leads)

1. Marc adds the lead to `config/kpi_owners.yaml` `leads:` with `email`,
   `display_name`, and `packs` (e.g. `[engineering]`).
2. Lead signs into KPI web with LeanDNA Google account (same email).
3. CLI (optional): set `CORTEX_KPI_ACTOR=lead@leandna.com` or pass `--as-user`.
4. Day-to-day:
   - **View all** KPIs; **edit only own**
   - Prefer tags that satisfy their topic pack (`required_any_tags`)
   - Use dry-run before first real write
5. Marc retains full catalog and ownership-config edits.

---

## User guide (pointers)

### CLI

```bash
cortex kpi --help
cortex kpi --me --mode stored          # my KPIs, stored values
cortex kpi engineering --mode live     # tag filter
cortex kpi list --owner me
cortex kpi add "Name" --owner me --tags engineering --dry-run
cortex kpi edit "Name" --description "…"
cortex kpi-snapshot --skip-s3          # local persist smoke
./bin/metrics-digest --dry-run
```

### Web

```bash
# Local only
bin/kpi-web --dev-user marc.schriftman@leandna.com --skip-s3

# Production: SSO env from KPI_WEB.md; no ALLOW_DEV_AUTH
bin/kpi-web
```

Open the app → filter → detail → Add / Edit / Delete (dry-run first).

---

## Env vars (KPI surfaces)

Documented in `.env.example`. **Required** depends on which surface you enable.

| Variable | Surface | Notes |
|----------|---------|--------|
| `CORTEX_KPI_ACTOR` | CLI | Optional; acting user when not `--as-user` |
| `CORTEX_KPI_STORE_S3_URI` | Snapshot / stored / web | Optional; EFS/local without it |
| `CORTEX_METRICS_DIGEST_TO` | Digest send | **Required to send** (fail loud if missing) |
| `CORTEX_METRICS_DIGEST_FROM` | Digest send | **Required to send** (fail loud if missing) |
| `CORTEX_KPI_WEB_BASE_URL` | Web SSO | Public origin / redirect base |
| `CORTEX_KPI_WEB_GOOGLE_CLIENT_ID` | Web SSO | **Required for prod SSO** |
| `CORTEX_KPI_WEB_GOOGLE_CLIENT_SECRET` | Web SSO | **Required for prod SSO** |
| `CORTEX_KPI_WEB_SESSION_SECRET` | Web | **Required** unless dev auth |
| `CORTEX_KPI_WEB_ALLOWED_DOMAINS` | Web | Default `leandna.com` |
| `CORTEX_KPI_WEB_HOST` / `PORT` | Web | Bind defaults `127.0.0.1:8080` |
| `CORTEX_KPI_WEB_ALLOW_DEV_AUTH` | Web local | **Never prod** |
| `CORTEX_KPI_WEB_DEV_USER` | Web local | Dev login email |
| `CORTEX_KPI_WEB_SKIP_S3` | Web / CLI store | Skip S3 pull |
| `CORTEX_KPI_WEB_REGISTRY` / `OWNERS` | Web tests/dev | Alternate YAML paths |
| `CORTEX_KPI_WEB_COOKIE_SECURE` | Web | Auto when `https` base URL |
| `CORTEX_KPI_WEB_COOKIE_NAME` | Web | Default `cortex_kpi_session` |
| `CORTEX_KPI_WEB_SESSION_TTL_SECONDS` | Web | Default `86400` |
| `CORTEX_CACHE_DIR` | Snapshot path | SQLite under `$CORTEX_CACHE_DIR/kpi/` |

Generator/live mode also needs the usual integration credentials (Jira, etc.) —
same as other Cortex jobs; not KPI-specific invent.

---

## Test matrix

### Unit (CI / local)

```bash
pytest \
  tests/test_kpi_ownership.py \
  tests/test_kpi_manage_cli.py \
  tests/test_kpi_service.py \
  tests/test_kpi_snapshot.py \
  tests/test_kpi_store.py \
  tests/test_kpi_web_api.py \
  tests/test_kpi_web_maintain.py \
  tests/test_kpi_web_situation.py \
  tests/test_metrics_registry.py \
  tests/test_metrics_digest.py \
  tests/test_kpi_metric_card.py
```

### Smoke (manual / staging)

| Check | Command / action | Expect |
|-------|------------------|--------|
| Ownership load | `cortex kpi owners` | Admin + leads list |
| CLI me filter | `cortex kpi --me --mode stored --skip-s3` | Rows or empty+warnings, no crash |
| CLI unauthorized | lead `--as-user` edit Marc KPI | Exit non-zero, YAML unchanged |
| Snapshot | `./bin/kpi-snapshot --skip-s3 --db /tmp/kpi.sqlite --tag engineering` | Persist summary |
| Stored read | `cortex kpi --all --mode stored --skip-s3 --db /tmp/kpi.sqlite` | Observations present |
| Web SSO | Login via Google on prod URL | Session; `/api/me` ok |
| Web deny | Unknown Workspace user | 403 |
| Web dry-run add | UI or `POST /api/kpis?dry_run=1` | Preview JSON; no YAML write |
| Digest dry-run | `./bin/metrics-digest --dry-run` | Body printed; no send |
| Digest missing env | send without From/To | Non-zero **before** generators |

---

## Marc-only blockers (ops outside this PR)

These are **not** closed by docs alone:

1. **SES DKIM / DNS** for the digest From domain (and sandbox/production access)
2. Populate **`leads: []`** in `config/kpi_owners.yaml` when leads should edit
3. **Google OAuth** web client (IDs/secrets) for KPI web prod
4. **`terraform apply`**: confirm `kpi-snapshot` schedule; set optional
   `kpi_store_s3_uri`; enable `morning-report` only after SES ready
5. Secrets Manager: `CORTEX_METRICS_DIGEST_*` (+ web SSO secrets if hosting web on ECS)

---

## Out of scope (this release doc)

- Broad Cortex export / deck documentation rewrite
- In-browser Python generator authoring
- LeanDNA metric creation UI
- Per-owner digest jobs
- Salesforce-tied KPI ownership
