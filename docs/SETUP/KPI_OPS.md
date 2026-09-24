# KPI persistence & morning digest ops

Cortex KPI **source of truth** for v1 is the registry YAML (`config/my-metrics.yaml`)
plus the SQLite observation store. LeanDNA upsert / `kpi mine` remain optional.
Salesforce is **not** involved in KPI inventory or ownership.

Ship gate / owner onboarding / env + smoke matrix: **[`KPI_RELEASE.md`](./KPI_RELEASE.md)**.
**SES DKIM** for morning digest remains a Marc AWS ops item until verified — see
digest section below (do not treat digest as “done” until that gate clears).

## Grain-aware snapshot (`kpi-snapshot`)

| Piece | Location |
|-------|----------|
| Job YAML | `config/jobs/kpi-snapshot.yaml` — **persists** (not dry-run) |
| EventBridge | `cortex-kpi-snapshot` — **07:15 UTC** daily (`enabled = true`) |
| Secret profile | `metrics` (integrations only) |
| Local path | `$CORTEX_CACHE_DIR/kpi/observations.sqlite` (EFS on ECS) |
| Optional S3 | `CORTEX_KPI_STORE_S3_URI=s3://bucket/key` |

### Enable / verify persistence

```bash
# Local smoke (writes SQLite only)
./bin/kpi-snapshot --skip-s3 --db /tmp/kpi.sqlite --tag engineering

# Read stored rows
./bin/metrics-by-tag --all --mode stored --skip-s3 --db /tmp/kpi.sqlite
# or: cortex kpi --all --mode stored --skip-s3
```

On ECS, the scheduled job writes to the EFS cache mount. After a successful run,
`engineering-kpis` (07:45 UTC) and `cortex kpi --mode stored` can read those rows
from the same path (or from S3 when configured).

Every registry row has a required `grain`: `hourly`, `daily`, `weekly`,
`monthly`, or `quarterly`. The snapshot caller passes that grain into the
generator context and derives the SQLite `period_key` from it. Repeated runs in
the same period replace the existing row, so a weekly KPI has one row per ISO
week and a monthly KPI has one row per closed calendar month. The current
EventBridge snapshot runs daily; an `hourly` KPI therefore needs a separate
hourly caller before it can actually collect every hour.

### S3 round-trip (optional but recommended)

Code path (already covered by unit tests):

1. Download object → local SQLite (missing object creates empty schema)
2. Upsert observations
3. Upload SQLite back to the same URI

**Enable in production:**

1. Create (or reuse) an S3 object location, e.g. `s3://your-bucket/kpi/observations.sqlite`
2. Set Terraform `kpi_store_s3_uri = "s3://your-bucket/kpi/observations.sqlite"` and `terraform apply`
   — injects `CORTEX_KPI_STORE_S3_URI` on the task definition and grants
   `s3:GetObject` / `s3:PutObject` on that object to ECS task roles
3. Optionally also put the same key in the **integrations** Secrets Manager JSON
   for local/laptop use (`.env.example` documents the name)
4. Smoke: `./bin/kpi-snapshot --tag engineering` with the URI set (no `--skip-s3`)

If the URI is **unset**, snapshot still persists locally and logs a warning
(`s3=skipped_no_uri`). It does not invent a bucket.

### Backfill months a source cannot cover

`kpi-snapshot --history-months N` walks back through closed periods. When a source
has no data that far back, the generator must **error** for that period rather than
return `0` — a stored zero is indistinguishable from a real measurement and draws a
false trend on the deck chart. `PRs Merged` follows this: zero merged PRs across all
repos means the window predates GitHub coverage, so it returns an error and the
period is stored as unavailable (charts skip it). Metrics that were genuinely zero
before adoption (Cursor spend and token KPIs) keep their zeros.

## Engineering KPIs deck (`engineering-kpis`)

The deck is exactly four pages:

1. title;
2. no more than the three largest period-over-period KPI changes;
3. separate Engineering/DevOps and Support health narratives;
4. the active Engineering/DevOps KPI list: Name, Value, Up/Down, Target, Definition.

Implementation is omitted until its registry KPIs have instrumented sources. Claude
designs pages 2–3; pages 1 and 4 are deterministic. Control it with `--claude` /
`--no-claude`, or `CORTEX_METRICS_CLAUDE_SLIDES` (default on when
`ANTHROPIC_API_KEY` is set). A Claude or parse failure **fails the job** unless
`CORTEX_METRICS_CLAUDE_ALLOW_FALLBACK=1`, which uses factual fixed-layout pages 2–3
and logs that it did.

## Morning digest (`metrics-digest` / `morning-report`)

| Piece | Location |
|-------|----------|
| Job YAML | `config/jobs/morning-report.yaml` |
| EventBridge | `cortex-morning-report` — **12:00 UTC** — **DISABLED** until SES ready |
| Env | `CORTEX_METRICS_DIGEST_TO`, `CORTEX_METRICS_DIGEST_FROM` |

**Explicit disabled state (current):** Terraform `scheduled_jobs.morning-report.enabled = false`
and catalog summary state `DISABLED`. This is intentional until SES domain DKIM/DNS
and recipients are in place (Marc AWS action).

### Production enable checklist (v1 includes digest)

1. SES in `us-east-1`: verify From identity; complete **DKIM** for `leandna.com` (or chosen domain)
2. Sandbox: also verify each recipient (or request production access)
3. Secrets (`cortex/prod/integrations`):
   - `CORTEX_METRICS_DIGEST_TO` — comma-separated recipients
   - `CORTEX_METRICS_DIGEST_FROM` — verified From address
4. Terraform: `ses_identity` / optional `ses_from_address`; set
   `scheduled_jobs.morning-report.enabled = true`; `terraform apply`
5. Local smoke: `./bin/metrics-digest --dry-run`
6. One-shot send (fails loud if From/To missing): `./bin/metrics-digest` (no `--dry-run`)

When send is requested without From/To, `metrics-digest` **exits non-zero before
running generators** (`SesEmailError` / `CORTEX_METRICS_DIGEST_*` message).

## Failure visibility

Existing schedule alarms (when `enable_schedule_alarms = true`) cover:

- CloudWatch metric filter on `CORTEX_RUN_SUMMARY` with `"success":false`
- EventBridge `FailedInvocations` per enabled scheduled job (including `kpi-snapshot`
  and `morning-report` once enabled)
- ECS task stop / failed-to-start → SNS

Morning digest also opens with last night’s job outcomes (CloudWatch), so a failed
`kpi-snapshot` shows up in the email once digest is enabled.

## Registry rollback

Catalog CRUD (CLI or web) edits `config/my-metrics.yaml`. To undo a bad change:

1. Prefer `git revert` / restore that file from a known-good commit
2. Redeploy or sync config so running jobs and KPI web see the restored file
3. SQLite observations are independent — re-run `kpi-snapshot` if history should
   reflect the restored definitions

Dry-run (`cortex kpi … --dry-run`, web `?dry_run=1`) avoids writing until ready.

## Related commands

```bash
cortex run-job --job kpi-snapshot --dry-run   # print argv only
cortex run-job --job kpi-snapshot             # persist
cortex run-job --job morning-report --dry-run
./bin/metrics-digest --dry-run
./bin/kpi-snapshot --dry-run                  # generate, do not write
```
