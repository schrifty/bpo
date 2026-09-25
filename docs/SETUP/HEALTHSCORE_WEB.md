# Healthscore web app

`/healthscore` is an experimental Customer Success product served by the
existing Cortex web service. It intentionally shares only authentication,
header chrome, and deployment with `/kpis`; its framework, API namespace,
static bundle, and SQLite observations are separate.

## Current model

- Salesforce `Account.Type = 'Customer Entity'` is the customer inventory.
- Churned, cancelled, terminated, expired, and closed entities are excluded.
- The draft framework is `config/healthscore_framework.yaml`: 27 weighted
  inputs, four override flags, and 83% currently configured weight.
- ROI multiple has no source weight and remains unscored rather than receiving
  an invented value.
- The displayed score is provisional and normalized only across inputs that
  have observations. Weight coverage is always shown against the configured
  83%.
- Observations live at
  `CORTEX_CACHE_ROOT/healthscore/observations.sqlite`, separate from KPI data.

## Schema alignment with `/kpis`

The two catalogs are meant to merge, so Healthscore uses the KPI field names.

`config/healthscore_framework.yaml` components carry `description`, `owner`
(email, defaulting to the framework-level owner), `metric-id`,
`metric-generator`, `grain`, and `tags` exactly as `config/my-metrics.yaml`
does. `grain` is one of the KPI grains, or `null` for the few inputs the
source framework defines by event instead of by period (`Once per onboarding`,
`Per renewal`); those keep the prose in `cadence_note`, cannot be stored
against a period, and the UI says so rather than inventing a grain.

`healthscore_observation` mirrors `kpi_observation` — `metric_name`, `grain`,
`period_key`, `captured_at`, `value`, `generator`, `tags_json`, `meta_json`,
`error`, `as_of`, `override_value`, `override_by`, `override_at` — and adds
only `entity_id`, `entity_name`, the weighted `points`, `override_points`, and
`override_note`. Period keys come from `src.kpi_snapshot.period_key_for`, so
weekly Healthscore rows and weekly KPI rows use identical keys.

Manual entry follows the KPI override model: a human reading is written to the
`override_*` columns, shadows the generated value in the score, and never
overwrites what the generator wrote. In the detail pane, click the Points value
(or the flag value for override flags) to type an override for the latest
period; Enter saves, Escape cancels, an empty box or **Restore** clears the
override (`PUT` with `points: null, value: null`) so the generated reading shows
again.

The catalog admin from `config/kpi_owners.yaml` also sees a pencil in the
detail pane. It edits the input name, pillar, and weight through
`PUT /healthscore/api/framework/components/{key}`, which rewrites
`config/healthscore_framework.yaml` (header comments preserved) and recomputes
`configured_weight`. Other users get 403.

The first automated generator is **Usage level (breadth & depth)**, weekly,
owned by Lindsay Brown (`lindsay.brown@leandna.com`). `get_usage_level` reads
CS Report week factories, matches them to active Salesforce Customer Entities,
and scores Weekly Active Buyers % (or a named Usage component inside
`automatedHealthScores`) with the draft bands: no data=0; 0–30%=1; 31–50%=2;
51–65%=3; 66–80%=4; 81–92%=5; 93–100%=6. Join misses are warnings; they are
not scored from CSR customer lists.

**Usage trend / velocity** (`get_usage_trend`) does not read CSR again. It
uses the generated `usage_level` percent (`value`, not a points override)
over a trailing 13 ISO weeks, or fewer if the store is still filling in.
Percent change is oldest usable week → newest. `>+10%` = 5, within `±10%` =
3, `<-10%` = 0. A rise from a 0% baseline is treated as +100% (5 points).
Fewer than two numeric weeks stays unscored (`points` null) with a warning.
The Monday job runs usage_level first, then usage_trend.

### History depth

The live Monday job still reads **this week's** workbook (newest file). Daily
CS Report files in Data Exports go back months; `--history-weeks N` takes the
latest file in each of the newest *N* ISO weeks and writes `usage_level` for
those periods. Weeks with no workbook stay missing. Each workbook's `delta=week`
rows are still that week's start/end, not a nested 15-week series.

`config/jobs/healthscore-snapshot.yaml` runs weekly on EventBridge
(`cortex-healthscore-snapshot`, `cron(20 7 ? * MON *)`), after the CSR dumps
and `kpi-snapshot`. It uses the `decks` secret profile because it needs both
Google (CS Report via Drive) and Salesforce.

```bash
cortex run-job --job healthscore-snapshot --dry-run
```

```bash
cortex healthscore-snapshot --dry-run
```

Both the CLI and the job exit non-zero when CS Report or Salesforce is
unreachable. They never substitute a placeholder reading.

Authenticated UI/API: `POST /healthscore/api/generate/usage_level`.

Do not use this draft score for customer decisions until the framework,
weights, sources, and governance rules are validated.

## Routes

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthscore` | Separate Healthscore UI |
| GET | `/healthscore/api/framework` | Validated draft framework |
| GET | `/healthscore/api/entities` | Active Salesforce Customer Entities |
| GET | `/healthscore/api/entities/{id}/score` | Score, coverage, influence, history |
| PUT | `/healthscore/api/entities/{id}/components/{key}` | Record a manual observation |
| POST | `/healthscore/api/generate/usage_level` | Run the CSR usage-level generator |

All API routes require the same Google Workspace session as `/kpis`.
Salesforce inventory failures return an error; the app does not substitute
Pendo, Jira, or another customer list.

## Local verification

```bash
bin/kpi-web --dev-user marc.schriftman@leandna.com --skip-s3
```

Open `http://127.0.0.1:8080/healthscore`. The production ECS service and
CloudFront distribution require no additional path behavior because the
existing default behavior forwards both products to the same application.
