# Healthscore web app

`/healthscore` is an experimental Customer Success product served by the
existing Cortex web service. It intentionally shares only authentication,
header chrome, and deployment with `/kpis`; its framework, API namespace,
static bundle, and SQLite observations are separate.

## Current model

- Salesforce `Account.Type = 'Customer Entity'` is the customer inventory.
- Churned, cancelled, terminated, expired, and closed entities are excluded.
- The draft framework is `config/healthscore_framework.yaml`: 28
  inputs, four override flags, and 83% currently configured weight.
- ROI multiple and Enhancement Engagement & Delivery have no source weight
  and stay out of the weighted score rather than receiving an invented value.
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

Every component `description` states what is measured, where the number comes
from, over what window, and how it becomes points, in plain language. It has
to be specific enough to write a generator from and readable by someone
outside engineering. When a source or threshold is not yet defined, the
description says so rather than implying the data exists.

The component list has two separate columns. **Source** is `data_source`, a
list, so a component can name more than one system. A chip is green when that
label matches a system in `config/data_source_registry.yaml` (a system Cortex
can read). Any other label is red, including sources that
are named but not connected yet, such as Verified-outcome log. **Status** is
`automation`: Automated, Manual, or Blocked.

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

**Champion login continuity** (`get_champion_login_continuity`) reads the
Salesforce Customer Entity Account executive sponsor (`Executive_Sponsor__c`,
plus email or first name when the lookup is blank) and
`Executive_Sponsor_Last_Login__c`. The stored value is days since that login.
Last login within 7 days scores 2; a named sponsor with no login in that
window scores 0. An Account with no named executive sponsor stays unscored
and is reported as a warning. HubSpot buying-role "Champion" contacts are not
used; Salesforce does not store a login time for them.

**ROI multiple** (`get_roi_multiple`) divides the CS Report's previous-period
inventory-action savings (`inventoryActionPreviousReportingPeriodSavings`),
summed across the entity's factories, by the Customer Entity's Salesforce
`ARR__c`. 7x or more scores 6, from 5 up to 7 scores 4, from 3 up to 5 scores
2, and under 3 scores 0. A missing CS Report match, missing savings, or
missing or non-positive ARR stays unscored. The component still has no
weight, so the reading does not enter the weighted score.

**Summit attendance** (`get_summit_attendance`) reads Salesforce Campaign
Members on Manufacturing Excellence Summit campaigns from the trailing 12
months, plus any upcoming one. Before the campaign start date, Registered
counts. On and after that date, only Attended counts. A contact on the
Customer Entity counts for that entity. A contact on the parent Customer
account counts for that parent's entities. Yes scores 1, no scores 0. The
campaign end date is ignored. If a past summit still has registrations and
no Attended members, the run warns and scores those entities 0.

**Enhancement Engagement & Delivery** (`get_enhancement_engagement`) counts
Aha Product Ideas created in the trailing 12 months. R is ideas submitted by
a customer portal user. D is the subset whose workflow status is Shipped.
The score is `min(10, 3 * min(R, 5) / 5 + 7 * D / R)`, rounded to one decimal.
No requests is N/A. The portal user's email domain matches an Aha idea
organization's `email_domains`, and that organization's Salesforce Account id
is applied to every active Customer Entity under that parent. `leandna.com`
submitters are excluded. Already exists and Will not implement stay in R and
out of D. The component has no weight, so the reading does not enter the
weighted score.

**SLA adherence** (`get_sla_adherence`) uses the same Jira Service Management
HELP definition as the KPI catalog metric SLA Adherence (30 Days): among
tickets resolved in the trailing 30 days with a completed time-to-first-response
or time-to-resolution cycle, the percent that did not breach. Tickets are
attributed by JSM organization. 90% or more scores 3, below that scores 0.
No organization match or no measured cycle stays unscored.

### History depth

The live Monday job still reads **this week's** workbook (newest file). Daily
CS Report files in Data Exports go back months; `--history-weeks N` takes the
latest file in each of the newest *N* ISO weeks and writes `usage_level` for
those periods. Weeks with no workbook stay missing. Each workbook's `delta=week`
rows are still that week's start/end, not a nested 15-week series.

With `--component usage_trend` (or `all`), `--history-weeks N` also scores
`usage_trend` once per ISO week for the newest *N* weeks. Because each trend
week reads the trailing 13 weeks of `usage_level`, the run first extends
`usage_level` back `N + 12` weeks from whatever workbooks Drive still holds;
if Drive runs out sooner the run warns and the oldest trend weeks score on a
shorter baseline (`meta.weeks_used` says how many weeks fed each reading). A
week with no workbook still gets a trend reading from the surrounding history
(`meta.current_period` names the week actually used).

Reruns are idempotent: weeks that already have readings are skipped and their
workbooks are not downloaded again. Pass `--refresh-history` to recompute them.

```bash
cortex healthscore-snapshot --component usage_trend --history-weeks 26 --dry-run
```

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

Authenticated UI/API: `POST /healthscore/api/generate/usage_level`,
`POST /healthscore/api/generate/usage_trend`, and
`POST /healthscore/api/generate/champion_login_continuity`.

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
| POST | `/healthscore/api/generate/usage_trend` | Run the usage-trend generator |
| POST | `/healthscore/api/generate/champion_login_continuity` | Run the executive-sponsor login generator |
| POST | `/healthscore/api/generate/roi_multiple` | Run the savings / ARR generator |
| POST | `/healthscore/api/generate/summit_attendance` | Run the Summit registration / attendance generator |
| POST | `/healthscore/api/generate/enhancement_engagement` | Run the Aha enhancement engagement generator |
| POST | `/healthscore/api/generate/meeting_cadence` | Run the Chorus meeting cadence generator |
| POST | `/healthscore/api/generate/sla_adherence` | Run the JSM HELP SLA adherence generator |
| POST | `/healthscore/api/generate/call_sentiment` | Run the Chorus call-sentiment generator |

**Call sentiment / tone** (`get_call_sentiment`) scores finished Chorus meetings
and dials in the trailing 30 days. Chorus has no numeric sentiment field. The
generator reads the "overall sentiment was …" sentence in each recording's
summary. Positive language is +1, negative language is -1, and a clause with
both or neither is 0. The reading is the mean. Above 0 scores 2, below 0 scores
0, and exactly 0 scores 1. Recordings without that sentence are left out. An
entity with no recorded call in the window scores 0. `--history-months N`
writes a 30-day window ending on each of the newest N month-ends and skips
periods already stored.

```bash
cortex healthscore-snapshot --component call_sentiment --history-months 6
```

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
