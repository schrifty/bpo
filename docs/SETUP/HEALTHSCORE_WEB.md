# Customer Health Score web app

`/healthscore` is an experimental Customer Success product served by the
existing Cortex web service. It intentionally shares only authentication and
deployment with `/kpis`; its framework, API namespace, static bundle, and
SQLite observations are separate.

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
- Each reading records the period, raw value, points, manual/automated
  provenance, note, actor, and write time.
- Observations live at
  `CORTEX_CACHE_ROOT/healthscore/observations.sqlite`, separate from KPI data.

Do not use this draft score for customer decisions until the framework,
weights, sources, and governance rules are validated.

## Routes

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthscore` | Separate Health Score UI |
| GET | `/healthscore/api/framework` | Validated draft framework |
| GET | `/healthscore/api/entities` | Active Salesforce Customer Entities |
| GET | `/healthscore/api/entities/{id}/score` | Score, coverage, influence, history |
| PUT | `/healthscore/api/entities/{id}/components/{key}` | Record a manual observation |

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
