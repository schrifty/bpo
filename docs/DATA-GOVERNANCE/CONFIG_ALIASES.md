# Config alias and cohort files

All customer alias maps and cohort classification live under **`config/`**. Code loads paths from `src/config_paths.py`.

## List alias maps (same schema)

Top-level keys are **case-insensitive** customer keys (`--customer`, Pendo prefix, or Salesforce rollup label). Values are **lists of search strings** merged into lookups.

| File | Used by |
|------|---------|
| `config/jsm_organization_aliases.yaml` | Jira HELP — JSM `Organizations` fuzzy match |
| `config/cs_report_customer_aliases.yaml` | CS Report — `customer` column lookup |
| `config/slack_customer_aliases.yaml` | Slack — channel name fragments |
| `config/sf_portfolio_pendo_aliases.yaml` | Portfolio — SF portfolio label → Pendo prefix |

Example:

```yaml
JCI:
  - Johnson Controls
  - Johnson Controls International
```

## Other config maps

| File | Schema |
|------|--------|
| `config/customer_identity_map.yaml` | Per customer: `salesforce_account_id` or `salesforce_account_ids` (+ optional `salesforce_primary_account_id`) |
| `config/cohorts.yaml` | Top-level `cohorts:` — per-prefix `name`, `cohort`, `aliases`, `exclude`, etc. (see [CUSTOMER_COHORTS.md](./CUSTOMER_COHORTS.md)) |
| `config/pendo_orphans.yaml` | Pendo prefixes to **exclude** from portfolio rollup (not aliases) |

Salesforce remains the system of record for customer inventory and churn; these files are adjuncts for cross-system joins and presentation.

## CLI: `match-companies`

```bash
./bin/match-companies
./bin/match-companies --format json --out output/match-companies.json
```

Lists every Salesforce portfolio label grouped by contract status (active, churned, renewal in negotiation) and shows resolved Pendo, CS Report, and JSM names plus alias provenance when a match used a YAML map.
