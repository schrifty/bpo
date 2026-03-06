# bpo

Automated Customer Success deck generation powered by Pendo, JIRA, and Google Slides. Generates per-customer health reviews, executive summaries, product adoption reports, and portfolio-level book-of-business decks.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # then edit with your keys
```

Config is in `src/config.py` (reads from env).

**Required:** `PENDO_INTEGRATION_KEY`, `OPENAI_API_KEY`

**Optional:** `PENDO_BASE_URL`, `PENDO_MAX_RESULTS`, `PENDO_MAX_OUTPUT_CHARS`, `LOG_LEVEL`

### Google Slides (domain-wide delegation)

To create slide decks in your Drive (using your quota instead of the service account's 15 GB):

1. Create a folder in your Google Drive and share it with `bpo-slides-account@bpo-slides.iam.gserviceaccount.com` (Editor).
2. Add to `.env`: `GOOGLE_DRIVE_FOLDER_ID=<folder-id-from-url>`, `GOOGLE_DRIVE_OWNER_EMAIL=<your-google-email>`, `GOOGLE_APPLICATION_CREDENTIALS=<path-to-service-account.json>`.
3. **Enable domain-wide delegation** (requires Google Workspace Super Admin):
   - **GCP Console** → [IAM & Admin → Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts?project=bpo-slides) → click `bpo-slides-account` → Details → Advanced settings → copy **Client ID** (numeric).
   - **Google Workspace Admin** → [Manage Domain Wide Delegation](https://admin.google.com/ac/owl/domainwidedelegation) → Add new → paste Client ID → add scopes: `https://www.googleapis.com/auth/drive`, `https://www.googleapis.com/auth/presentations` → Authorize.
4. Run `python scripts/test_slides_auth.py` to verify.

### JIRA (optional)

For the Support Summary slide (HELP tickets, SLAs, engineering pipeline):

Add to `.env`: `JIRA_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN`

## Generating Decks

```bash
# Single customer
python run_decks.py cs_health_review --customers Carrier --days 30

# Multiple specific customers
python run_decks.py cs_health_review --customers Carrier Daikin Siemens --days 30

# All active customers
python run_decks.py cs_health_review --days 30

# Different deck type
python run_decks.py executive_summary --customers Carrier
python run_decks.py product_adoption --customers Carrier --days 60

# Portfolio (book of business — single cross-customer deck)
python run_decks.py portfolio_review --days 30

# See all available deck types
python run_decks.py --list
```

**Useful flags:**

| Flag | Description |
|------|-------------|
| `--days 30` | Lookback window (default: 30) |
| `--max 5` | Cap number of customers (good for testing) |
| `--workers 2` | Parallel threads (default: 4, reduce if rate-limited) |
| `--customers X Y Z` | Generate only for named customers |

### Drive Config Sync

Deck definitions and recipes can be edited on Google Drive so non-developers can customize them. To push local configs to Drive:

```bash
# Upload new files (won't overwrite existing)
python run_decks.py --sync-config

# Overwrite Drive files with local versions
python run_decks.py --sync-config --sync-overwrite
```

After syncing, the app reads from Drive first and falls back to local files if a Drive file has errors. Parse failures are surfaced on the Data Quality slide.

## Interactive Agent Mode

```bash
# Single query
python main.py "Get usage data for customer Carrier for the last 30 days"

# Interactive mode
python main.py -i

# Custom model
python main.py -m "anthropic:claude-sonnet-4" "Generate a health review for Daikin"
```

## Structure

```
bpo/
├── decks/                    # Deck definitions (YAML) — what slides to include per audience
│   ├── cs-health-review.yaml
│   ├── executive-summary.yaml
│   ├── product-adoption.yaml
│   └── portfolio-review.yaml
├── recipes/                  # Slide recipes (YAML) — individual slide definitions
│   ├── std-01-title.yaml
│   ├── std-02-health.yaml
│   ├── ...
│   └── std-99-data-quality.yaml
├── cohorts.yaml              # Customer manufacturing cohort classifications
├── docs/
│   └── CUSTOMER_COHORTS.md   # Cohort research documentation
├── src/
│   ├── config.py             # Environment config
│   ├── pendo_client.py       # Pendo API client (health, engagement, features, benchmarks)
│   ├── jira_client.py        # JIRA API client (tickets, SLAs, engineering pipeline)
│   ├── slides_client.py      # Google Slides/Drive client (slide builders, deck generation)
│   ├── deck_loader.py        # Deck definition loader (Drive-first with local fallback)
│   ├── recipe_loader.py      # Recipe loader (Drive-first with local fallback)
│   ├── drive_config.py       # Drive sync for editable configs
│   ├── qa.py                 # Data quality registry (cross-source validation)
│   ├── agent.py              # LangChain agent factory
│   └── tools/
│       └── pendo_tool.py     # LangChain tools for agent mode
├── run_decks.py              # CLI for batch deck generation
└── main.py                   # CLI for interactive agent mode
```

## Deck Types

| ID | Name | Audience | Slides |
|----|------|----------|--------|
| `cs_health_review` | Customer Success Health Review | CSMs | 14 slides — full account picture |
| `executive_summary` | Executive Summary | Leadership | 7 slides — high-signal only |
| `product_adoption` | Product Adoption Review | Product | 10 slides — feature/behavioral focus |
| `portfolio_review` | Book of Business Review | CS Leadership | 5 slides — cross-customer |

## Data Quality

Every deck ends with a Data Quality slide that reports the results of automated cross-source validation. Checks include:

- JIRA status/priority/type breakdowns sum to total issue count
- JIRA open + resolved = total
- SLA measured + waiting <= HELP ticket count
- Pendo engagement buckets sum to total visitors
- Active rate consistent with raw numbers
- Customer exists in cohorts.yaml (warns if missing)
- Unverified cohort classifications flagged
- Site count consistency between health summary and detail
- Drive config parse failures with local fallback

When all checks pass, the slide shows a green "All checks passed" message. Discrepancies show as errors (red) or warnings (amber) with expected vs. actual values.

## Cohort Benchmarking

Customers are classified into manufacturing cohorts in `cohorts.yaml` for peer benchmarking. The system computes cohort-specific median active rates (minimum 3 members) and falls back to all-customer medians. See `docs/CUSTOMER_COHORTS.md` for the full classification.
