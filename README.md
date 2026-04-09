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

The engineering portfolio deck (`decks eng portfolio`) loads **HELP**, **CUSTOMER**, and **LEAN** project snapshots (open totals, status mix, ages, assignee resolve table). Agents can call the **`jira_project_snapshot`** tool with a project key for the same JSON payload.

## Generating Decks

The `decks` command takes a natural-language prompt — no flags to memorize:

```bash
# Single customer
decks health review for Carrier

# Multiple specific customers
decks health review for Carrier, Daikin, and Siemens

# All active customers (default)
decks health review for all customers

# Quarter control
decks health review, Q4 2025
decks health review for last quarter
decks product adoption for Carrier, 60 day lookback

# Cap the run
decks health review, max 5 customers

# Portfolio (book of business — single cross-customer deck)
decks portfolio review

# With thumbnails
decks health review for Bombardier, with thumbnails

# See all available deck types
decks --list
```

The prompt is parsed by a lightweight LLM call (`gpt-4o-mini`) that extracts deck type, customers, quarter, lookback days, max, workers, and thumbnail preference. Anything not specified uses smart defaults (auto-detected quarter, all customers, 4 workers, no thumbnails).

### Evaluating Custom Slides

CSMs can submit custom slides for automation by sharing a Google Slides deck with the intake Google Group. Set `GOOGLE_HYDRATE_INTAKE_GROUP` in `.env` to that group’s email **exactly** as it appears in Share (e.g. `hydrate-deck@leandna.com`). Viewer or Editor on the group both work. The service account must use an identity that can see those files (e.g. domain-wide delegation to a user who is in that group). Then:

```bash
decks --evaluate            # assess each slide
decks --evaluate --verbose  # include full extracted text
decks hydrate               # same intake sources; fills live data
```

The evaluator exports a thumbnail of each slide, extracts text and layout structure, then uses GPT-4o vision to assess reproducibility against current data sources and slide-building capabilities. Output includes feasibility rating, data gaps, visual element analysis, effort estimate, and the closest existing slide type.

### Drive Config Sync

Deck definitions and slides can be edited on Google Drive so non-developers can customize them. To push local configs to Drive:

```bash
decks --sync-config
decks --sync-config --sync-overwrite
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

## Deploying to AWS

To run on AWS (EC2, Lambda, or ECS Fargate), see **[docs/AWS_DEPLOYMENT.md](docs/AWS_DEPLOYMENT.md)** for options, secrets setup, and scheduling.

## Structure

```
bpo/
├── decks/                    # Deck definitions (YAML) — what slides to include per audience
│   ├── cs-health-review.yaml
│   ├── executive-summary.yaml
│   ├── product-adoption.yaml
│   └── portfolio-review.yaml
├── slides/                   # Slide definitions (YAML) — individual slide configs
│   ├── qbr-01-cover.yaml     # QBR primary deck filenames (qbr-01 … qbr-18 …)
│   ├── qbr-05-health.yaml
│   ├── ...
│   └── qbr-18-data-quality.yaml
│   (other prefixes: std-*, cohort-*, eng-*, … for non-QBR decks)
├── cohorts.yaml              # Customer manufacturing cohort classifications
├── docs/
│   ├── data-schema/          # Data registry + per-source schemas (Jira, Pendo, CS Report, …)
│   └── CUSTOMER_COHORTS.md   # Cohort research documentation
├── src/
│   ├── config.py             # Environment config
│   ├── pendo_client.py       # Pendo API client (health, engagement, features, benchmarks)
│   ├── jira_client.py        # JIRA API client (tickets, SLAs, engineering pipeline)
│   ├── slides_client.py      # Google Slides/Drive client (slide builders, deck generation)
│   ├── deck_loader.py        # Deck definition loader (Drive-first with local fallback)
│   ├── slide_loader.py       # Slide loader (Drive-first with local fallback)
│   ├── drive_config.py       # Drive sync for editable configs
│   ├── qa.py                 # Data quality registry (cross-source validation)
│   ├── agent.py              # LangChain agent factory
│   └── tools/
│       ├── pendo_tool.py     # LangChain tools (Pendo, decks, CS report, …)
│       └── jira_tool.py      # `jira_project_snapshot` (HELP / CUSTOMER / LEAN metrics)
├── decks.py              # CLI for batch deck generation
└── main.py                   # CLI for interactive agent mode
```

## Deck Types

| ID | Name | Audience | Slides |
|----|------|----------|--------|
| `qbr` | Quarterly Business Review | Customer Success — QBR | 22 slides (`decks/qbr.yaml`, `slides/qbr-*.yaml`) |
| `cs_health_review` | Customer Success Health Review | CSMs | 21 slides — full account picture (`slides/cs-health-*.yaml`) |
| `engineering` | Engineering Review | Engineering / Product | 7 slides (`slides/eng-review-*.yaml`) |
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
