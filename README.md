# bpo

Automated Customer Success deck generation powered by Pendo, JIRA, and Google Slides. Generates per-customer health reviews, executive summaries, product adoption reports, and portfolio-level health decks.

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

1. Create your **QBR Generator** folder in Google Drive (or pick an existing one) and share it with `bpo-slides-account@bpo-slides.iam.gserviceaccount.com` (Editor).
2. Add to `.env`: `GOOGLE_QBR_GENERATOR_FOLDER_ID=<folder-id-from-url>`, `GOOGLE_DRIVE_OWNER_EMAIL=<your-google-email>`, `GOOGLE_APPLICATION_CREDENTIALS=<path-to-service-account.json>`.
3. **Enable domain-wide delegation** (requires Google Workspace Super Admin):
   - **GCP Console** → [IAM & Admin → Service Accounts](https://console.cloud.google.com/iam-admin/serviceaccounts?project=bpo-slides) → click `bpo-slides-account` → Details → Advanced settings → copy **Client ID** (numeric).
   - **Google Workspace Admin** → [Manage Domain Wide Delegation](https://admin.google.com/ac/owl/domainwidedelegation) → Add new → paste Client ID → add scopes: `https://www.googleapis.com/auth/drive`, `https://www.googleapis.com/auth/presentations` → Authorize.
4. Run `python scripts/test_slides_auth.py` to verify.

### JIRA (optional)

For the Support Summary slide (HELP tickets, SLAs, engineering pipeline):

Add to `.env`: `JIRA_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN`

The engineering portfolio deck (`decks engineering-portfolio` or `decks run --deck engineering-portfolio`) uses the shared Jira portfolio payload: **LEAN**-focused SDLC slides, a **LEAN** project snapshot (status and assignee charts), **Support Pressure** (HELP aggregates), and related metadata. The **implementations review** deck (`implementations_review`) is the **CUSTOMER** project snapshot only (same payload; dedicated deck). Agents can call the **`jira_project_snapshot`** tool with a project key for the same JSON payload.

## Generating Decks

**QBR (Drive template)** — explicit subcommand:

```bash
decks qbr "Customer Name"              # QBR deck from Drive template
python main.py qbr "Customer Name"     # equivalent entrypoint (same pipeline)
```

Other decks use **explicit** flags and subcommands (no LLM parsing). Some useful patterns:

```bash
# List deck ids and display names
decks --list

# One customer-scoped deck type (id from --list) — one or many customers
decks run --deck cs_health_review --customer Carrier
decks run --deck cs_health_review --customer Carrier --customer Daikin
decks run --deck product_adoption --all-customers
decks run --deck cs_health_review --all-customers --max-customers 10 --quarter prev

# Portfolio / cohort / Jira org decks
decks run --deck portfolio_review
decks cohort
decks engineering-portfolio
decks implementations-review
decks support
decks support-portfolio
decks run --deck csm_book_of_business --csm "Josh"

# Batch: every customer-scoped deck for one account, or every portfolio deck
decks --customer "Carrier" --quarter "Q1 2026" --thumbnails
decks --portfolio --max-customers 20
```

Use `decks --help` for the full command reference.

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

For a proposed cloud architecture on AWS (EC2, Lambda, or ECS Fargate), plus secrets and scheduling, see **[docs/DESIGN/PROPOSED_CLOUD_ARCH.md](docs/DESIGN/PROPOSED_CLOUD_ARCH.md)**.

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
│   ├── DATA-GOVERNANCE/     # Data governance: DATA_REGISTRY.md, DATA_DICTIONARY.md, per-source schemas, …
│   ├── DESIGN/              # Architecture / design notes (e.g. proposed cloud)
│   ├── PRESENTATION/        # Slide design standards, Pendo slide-builder narrative
│   ├── SETUP/               # Connection guides (e.g. Salesforce, LeanDNA Data API)
│   └── …                    # Other product / deck docs at docs root
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

## Deck types

Definitions live in `decks/*.yaml` (Drive can override when `GOOGLE_QBR_GENERATOR_FOLDER_ID` is set). **`decks --list`** prints every id and name, **sorted into two groups** — customer-scoped first, then portfolio / cross-customer.

### Customer-scoped decks

One deck run is built around **named account(s)** using the customer health report (Pendo-led, plus CS Report / Salesforce / Jira where configured). You can pass one customer, several, or “all customers” to mean **many separate health narratives** — each deck is still per-customer in structure.

| ID | Name |
|----|------|
| `cs_health_review` | Customer Success Health Review |
| `engineering` | Engineering Review |
| `executive_summary` | Executive Summary |
| `platform_value_summary` | Platform Value & ROI Summary |
| `product_adoption` | Product Adoption Review |
| `qbr` | Quarterly Business Review |
| `salesforce_comprehensive` | Salesforce Comprehensive Export |
| `supply_chain_review` | Supply Chain & Operations Review |
| `support` | Support Review (Jira HELP / related scope; see deck YAML for all-customer HELP options) |

### Portfolio and cross-customer decks

These use a **portfolio- or org-shaped** report (all customers, cohorts, CSM ownership slice, or Jira portfolio), not a single-account QBR arc.

| ID | Name |
|----|------|
| `cohort_review` | Manufacturing Cohort Review (`cohorts.yaml`) |
| `csm_book_of_business` | CSM Book of Business (Pendo owner filter) |
| `engineering-portfolio` | Engineering Portfolio Review (LEAN SDLC + HELP support pressure + LEAN snapshot) |
| `implementations_review` | Implementations Review (Jira CUSTOMER escalations) |
| `portfolio_review` | Portfolio Health Review |
| `support_review_portfolio` | Support Review Portfolio |

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

Customers are classified into manufacturing cohorts in `cohorts.yaml` for peer benchmarking. The system computes cohort-specific median active rates (minimum 3 members) and falls back to all-customer medians. See `docs/DATA-GOVERNANCE/CUSTOMER_COHORTS.md` for the full classification.
