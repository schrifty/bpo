# bpo

LangChain framework with Pendo API integration. An agent that can fetch usage and visitor data from Pendo and generate Google Slide decks per customer.

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

## Usage

**Single query:**
```bash
python main.py "Get usage data for customer acme-123 for the last 30 days"
```

**Generate slide decks:**
```bash
python main.py "Generate Google Slide decks for the last 30 days, active sites only, limit to 2 customers"
```

**Interactive mode:**
```bash
python main.py -i
```

**Custom model:**
```bash
python main.py -m "anthropic:claude-sonnet-4" "What visitors do we have from the last 7 days?"
```

## Structure

- `src/pendo_client.py` – Pendo aggregation API client
- `src/slides_client.py` – Google Slides/Drive client (creates decks per customer)
- `src/tools/pendo_tool.py` – LangChain tools:
  - `pendo_get_visitors` – Aggregate visitor data for a time range
  - `pendo_get_usage` – Usage data for a specific customer
  - `pendo_get_sites` – List sites
  - `pendo_get_usage_for_site` – Usage for a single site
  - `pendo_get_usage_by_site` – Usage grouped by site
  - `pendo_get_all_sites_usage_report` – Full sites report
  - `pendo_get_sites_by_customer` – Sites grouped by customer
  - `pendo_get_sites_with_usage` – Sites with usage metrics
  - `pendo_get_page_events` – Page view/event data
  - `pendo_get_feature_events` – Feature click/usage events
  - `pendo_get_track_events` – Custom track events (web, ios, etc.)
  - `pendo_save_usage` – Save usage data to a JSON file
  - `pendo_generate_slides` – Generate one Google Slide deck per customer
- `src/agent.py` – Agent factory with Pendo tools
- `main.py` – CLI entrypoint
- `scripts/test_slides_auth.py` – Test Google Slides/Drive auth

## Adding More Tools

Extend `get_pendo_tools()` in `src/tools/pendo_tool.py` or add new tools and pass them to `create_pendo_agent()`.
