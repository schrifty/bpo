# LeanDNA Data API connection (BPO)

BPO calls LeanDNA’s **Data API** (REST JSON under `/data/...`) for item master, material shortages, lean projects, metrics, and related QBR enrichments. This doc is the operational counterpart to **[`SALESFORCE_SETUP.md`](./SALESFORCE_SETUP.md)** — how to authenticate and which env vars matter.

**Deeper reference:** [`DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md) (resources and report keys). **Swagger:** run `scripts/fetch_leandna_swagger.py` once credentials work.

---

## What you need

You must configure **at least one** of:

| Mode | When to use |
|------|-------------|
| **Bearer token** | You have an integration / API token that LeanDNA issued for server use (`LEANDNA_DATA_API_BEARER_TOKEN`). |
| **Session cookie** | In-app calls work in the browser but Bearer returns **401** — copy the browser session cookies for the same host as `LEANDNA_DATA_API_BASE_URL`. |

You may set **both** (matches many in-app requests).

Implementation: shared headers in [`src/leandna_data_api_http.py`](../../src/leandna_data_api_http.py) (`build_leandna_data_api_headers`).

---

## Environment variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `LEANDNA_DATA_API_BASE_URL` | No | Default `https://app.leandna.com/api`. Must match where the token or session was issued (wrong host → 401). |
| `LEANDNA_DATA_API_BEARER_TOKEN` | One of Bearer or Cookie | `Authorization: Bearer …` |
| `LEANDNA_DATA_API_COOKIE` | One of Bearer or Cookie | Full **`Cookie`** header value from the browser (see below). |
| `LEANDNA_DATA_API_ORIGIN` | No | e.g. `https://app.leandna.com`. With cookie auth, defaults from the API base URL if unset. |
| `LEANDNA_DATA_API_REFERER` | No | Defaults to `{origin}/application/` if unset (browser-like request). |

### `EXECUTION_ENV` (optional): prefixed credentials

When **`EXECUTION_ENV`** is set, BPO reads **only** the matching prefixed variables (unprefixed `LEANDNA_DATA_API_*` values are **ignored** for that process):

| `EXECUTION_ENV` (case-insensitive) | Prefix | Example vars |
|-----------------------------------|--------|----------------|
| `Staging` | `ST_` | `ST_LEANDNA_DATA_API_BASE_URL`, `ST_LEANDNA_DATA_API_BEARER_TOKEN`, `ST_LEANDNA_DATA_API_COOKIE`, `ST_LEANDNA_DATA_API_ORIGIN`, `ST_LEANDNA_DATA_API_REFERER` |
| `Production` or `CI` (also `Production (CI)`, `production/ci`, etc.) | `PR_` | `PR_LEANDNA_DATA_API_BASE_URL`, … |

Any **other** non-empty value (e.g. `dev`) clears LeanDNA Data API settings so connections fail until you fix `EXECUTION_ENV` or switch to prefixed + valid staging/production.

When **`EXECUTION_ENV` is unset**, behavior is **unchanged**: use the unprefixed `LEANDNA_DATA_API_*` variables (legacy).

Implementation: [`src/config.py`](../../src/config.py) (`BPO_LEANDNA_DATA_API_EXECUTION_BUCKET`, `resolve_leandna_data_api_base_url`).

**Production / CI is read-only for LeanDNA mutations:** When `EXECUTION_ENV` is `Production` or `CI`, all Data API **POST**, **PUT**, and **DELETE** calls are rejected in-process (`data_api_mutate_json`, agent tool `leandna_data_api_mutate`). **GET** remains allowed. To run integration tests or emergency writes against prod, set `BPO_ALLOW_PRODUCTION_MUTATIONS=true` (logged; not recommended for routine use). Use `EXECUTION_ENV=Staging` for normal write testing.

The same mutation guard applies to **classic app API** writes (`set-metric`, `delete-metric-entry` via `src/leandna_app_metrics_client.py` when using the app fallback path).

### Classic app API (session auth — no Data API Bearer)

Same auth as `kpi/update-kpi`: log into the **web app** (`https://app.leandna.com` or staging), copy **`LDNASESSIONID`** from DevTools → Cookies (or set `LEANDNA_APP_SESSION_ID`).

| Variable | Purpose |
|----------|---------|
| `LEANDNA_APP_SESSION_ID` | Raw session id value |
| `LEANDNA_APP_COOKIE` | Full `Cookie` header (parsed for `LDNASESSIONID=`) |
| `LEANDNA_APP_API_SERVER` | Default `https://app.leandna.com` |
| `LEANDNA_APP_FACTORY_NDX` | Site context for `/api/2/factndx/{ndx}/…` (default `416`) |
| `LEANDNA_APP_METRICS_VIEW_QUERY` | Query string for `GET …/Metrics/View` |

CLI (from repo root, with `.env` loaded): `get-metrics-app`, `get-my-metrics-app`, `get-metrics-data-app`, `set-metric`, `delete-metric-entry-app`, `whoami-app` (see `bin/` wrappers; scripts live under `scripts/`).

`get-my-metrics-app` (script: `scripts/get-my-metrics.py`) resolves your user via `GET /api/data/identity` (session cookie), then `Metrics/View?metricOwner=…`.

Metric **`ndx`** from the app API may differ from Data API catalog **`id`**.

**Caching (optional):**

- `LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS` (default 24)
- `LEANDNA_SHORTAGE_CACHE_TTL_HOURS` (default 12)
- `LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS` (default 24)

---

## Bearer token path

1. Obtain a Data API bearer token from LeanDNA for the correct environment (prod vs staging).
2. Set `LEANDNA_DATA_API_BASE_URL` to that environment’s API root if not prod.
3. Set `LEANDNA_DATA_API_BEARER_TOKEN` in `.env` (local) or your secret store (CI/production).

---

## Session cookie path (mirror the logged-in app)

Use this when the **web app** loads data successfully but standalone Bearer fails.

1. Log into LeanDNA in Chrome (or similar) on the **same** host you use for `LEANDNA_DATA_API_BASE_URL`.
2. Open **DevTools → Network**.
3. Trigger any request whose URL contains **`/api/data/`** (or open a screen that loads Data API data).
4. Select that request → **Headers** → **Request Headers**.
5. Copy the entire **`Cookie`** value (long string; often multiple cookies separated by `; `).
6. Set in `.env`:
   - `LEANDNA_DATA_API_COOKIE=<paste>`
   - Leave Bearer empty if you rely only on cookie, or keep Bearer if your tenant sends both.

**Security:** Cookies are **session credentials**. Do **not** commit them; keep them in `.env` or a secret manager. Rotate by logging out / clearing session if leaked.

**Expiry:** Browser sessions expire; when enrichments start failing with 401, refresh the cookie from DevTools.

---

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| **401** | Wrong `LEANDNA_DATA_API_BASE_URL` for the token or cookie; expired cookie; token revoked. |
| **401 with Bearer only** | Tenant expects session-style auth — try cookie path; confirm host/staging URL. |
| Empty or partial data | Site scoping: optional `RequestedSites` header; customer→site mapping in enrich is still a known gap (see TODOs in `src/leandna_*_enrich.py` and schema doc). |

---

## Optional live integration tests

After Bearer and/or Cookie is in ``.env``, verify end-to-end reads (no export flags required):

```bash
python3 -m pytest tests/test_integration_leandna_data_api.py -v
```

Tests **skip** when credentials are missing so CI stays offline unless ``.env`` (or the job) supplies them.

Metric **display** (integration): ``tests/test-metrics.py`` — chart + field dump for metric **id 638**; **POST** then **DELETE** ``2026-05-12`` (POST failure ignored if row exists); DELETE must succeed.

## Related docs

- [`DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md) — endpoints BPO uses vs available.
- [`DATA-GOVERNANCE/LEANDNA_DATA_API_TOOLS.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_TOOLS.md) — broader integration ideas (not setup-focused).
