# LeanDNA Data API connection (Cortex)

Cortex calls LeanDNA’s **Data API** (REST JSON under `/data/...`) for item master, material shortages, lean projects, metrics, and related QBR enrichments. This doc is the operational counterpart to **[`SALESFORCE_SETUP.md`](./SALESFORCE_SETUP.md)** — how to authenticate and which env vars matter.

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

When **`EXECUTION_ENV`** is set, Cortex reads **only** the matching prefixed variables (unprefixed `LEANDNA_DATA_API_*` values are **ignored** for that process):

| `EXECUTION_ENV` (case-insensitive) | Prefix | Example vars |
|-----------------------------------|--------|----------------|
| `Staging` | `ST_` | `ST_LEANDNA_DATA_API_BASE_URL`, `ST_LEANDNA_DATA_API_BEARER_TOKEN`, `ST_LEANDNA_DATA_API_COOKIE`, `ST_LEANDNA_DATA_API_ORIGIN`, `ST_LEANDNA_DATA_API_REFERER` |
| `Production` or `CI` (also `Production (CI)`, `production/ci`, etc.) | `PR_` | `PR_LEANDNA_DATA_API_BASE_URL`, … |

Any **other** non-empty value (e.g. `dev`) clears LeanDNA Data API settings so connections fail until you fix `EXECUTION_ENV` or switch to prefixed + valid staging/production.

When **`EXECUTION_ENV` is unset**, behavior is **unchanged**: use the unprefixed `LEANDNA_DATA_API_*` variables (legacy).

Implementation: [`src/config.py`](../../src/config.py) (`CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET`, `resolve_leandna_data_api_base_url`).

**Production / CI is read-only for LeanDNA mutations:** When `EXECUTION_ENV` is `Production` or `CI`, all Data API **POST**, **PUT**, and **DELETE** calls are rejected in-process (`data_api_mutate_json`, agent tool `leandna_data_api_mutate`). **GET** remains allowed. To run integration tests or emergency writes against prod, set `CORTEX_ALLOW_PRODUCTION_MUTATIONS=true` (logged; not recommended for routine use). Use `EXECUTION_ENV=Staging` for normal write testing.

The same mutation guard applies to **Data API** writes (`entry-insert`, `entry-upsert`, `entry-delete` via `src/leandna_metrics_write.py`).

CLI (from repo root, with `.env` loaded): `metrics-get`, `metrics-get-mine`, `metrics-get-latest`, `metric-get-with-data`, `entry-insert`, `entry-upsert`, `entry-delete`, `metrics-upsert`, and **`decks metrics-upsert`** (see `bin/` wrappers; scripts live under `scripts/`).

`metrics-get-mine` resolves your user via Data API `GET /data/identity`, then filters `GET /data/Metric` by `ownerId`.

**Automated daily values:** ``config/my-metrics.yaml`` lists owned metrics. Rows with a non-null ``metric-generator`` are updated by::

```bash
decks metrics-upsert --dry-run          # generate only
decks metrics-upsert                    # upsert for today (use EXECUTION_ENV=Staging for writes)
decks metrics-upsert --metric "KPI Automation %"
```

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

Tests **skip** when credentials are missing and **fail** when ``EXECUTION_ENV=Production`` (or CI).
Set ``EXECUTION_ENV=Staging`` with ``ST_LEANDNA_DATA_API_*`` in ``.env`` so CI stays offline unless
configured for staging.

```bash
python3 -m pytest tests/test_integration_leandna_data_api.py tests/test-metrics.py -v -m leandna_data_api
```

Metric **display** (integration): ``tests/test-metrics.py`` — chart + field dump for metric **id 638**; **POST** then **DELETE** ``2026-05-12`` (POST failure ignored if row exists); DELETE must succeed. Mutations run on staging without ``CORTEX_ALLOW_PRODUCTION_MUTATIONS``.

## Related docs

- [`DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md) — endpoints Cortex uses vs available.
- [`DATA-GOVERNANCE/LEANDNA_DATA_API_TOOLS.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_TOOLS.md) — broader integration ideas (not setup-focused).
