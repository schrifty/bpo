# LeanDNA Data API connection (Cortex)

Cortex calls LeanDNA’s **Data API** (REST JSON under `/data/...`) for item master, material shortages, lean projects, metrics, and related QBR enrichments. This doc is the operational counterpart to **[`SALESFORCE_SETUP.md`](./SALESFORCE_SETUP.md)** — how to authenticate and which env vars matter.

**Deeper reference:** [`DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md) (resources and report keys). **Swagger:** run `scripts/fetch_leandna_swagger.py` once credentials work.

---

## What you need

You must configure **at least one** of:

| Mode | When to use |
|------|-------------|
| **API key** | Permanent secret from LeanDNA **My Account** (`LEANDNA_DATA_API_API_KEY` / `PR_` / `ST_`). Cortex calls Auth `POST /auth/data/session` with `{"secret": <key>}` and uses the returned `sessionId` as Bearer. Sessions last about an hour; Cortex refreshes automatically. |
| **Bearer token** | Fallback only: a short-lived session id pasted as `LEANDNA_DATA_API_BEARER_TOKEN`. Prefer the API key. |
| **Session cookie** | In-app calls work in the browser but API key is unavailable — copy the browser session cookies for the same host as `LEANDNA_DATA_API_BASE_URL`. |

Implementation: shared headers in [`src/leandna_data_api_http.py`](../../src/leandna_data_api_http.py); Auth exchange in [`src/leandna_auth_session.py`](../../src/leandna_auth_session.py).

---

## Environment variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `LEANDNA_DATA_API_BASE_URL` | No | Default `https://app.leandna.com/api`. Must match the environment that issued the API key (wrong host → 401). |
| `LEANDNA_DATA_API_API_KEY` | Preferred | Permanent Data API secret. Auth API on the same host (`…/auth/data/session`). |
| `LEANDNA_DATA_API_BEARER_TOKEN` | Fallback | `Authorization: Bearer …` session id (expires ~1 hour). Unused when API key is set. |
| `LEANDNA_DATA_API_COOKIE` | Optional | Full **`Cookie`** header from the browser. Unused when API key is set. |
| `LEANDNA_DATA_API_ORIGIN` | No | e.g. `https://app.leandna.com`. With cookie auth, defaults from the API base URL if unset. |
| `LEANDNA_DATA_API_REFERER` | No | Defaults to `{origin}/application/` if unset (browser-like request). |

### `EXECUTION_ENV` (optional): prefixed credentials

When **`EXECUTION_ENV`** is set, Cortex reads **only** the matching prefixed variables (unprefixed `LEANDNA_DATA_API_*` values are **ignored** for that process):

| `EXECUTION_ENV` (case-insensitive) | Prefix | Example vars |
|-----------------------------------|--------|----------------|
| `Staging` | `ST_` | `ST_LEANDNA_DATA_API_BASE_URL`, `ST_LEANDNA_DATA_API_API_KEY`, `ST_LEANDNA_DATA_API_BEARER_TOKEN`, `ST_LEANDNA_DATA_API_COOKIE`, `ST_LEANDNA_DATA_API_ORIGIN`, `ST_LEANDNA_DATA_API_REFERER` |
| `Production` or `CI` (also `Production (CI)`, `production/ci`, etc.) | `PR_` | `PR_LEANDNA_DATA_API_BASE_URL`, … |

Any **other** non-empty value (e.g. `dev`) clears LeanDNA Data API settings so connections fail until you fix `EXECUTION_ENV` or switch to prefixed + valid staging/production.

When **`EXECUTION_ENV` is unset**, behavior is **unchanged**: use the unprefixed `LEANDNA_DATA_API_*` variables (legacy).

Implementation: [`src/config.py`](../../src/config.py) (`CORTEX_LEANDNA_DATA_API_EXECUTION_BUCKET`, `resolve_leandna_data_api_base_url`).

Data API **POST**, **PUT**, and **DELETE** (metric datapoints, Lean projects, write-back) are allowed whenever credentials resolve, including `EXECUTION_ENV=Production`. Use `EXECUTION_ENV=Staging` with `ST_*` credentials when you intend to write to staging instead of production.

**Data API** writes include `entry-insert`, `entry-upsert`, `entry-delete`, and `metrics-upsert` (`src/leandna_metrics_write.py`).

CLI (from repo root, with `.env` loaded): `metrics-get`, `metrics-get-mine`, `metrics-get-latest`, `metric-get-with-data`, `entry-insert`, `entry-upsert`, `entry-delete`, `metrics-upsert`, and **`decks metrics-upsert`** (see `bin/` wrappers; scripts live under `scripts/`).

`metrics-get-mine` resolves your user via Data API `GET /data/identity`, then filters `GET /data/Metric` by `ownerId`.

**Automated daily values:** ``config/my-metrics.yaml`` lists owned metrics. Rows with a non-null ``metric-generator`` are updated by::

```bash
decks metrics-upsert --dry-run          # generate only
decks metrics-upsert                    # upsert for today (use EXECUTION_ENV=Staging for writes)
decks metrics-upsert --metric "KPI Automation %"
```

**Morning KPI digest email:** EventBridge job ``morning-report`` (12:00 UTC) runs ``metrics-digest``, which live-generates every registry row with a generator, compares to ``target`` / ``direction``, and emails via SES. Set ``CORTEX_METRICS_DIGEST_TO`` and ``CORTEX_METRICS_DIGEST_FROM`` in `.env` / Secrets Manager (see ``infra/terraform/README.md``). Local smoke::

```bash
./bin/metrics-digest --dry-run
```

**Caching (optional):**

- `LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS` (default 24)
- `LEANDNA_SHORTAGE_CACHE_TTL_HOURS` (default 12)
- `LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS` (default 24)

---

## API key path (recommended)

1. Create a Data API key on the LeanDNA [account page](https://www.leandna.com/application/f/home.html#/account/edit) (secret is shown once).
2. Set `PR_LEANDNA_DATA_API_API_KEY` (Production) or `ST_LEANDNA_DATA_API_API_KEY` (Staging), plus the matching `*_BASE_URL`.
3. Cortex exchanges the key for a session via `POST https://<host>/auth/data/session` with JSON `{"secret":"<identifier>:<passcode>"}` before Data API calls. You do **not** need to paste a Bearer session into `.env`.

The secret is **two parts joined by a colon** (shown once when the key is created). A 52-character session id is not an API key.

Auth OpenAPI UI: `https://app.leandna.com/application/apidocs/dist/index.html?urls.primaryName=Auth`.

## Bearer token path (legacy)

1. Only if you cannot store the API key: paste a current session id as `LEANDNA_DATA_API_BEARER_TOKEN`.
2. Set `LEANDNA_DATA_API_BASE_URL` to that environment’s API root if not prod.
3. Expect **401 Session not found** after about an hour — switch to the API key.

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
| **401** | Wrong `LEANDNA_DATA_API_BASE_URL` for the API key; expired cookie; invalid API key. |
| **401 Session not found** | Stale pasted Bearer — set `*_LEANDNA_DATA_API_API_KEY` instead of a session token. |
| Empty or partial data | Site scoping: optional `RequestedSites` header; customer→site mapping in enrich is still a known gap (see TODOs in `src/leandna_*_enrich.py` and schema doc). |

---

## Optional live integration tests

Tests **skip** when credentials are missing and **fail** when ``EXECUTION_ENV=Production`` (or CI).
Set ``EXECUTION_ENV=Staging`` with ``ST_LEANDNA_DATA_API_*`` in ``.env`` so CI stays offline unless
configured for staging.

```bash
python3 -m pytest tests/test_integration_leandna_data_api.py tests/test-metrics.py -v -m leandna_data_api
```

Metric **display** (integration): ``tests/test-metrics.py`` — chart + field dump for metric **id 638**; **POST** then **DELETE** ``2026-05-12`` (POST failure ignored if row exists); DELETE must succeed. Prefer ``EXECUTION_ENV=Staging`` so those writes hit staging.

## Related docs

- [`DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md) — endpoints Cortex uses vs available.
- [`DATA-GOVERNANCE/LEANDNA_DATA_API_TOOLS.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_TOOLS.md) — broader integration ideas (not setup-focused).
