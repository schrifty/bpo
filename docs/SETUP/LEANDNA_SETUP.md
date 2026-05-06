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

## Related docs

- [`DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_SCHEMA.md) — endpoints BPO uses vs available.
- [`DATA-GOVERNANCE/LEANDNA_DATA_API_TOOLS.md`](../DATA-GOVERNANCE/LEANDNA_DATA_API_TOOLS.md) — broader integration ideas (not setup-focused).
- [`LEANDNA_AND_BUNDLE_COMPLETE.md`](../LEANDNA_AND_BUNDLE_COMPLETE.md) — historical milestone notes; bundle/slide counts may be stale.
