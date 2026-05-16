# LeanDNA Data API schema (BPO)

LeanDNA **Data API** — REST JSON under the resolved base URL (`resolve_leandna_data_api_base_url()` in `src/config.py`; optional `EXECUTION_ENV` with `ST_*` / `PR_*` — see **[`../SETUP/LEANDNA_SETUP.md`](../SETUP/LEANDNA_SETUP.md)**). **Auth:** Bearer (`LEANDNA_DATA_API_BEARER_TOKEN`) and/or browser **Cookie** (`LEANDNA_DATA_API_COOKIE`) plus optional `Origin`/`Referer` — same setup doc. **Optional site scope:** `RequestedSites: <comma-separated site ids>`.

Canonical registry identifiers: [`DATA_REGISTRY.md`](./DATA_REGISTRY.md) (LeanDNA Data API section). Integration analysis: [`LEANDNA_DATA_API_TOOLS.md`](./LEANDNA_DATA_API_TOOLS.md). OpenAPI: fetch with authenticated `scripts/fetch_leandna_swagger.py`.

**BPO implementation today:** `src/leandna_item_master_client.py`, `src/leandna_shortage_client.py`, `src/leandna_lean_projects_client.py`, `src/leandna_metrics_client.py` (metrics HTTP only — no QBR `report[]` merge yet), matching `*_enrich.py`, and **LangChain** ``GET /data/...`` helpers: `src/leandna_data_api_request.py`, `src/tools/leandna_data_api_tool.py` (wired via ``get_pendo_tools``); QBR wires enrichments in `src/qbr_template.py`.

---

## 1. HTTP conventions

| Concern | Detail |
|--------|--------|
| Base | `resolve_leandna_data_api_base_url()` / `LEANDNA_DATA_API_BASE_URL` in `src/config.py` (see `EXECUTION_ENV` + `ST_*` / `PR_*` in setup doc) |
| Auth | At least one of `LEANDNA_DATA_API_BEARER_TOKEN`, `LEANDNA_DATA_API_COOKIE` (optional `LEANDNA_DATA_API_ORIGIN`, `LEANDNA_DATA_API_REFERER`) — see [`../SETUP/LEANDNA_SETUP.md`](../SETUP/LEANDNA_SETUP.md) |
| JSON | Responses are JSON unless noted (Data Share returns URL metadata for Parquet) |
| Errors | Typical HTTP 401 without valid Bearer/cookie or wrong base URL; field shapes vary by tenant — validate with live calls |

---

## 2. Surfaces BPO uses in production flows

| Resource | HTTP | Report / usage |
|----------|------|----------------|
| Item Master | `GET /data/ItemMasterData` | `report["leandna_item_master"]` — aggregates (DOI backward, risk, ABC, lead-time variance, excess) |
| Shortages (weekly) | `GET /data/MaterialShortages/ShortagesByItem/Weekly` | `report["leandna_shortage_trends"]` — forecast buckets, critical timeline |
| Shortages + deliveries | `GET /data/MaterialShortages/ShortagesByItemWithScheduledDeliveries/Weekly` | Same report key — `scheduled_deliveries` summary (best-effort) |
| Lean projects | `GET /data/LeanProject` (+ query params for date range) | `report["leandna_lean_projects"]` |
| Project savings | `GET /data/LeanProject/{projectIds}/Savings` | Monthly actual/target in enrichment |
| Metric catalog | `GET /data/Metric` | Call via `leandna_metrics_client.list_metric_definitions` — not on `report` yet |
| Metric report | `GET /data/MetricReport` | Call via `leandna_metrics_client.fetch_metric_report` — not on `report` yet |
| Metric data points | `GET` / `POST` / `DELETE /data/Metric/{metricId}/MetricDataPoint` | Raw result rows (date range on GET/DELETE); **mutations** via `data_api_mutate_json` / tool `leandna_data_api_mutate` (blocked when `EXECUTION_ENV` is Production/CI — see setup doc) |
| Lean project create/update | `POST /data/LeanProject`, `PUT /data/LeanProject/{projectId}` | **Mutations** — `RequestedSites` single site on POST per OpenAPI |
| Lean project tasks/issues | `POST`/`PUT` `.../Task`, `.../Issue` | **Mutations** — bodies per OpenAPI definitions |
| Write-back transitions | `PUT /data/WriteBack/v1/TransitionActions` | **Mutation** — array of `WriteBackTransition` |

**Caching:** Drive + in-memory TTLs — `LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS`, `LEANDNA_SHORTAGE_CACHE_TTL_HOURS`, `LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS` in `src/config.py`.

**Site mapping:** Enrich functions currently pass `RequestedSites` as *all authorized sites* (`None`); customer → site id resolution is a known gap (see TODOs in `*_enrich.py`). `/data/identity` is the intended preflight for authorized sites.

---

## 3. Surfaces available in the API but not wired in BPO

| Area | Typical path pattern | Notes |
|------|----------------------|--------|
| Shortages monthly | `GET /data/MaterialShortages/ShortagesByItem/Monthly` | Not in `leandna_shortage_client.py` |
| Shortages + deliveries (daily/monthly) | `.../ShortagesByItemWithScheduledDeliveries/Daily` — Monthly | Not called |
| Shortages daily / by order | `.../ShortagesByItem/Daily`, `.../ShortagesByOrder` | **Client exists**; QBR enrich uses weekly only |
| Purchase orders | `GET /data/SupplyOrder/PurchaseOrder` | Late PO / lead-time / PPV narratives |
| Purchased inventory | `GET /data/Inventory/Purchased` | Location-level on-hand cross-check |
| Metric catalog | `GET /data/Metric` | **`src/leandna_metrics_client.list_metric_definitions`** — not on `report` yet |
| Metric report | `GET /data/MetricReport` | **`src/leandna_metrics_client.fetch_metric_report`** — not on `report` yet |
| Data Share | `GET /data/DataShare` | Signed Parquet bulk exports (CTB multi-level, POs, supplier performance, …) |
| Identity | `GET /data/identity` | User + `authorizedSites[]` for mapping |
| Write-back | `GET .../WriteBack/v1/PurchaseOrderActions`; `PUT .../WriteBack/v1/TransitionActions` | GET pending actions; **PUT** transitions via `data_api_mutate_json` / `leandna_data_api_mutate` |
| Lean project tasks | `GET /data/LeanProject/{projectId}/Tasks` | Project health |
| Lean project issues | `GET /data/LeanProject/{projectId}/Issues` | Open issue counts |
| Stage history | `GET /data/LeanProject/{projectIds}/Stage/History` | Audit of stage changes |
| Taxonomy | `GET /data/LeanProject/Areas`, `/Types`, `/Categories` | Reference lists |

Exact query parameters and response fields **follow the tenant’s OpenAPI** — this document stays stable at the resource level; use swagger for per-field truth.

---

## 4. Representative fields (high level)

### 4.1 Item Master (`/data/ItemMasterData`)

Examples BPO aggregates today: `daysOfInventoryBackward`, `daysOfInventoryForward`, `aggregateRiskScore`, `riskLevel`, `abcRank`, `leadTime`, `observedLeadTime`, `excessOnHandValue`, `ctbShortageImpactedValue`, `daysOfCoverageWorkDays`, `criticalityLevel`, `weeklyDemandStdDev`, `futureDemandDaily`, …

### 4.2 Material shortages (weekly / daily rows)

Bucket columns (`bucket1…` / `day1…`), `criticalityLevel`, `daysInShortage`, `ctbShortageImpactedValue`, PO dates (`firstPORequestedDate`, `firstPOCommitDate`), `firstImpactedOrder`, scheduled delivery fields on the **WithScheduledDeliveries** variant, etc.

### 4.3 Lean Project (list + savings)

`name`, `stage`, `state`, `startDate`, `dueDate`, `projectManager`, `sponsor`, `totalActualSavingsForPeriod`, `totalTargetSavingsForPeriod`, `isBestPractice`, `isProjectResultsValidated`, `customFieldValues`, `link`. Savings array: `month`, `actual`, `target`, `savingsCategory`, `savingsType`, `includeInTotals`, …

### 4.4 Purchase Order (unused)

`poStatus`, commit and delivery dates, `depthOfDelay`, `lateDeliveryCause`, `openPoValue`, `futurePPV`, lead time fields — see [`LEANDNA_DATA_API_TOOLS.md`](./LEANDNA_DATA_API_TOOLS.md).

### 4.5 Identity (unused)

`userId`, `customerId`, `userName`, `emailAddress`, `authorizedSites[]` with `siteId`, `siteName`, `entity`, `division`, `businessUnit`, `currencyCode`.

### 4.6 Metrics (`/data/Metric`, `/data/MetricReport`)

**Definitions:** tenant-dependent metadata (`name`, `siteId`, type, value streams/categories per swagger).

**Report:** fiscal-year object often including `metrics`, `metricValues`, `fiscalYear`, timestamps, `currency` — confirm keys against OpenAPI.

**Data points:** ``GET|POST|DELETE /data/Metric/{metricId}/MetricDataPoint`` — raw metric result rows; ``MetricDataPoint`` body fields include ``dataPointDate``, ``valueStreamId``, ``category``, ``value``, ``numeratorValue``, ``denominatorValue`` (see OpenAPI). Mutations use ``leandna_data_api_mutate`` / ``data_api_mutate_json``.

---

## 5. `config/comprehensive_data_element_list.json` paths

Logical paths for hydrate / LLM catalog (not all exist on `report` until integrated):

- **Wired:** `leandna_item_master.*`, `leandna_shortage_trends.*`, `leandna_lean_projects.*`
- **API-only (future):** `leandna_data_api.*` — see catalog entries for shortages, PO, inventory, metrics, Data Share, identity, write-back, extended Lean Project endpoints.

---

## 6. Related source files

| File | Role |
|------|------|
| `src/leandna_item_master_client.py` | Item Master HTTP + helpers |
| `src/leandna_item_master_enrich.py` | QBR `leandna_item_master` payload |
| `src/leandna_shortage_client.py` | Shortage HTTP + aggregation |
| `src/leandna_shortage_enrich.py` | QBR `leandna_shortage_trends` |
| `src/leandna_lean_projects_client.py` | Projects + savings HTTP |
| `src/leandna_lean_projects_enrich.py` | QBR `leandna_lean_projects` |
| `src/leandna_metrics_client.py` | Metric definitions + MetricReport HTTP |
| `src/data_sources/registry.py` | `SourceId` enum for LeanDNA surfaces |
