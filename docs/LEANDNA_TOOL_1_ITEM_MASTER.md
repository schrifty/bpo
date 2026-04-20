# Tool #1: LeanDNA Item Master Data Integration

## Overview

Integrate the **LeanDNA Item Master Data API** (`/data/ItemMasterData`) to enrich BPO's supply chain and platform health reporting with item-level intelligence currently missing from the CS Report XLSX export.

---

## Tools to Create

### 1. **`leandna_item_master_client`** (Client Module)

**File:** `src/leandna_item_master_client.py`

**Purpose:** HTTP client for Item Master Data API with caching and normalization

**Key Functions:**
- `get_item_master_data(sites: str | None = None, cache_ttl_hours: int = 24) -> list[dict]`
  - Fetch full item master dataset for requested sites
  - Cache on Drive as JSON snapshot with TTL
  - Thread-safe in-memory cache for multiple QBR runs
  
- `get_high_risk_items(threshold: int = 80, max_items: int = 50) -> list[dict]`
  - Filter by `aggregateRiskScore >= threshold`
  - Return top N sorted by risk score descending
  
- `get_doi_backwards_summary(sites: str | None = None) -> dict`
  - Aggregate DOI backwards metrics (mean, median, min, max, total items)
  - This is the **NEW** field user specifically asked about
  
- `get_abc_distribution(sites: str | None = None) -> dict[str, int]`
  - Count items by ABC rank (A/B/C classification)
  
- `get_lead_time_variance(supplier: str | None = None) -> list[dict]`
  - Compare `leadTime` vs `observedLeadTime` per item/supplier
  - Flag items with >20% variance as supply risk

**Cache Strategy:**
- **Drive snapshot:** `{GOOGLE_QBR_GENERATOR_FOLDER_ID}/cache/item_master_{customer}_{sites_hash}_{date}.json`
- **TTL:** 24 hours (configurable via env `LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS`)
- **In-memory:** Process-level dict with lock (same pattern as Pendo preload)

---

### 2. **`leandna_item_master_enrich`** (QBR Enrichment)

**File:** `src/leandna_item_master_enrich.py`

**Purpose:** Augment QBR report dict with LeanDNA data before slide generation

**Key Function:**
- `enrich_qbr_with_item_master(report: dict, customer: str, requested_sites: str | None = None) -> dict`
  - Called in `run_qbr_from_template()` after CSR load, before slide gen
  - Adds `report["leandna_item_master"]` with:
    ```python
    {
      "enabled": True,
      "item_count": 1234,
      "doi_backwards": {
        "mean": 45.2,
        "median": 38.0,
        "min": 5.0,
        "max": 180.0,
        "items_over_60_days": 234,
      },
      "high_risk_items": [
        {"itemCode": "...", "itemDescription": "...", "aggregateRiskScore": 95, "site": "..."},
        # ... top 10
      ],
      "abc_distribution": {"A": 145, "B": 389, "C": 700},
      "lead_time_variance": {
        "high_variance_count": 23,  # >20% delta
        "worst_performers": [
          {"itemCode": "...", "supplier": "...", "planned": 14, "observed": 35, "variance_pct": 150},
          # ... top 5
        ],
      },
      "excess_breakdown": {
        "total_excess_items": 89,
        "excess_on_hand_value": 1234567,  # aggregate across all items
        "top_excess_items": [
          {"itemCode": "...", "excessOnHandValue": 50000, "excessOnHandQty": 1200},
          # ... top 5
        ],
      },
    }
    ```

---

### 3. **`leandna_item_master_tool`** (LangChain Agent Tool)

**File:** `src/tools/leandna_tool.py` (new file for all LeanDNA agent tools)

**Purpose:** Expose Item Master Data to agent for ad-hoc queries

**Tools:**
- `@tool leandna_get_high_risk_items(threshold: int = 80, max_items: int = 20) -> str`
  - Agent query: "What are the highest risk items?"
  - Returns: Markdown table with itemCode, description, risk score, site
  
- `@tool leandna_get_doi_backwards_summary() -> str`
  - Agent query: "What is the DOI backwards for this customer?"
  - Returns: JSON or natural language summary
  
- `@tool leandna_search_item(item_code: str) -> str`
  - Agent query: "Tell me about item XYZ-123"
  - Returns: Full Item Master Data record for that item

---

## Impact on Existing Decks

### **QBR Deck** (`decks/qbr.yaml`)

#### Slide: `qbr-15-supply-chain` (Supply Chain Overview)

**Current State:**
- 3 KPI cards: On-hand, On-order, Excess on-hand (aggregate $)
- Table: Factory, On-Hand, On-Order, Excess, **DOI** (forwards only), Late POs
- Data source: CS Report `customer_supply_chain`

**Enhancement with LeanDNA:**

1. **Add DOI Backwards column** to table (new field!)
   - Header: "DOI Bwd"
   - Value: Site-level mean `daysOfInventoryBackward` from Item Master Data
   - Visual: Color-code >60 days as yellow (risk indicator)

2. **Add "High DOI Backwards" KPI card** (4th card)
   - Label: "Items >60d DOI Bwd"
   - Value: Count of items with `daysOfInventoryBackward > 60`
   - Accent: Orange if count > 10, Blue otherwise

3. **Enrich speaker notes** with:
   - "DOI Backwards (looking at historical consumption) averages X days vs DOI Forwards (future-looking) at Y days."
   - "Z items show >60 days backward DOI, suggesting slow-moving or excess stock."
   - "Top 3 excess items: [itemCode] ($XXk), [itemCode] ($YYk), ..."

**Visual Mockup (Table):**
```
| Factory      | On-Hand  | On-Order | Excess | DOI Fwd | DOI Bwd | Late POs |
|--------------|----------|----------|--------|---------|---------|----------|
| Plant A      | $2.3M    | $1.1M    | $450K  | 45d     | 52d     | 3        |
| Plant B      | $1.8M    | $900K    | $200K  | 38d     | 41d     | 1        |
```

**Implementation:**
- Modify `_supply_chain_slide()` in `src/slides_client.py`:
  - Add `col_widths` entry for DOI Bwd (55pt, same as DOI Fwd)
  - Pull `daysOfInventoryBackward` from `report["leandna_item_master"]["doi_backwards"]` per site
  - Add 4th KPI card logic
  - Append to speaker notes

---

#### Slide: `qbr-14-platform-health` (Platform Health)

**Current State:**
- Header: Health distribution summary (X GREEN, Y YELLOW, Z shortages)
- Table: Factory, Health, CTB%, CTC%, Comp Avail%, Shortages, Critical
- Data source: CS Report `customer_platform_health`

**Enhancement with LeanDNA:**

1. **Add "High Risk Items" badge** to header
   - Text: " · 15 high-risk items (score >80)"
   - Color: Red if >10, Orange if >5, Gray otherwise

2. **Enrich speaker notes** with:
   - "Item-level risk analysis shows X items with aggregate risk score >80 (out of 100)."
   - "Top risk drivers: [itemCode] (95 score, critical shortage), [itemCode] (92 score, lead time variance), ..."
   - "ABC classification: A-items (critical) = X%, B-items = Y%, C-items = Z%."

**Implementation:**
- Modify `_platform_health_slide()` in `src/slides_client.py`:
  - Extend `summary_hdr` string with risk badge
  - Pull `high_risk_items` count from `report["leandna_item_master"]`
  - Append to speaker notes

---

### **CS Health Review Deck** (`decks/cs-health-review.yaml`)

#### Slides: `cs-health-16-platform-health`, `cs-health-17-supply-chain`

**Enhancement:** Same as QBR slides above (shared `slide_type` builders)

---

### **Engineering Deck** (`decks/engineering.yaml`)

**No Impact:** Engineering deck does not include supply chain or platform health slides

---

### **Cohort Review Deck** (`decks/cohort-review.yaml`)

**No Impact:** Cohort is account-level, not item-level

---

### **Portfolio Review Deck** (`decks/portfolio-review.yaml`)

**No Impact:** Portfolio aggregates customer-level metrics, not item-level

---

### **Executive Summary Deck** (`decks/executive-summary.yaml`)

**Potential Enhancement (future):**
- Add "Supply Chain Risk Snapshot" slide to exec summary
- Show top 3 high-risk items across all customers (portfolio view)
- Requires `leandna_item_master_data` for all customers in portfolio

---

## New Deck to Create

### **Supply Chain Deep Dive Deck** (`decks/supply-chain-deep-dive.yaml`)

**Purpose:** Item-level supply chain intelligence deck for CS/Ops teams (not exec-facing)

**Slides:**

1. **Title Slide** (`std-01-title`)
   - Standard title with customer name

2. **Supply Chain Summary** (new slide type: `supply_chain_summary`)
   - 4 KPI cards:
     - Total Items Managed
     - High-Risk Items (score >80)
     - Avg DOI Backwards
     - Total Excess Value
   - Data: `report["leandna_item_master"]`

3. **DOI Analysis** (new slide type: `doi_analysis`)
   - Dual-axis chart: DOI Forwards vs DOI Backwards by site
   - Scatter plot: Each dot = one item (X = DOI Fwd, Y = DOI Bwd, size = value)
   - Quadrants:
     - Top-right: High both (slow movers)
     - Top-left: High backward, low forward (overstock clearing)
     - Bottom-right: Low backward, high forward (demand spike)
     - Bottom-left: Optimal (balanced)

4. **High-Risk Items** (new slide type: `high_risk_items_table`)
   - Table: Item Code, Description, Site, Risk Score, CTB Impact, Lead Time Variance
   - Top 20 items sorted by risk score descending
   - Color-code risk score: >90 red, 80-90 orange, <80 gray

5. **ABC Classification** (new slide type: `abc_classification_pie`)
   - Pie chart: A-items, B-items, C-items (% of total)
   - Table: Category, Count, Total Value, Avg DOI
   - Speaker notes: "A-items are critical high-value parts requiring tight control..."

6. **Lead Time Variance** (new slide type: `lead_time_variance`)
   - Bar chart: Top 10 items with highest variance (planned vs observed lead time)
   - X-axis: Item Code
   - Y-axis: Days variance (observed - planned)
   - Color: Red if variance >50%, Orange if >20%, Blue otherwise

7. **Excess Inventory Breakdown** (new slide type: `excess_inventory_detail`)
   - Table: Item Code, Description, Site, Excess Qty, Excess Value, Days on Hand
   - Top 25 items sorted by excess value descending
   - Speaker notes: "Excess inventory ties up working capital. Top items shown are candidates for write-down, redistribution, or demand stimulation."

8. **Supplier Performance** (new slide type: `supplier_lead_time_perf`)
   - Table: Supplier, Avg Planned Lead Time, Avg Observed Lead Time, Variance %, Items
   - Sorted by variance % descending
   - Only suppliers with >5 items to avoid noise

**Data Tools:**
- All slides use `leandna_item_master` enrichment (no new data tools needed)

**Customer Targeting:**
- `customers: all` (available for any customer where `LEANDNA_DATA_API_BEARER_TOKEN` is set)

**Deck YAML:**
```yaml
id: supply_chain_deep_dive
name: Supply Chain Deep Dive
description: Item-level supply chain intelligence from LeanDNA Item Master Data
version: 1.0
slides:
  - std-01-title
  - supply_chain_summary
  - doi_analysis
  - high_risk_items_table
  - abc_classification_pie
  - lead_time_variance
  - excess_inventory_detail
  - supplier_lead_time_perf
```

**Generation Command:**
```bash
python main.py deck CustomerName supply_chain_deep_dive
```

---

## Implementation Summary

### Files to Create

1. `src/leandna_item_master_client.py` — HTTP client + cache
2. `src/leandna_item_master_enrich.py` — QBR enrichment logic
3. `src/tools/leandna_tool.py` — Agent tools
4. `decks/supply-chain-deep-dive.yaml` — New deck definition
5. `slides/supply-chain-summary.yaml` — New slide (KPI cards)
6. `slides/doi-analysis.yaml` — New slide (scatter chart)
7. `slides/high-risk-items-table.yaml` — New slide (table)
8. `slides/abc-classification-pie.yaml` — New slide (pie chart + table)
9. `slides/lead-time-variance.yaml` — New slide (bar chart)
10. `slides/excess-inventory-detail.yaml` — New slide (table)
11. `slides/supplier-lead-time-perf.yaml` — New slide (table)
12. `tests/test_leandna_item_master_client.py` — Unit tests

### Files to Modify

1. `src/slides_client.py`:
   - `_supply_chain_slide()` — add DOI Bwd column + KPI card
   - `_platform_health_slide()` — add risk badge
   - Add 7 new slide builder functions for deep dive deck

2. `src/qbr_template.py`:
   - Call `enrich_qbr_with_item_master()` after CSR load

3. `src/config.py`:
   - Add `LEANDNA_DATA_API_BASE_URL`, `LEANDNA_DATA_API_BEARER_TOKEN`, `LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS`

4. `.env.example`:
   - Document LeanDNA env vars (already done in earlier step)

5. `docs/data-schema/DATA_REGISTRY.md`:
   - Add `DAYS-OF-INVENTORY-BACKWARD`, `AGGREGATE-RISK-SCORE`, etc.

6. `requirements.txt`:
   - No new deps (uses existing `requests`)

---

## Value Proposition

### For Current Decks (QBR, CS Health Review)

| Metric | Current State | Enhanced State | Impact |
|--------|---------------|----------------|--------|
| **DOI Backwards** | ❌ Missing | ✅ Site & item level | User-requested feature; answers "how much inventory is slow-moving?" |
| **Risk Scores** | ❌ Missing | ✅ Aggregate + top items | Proactive risk management; prioritize CS/Ops actions |
| **ABC Classification** | ❌ Missing | ✅ Distribution + narrative | Align CS conversations with criticality |
| **Lead Time Variance** | ❌ Missing | ✅ Supplier + item level | Supplier performance narrative; SLA accountability |
| **Excess Detail** | ⚠️ Aggregate only | ✅ Top items by value | Actionable insight; CS can work with customer on specific SKUs |

**ROI:** Minimal dev effort (reuses existing slide patterns, no new APIs beyond Item Master), high customer value (answers "which items?" instead of just "how many?").

### For New Deck (Supply Chain Deep Dive)

**Target Audience:** CS Directors, Supply Chain Analysts, Ops Managers (not C-suite)

**Use Cases:**
1. **Quarterly deep-dive** — Partner with customer supply chain team on optimization opportunities
2. **Implementation review** — Show item-level impact 90 days post-deployment
3. **Renewal prep** — Quantify ROI at SKU level ("LeanDNA reduced risk on 45 A-items, saved $2M in excess")
4. **Upsell** — Identify gaps (e.g., "You have 23 high-risk items; our Premium tier offers predictive shortage alerts")

**Differentiation:** This deck **does not exist in BPO today**. It's a net-new capability unlocked by LeanDNA API.

---

## Open Questions

1. **Site Mapping:** How to resolve BPO `customer` → LeanDNA `RequestedSites` header?
   - Option A: Add `leandna_site_ids` to `teams.yaml` per customer
   - Option B: Call `/data/identity` to list authorized sites, fuzzy-match on `siteName`
   - **Recommendation:** Option A for explicit control

2. **DOI Backwards Definition:** Confirm with LeanDNA what "backwards" means (historical consumption vs backward-looking forecast)?
   - **Action:** Validate with live API call + LeanDNA support docs

3. **Performance:** Item Master Data can be large (thousands of items). Cache strategy adequate?
   - **Mitigation:** 24h TTL + pagination if API supports it (check swagger `parameters`)

4. **Field Coverage:** Does every customer have `daysOfInventoryBackward` populated?
   - **Action:** Test with 2-3 pilot customers; add graceful fallback if missing

5. **Chart Library:** Deep Dive deck scatter/pie charts — use Sheets charts or Slides native shapes?
   - **Recommendation:** Sheets charts (existing pattern in BPO) for scatter; Slides shapes for pie (like legend work you just did)

---

## Next Steps

1. **Prototype:** Implement `leandna_item_master_client.py` + enrichment for QBR supply chain slide DOI Bwd column
2. **Validate:** Run test QBR for one customer (Bombardier?) with live API
3. **Review:** User confirms DOI Backwards field, schema, and slide layout
4. **Expand:** Add platform health risk badge, then deep dive deck slides one-by-one
5. **Document:** Update `DATA_REGISTRY.md` and slide design standards

**Estimated Effort:** 2-3 days for QBR enhancements + deep dive deck (assuming API is stable and user has valid bearer token).
