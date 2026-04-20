# Tool #2: LeanDNA Material Shortage Trends Integration

## Overview

Integrate the **LeanDNA Material Shortages API** (multiple endpoints) to add time-series shortage intelligence to BPO. This replaces static aggregate shortage counts with **daily/weekly buckets** showing shortage forecasts, criticality trends, and scheduled delivery timelines.

---

## Tools to Create

### 1. **`leandna_shortage_client`** (Client Module)

**File:** `src/leandna_shortage_client.py`

**Purpose:** HTTP client for Material Shortages endpoints with caching and normalization

**Key Functions:**

- `get_shortages_by_item_weekly(sites: str | None = None, cache_ttl_hours: int = 12) -> list[dict]`
  - Fetch **weekly shortage buckets** (up to 32 weeks forward)
  - Each row = one item + 32 bucket slots (quantity, criticality, date range)
  - Cache on Drive with shorter TTL (12h default vs 24h for Item Master)
  
- `get_shortages_by_item_daily(sites: str | None = None) -> list[dict]`
  - Fetch **daily shortage buckets** (up to 45 days forward)
  - Each row = one item + 45 day slots (quantity, supply, requirements, criticality)
  - Use for near-term tactical view (next 2 weeks)
  
- `get_shortages_by_order(sites: str | None = None) -> list[dict]`
  - Fetch **shortage-by-production-order** report
  - Links shortages to customer orders, production orders, and requirement dates
  - Use for "first impacted order" drill-down
  
- `get_shortages_with_scheduled_deliveries_weekly(sites: str | None = None) -> list[dict]`
  - Weekly buckets **plus** scheduled delivery tracking
  - Includes `scheduledDeliveries` count, `firstDeliveryDate`, `firstDeliveryTrackingNumber`
  - Use when PO tracking is critical (late deliveries, commit date variance)

- `aggregate_shortage_forecast(weekly_data: list[dict], weeks: int = 12) -> dict`
  - Summarize next N weeks: total shortage value, critical item count, bucket-by-bucket totals
  - Returns time-series dict suitable for chart generation
  
- `get_critical_shortages_timeline(weekly_data: list[dict], threshold: int = 3) -> list[dict]`
  - Extract items with `criticalityLevel >= threshold` across any bucket
  - Sort by first critical bucket date + CTB impact value
  - Top 20 for slide table

**Cache Strategy:**
- **Drive snapshot:** `{GOOGLE_QBR_GENERATOR_FOLDER_ID}/cache/shortages_weekly_{customer}_{sites_hash}_{datetime}.json`
- **TTL:** 12 hours (vs 24h for Item Master — shortage data is more time-sensitive)
- **In-memory:** Process-level dict with lock

---

### 2. **`leandna_shortage_enrich`** (QBR Enrichment)

**File:** `src/leandna_shortage_enrich.py`

**Purpose:** Augment QBR report dict with shortage trends before slide generation

**Key Function:**

- `enrich_qbr_with_shortage_trends(report: dict, customer: str, weeks_forward: int = 12) -> dict`
  - Called in `run_qbr_from_template()` after LeanDNA Item Master enrichment
  - Adds `report["leandna_shortage_trends"]` with:
    ```python
    {
      "enabled": True,
      "data_fetched_at": "2026-04-20T18:10:00Z",
      "weeks_forward": 12,
      "total_items_in_shortage": 234,
      "critical_items": 45,
      "forecast": {
        "buckets": [
          {"week_start": "2026-04-21", "week_end": "2026-04-27", "total_qty": 1234.5, "critical_items": 8},
          # ... 12 weeks
        ],
        "peak_week": "2026-05-12",  # week with highest shortage qty
        "total_shortage_value": 2345678.0,
      },
      "critical_timeline": [
        {"itemCode": "...", "site": "...", "firstCriticalWeek": "2026-04-28", "ctbImpact": 120000, "criticality": 5},
        # ... top 20 critical items
      ],
      "scheduled_deliveries": {
        "items_with_schedules": 89,
        "avg_deliveries_per_item": 2.3,
        "next_7_days_scheduled_qty": 5678.9,
      },
    }
    ```

---

### 3. **`leandna_shortage_tool`** (LangChain Agent Tool)

**File:** `src/tools/leandna_tool.py` (extend from Tool #1)

**Purpose:** Expose shortage trends to agent for ad-hoc queries

**Tools:**

- `@tool leandna_get_shortage_forecast(weeks: int = 8) -> str`
  - Agent query: "What does the shortage forecast look like?"
  - Returns: Markdown table with weekly buckets, critical item counts
  
- `@tool leandna_get_critical_shortages() -> str`
  - Agent query: "What are the most critical shortages?"
  - Returns: Top 20 critical items with timeline and CTB impact
  
- `@tool leandna_search_item_shortage(item_code: str) -> str`
  - Agent query: "When will item XYZ-123 be in shortage?"
  - Returns: Daily/weekly bucket breakdown for that specific item

---

## Impact on Existing Decks

### **QBR Deck** (`decks/qbr.yaml`)

#### No changes to existing slides (all enhancements are NEW slides)

**Rationale:** Shortage trends are net-new intelligence; they don't replace existing platform health or supply chain slides, they **complement** them.

---

## New Slides to Create

### 1. **Shortage Forecast Slide** (`slides/qbr-15b-shortage-forecast.yaml`)

**Type:** `shortage_forecast` (new slide type)

**Placement:** After supply chain slide (order 15b, between supply chain and platform value)

**Layout:**

- **Title:** "Material Shortage Forecast — Next 12 Weeks"

- **Chart:** Stacked area chart (Sheets or Slides shapes)
  - **X-axis:** Week start dates (12 weeks)
  - **Y-axis:** Shortage quantity
  - **Series:**
    - Critical (red area)
    - High (orange area)
    - Medium (yellow area)
    - Low (blue area)
  - **Peak annotation:** Arrow/callout on week with highest shortage

- **KPI Cards (below chart):**
  - "Total Items in Shortage" (count)
  - "Critical Items" (count, red accent if >10)
  - "Peak Week" (date, e.g., "Week of May 12")
  - "Total Shortage Value" ($, CTB impact)

**Data Source:** `report["leandna_shortage_trends"]["forecast"]`

**Speaker Notes:**
- "Shortage forecast shows next 12 weeks based on current on-order schedule and production requirements."
- "Peak shortage week is {date} with {N} critical items and ${X} CTB impact."
- "Items with scheduled deliveries: {M} items, {Q} qty arriving in next 7 days."

**Visual Mockup:**

```
┌─────────────────────────────────────────────────────────────────┐
│ Material Shortage Forecast — Next 12 Weeks                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Shortage Qty                          ↑ Peak: May 12         │
│   5000 ┤                               /█\                      │
│        │                              / █ \                     │
│   4000 ┤                             /  █  \                    │
│        │                    ████    /   █   \   ████            │
│   3000 ┤         ████      ██████  /    █    \ ██████           │
│        │    ████ ██████   ████████/     █     \████████  ████   │
│   2000 ┤   ████████████████████████     █      ████████████████ │
│        │  ██████████████████████████    █     ██████████████████│
│   1000 ┤ ████████████████████████████   █    ███████████████████│
│        │████████████████████████████████████████████████████████│
│      0 └──┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬─▶│
│         Apr21 Apr28 May5 May12 May19 May26 Jun2 Jun9 Jun16...  │
│                                                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐         │
│  │  234     │ │   45     │ │ May 12   │ │ $2.3M    │         │
│  │Items     │ │Critical  │ │Peak Week │ │Shortage  │         │
│  │in Short. │ │Items     │ │          │ │Value     │         │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

---

### 2. **Critical Shortages Detail Slide** (`slides/qbr-15c-critical-shortages.yaml`)

**Type:** `critical_shortages_detail` (new slide type)

**Placement:** After shortage forecast (order 15c)

**Layout:**

- **Title:** "Critical Material Shortages — Next 90 Days"

- **Table:** Top 20 critical items

| Item Code | Description | Site | First Critical | Days in Short | CTB Impact | PO Status |
|-----------|-------------|------|----------------|---------------|------------|-----------|
| ABC-123   | Bearing Asm | Plant A | Apr 28 | 12d | $120K | Late |
| XYZ-456   | Circuit Brd | Plant B | May 5  | 8d  | $85K  | On-time |
| ...       |             |        |        |      |        |        |

- **Color-code "First Critical" column:**
  - Red if <7 days out
  - Orange if 7-14 days
  - Yellow if 14-30 days
  - Gray if >30 days

**Data Source:** `report["leandna_shortage_trends"]["critical_timeline"]`

**Speaker Notes:**
- "Top 20 critical shortages ranked by CTB impact value."
- "First critical week is the earliest bucket where shortage reaches criticality threshold."
- "Items marked 'Late' have PO commit dates past the first impacted order date."

---

### 3. **Shortage vs Scheduled Deliveries Slide** (`slides/qbr-15d-shortage-deliveries.yaml`)

**Type:** `shortage_deliveries` (new slide type)

**Placement:** After critical shortages (order 15d)

**Layout:**

- **Title:** "Shortage Resolution — Scheduled Deliveries"

- **Dual chart:**
  - **Top:** Line chart showing shortage quantity over 8 weeks
  - **Bottom:** Bar chart showing scheduled delivery quantity over same 8 weeks
  - **Overlay:** Highlight weeks where deliveries > shortage (resolution points)

- **KPI Cards:**
  - "Items with Scheduled Deliveries" (count)
  - "Avg Deliveries per Item" (e.g., 2.3)
  - "Next 7 Days Scheduled Qty" (total qty arriving)

**Data Source:** `report["leandna_shortage_trends"]["scheduled_deliveries"]` + weekly bucket data

**Speaker Notes:**
- "Scheduled deliveries show expected PO arrivals vs shortage requirements."
- "Weeks where delivery bars exceed shortage line are resolution points (shortage clears)."
- "{N} items have confirmed delivery schedules; {M} items lack PO tracking."

---

## New Deck to Create

### **Material Shortages Deep Dive Deck** (`decks/shortage-deep-dive.yaml`)

**Purpose:** Tactical shortage management deck for supply chain / ops teams (weekly review cadence)

**Slides:**

1. **Title Slide** (`std-01-title`)

2. **Shortage Summary Dashboard** (new: `shortage_summary`)
   - 6 KPI cards:
     - Total Items in Shortage
     - Critical Items (>= level 3)
     - Avg Days in Shortage
     - Total CTB Impact ($)
     - Items with Scheduled Deliveries
     - Items Without PO Tracking
   - 2-line header with date range: "Shortage data as of {date} — forecast through {date+90d}"

3. **12-Week Shortage Forecast** (reuse: `shortage_forecast` from QBR)

4. **Critical Shortages Timeline** (reuse: `critical_shortages_detail` from QBR)

5. **Shortage by Production Order** (new: `shortage_by_order`)
   - Table: Top 25 production orders impacted by shortages
   - Columns: Prod Order, Customer Order, Due Date, Shortage Items Count, First Shortage Item, CTB Impact
   - Sort by CTB impact descending
   - Data: `/data/MaterialShortages/ShortagesByOrder`

6. **Daily Shortage Detail — Next 14 Days** (new: `shortage_daily_detail`)
   - Heatmap table: Items (rows) × Days (14 columns)
   - Cell color: Red (critical), Orange (high), Yellow (medium), Green (ok/none)
   - Shows which days each item is in shortage
   - Data: `/data/MaterialShortages/ShortagesByItem/Daily` (first 14 days)

7. **Scheduled Deliveries vs Shortages** (reuse: `shortage_deliveries` from QBR)

8. **Shortage History — Past 90 Days** (new: `shortage_history`)
   - Line chart: Daily shortage item count over past 90 days
   - Requires historical API calls or parquet Data Share (may not be in real-time API)
   - Fallback: "Historical trend not available; use parquet Data Share for time-series"

9. **Supplier Late Delivery Analysis** (new: `supplier_late_delivery`)
   - Table: Suppliers with late POs causing shortages
   - Columns: Supplier, Late PO Count, Total Shortage Impact ($), Avg Delay (days)
   - Derived from shortage-by-item + PO commit dates
   - Cross-links to Tool #1 lead time variance

10. **Action Items** (new: `shortage_action_items`)
    - Bullet list of recommended actions based on data
    - Generated by LLM or rule-based (e.g., "Expedite PO ABC-123 for item XYZ critical on Apr 28")
    - Uses same pattern as Notable Signals slide

**Data Tools:**
- `leandna_shortage_weekly` (new)
- `leandna_shortage_daily` (new)
- `leandna_shortage_by_order` (new)

**Deck YAML:**
```yaml
id: shortage_deep_dive
name: Material Shortages Deep Dive
description: Tactical shortage forecast and resolution tracking from LeanDNA
version: 1.0
slides:
  - std-01-title
  - shortage_summary
  - shortage_forecast
  - critical_shortages_detail
  - shortage_by_order
  - shortage_daily_detail
  - shortage_deliveries
  - shortage_history
  - supplier_late_delivery
  - shortage_action_items
```

**Generation Command:**
```bash
python main.py deck CustomerName shortage_deep_dive
```

---

## Implementation Details

### API Endpoints Used

| Endpoint | Purpose | Response Shape | Cache TTL |
|----------|---------|----------------|-----------|
| `/data/MaterialShortages/ShortagesByItem/Weekly` | 32-week shortage buckets per item | Array of ~100-500 items × 32 buckets | 12h |
| `/data/MaterialShortages/ShortagesByItem/Daily` | 45-day daily buckets per item | Array of items × 45 days | 6h |
| `/data/MaterialShortages/ShortagesByOrder` | Shortage-to-production-order mapping | Array of PO records with shortage links | 12h |
| `/data/MaterialShortages/ShortagesByItemWithScheduledDeliveries/Weekly` | Weekly buckets + delivery tracking | Array with extra delivery fields | 12h |

### Key Fields (Weekly Schema)

From swagger `ShortageByItemWeeklyAdapted`:

**Shortage metadata:**
- `daysInShortage` — cumulative days item has been short
- `ctbShortageImpactedValue` — dollar impact on Clear-to-Build
- `ctbImpactedOrdersSingleShortageCount` — how many orders blocked by this item
- `criticalityLevel` — numeric (1-5) or label
- `buyer`, `planner`, `supplierName`

**Bucket fields (32 weeks):**
- `bucket1quantity` through `bucket32quantity` — shortage qty in that week
- `bucket1startDate` through `bucket32startDate` — week start (ISO date)
- `bucket1endDate` through `bucket32endDate` — week end
- `bucket1criticality` through `bucket32criticality` — text label (Critical/High/Medium/Low)

**PO tracking:**
- `firstPurchaseOrder`, `firstPOSupplier`, `firstPORequestedDate`, `firstPOQty`
- `firstPoStatus`, `firstPoTrackingNumber`, `firstPOERPStatus`
- `firstPOCommitDate`, `firstPOOriginalRequestedDate`

**Impacted orders:**
- `firstImpactedOrder`, `firstImpactedOrderDate`, `firstImpactedOrderQty`
- `firstImpactedSO` (sales order), `firstImpactedSODate`

**Inventory context:**
- `onHand`, `onOrder`, `safetyStock`, `avgDailyDemand`
- `pastDueOnOrder`, `pastDueDemand`, `currentShortage`

### Chart Generation Strategy

**Stacked area chart (shortage forecast):**

1. **Option A (Sheets chart):**
   - Use `_create_chart_via_sheets()` pattern from `src/charts.py`
   - Upload weekly buckets as Sheets data
   - Create stacked area chart
   - Embed in slide

2. **Option B (Slides shapes):**
   - Draw with Slides API polylines + fills (like existing charts)
   - More control, no Sheets dependency
   - Higher code complexity

**Recommendation:** Option A (Sheets) for stacked area; easier to maintain, consistent with existing charts.

---

## Impact on Existing Decks (Detail)

### **QBR Deck** (`decks/qbr.yaml`)

**No modifications to existing slides.** Add 3 new slides:

```yaml
# In decks/qbr.yaml, after qbr-15-supply-chain:
slides:
  # ... existing slides ...
  - qbr-15-supply-chain
  - qbr-15b-shortage-forecast        # NEW
  - qbr-15c-critical-shortages       # NEW
  - qbr-15d-shortage-deliveries      # NEW
  - qbr-16-platform-value
  # ... rest unchanged ...
```

**Impact:**
- QBR deck length: +3 slides (from ~20 slides to ~23 slides)
- Target audience: Still exec-facing, but with tactical depth for CSMs to reference
- When to include: Optional per customer; add `shortage_trends: true` flag in customer config if they want granular shortage views

---

### **CS Health Review Deck** (`decks/cs-health-review.yaml`)

**Enhancement:** Add shortage forecast slide (same as QBR)

```yaml
slides:
  # ... existing ...
  - cs-health-17-supply-chain
  - cs-health-17b-shortage-forecast   # NEW (reuses qbr-15b slide_type)
  # ... rest unchanged ...
```

**Rationale:** CS Health Review is already deep; shortage trends fit the scope.

---

### **Engineering Deck** (`decks/engineering.yaml`)

**No Impact:** Engineering deck is product/JIRA-focused, not supply chain.

---

### **Cohort Review, Portfolio Review, Executive Summary**

**No Impact:** These are account-level aggregates; shortage trends are customer/site-specific.

---

### **Support Deck** (`decks/support.yaml`)

**No Impact:** Support is JIRA/JSM-focused.

---

## Value Proposition

### For QBR Deck

| Capability | Current State | Enhanced State | Impact |
|------------|---------------|----------------|--------|
| **Shortage visibility** | Aggregate count (e.g., "45 shortages") | **12-week forecast** with weekly buckets | Proactive vs reactive; "when will shortages peak?" |
| **Criticality** | Total critical count | **Timeline** showing when items become critical | Prioritize CS/Ops actions by urgency |
| **CTB impact** | Implied (from platform health) | **Explicit $ impact** per item + forecast | Quantify business risk of shortages |
| **Delivery tracking** | CS Report shows "late POs" count | **Scheduled deliveries** vs shortage timeline | "When will shortages resolve?" |
| **Tactical detail** | None (exec summary only) | **Item-level table** with buyer, planner, supplier | CS can drill into specific SKUs with customer |

**ROI:** High customer value (shortage management is core LeanDNA use case); moderate dev effort (chart generation + 3 new slide builders).

---

### For New Deck (Shortage Deep Dive)

**Target Audience:** Supply chain analysts, buyer teams, ops managers (weekly review)

**Use Cases:**
1. **Weekly shortage meeting** — Review next 12 weeks, assign buyers to expedite critical POs
2. **Escalation prep** — Surface top 5 critical items to exec team with CTB impact
3. **Supplier performance review** — Late delivery analysis by supplier (cross-links to Tool #1 lead time variance)
4. **Delivery validation** — Compare scheduled deliveries vs ERP commitments; flag tracking gaps

**Differentiation:** This is **tactical, forward-looking** intelligence. Current BPO decks are **strategic, historical** (last 90 days). Shortage Deep Dive bridges the gap.

---

## Data Comparison: LeanDNA Shortage API vs CS Report

| Metric | CS Report (current) | LeanDNA Shortage API | Advantage |
|--------|---------------------|----------------------|-----------|
| **Shortage count** | ✅ Aggregate per site | ✅ Item-level + time-series | API: granular + forecast |
| **Critical shortages** | ✅ Aggregate count | ✅ Item list + timeline | API: actionable |
| **CTB impact** | ⚠️ Implied from CTB% | ✅ Explicit $ per item | API: quantified risk |
| **Time horizon** | ❌ Snapshot only | ✅ 12 weeks forward (32 buckets) | API: forecast |
| **Daily detail** | ❌ | ✅ 45 days × item | API: near-term precision |
| **PO tracking** | ⚠️ Late PO count only | ✅ Commit dates, tracking #s, schedules | API: delivery visibility |
| **Production order link** | ❌ | ✅ Shortage-to-customer-order mapping | API: impact to revenue |
| **Supplier attribution** | ❌ | ✅ Supplier per item, late delivery cause | API: accountability |

**Conclusion:** LeanDNA Shortage API is a **superset** for shortage intelligence. CS Report gives high-level health; API gives tactical action plan.

---

## Implementation Plan

### Phase 1: Core Client (Day 1)

1. Create `src/leandna_shortage_client.py`
   - `get_shortages_by_item_weekly()` with Drive cache
   - `aggregate_shortage_forecast()` helper
   - `get_critical_shortages_timeline()` helper

2. Create `src/leandna_shortage_enrich.py`
   - `enrich_qbr_with_shortage_trends()` (integrate in `qbr_template.py`)

3. Tests: `tests/test_leandna_shortage_client.py`

### Phase 2: QBR Slides (Day 2)

1. Create `slides/qbr-15b-shortage-forecast.yaml`
   - Slide builder: `_shortage_forecast_slide()` in `slides_client.py`
   - Stacked area chart via Sheets
   - 4 KPI cards below chart

2. Create `slides/qbr-15c-critical-shortages.yaml`
   - Slide builder: `_critical_shortages_detail_slide()`
   - Table with top 20, color-coded timeline

3. Create `slides/qbr-15d-shortage-deliveries.yaml`
   - Slide builder: `_shortage_deliveries_slide()`
   - Dual chart (line + bar)

4. Update `decks/qbr.yaml` and `decks/cs-health-review.yaml` to include new slides

### Phase 3: Deep Dive Deck (Day 3)

1. Create `decks/shortage-deep-dive.yaml`
2. Build remaining slide types:
   - `shortage_summary` (6 KPIs)
   - `shortage_by_order` (table)
   - `shortage_daily_detail` (heatmap)
   - `shortage_history` (line chart, if historical data available)
   - `supplier_late_delivery` (table)
   - `shortage_action_items` (LLM-generated or rule-based)

### Phase 4: Agent Tools (Day 4)

1. Extend `src/tools/leandna_tool.py`
2. Add shortage forecast, critical shortages, item search tools

---

## Files to Create

1. `src/leandna_shortage_client.py` (~350 lines)
2. `src/leandna_shortage_enrich.py` (~250 lines)
3. `slides/qbr-15b-shortage-forecast.yaml` (~30 lines)
4. `slides/qbr-15c-critical-shortages.yaml` (~30 lines)
5. `slides/qbr-15d-shortage-deliveries.yaml` (~30 lines)
6. `slides/shortage-summary.yaml` (~25 lines)
7. `slides/shortage-by-order.yaml` (~30 lines)
8. `slides/shortage-daily-detail.yaml` (~35 lines)
9. `slides/shortage-history.yaml` (~30 lines)
10. `slides/supplier-late-delivery.yaml` (~30 lines)
11. `slides/shortage-action-items.yaml` (~25 lines)
12. `decks/shortage-deep-dive.yaml` (~40 lines)
13. `tests/test_leandna_shortage_client.py` (~250 lines)
14. `docs/LEANDNA_TOOL_2_SHORTAGE_TRENDS.md` (this file)

**Total:** ~1,200 new lines + ~200 lines modifications to existing files

---

## Files to Modify

1. `src/slides_client.py`:
   - Add 8 new slide builder functions (~600 lines total)
   - Register builders in `_SLIDE_BUILDERS` dict
   
2. `src/qbr_template.py`:
   - Add enrichment call (~4 lines)
   
3. `decks/qbr.yaml`:
   - Add 3 slide references (~3 lines)
   
4. `decks/cs-health-review.yaml`:
   - Add 1 slide reference (~1 line)
   
5. `src/config.py`:
   - Add cache TTL config for shortages (~5 lines)
   
6. `docs/data-schema/DATA_REGISTRY.md`:
   - Add shortage API field entries (~30 lines)

---

## Configuration

Add to `.env`:

```bash
# LeanDNA Shortage API (optional; extends Item Master Data integration)
# Uses same bearer token as Item Master
LEANDNA_SHORTAGE_CACHE_TTL_HOURS=12
```

Already documented in `.env.example` (DATA_API_BEARER_TOKEN applies to all endpoints).

---

## Data Schema Notes

### Bucket Normalization

Weekly shortage data has **32 buckets** with numbered fields (`bucket1quantity`, `bucket2quantity`, ...). Normalize to list:

```python
def _normalize_weekly_buckets(row: dict) -> list[dict]:
    """Convert bucket1...bucket32 fields to list of bucket objects."""
    buckets = []
    for i in range(1, 33):
        qty = row.get(f"bucket{i}quantity")
        if qty is None:
            continue
        buckets.append({
            "week_num": i,
            "quantity": float(qty),
            "start_date": row.get(f"bucket{i}startDate"),
            "end_date": row.get(f"bucket{i}endDate"),
            "criticality": row.get(f"bucket{i}criticality") or "Unknown",
        })
    return buckets
```

Daily is similar (day1...day45 with `supply`, `requirements`, `criticality`, `date`).

---

## Chart Data Format (Shortage Forecast)

**For Sheets stacked area chart:**

```python
# rows = [header, week1, week2, ..., week12]
chart_data = [
    ["Week", "Critical", "High", "Medium", "Low"],
    ["Apr 21", 8, 15, 22, 35],
    ["Apr 28", 12, 18, 20, 30],
    # ... 12 rows total
]
```

Derived from:
1. Filter to items with `firstCriticalBucketWeek` in next 12 weeks
2. For each bucket, count items by `bucketNcriticality` (Critical/High/Medium/Low)
3. Aggregate counts per week

---

## Integration with Tool #1

### Cross-Enrichment

**Shortage trends enrich Item Master:**
- Item Master shows `aggregateRiskScore` (0-100) — **predictive** risk model
- Shortage trends show `criticalityLevel` (1-5) — **current** shortage severity
- **Combined insight:** "High aggregate risk + not yet in shortage = watch list"

**Example speaker notes:**
> "Item XYZ-123 has aggregate risk score 92 (top 5%) but is not currently in shortage. However, shortage forecast shows it will reach criticality on May 5 with $85K CTB impact. Recommend expediting PO now before shortage materializes."

### Joint Slide (Future Enhancement)

**Risk vs Shortage Matrix** (new slide type for deep dive deck):
- Scatter plot: X = aggregate risk score, Y = CTB shortage impact
- Quadrants:
  - Top-right: High risk + high impact (red, urgent)
  - Top-left: Low risk + high impact (orange, monitor)
  - Bottom-right: High risk + low impact (yellow, watch)
  - Bottom-left: Low risk + low impact (green, ok)

Requires both Tool #1 and Tool #2 enrichments.

---

## Open Questions

1. **Historical data:** Does the API provide past shortage data, or only forward-looking buckets?
   - If not: use parquet Data Share `PastDueRequirements` report for history
   - If yes: add `get_shortage_history()` function

2. **Bucket semantics:** Are weekly buckets **rolling** (next 32 weeks from today) or **fixed** (calendar weeks)?
   - Test with live API and inspect `bucket1startDate`

3. **Criticality thresholds:** What do numeric criticality levels mean (1-5 scale)?
   - Validate with LeanDNA docs or customer SME

4. **PO tracking coverage:** What % of shortage items have `scheduledDeliveries` populated?
   - If low: de-emphasize delivery tracking slide
   - If high: make it prominent

5. **Production order mapping:** Is shortage-by-order report always populated, or only for MTO customers?
   - Test with 2-3 customers; may need conditional slide inclusion

---

## Testing Plan

### Unit Tests

```bash
# Run all shortage client tests
python -m pytest tests/test_leandna_shortage_client.py -v
```

**Coverage:**
- Bucket normalization (32 weekly, 45 daily)
- Forecast aggregation (time-series builder)
- Critical timeline extraction
- Scheduled delivery summary
- Error handling (API timeout, invalid response)

### Integration Tests

```bash
# Fetch live data and print summary
python -c "
from src.leandna_shortage_client import get_shortages_by_item_weekly
data = get_shortages_by_item_weekly()
print(f'Fetched {len(data)} items')
print(f'Sample: {data[0].keys()}')
"

# Generate QBR with shortage trends
python main.py qbr Bombardier

# Check logs for:
# [INFO] LeanDNA shortage trends: fetched N items from API
# [INFO] LeanDNA shortage trends: forecast next 12 weeks, M critical items
```

### Visual QA

1. Open generated QBR deck in Slides
2. Navigate to "Material Shortage Forecast" slide (after supply chain)
3. Verify:
   - Stacked area chart renders correctly
   - 4 KPI cards show non-zero values
   - Chart peak matches "Peak Week" KPI
4. Check critical shortages table:
   - 20 rows (or fewer if <20 critical items)
   - "First Critical" column color-coded by urgency
   - CTB Impact values are reasonable ($K-$M range)

---

## Success Criteria

✅ Weekly shortage forecast chart (12 weeks forward)  
✅ Critical shortages table with timeline + CTB impact  
✅ Scheduled deliveries vs shortage dual chart  
✅ QBR deck +3 slides (optional per customer)  
✅ Shortage Deep Dive deck (10 slides, tactical focus)  
✅ All tests passing  
✅ Graceful degradation if bearer token missing  
✅ Zero linter errors  
✅ Data registry updated with shortage API fields

---

## Estimated Effort

- **Client + enrichment:** 1 day
- **QBR slide builders (3 slides):** 1 day
- **Deep dive deck (7 additional slides):** 1.5 days
- **Tests + docs:** 0.5 day

**Total:** 3-4 days (vs Tool #1 which took ~4-5 hours, this is larger scope due to chart complexity)

---

## Dependencies

**External:**
- `requests` (already in requirements.txt)
- `openpyxl` (already used for CS Report; may need for Sheets chart data if using A1 notation)

**Internal:**
- Tool #1 (`leandna_item_master_client`) for risk score cross-enrichment (optional)
- Existing Sheets chart pipeline (`src/charts.py`) for stacked area chart

---

## Risk Mitigation

1. **Large response size:** Weekly data with 32 buckets × hundreds of items can be 10+ MB JSON
   - **Mitigation:** Drive cache with 12h TTL; in-memory cache for same-session reuse
   
2. **Chart rendering complexity:** Stacked area with 4 series × 12 points is non-trivial
   - **Mitigation:** Use Sheets chart (existing pattern); fallback to simple line chart if stack fails
   
3. **Customer variability:** Not all customers may have shortage data
   - **Mitigation:** Check `len(data) == 0` and render "No shortage data available" slide
   
4. **API rate limits:** 3 endpoints (weekly, daily, by-order) called per QBR
   - **Mitigation:** Sequential calls with cache; only fetch what slide requires (weekly for forecast, by-order if included)

---

## Future Enhancements (Post-Tool #2)

1. **Shortage alerts:** LLM-generated action items slide ("Expedite these 5 POs by Friday")
2. **Supplier scorecard:** Combine shortage API + Tool #1 lead time variance → supplier risk index
3. **Parquet integration:** Replace JSON endpoints with Data Share parquet for historical trends
4. **Real-time shortage monitor:** Agent tool for "What shortages appeared in the last 24 hours?"
5. **Delivery prediction:** ML model on top of scheduled deliveries to predict late arrivals (if LeanDNA doesn't already provide this)

---

## Related Docs

- Tool #1 implementation: [`LEANDNA_TOOL_1_COMPLETE.md`](./LEANDNA_TOOL_1_COMPLETE.md)
- Full API analysis: [`LEANDNA_DATA_API_TOOLS.md`](./LEANDNA_DATA_API_TOOLS.md)
- Swagger spec: [`leandna-data-api-swagger.json`](./leandna-data-api-swagger.json)
- Data registry: [`data-schema/DATA_REGISTRY.md`](./data-schema/DATA_REGISTRY.md)

---

## Summary Table

| Aspect | Tool #1 (Item Master) | Tool #2 (Shortage Trends) |
|--------|----------------------|---------------------------|
| **API endpoints** | 1 (`/data/ItemMasterData`) | 4 (weekly/daily/by-order/with-deliveries) |
| **New QBR slides** | 0 (enhanced 2 existing) | 3 (forecast, critical, deliveries) |
| **New deep dive deck** | Planned (Supply Chain DD) | Included (Shortage DD, 10 slides) |
| **Data freshness** | 24h cache | 12h cache (more time-sensitive) |
| **Chart complexity** | None (tables + KPIs only) | Stacked area, dual chart, heatmap |
| **Effort** | 4-5 hours | 3-4 days |
| **User-requested** | ✅ Yes (DOI backwards) | ⚠️ Not explicitly, but high business value |
| **Impact** | Enhances 2 existing slides | Adds 3 QBR slides + 10-slide deck |

**Recommendation:** Start with Tool #2 QBR slides (3 slides, ~1.5 days) before committing to full deep dive deck. Validate with 1-2 customers that shortage forecast chart is actionable and worth the extra slides.
