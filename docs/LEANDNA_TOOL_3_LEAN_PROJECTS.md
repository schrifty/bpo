# Tool #3: LeanDNA Lean Projects Integration

## Overview

Integrate the **LeanDNA Lean Projects API** to add project portfolio management and savings tracking to BPO. This enables QBRs to show:
- Active Lean projects with stage/state health
- Monthly savings (actual vs target) with waterfall charts
- Task/issue completion rates
- Best practice projects and validated results

Unlike Tools #1 and #2 (supply chain intelligence), Tool #3 is **operational excellence tracking** — showing how customers are driving continuous improvement initiatives and capturing value from LeanDNA's Lean Projects module.

---

## Business Value

### For CS Teams
- **QBR prep:** Show concrete ROI (e.g., "$500K saved this quarter across 12 active projects")
- **Renewal justification:** "Lean Projects delivered 3x savings vs target; expanding usage to 2 more sites"
- **Adoption coaching:** Identify customers not using Lean Projects (expansion opportunity)

### For Customers
- **Executive summary:** Portfolio health at a glance (stage distribution, on-track vs at-risk)
- **Accountability:** Transparent savings tracking (actual vs target by month/project)
- **Best practices:** Highlight validated projects for internal sharing

### Data Currently Missing from BPO
- **Lean Projects entirely absent** — CSR XLSX doesn't include project data
- **Savings tracking** — No way to show continuous improvement ROI
- **Project health** — No insight into task completion, overdue milestones

---

## API Endpoints Used

### Core Endpoints

| Endpoint | Purpose | Response Shape | Cache TTL |
|----------|---------|----------------|-----------|
| `/data/LeanProject` | List all projects (filter by date range) | Array of project objects | 24h |
| `/data/LeanProject/{projectIds}/Savings` | Monthly savings (actual vs target) | Array of monthly savings per project | 24h |
| `/data/LeanProject/{projectId}/Tasks` | Task list for a project | Array of tasks with status/dates | 24h |
| `/data/LeanProject/{projectId}/Issues` | Issue list for a project | Array of issues with priority/state | 24h |
| `/data/LeanProject/{projectIds}/Stage/History` | Stage transition history | Array of stage changes with timestamps | 24h |

### Metadata Endpoints (for dropdowns/validation)

| Endpoint | Purpose |
|----------|---------|
| `/data/LeanProject/Areas` | Available project areas per site |
| `/data/LeanProject/Types` | Available project types |
| `/data/LeanProject/Categories` | Available project categories |
| `/data/LeanProject/Stages` | Available project stages |
| `/data/LeanProject/CustomFields` | Custom field definitions |

---

## Key Data Schema

### Project Object (`LeanProject`)

```json
{
  "id": "PROJ-123",
  "name": "Reduce Lead Time - PCB Supplier",
  "siteId": 172,
  "area": "Procurement",
  "type": "Cost Reduction",
  "categories": ["Supplier Management", "Lead Time"],
  "stage": "Execution",
  "state": "good",  // good | warn | bad
  "startDate": "2026-01-15",
  "dueDate": "2026-06-30",
  "createdDateTime": "2026-01-10T08:00:00Z",
  "lastUpdateDateTime": "2026-04-18T14:30:00Z",
  "projectManager": {
    "id": 456,
    "name": "Jane Smith",
    "emailAddress": "jane.smith@customer.com"
  },
  "sponsor": {
    "id": 789,
    "name": "John Doe",
    "emailAddress": "john.doe@customer.com"
  },
  "totalActualSavingsForPeriod": 125000.0,
  "totalTargetSavingsForPeriod": 100000.0,
  "isBestPractice": true,
  "isProjectResultsValidated": true,
  "customFieldValues": [...],
  "link": "https://app.leandna.com/leanProjects/PROJ-123"
}
```

### Savings Object (`LeanProjectMonthlySavings`)

```json
{
  "projectId": "PROJ-123",
  "siteId": 172,
  "savings": [
    {
      "month": "2026-01",
      "savingsCategory": "Inventory Reduction",
      "savingsType": "savings",  // savings | spend
      "includeInTotals": true,
      "actual": 25000.0,
      "target": 20000.0,
      "weightedTarget": 15000.0  // target * stage weight
    },
    {
      "month": "2026-02",
      "savingsCategory": "Inventory Reduction",
      "savingsType": "savings",
      "includeInTotals": true,
      "actual": 30000.0,
      "target": 25000.0,
      "weightedTarget": 18750.0
    }
    // ... more months
  ]
}
```

### Task Object (`LeanProjectTask`)

```json
{
  "id": 1,
  "name": "Negotiate new terms with supplier",
  "state": "Started",  // Pending | Started | Completed
  "priority": "High",  // Low | Medium | High
  "percentComplete": 75,
  "responsibleEmail": "buyer1@customer.com",
  "plannedDueDate": "2026-04-30",
  "actualCompletionDate": null,
  "taskGroup": "Supplier Negotiations",
  "status": "good"  // good | warn | bad (based on due date proximity)
}
```

---

## Tools to Create

### 1. **`leandna_lean_projects_client.py`** (Client Module)

**Location:** `src/leandna_lean_projects_client.py`

**Functions:**

```python
def get_lean_projects(
    start_month: str,
    end_month: str,
    sites: str | None = None,
    force_refresh: bool = False,
) -> list[dict]:
    """Retrieve all Lean Projects for a date range.
    
    Args:
        start_month: Start month (YYYY-MM format, e.g., "2026-01")
        end_month: End month (YYYY-MM format, e.g., "2026-03")
        sites: Comma-separated site IDs (optional)
        force_refresh: Bypass cache
    
    Returns:
        List of project objects with savings filtered to date range
    """

def get_project_savings(
    project_ids: list[str],
    sites: str | None = None,
) -> list[dict]:
    """Retrieve monthly savings for given projects.
    
    Args:
        project_ids: List of project IDs
        sites: Comma-separated site IDs
    
    Returns:
        List of project savings objects (monthly breakdown)
    """

def get_project_portfolio_summary(
    projects: list[dict],
) -> dict:
    """Aggregate portfolio-level stats.
    
    Returns:
        {
          "total_projects": 15,
          "active_projects": 12,
          "completed_projects": 3,
          "stage_distribution": {"Execution": 8, "Planning": 3, "Closed": 4},
          "state_distribution": {"good": 10, "warn": 3, "bad": 2},
          "total_actual_savings": 500000.0,
          "total_target_savings": 400000.0,
          "savings_vs_target_pct": 125.0,
          "best_practice_count": 3,
          "validated_count": 10,
        }
    """

def get_project_savings_timeseries(
    savings_data: list[dict],
    months: int = 3,
) -> dict:
    """Build time-series for savings waterfall chart.
    
    Returns:
        {
          "months": ["2026-01", "2026-02", "2026-03"],
          "actual": [125000, 150000, 180000],
          "target": [100000, 120000, 140000],
          "cumulative_actual": [125000, 275000, 455000],
          "cumulative_target": [100000, 220000, 360000],
        }
    """

def get_overdue_tasks(
    project_id: str,
    sites: str | None = None,
) -> list[dict]:
    """Get overdue tasks for a project (actual vs planned due date)."""

def get_best_practice_projects(
    projects: list[dict],
) -> list[dict]:
    """Filter projects with isBestPractice=true, sort by savings desc."""
```

**Caching:**
- Drive JSON snapshots: `lean_projects_{cache_key}_{date}.json`
- TTL: 24 hours (project data changes less frequently than shortages)
- In-memory cache with lock

---

### 2. **`leandna_lean_projects_enrich.py`** (QBR Enrichment)

**Location:** `src/leandna_lean_projects_enrich.py`

**Function:**

```python
def enrich_qbr_with_lean_projects(
    report: dict[str, Any],
    customer: str,
    quarter_start: datetime,
    quarter_end: datetime,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Enrich QBR report with Lean Projects data.
    
    Adds:
      report["leandna_lean_projects"] = {
        "enabled": True,
        "data_fetched_at": "2026-04-20T...",
        "quarter_start": "2026-01-01",
        "quarter_end": "2026-03-31",
        "portfolio_summary": {...},  # from get_project_portfolio_summary()
        "projects": [...],  # top 10 by savings
        "savings_timeseries": {...},  # for waterfall chart
        "best_practice_projects": [...],  # top 3
        "overdue_task_count": 8,  # across all projects
      }
    """
```

**Graceful Degradation:**
- Checks `LEANDNA_DATA_API_BEARER_TOKEN` before fetching
- Logs warnings on errors
- Returns `enabled: False, reason: "..."` if bearer token missing
- Returns `enabled: True, error: "..."` if API fails (allows QBR to continue)

---

### 3. **New Slide Builders** (2-3 slides)

#### **Lean Project Portfolio** (`qbr-18-lean-projects.yaml`)

**Slide ID:** `lean_projects_portfolio`  
**Builder:** `_lean_projects_portfolio_slide()` in `slides_client.py`

**Layout:**

- **Title:** "Lean Project Portfolio — Q1 2026"

- **Summary KPI Cards (4 across):**
  - Total Projects
  - Active Projects
  - Total Savings (Actual)
  - Savings vs Target (e.g., "125%")

- **Stage Distribution (donut chart or table):**
  - Planning: 3 projects
  - Execution: 8 projects
  - Closed: 4 projects

- **State Distribution (badges or KPIs):**
  - On-Track: 10 projects (green)
  - At-Risk: 3 projects (yellow)
  - Off-Track: 2 projects (red)

- **Top 5 Projects Table:**

| Project | Stage | Savings (Actual) | Target | Status |
|---------|-------|------------------|--------|--------|
| Reduce Lead Time - PCB | Execution | $125K | $100K | ✓ |
| Excess Inventory Reduction | Execution | $80K | $90K | ⚠ |
| Supplier Consolidation | Planning | $0K | $50K | — |
| ... | | | | |

**Data Source:** `report["leandna_lean_projects"]["portfolio_summary"]` + `projects`

**Speaker Notes:**
- "15 active Lean projects this quarter, 12 in execution stage."
- "Actual savings of $500K vs target of $400K (125% achievement)."
- "3 best practice projects validated for internal sharing."

---

#### **Savings Waterfall** (`qbr-18b-savings-waterfall.yaml`)

**Slide ID:** `lean_projects_savings_waterfall`  
**Builder:** `_lean_projects_savings_waterfall_slide()`

**Layout:**

- **Title:** "Lean Project Savings — Q1 2026"

- **Waterfall Chart (via DeckCharts or Slides shapes):**
  - X-axis: Months (Jan, Feb, Mar)
  - Y-axis: Cumulative savings ($)
  - Bars/columns:
    - Jan: $125K actual vs $100K target
    - Feb: +$25K actual vs +$20K target (incremental)
    - Mar: +$30K actual vs +$20K target
    - Total: $180K actual vs $140K target

- **KPI Cards (below chart):**
  - Q1 Actual Savings: $180K
  - Q1 Target Savings: $140K
  - Achievement: 129%
  - Projects Contributing: 12

**Data Source:** `report["leandna_lean_projects"]["savings_timeseries"]`

**Speaker Notes:**
- "Q1 savings exceeded target by 29% ($180K actual vs $140K target)."
- "Largest contributor: 'Reduce Lead Time - PCB' project ($125K)."
- "3 projects validated and marked as best practice."

---

#### **Best Practice Projects** (Optional slide or speaker notes section)

**Slide ID:** `lean_projects_best_practices`  
**Builder:** `_lean_projects_best_practices_slide()`

**Layout:**

- **Title:** "Best Practice Projects — Q1 2026"

- **Project Cards (3 across):**
  - Each card shows:
    - Project name
    - Savings achieved
    - Key learnings (from custom fields or description)
    - Link to LeanDNA (for internal sharing)

**Data Source:** `report["leandna_lean_projects"]["best_practice_projects"]`

**Use Case:** Showcase validated projects for cross-site replication

---

## Deck Updates

### `decks/qbr.yaml`

Add 2-3 slides in a new "Continuous Improvement" section (after Platform Health):

```yaml
  - slide: platform_value
  - slide: qbr_divider
    title: Continuous improvement
  - slide: lean_projects_portfolio       # NEW
  - slide: lean_projects_savings_waterfall  # NEW
  # Optional: lean_projects_best_practices
  - slide: qbr_divider
    title: Summary & next steps
  - slide: signals
```

### `decks/cs-health-review.yaml`

Add Lean Projects portfolio slide (summary only, no waterfall):

```yaml
  - slide: cs_health_platform_value
  - slide: lean_projects_portfolio  # NEW
  - slide: cs_health_signals
```

---

## Configuration

Add to `src/config.py`:

```python
# LeanDNA Lean Projects (optional; extends Item Master and Shortage Trends)
# Uses same bearer token as other LeanDNA endpoints
LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS = int(os.environ.get("LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS", "24"))
```

Already uses `LEANDNA_DATA_API_BEARER_TOKEN` from Tool #1/Tool #2.

---

## Implementation Notes

### Quarter Date Mapping

Lean Projects API filters by `startMonth` and `endMonth` (YYYY-MM format). QBR template already has `quarter_start` and `quarter_end`. Convert:

```python
# In enrich function
start_month = quarter_start.strftime("%Y-%m")
end_month = quarter_end.strftime("%Y-%m")
projects = get_lean_projects(start_month, end_month, sites=sites)
```

### Savings Aggregation

API returns savings **per project per month**. Aggregate:

```python
total_actual = sum(
    s["actual"]
    for proj_savings in all_savings
    for s in proj_savings["savings"]
    if s["includeInTotals"]
)
```

### Waterfall Chart Generation

Two options:
1. **Sheets chart** (via `DeckCharts`) — stacked column chart with actual vs target series
2. **Slides shapes** — draw bars manually with `_box()` and annotations

Recommend Sheets for maintainability (same as shortage forecast).

---

## Testing Plan

### Unit Tests (`tests/test_leandna_lean_projects.py`)

**Coverage:**
- Project list fetching and caching
- Portfolio summary aggregation
- Savings timeseries builder
- Best practice filtering
- Enrichment with mock data
- Graceful degradation (no bearer token, API error)

**Test Count:** ~10-12 tests (similar to Tool #1 and Tool #2)

### Integration Test

```bash
# Generate QBR with Lean Projects
python main.py qbr Bombardier

# Expected logs:
# [INFO] LeanDNA Lean Projects: fetching for Q1 2026 (2026-01 to 2026-03)
# [INFO] LeanDNA Lean Projects: fetched 15 projects from API
# [INFO] LeanDNA Lean Projects complete: 15 projects, $500K actual, 125% vs target

# Expected slides:
# - Lean Project Portfolio (order ~18)
# - Savings Waterfall (order ~18b)
```

---

## Data Registry Updates

Add to `docs/data-schema/DATA_REGISTRY.md`:

### New Section: LeanDNA Lean Projects

| Identifier | Description | Source field / query surface | Where used | Status note |
|------------|-------------|------------------------------|------------|-------------|
| `LEAN-PROJECT-LIST` | List of Lean projects for date range | `GET /data/LeanProject` | `leandna_lean_projects_client.py`, portfolio slide | Core surface |
| `LEAN-PROJECT-SAVINGS` | Monthly savings breakdown per project | `GET /data/LeanProject/{projectIds}/Savings` | `leandna_lean_projects_client.py`, waterfall slide | Core surface |
| `LEAN-PROJECT-ID` | Project unique identifier | `id` (LeanProject) | `leandna_lean_projects_client.py` | Core field |
| `LEAN-PROJECT-NAME` | Project name | `name` (LeanProject) | Portfolio slide | Core field |
| `LEAN-PROJECT-STAGE` | Project stage (Planning/Execution/Closed) | `stage` (LeanProject) | Portfolio slide, stage distribution | Core field |
| `LEAN-PROJECT-STATE` | Project health (good/warn/bad) | `state` (LeanProject) | Portfolio slide, state distribution | Core field |
| `LEAN-PROJECT-ACTUAL-SAVINGS` | Actual savings for period | `totalActualSavingsForPeriod` (LeanProject) | Portfolio slide, waterfall chart | Core field |
| `LEAN-PROJECT-TARGET-SAVINGS` | Target savings for period | `totalTargetSavingsForPeriod` (LeanProject) | Portfolio slide, waterfall chart | Core field |
| `LEAN-PROJECT-BEST-PRACTICE-FLAG` | Whether project is marked as best practice | `isBestPractice` (LeanProject) | Best practice slide filter | Core field |
| `LEAN-PROJECT-VALIDATED-FLAG` | Whether project results are validated | `isProjectResultsValidated` (LeanProject) | Portfolio summary | Core field |
| `LEAN-PROJECT-MONTHLY-ACTUAL` | Actual savings for a specific month | `actual` (LeanProjectMonthlySavings.savings[]) | Waterfall chart, time-series | Core field |
| `LEAN-PROJECT-MONTHLY-TARGET` | Target savings for a specific month | `target` (LeanProjectMonthlySavings.savings[]) | Waterfall chart, time-series | Core field |

**Note:** Lean Projects data is **optional** and only available when `LEANDNA_DATA_API_BEARER_TOKEN` is configured and customer uses LeanDNA Lean Projects module.

---

## Estimated Effort

| Task | Effort |
|------|--------|
| Client module (`leandna_lean_projects_client.py`) | 3-4 hours |
| Enrichment module (`leandna_lean_projects_enrich.py`) | 1-2 hours |
| Slide builders (2-3 slides) | 3-4 hours |
| Slide YAML configs | 30 min |
| Deck YAML updates | 15 min |
| Unit tests | 2-3 hours |
| Data registry updates | 30 min |
| Documentation | 1 hour |

**Total:** 1-1.5 days (vs 2 hours for Tool #2, 4-5 hours for Tool #1)

**Why longer?**
- More complex aggregation logic (portfolio summary, timeseries)
- Multiple endpoints (projects + savings + tasks)
- Waterfall chart generation (if using Sheets)

---

## Success Criteria

✅ Lean Projects data fetched and cached  
✅ Portfolio summary KPIs (total projects, savings, stage/state distribution)  
✅ Savings waterfall chart (actual vs target over quarter)  
✅ Top projects table with status indicators  
✅ Best practice projects filtered and displayed  
✅ QBR deck +2-3 slides (optional per customer)  
✅ All unit tests passing  
✅ Graceful degradation if bearer token missing or API fails  
✅ Zero linter errors  
✅ Data registry updated

---

## Open Questions

1. **Project filtering:** Should we exclude `Closed` stage projects, or show them with cumulative savings?
   - **Recommendation:** Include closed projects for the quarter (show full achievement)

2. **Savings categories:** API returns savings by category (e.g., "Inventory Reduction", "Supplier Management"). Should we break down by category or just show totals?
   - **Recommendation:** Show totals for QBR slides; add category breakdown to speaker notes

3. **Task/issue detail:** Should we show overdue tasks in the portfolio slide, or create a separate "Project Health" slide?
   - **Recommendation:** Add overdue task count to portfolio summary; defer detailed task slide to future "Project Deep Dive" deck

4. **Custom fields:** Some customers use custom fields extensively. Should we display them?
   - **Recommendation:** Ignore custom fields for v1; add as "advanced enrichment" in v2 if customer requests

5. **Multi-quarter trends:** Should we support YoY or multi-quarter comparisons?
   - **Recommendation:** Defer to v2; focus on single-quarter view for QBR

---

## Risk Mitigation

1. **Not all customers use Lean Projects**
   - **Mitigation:** Check if any projects exist before rendering slides; skip section if 0 projects
   - Log: "LeanDNA Lean Projects: 0 projects found for Q1 2026; skipping slides"

2. **Savings data may be incomplete**
   - **Mitigation:** Handle cases where `totalActualSavingsForPeriod` is 0 or null
   - Show "$0K" in slides (don't hide the slide); add speaker note about data quality

3. **API may return large project lists** (100+ projects)
   - **Mitigation:** Cache full list, but only show top 10-20 in slides (sorted by savings)
   - Aggregate stats use full list

4. **Waterfall chart complexity**
   - **Mitigation:** Start with simple stacked column chart (actual vs target per month)
   - Defer true "waterfall" visualization (with bridges) to v2 if Sheets doesn't support it easily

---

## Future Enhancements (Post-Tool #3)

1. **Project Deep Dive Deck** — 8-10 slides with task completion rates, issue tracking, stage history timeline
2. **Multi-quarter trends** — YoY savings comparison, project velocity (avg time from planning to closed)
3. **Category breakdown** — Savings by category (Inventory, Supplier, Process Improvement)
4. **Task health slide** — Overdue tasks, blocked tasks, completion forecast
5. **Agent tool** — Expose Lean Projects data to agent for ad-hoc queries ("What's our top savings project?")

---

## Related Documentation

- **Tool #1 Complete:** [`LEANDNA_TOOL_1_COMPLETE.md`](./LEANDNA_TOOL_1_COMPLETE.md)
- **Tool #2 Complete:** [`LEANDNA_TOOL_2_COMPLETE.md`](./LEANDNA_TOOL_2_COMPLETE.md)
- **Full API Analysis:** [`LEANDNA_DATA_API_TOOLS.md`](./LEANDNA_DATA_API_TOOLS.md)
- **Swagger Spec:** [`leandna-data-api-swagger.json`](./leandna-data-api-swagger.json)
- **Data Registry:** [`data-schema/DATA_REGISTRY.md`](./data-schema/DATA_REGISTRY.md)

---

## Summary Table: Tool #3 vs Tool #1 & Tool #2

| Aspect | Tool #1 (Item Master) | Tool #2 (Shortage Trends) | Tool #3 (Lean Projects) |
|--------|----------------------|---------------------------|-------------------------|
| **API Endpoints** | 1 | 4 | 5+ |
| **New QBR Slides** | 0 (enhanced 2) | 3 | 2-3 |
| **Data Type** | Inventory metrics | Shortage forecasts | Project savings |
| **Business Focus** | Supply chain efficiency | Production risk | Continuous improvement ROI |
| **Cache TTL** | 24h | 12h | 24h |
| **Chart Complexity** | None | Stacked area, dual | Waterfall/stacked column |
| **Effort** | 4-5 hours | ~2 hours | 1-1.5 days |
| **Customer Applicability** | All (if Item Master API enabled) | All (if Shortages API enabled) | Only customers using Lean Projects module |

---

**Recommendation:** Implement Tool #3 after validating Tool #2 with live data. Lean Projects adds high business value (ROI tracking) but requires more complex aggregation logic and chart generation than Tools #1-2.
