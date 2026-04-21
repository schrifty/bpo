# LeanDNA Tools #1 & #2 — Testing Guide

## Status

- ✅ **Tool #1: Item Master Data** — Implemented and tested (unit tests passing)
- ✅ **Tool #2: Material Shortage Trends** — Implemented and tested (unit tests passing)
- ⏸ **Tool #3: Lean Projects** — Spec'd but not built yet (waiting for Tools #1-2 validation)

---

## Pre-Testing Checklist

### 1. **Configure API Access**

Verify your `.env` file has the LeanDNA bearer token:

```bash
# In /Users/Marc.Schriftman/Documents/projects/bpo/.env
LEANDNA_DATA_API_BEARER_TOKEN=your_actual_bearer_token_here

# Optional overrides (defaults shown):
LEANDNA_DATA_API_BASE_URL=https://app.leandna.com/api
LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS=24
LEANDNA_SHORTAGE_CACHE_TTL_HOURS=12
```

**To check if token is configured:**

```bash
grep LEANDNA_DATA_API_BEARER_TOKEN .env
```

### 2. **Verify Slide Registration**

Check that new slide builders are registered:

```bash
grep "shortage_forecast\|critical_shortages_detail\|shortage_deliveries" src/slides_client.py
```

Should show 3 entries in `_SLIDE_BUILDERS` dict.

### 3. **Verify Deck Configuration**

Check that new slides are included in QBR deck:

```bash
grep "shortage" decks/qbr.yaml
```

Should show:
- `shortage_forecast`
- `critical_shortages_detail`
- `shortage_deliveries`

---

## Test Scenarios

### **Scenario 1: Full Integration Test (Recommended First)**

**Goal:** Generate a QBR with LeanDNA enrichment enabled and verify slides appear.

**Command:**

```bash
cd /Users/Marc.Schriftman/Documents/projects/bpo
python main.py qbr Bombardier
```

**What to Watch For:**

**Expected Logs (Tool #1 - Item Master):**
```
[INFO] LeanDNA enrichment: fetching Item Master Data for customer=Bombardier sites=all
[INFO] LeanDNA Item Master: fetched N items from API
[INFO] LeanDNA enrichment complete: N items, M high-risk, DOI bwd mean=X.X, excess=$Y
```

**Expected Logs (Tool #2 - Shortage Trends):**
```
[INFO] LeanDNA shortage trends: fetching weekly data for customer=Bombardier sites=all
[INFO] LeanDNA Shortage (weekly): fetched N items from API
[INFO] LeanDNA shortage trends complete: N items, M critical, peak week=YYYY-MM-DD, CTB impact=$X
```

**Expected Outcome:**
- QBR deck generated successfully
- No errors in logs
- New slides appear in deck (orders 15b, 15c, 15d)

**What the Slides Should Show:**

1. **Supply Chain Overview** (existing, enhanced):
   - Table now has 7 columns (added "DOI Bwd")
   - 4th KPI card: "Items >60d DOI Bwd" (if LeanDNA enabled)

2. **Platform Health** (existing, enhanced):
   - Header shows "X high-risk items" badge (if high-risk items found)

3. **Material Shortage Forecast** (NEW - order 15b):
   - Gray placeholder box (chart TODO)
   - 4 KPI cards: Total Items, Critical Items, Peak Week, Shortage Value

4. **Critical Material Shortages** (NEW - order 15c):
   - Table with up to 20 rows
   - Color-coded "First Critical" column (red/orange/yellow based on urgency)

5. **Shortage Resolution** (NEW - order 15d):
   - Gray placeholder box (chart TODO)
   - 3 KPI cards: Items with Schedules, Avg Deliveries, Next 7 Days Qty

---

### **Scenario 2: Graceful Degradation Test**

**Goal:** Verify QBR generation continues when LeanDNA is disabled.

**Steps:**

1. **Temporarily remove bearer token:**

```bash
# Comment out in .env
# LEANDNA_DATA_API_BEARER_TOKEN=...
```

2. **Generate QBR:**

```bash
python main.py qbr Bombardier
```

**Expected Logs:**
```
[DEBUG] LeanDNA enrichment skipped: LEANDNA_DATA_API_BEARER_TOKEN not set
[DEBUG] LeanDNA shortage trends skipped: LEANDNA_DATA_API_BEARER_TOKEN not set
```

**Expected Outcome:**
- QBR deck generates successfully
- Shortage slides are **skipped** (not included)
- Supply chain slide reverts to 6 columns (no DOI Bwd)
- Platform health slide has no high-risk badge

3. **Restore bearer token** after test

---

### **Scenario 3: Cache Behavior Test**

**Goal:** Verify Drive caching works and reduces API calls.

**Steps:**

1. **First run (cold cache):**

```bash
python main.py qbr Bombardier
```

**Expected:** Logs show "fetching from API"

2. **Check Drive for cache files:**

Go to Google Drive → QBR Generator folder → look for:
- `item_master_all_sites_YYYYMMDD.json`
- `shortage_weekly_all_sites_YYYYMMDD.json`

3. **Second run (warm cache):**

```bash
python main.py qbr Bombardier
```

**Expected:** Logs show "loaded N items from Drive cache" (no API call)

4. **Force refresh:**

```bash
# In Python (or modify qbr_template.py temporarily):
# force_refresh=True in enrichment calls
```

---

### **Scenario 4: API Error Handling Test**

**Goal:** Verify graceful error handling when API fails.

**Steps:**

1. **Use invalid bearer token:**

```bash
# In .env, set invalid token
LEANDNA_DATA_API_BEARER_TOKEN=invalid_token_12345
```

2. **Generate QBR:**

```bash
python main.py qbr Bombardier
```

**Expected Logs:**
```
[ERROR] LeanDNA enrichment failed: <error message>
[ERROR] LeanDNA shortage trends enrichment failed: <error message>
```

**Expected Outcome:**
- QBR still generates (doesn't crash)
- Shortage slides are skipped
- Other slides (health, engagement, etc.) work normally

3. **Restore valid token** after test

---

## What to Validate in Generated Deck

### **Open the Generated Deck in Google Slides**

Navigate to the output deck (check logs for URL or folder name like `2026-04-20 - Output/`)

### **Check: Supply Chain Slide (Tool #1 Enhancement)**

**What to Look For:**
- ✅ Table has 7 columns (Factory, On-Hand, On-Order, Excess, DOI Fwd, **DOI Bwd**, Late POs)
- ✅ "DOI Bwd" column has numeric values (not all dashes)
- ✅ 4th KPI card shows "Items >60d DOI Bwd" with count
  - If count > 10, card should be orange accent
  - If count ≤ 10, card should be blue accent

**If DOI Bwd column is all dashes:**
- Check logs: Did Item Master API return `daysOfInventoryBackward` field?
- Possible cause: API field not populated for this customer/site

### **Check: Platform Health Slide (Tool #1 Enhancement)**

**What to Look For:**
- ✅ Summary header shows badge like "15 high-risk items" (if high-risk items exist)
- ✅ Badge text is red/orange if count is high

**If no badge appears:**
- Check logs: How many high-risk items found? (threshold is `aggregateRiskScore >= 80`)
- May be correct if customer has no high-risk items

### **Check: Shortage Forecast Slide (Tool #2)**

**What to Look For:**
- ✅ Slide exists (title: "Material Shortage Forecast — Next 12 Weeks")
- ✅ Gray placeholder box with text "[Stacked Area Chart: Weekly Shortage Forecast]"
- ✅ 4 KPI cards below with non-zero values:
  - Total Items in Shortage: (count)
  - Critical Items: (count, orange if >10)
  - Peak Week: (date like "May 12")
  - Shortage Value: (dollar amount like "$2.3M")

**If KPI values are all zero:**
- Customer may have no shortages (which is good!)
- Or API returned empty shortage list

### **Check: Critical Shortages Table (Tool #2)**

**What to Look For:**
- ✅ Slide exists (title: "Critical Material Shortages — Next 90 Days")
- ✅ Table with up to 20 rows
- ✅ 7 columns: Item Code, Description, Site, First Critical, Days Short, CTB Impact, PO Status
- ✅ "First Critical" column has color-coding:
  - Red background: Items critical within 7 days
  - Orange background: 7-14 days
  - Yellow background: 14-30 days
  - White/no color: >30 days

**If table is empty:**
- Check logs: How many critical items found?
- Filter threshold is `criticalityLevel >= 3`

### **Check: Shortage Deliveries Slide (Tool #2)**

**What to Look For:**
- ✅ Slide exists (title: "Shortage Resolution — Scheduled Deliveries")
- ✅ Gray placeholder box with text "[Dual Chart: Shortage vs Scheduled Deliveries]"
- ✅ 3 KPI cards with values:
  - Items with Schedules: (count)
  - Avg Deliveries/Item: (decimal like "2.3")
  - Next 7 Days Qty: (quantity)

**If KPI values are low/zero:**
- Customer may not have PO delivery tracking enabled
- Or no deliveries scheduled in next 7 days

---

## Common Issues & Troubleshooting

### **Issue: "LEANDNA_DATA_API_BEARER_TOKEN not configured"**

**Cause:** Bearer token not set in `.env`

**Fix:**
```bash
# Add to .env
LEANDNA_DATA_API_BEARER_TOKEN=your_token_here
```

---

### **Issue: "401 Unauthorized" from API**

**Cause:** Invalid or expired bearer token

**Fix:**
1. Verify token is correct (copy from LeanDNA admin panel)
2. Check token hasn't expired
3. Confirm user account has API access enabled

---

### **Issue: "No items returned from API"**

**Cause:** Site filtering issue or no data for customer

**Fix:**
1. Check `/data/identity` endpoint to see authorized sites
2. Verify customer name maps to correct LeanDNA sites
3. Try running script to probe API directly:

```bash
python scripts/fetch_leandna_data_api_swagger.py --probe-auth
```

---

### **Issue: Shortage slides don't appear in deck**

**Possible Causes:**
1. API returned 0 shortage items (customer has no shortages)
2. Enrichment failed silently (check logs)
3. Slides are conditionally excluded if no data

**Debug:**
```bash
# Check enrichment result
grep "leandna_shortage_trends" <log_file>

# Check slide inclusion
grep "shortage" decks/qbr.yaml
```

---

### **Issue: DOI Bwd column is all dashes**

**Possible Causes:**
1. API doesn't return `daysOfInventoryBackward` for this customer
2. Field is null/missing in API response

**Debug:**
```python
# In Python console:
from src.leandna_item_master_client import get_item_master_data
items = get_item_master_data()
print(items[0].keys())  # Check if 'daysOfInventoryBackward' exists
print([i.get('daysOfInventoryBackward') for i in items[:5]])  # Check values
```

---

### **Issue: Charts are gray placeholders**

**Expected Behavior:** Charts for Tool #2 are deliberately placeholders (stacked area and dual charts not implemented yet)

**Future Work:** Will add chart generation using `DeckCharts` class (2-3 hours per chart)

---

## Data Quality Checks

After generating a deck, review these data points for reasonableness:

### **Item Master (Tool #1):**
- ✅ High-risk item count: Should be <20% of total items (if higher, may indicate data quality issue)
- ✅ DOI Backwards mean: Typical range is 30-90 days (if 0 or >300, verify)
- ✅ Items >60d DOI Bwd: Should correlate with excess inventory

### **Shortage Trends (Tool #2):**
- ✅ Critical items: Should be <30% of total shortage items
- ✅ Peak week: Should be within next 12 weeks (not in past)
- ✅ CTB impact: Should be reasonable relative to customer size (e.g., $100K-$10M range)
- ✅ Scheduled deliveries: Count should be reasonable (not 0 for all items, not 100% coverage either)

---

## Success Criteria

After testing, you should be able to confirm:

- ✅ QBR generates without errors (with bearer token configured)
- ✅ QBR generates without errors (with bearer token NOT configured — graceful degradation)
- ✅ Supply chain slide shows DOI Bwd column (if Item Master API has data)
- ✅ Platform health slide shows high-risk badge (if high-risk items exist)
- ✅ 3 new shortage slides appear (forecast, critical, deliveries)
- ✅ Critical shortages table has color-coded "First Critical" column
- ✅ KPI cards show non-zero values (assuming customer has shortages)
- ✅ Drive cache files are created and reused on second run
- ✅ All unit tests pass (12 shortage tests, 16 item master tests)

---

## Next Steps After Validation

### **If Tool #1 & #2 Work Well:**
1. ✅ Mark as production-ready
2. Document any customer-specific quirks found
3. Proceed to Tool #3 (Lean Projects) implementation

### **If Issues Found:**
1. Log specific API response anomalies
2. Adjust client code for edge cases
3. Add more unit tests for discovered scenarios
4. Re-test before moving to Tool #3

### **Chart Enhancement (Optional):**
Before Tool #3, consider adding real charts for Tool #2:
- Stacked area chart (shortage forecast)
- Dual line+bar chart (deliveries)
- Estimated effort: 4-6 hours total

---

## Test Run Command Summary

```bash
# Navigate to project root
cd /Users/Marc.Schriftman/Documents/projects/bpo

# Run unit tests first
python -m pytest tests/test_leandna_item_master.py tests/test_leandna_shortage.py -v

# Generate QBR with LeanDNA enrichment
python main.py qbr Bombardier

# Check logs for enrichment success
# Open generated deck in Google Slides
# Verify new slides and enhancements
```

---

## Questions to Answer During Testing

1. **Does the Item Master API return `daysOfInventoryBackward` for Bombardier?**
   - Check DOI Bwd column in supply chain slide
   
2. **Does the Shortage API return forecast data for Bombardier?**
   - Check if shortage slides appear and have non-zero KPIs

3. **How many high-risk items does Bombardier have?**
   - Check platform health slide badge

4. **What is the shortage peak week and CTB impact?**
   - Check shortage forecast KPI cards

5. **Do color-coded cells render correctly in critical shortages table?**
   - Verify red/orange/yellow backgrounds appear

6. **Are Drive cache files created and reused?**
   - Check Google Drive folder for JSON files
   - Re-run QBR and verify "loaded from Drive cache" log

---

## Contact & Support

**Implementation Docs:**
- Tool #1: `docs/LEANDNA_TOOL_1_COMPLETE.md`
- Tool #2: `docs/LEANDNA_TOOL_2_COMPLETE.md`
- Tool #3 (spec): `docs/LEANDNA_TOOL_3_LEAN_PROJECTS.md`

**Code Locations:**
- Item Master client: `src/leandna_item_master_client.py`
- Item Master enrichment: `src/leandna_item_master_enrich.py`
- Shortage client: `src/leandna_shortage_client.py`
- Shortage enrichment: `src/leandna_shortage_enrich.py`
- Slide builders: `src/slides_client.py` (search for "LeanDNA")
- Tests: `tests/test_leandna_item_master.py`, `tests/test_leandna_shortage.py`

---

**Ready to test? Run the command above and let me know what you see!**
