# Support Review Fixes - Summary

## Issue 1: Missing "Recently opened HELP tickets (SED)" slide

**Problem**: The Support Review deck slides "Recently opened HELP tickets (SED)" and "Recently closed HELP tickets (SED)" were showing as unavailable because the required data (`customer_help_recent`) was never being fetched.

**Root Cause**: While `jira_client.py` had a `get_customer_help_recent_tickets()` function, it was never called when generating the Support Review deck. The `create_health_deck()` function had special data-fetching logic for the Salesforce deck but not for the Support deck.

**Fix**: Added special handling in `create_health_deck()` (lines 7285-7325 in `src/slides_client.py`) to fetch support-specific Jira data when `deck_id == "support"`:
- Fetches `customer_ticket_metrics` if not already in report (provides TTR/TTFR SLA data)
- Fetches `customer_help_recent` for the specific customer (tickets opened/closed in last 45 days)
- Works for any customer (QBR bundle uses the QBR's customer, not hardcoded)
- Includes proper error handling

**Files Changed**:
- `src/slides_client.py` (lines 7285-7325)

---

## Issue 2: Incorrect TTR/TTFR Median Calculation

**Problem**: The "TTR (1yr median)" value displayed "8.0h" when it should have been "7.0h" for an even-length dataset.

**Root Cause**: The `_compute_sla()` function used an incorrect median calculation:
```python
med_ms = values[len(values) // 2]  # WRONG for even-length lists
```

This works for odd-length lists but fails for even-length lists. For example:
- Dataset: [4h, 6h, 8h, 10h]
- Old calculation: `values[2]` = **8.0h** ❌
- Correct median: `(6h + 8h) / 2` = **7.0h** ✓

**Why Manual Calculation Was Necessary**: JSM's REST API only provides SLA data at the individual ticket level (elapsed time in milliseconds per ticket). There is no API endpoint that provides aggregated statistics like median or average. Client-side calculation is required.

**Fix**: 
1. Replaced manual median calculation with Python's `statistics.median()` in `src/jira_client.py` (line 992)
2. Added comprehensive unit tests in `tests/test_jira_median_fix.py` to verify correctness
3. All 5 tests pass, confirming the fix handles both even and odd-length lists correctly

**Files Changed**:
- `src/jira_client.py` (lines 978-992)
- `tests/test_jira_median_fix.py` (new file, 5 passing tests)

---

## Enhancement 1: Added Average TTR/TTFR to Support Review

**Addition**: Added two new KPI cards to the "Customer Ticket Metrics" slide showing average TTR and TTFR alongside the existing median values.

**Implementation**:
- Added Row 3 with two new KPI cards in `_customer_ticket_metrics_slide()` (lines 3031-3040)
- Cards display `ttr.get("avg")` and `ttfr.get("avg")` (already calculated by `_compute_sla()`)
- Updated chart positioning to accommodate the new row

**Layout**:
- Row 1: Unresolved tickets | Resolved (6mo) | SLA adherence
- Row 2: **TTR (1y median)** | **TTFR (1y median)**
- Row 3: **TTR (1y average)** ⬅️ NEW | **TTFR (1y average)** ⬅️ NEW
- Below: Bar charts (by type, by status)

**Files Changed**:
- `src/slides_client.py` (lines 3031-3046)
- `src/evaluate.py` (updated descriptions to mention "median & average")

---

## Issue 3: Support Deck Customer Hardcoded to SED

**Problem**: When the Support Review deck was generated as part of a QBR bundle, it needed to use the QBR's customer data, not hardcoded "Safran Electronics & Defense (SED)".

**Root Cause**: The support deck enrichment in `create_health_deck()` was only fetching `customer_help_recent`, but not ensuring `customer_ticket_metrics` was populated for the specific customer. The QBR's base health report includes generic Jira data from `get_customer_jira()` but not the detailed per-customer ticket metrics needed for the support deck slides.

**Fix**: Updated the support deck enrichment logic to:
1. Check if `customer_ticket_metrics` exists in the report for this customer
2. If not, fetch it using `jira_client.get_customer_ticket_metrics(customer)`
3. Always fetch `customer_help_recent` for the specific customer
4. Use the `customer` variable from the report, which is the QBR's customer when run in bundle context

**Impact**: The Support Review deck now correctly generates for whatever customer the QBR is run for (e.g., `python main.py qbr Bombardier` will create a Support deck for Bombardier, not SED).

**Files Changed**:
- `src/slides_client.py` (lines 7285-7325)

---

## Issue 4: Recent Tickets Slide Shows Too Many Tickets

**Problem**: The "Recently opened HELP tickets" and "Recently closed HELP tickets" slides were showing all tickets (up to 45) in a text list format, running off the page and making it hard to scan.

**Root Cause**: The slide builder used a simple text box list format with pagination, showing as many tickets as would fit (28+ rows). No table structure, and too much data made it overwhelming.

**Fix**: Completely redesigned the recent tickets slides:
1. **Table format**: Now displays tickets in a clean 6-column table:
   - **ID** (clickable link to Jira)
   - **Title** (truncated to 60 chars)
   - **Status**
   - **Priority** ⬅️ NEW FIELD
   - **Created** (date)
   - **Resolved** (date)

2. **Limited to 15 rows**: Shows only the 15 most recent tickets to fit comfortably on one page

3. **Added priority field**: Updated Jira client to fetch priority field in `_CUSTOMER_TICKET_SLIDE_FIELDS`

4. **Better formatting**: 
   - Proper table headers with bold text
   - Ticket IDs are bold blue hyperlinks
   - Footnote shows if more tickets exist ("Showing 15 of 45 tickets")

**Files Changed**:
- `src/jira_client.py` (lines 58-61, 1202-1367): Added priority field, created generic project tickets function, added resolved-by-assignee function
- `src/slides_client.py` (lines 3108-3644): Rewrote ticket slides to use table format, added generic helpers, added assignee slides
- `decks/support.yaml`: Added 6 new slides (CUSTOMER, LEAN, assignee breakdowns)

---

## Enhancement 3: Added Resolved Tickets by Assignee Slides

**Addition**: Added two new slides showing resolved tickets grouped by assignee for workload visibility.

**Implementation**:
1. **New Jira client function**: Created `get_resolved_tickets_by_assignee()` that:
   - Fetches resolved tickets for a project and customer in the last 90 days
   - Groups them by assignee name
   - Sorts by count descending to show who's resolving the most tickets

2. **New slide builders**: Created 2 new slide builders:
   - `_help_resolved_by_assignee_slide()` - HELP project resolved tickets by assignee
   - `_customer_resolved_by_assignee_slide()` - CUSTOMER project resolved tickets by assignee

3. **Table format**: Both slides display as clean 2-column tables:
   - **Assignee** (name)
   - **Tickets Resolved** (count)
   - Sorted by count descending (highest first)
   - Limited to top 20 assignees to fit on one page

4. **Support deck enrichment**: Updated to fetch resolved-by-assignee data for both HELP and CUSTOMER projects

**New Slides**:
- **HELP Tickets Resolved by Assignee (Last 90 Days)** - Shows who's resolving HELP tickets
- **CUSTOMER Tickets Resolved by Assignee (Last 90 Days)** - Shows who's resolving CUSTOMER tickets

**Use Case**: Helps identify workload distribution, recognize high performers, and spot potential bottlenecks in ticket resolution.

**Files Changed**:
- `src/jira_client.py` (lines 1299-1367): Added `get_resolved_tickets_by_assignee()` function
- `src/slides_client.py` (lines 3470-3644, 7292-7293, 7356-7357): Added slide builders and registered them
- `src/slides_client.py` (lines 7343-7420): Updated support deck enrichment to fetch assignee data
- `decks/support.yaml`: Added 2 new assignee slides

---

## Testing

All changes have been validated:
1. ✅ Unit tests pass for median calculation fix (5/5)
2. ✅ No linter errors introduced
3. ✅ Existing slide layout logic correctly accommodates new KPI cards
4. ✅ Support deck now dynamically uses the QBR customer
5. ✅ Recent tickets table fits on one page with proper formatting

---

## Usage

### Standalone Support Deck
```bash
python decks.py support
```
Generates for hardcoded "Safran Electronics & Defense (SED)" with match terms.

### Support Deck in QBR Bundle
```bash
python main.py qbr Bombardier
```
Automatically generates a Support Review deck for Bombardier as part of the companion decks.

---

## Impact

1. **Support Review deck is now complete** - All 10 slides will render with data (was 4, now 10)
2. **More accurate SLA metrics** - Median values now correctly calculated
3. **Enhanced analytics** - Average provides additional context alongside median for understanding SLA distribution
4. **Customer-specific support decks** - QBR bundles now generate support decks for the correct customer, not hardcoded SED
5. **Better ticket visualization** - Recent tickets now in scannable table format with priority, limited to 15 most recent
6. **Improved usability** - Table format with clickable ticket IDs makes it easy to drill into specific tickets
7. **Multi-project visibility** - Now shows recent tickets from HELP, CUSTOMER, and LEAN projects for complete support picture
8. **Workload visibility** - New assignee breakdown slides show who's resolving tickets and workload distribution
