# LeanDNA Tool #1 Implementation Complete

## Summary

Successfully implemented LeanDNA Item Master Data integration with QBR deck enhancements.

**Completion Date:** 2026-04-20  
**Status:** ✅ Ready for testing with live API

---

## What Was Built

### Core Infrastructure

1. **`src/leandna_item_master_client.py`** (280 lines)
   - HTTP client for `/data/ItemMasterData` endpoint
   - Thread-safe caching (in-memory + Drive backup, 24h TTL)
   - Data access functions:
     - `get_item_master_data(sites, force_refresh)` — fetch all items
     - `get_high_risk_items(threshold=80)` — filter by risk score
     - `get_doi_backwards_summary()` — aggregate DOI backwards stats
     - `get_abc_distribution()` — count by ABC rank
     - `get_lead_time_variance()` — compare planned vs observed
     - `get_excess_items()` — top excess inventory items
     - `check_reachable()` — health check

2. **`src/leandna_item_master_enrich.py`** (180 lines)
   - `enrich_qbr_with_item_master(report, customer)` — augments report dict
   - `format_leandna_speaker_notes_supplement(report)` — markdown for speaker notes
   - Graceful degradation: returns `enabled: False` if bearer token not set; logs error but continues on API failure

3. **`src/config.py`** (updated)
   - `LEANDNA_DATA_API_BASE_URL` — defaults to `https://app.leandna.com/api`
   - `LEANDNA_DATA_API_BEARER_TOKEN` — required for API access
   - `LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS` — defaults to 24h, range 1h-7d

4. **`tests/test_leandna_item_master.py`** (190 lines)
   - 16 tests covering all client functions and enrichment logic
   - ✅ **All tests passing**

---

## QBR Deck Enhancements

### Supply Chain Slide (`qbr-15-supply-chain`, `cs-health-17-supply-chain`)

**Before:**
| Factory | On-Hand | On-Order | Excess | DOI | Late POs |
|---------|---------|----------|--------|-----|----------|
| ... 6 columns ... |

**After (with LeanDNA enabled):**
| Factory | On-Hand | On-Order | Excess | DOI Fwd | **DOI Bwd** | Late POs |
|---------|---------|----------|--------|---------|-------------|----------|
| ... 7 columns with DOI Backwards ... |

**KPI Cards:**
- Original 3: On-hand, On-order, Excess (unchanged)
- **NEW 4th card:** "Items >60d DOI Bwd" (count, accent=ORANGE if >10 else BLUE)

**Layout:** 4 narrower KPI cards when LeanDNA enabled (vs 3 wider cards originally)

---

### Platform Health Slide (`qbr-14-platform-health`, `cs-health-16-platform-health`)

**Before:**
```
3 GREEN · 2 YELLOW · 45 shortages (12 critical)
```

**After (with LeanDNA enabled):**
```
3 GREEN · 2 YELLOW · 45 shortages (12 critical) · **15 high-risk items**
```

**Badge Color Logic:** Red if >10, Orange if >5, Gray otherwise (future enhancement)

---

## Configuration

Add to `.env`:

```bash
# LeanDNA Data API (optional)
LEANDNA_DATA_API_BASE_URL=https://app.leandna.com/api
LEANDNA_DATA_API_BEARER_TOKEN=your-bearer-token-here
LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS=24
```

Already documented in `.env.example` (lines 133-141).

---

## Data Registry Updates

Added to `docs/data-schema/DATA_REGISTRY.md`:

- **15 new field identifiers** including:
  - `DAYS-OF-INVENTORY-BACKWARD` (user-requested!)
  - `AGGREGATE-RISK-SCORE-ITEM`
  - `ABC-RANK`
  - `LEAD-TIME-OBSERVED` (vs planned)
  - `EXCESS-ON-HAND-VALUE-ITEM`
  - And 10 more item-level metrics

All marked with source, usage, and `**NEW**` status notes.

---

## Testing

```bash
# Run unit tests (16 tests, all passing)
python -m pytest tests/test_leandna_item_master.py -v

# Test API connectivity (requires bearer token in .env)
python -c "from src.leandna_item_master_client import check_reachable; print(check_reachable())"

# Inspect raw Cell for CSR DOI comparison
python scripts/inspect_csr_kpi_cells.py Bombardier

# Generate test QBR with LeanDNA enrichment
python main.py qbr Bombardier
```

---

## Behavior

### When `LEANDNA_DATA_API_BEARER_TOKEN` is set:

1. QBR pipeline calls `enrich_qbr_with_item_master()` after CS Report load
2. Client fetches Item Master Data (or uses cache if <24h old)
3. Supply chain slide shows 7 columns (with DOI Bwd) + 4 KPI cards
4. Platform health slide shows high-risk items badge
5. `report["leandna_item_master"]` contains full enrichment payload for future slide builders

### When bearer token is NOT set:

1. Enrichment adds `report["leandna_item_master"] = {"enabled": False}`
2. Supply chain slide shows original 6 columns + 3 KPI cards (no change)
3. Platform health slide shows original header (no high-risk badge)
4. **No errors, no warnings** — graceful degradation

### On API error:

1. Error is logged
2. `report["leandna_item_master"] = {"enabled": True, "error": "..."}`
3. Slides degrade to non-LeanDNA layout
4. QBR generation continues

---

## Files Changed

**Created:**
- `src/leandna_item_master_client.py`
- `src/leandna_item_master_enrich.py`
- `tests/test_leandna_item_master.py`
- `docs/LEANDNA_TOOL_1_ITEM_MASTER.md`
- `docs/LEANDNA_DATA_API_TOOLS.md`
- `docs/leandna-data-api-swagger.json`

**Modified:**
- `src/config.py` (+10 lines: LeanDNA config vars)
- `src/qbr_template.py` (+6 lines: enrichment call)
- `src/slides_client.py`:
  - `_supply_chain_slide()` (+45 lines: DOI Bwd column, 4th KPI card, conditional layout)
  - `_platform_health_slide()` (+10 lines: high-risk items badge)
- `.env.example` (+8 lines: DATA_API_ docs, already done earlier)
- `docs/data-schema/DATA_REGISTRY.md` (+18 lines: LeanDNA section with 15 field entries)

---

## Next Steps

### Immediate (Testing with Live API)

1. **Add bearer token to `.env`:**
   ```bash
   LEANDNA_DATA_API_BEARER_TOKEN=your-real-token
   ```

2. **Test API connectivity:**
   ```bash
   python -c "from src.leandna_item_master_client import check_reachable; print(check_reachable())"
   ```
   Expected: `{'status': 'ok', 'item_count': XXXX, 'response_time_ms': YYY}`

3. **Generate test QBR:**
   ```bash
   python main.py qbr Bombardier
   ```
   Look for log lines:
   - `[INFO] LeanDNA enrichment: fetching Item Master Data...`
   - `[INFO] LeanDNA enrichment complete: N items, M high-risk, DOI bwd mean=X.X...`
   
4. **Inspect deck:**
   - Supply chain slide: verify 7 columns (DOI Fwd, DOI Bwd), 4 KPI cards
   - Platform health slide: verify high-risk items badge in header

### Follow-Up (After Validation)

1. **Site mapping:** Implement `_resolve_customer_sites()` in enrichment module (currently returns None for "all sites")
2. **Per-site DOI Bwd:** Refactor enrichment to aggregate DOI backwards per factory (vs global mean)
3. **Agent tools:** Create `src/tools/leandna_tool.py` with LangChain wrappers for ad-hoc queries
4. **Deep Dive deck:** Build Supply Chain Deep Dive deck (8 slides with scatter charts, risk tables, etc.)
5. **Tool #2:** Shortage Trends API integration (weekly buckets, CTB forecasts)
6. **Tool #3:** Lean Projects API integration (savings waterfall, project portfolio slide)

### Documentation

- ✅ Field registry updated (`DATA_REGISTRY.md`)
- ✅ Tool analysis docs created (`LEANDNA_TOOL_1_ITEM_MASTER.md`, `LEANDNA_DATA_API_TOOLS.md`)
- ⏭️ Slide design standards: add DOI Bwd column spec (future, not urgent)

---

## Known Limitations / TODOs

1. **Site mapping:** Currently uses all authorized sites; need customer → site ID mapping (teams.yaml or identity API)
2. **Per-site DOI Bwd:** Currently shows global mean in every row; needs per-factory aggregation
3. **Speaker notes:** Enrichment supplement text is formatted but not yet appended to slides (speaker notes are written separately in hydrate phase; integrate there)
4. **Drive cache subfolder:** Cache files written to generator root; should use `cache/` subfolder for organization
5. **Parquet Data Share:** Not implemented yet; `get_item_master_data` fetches JSON, not parquet bulk export

---

## Success Criteria Met

✅ User-requested feature: **DOI Backwards** now available  
✅ Supply chain slide enhanced with new column + KPI card  
✅ Platform health slide shows high-risk items count  
✅ All tests passing (16/16)  
✅ Graceful degradation when bearer token missing or API fails  
✅ Zero linter errors  
✅ Documentation complete (registry, tool analysis, implementation notes)

---

## Estimated Value

**For QBR/CS Health decks:**
- Answers "which items are slow-moving?" (DOI backwards >60 days)
- Surfaces top risk items proactively
- Enables data-driven CS conversations ("here are your 5 riskiest SKUs")

**Development effort:** ~4-5 hours (vs estimated 1 day; faster than planned)

**Next integration:** Tool #2 (Shortage Trends) — weekly shortage buckets + CTB forecasts → new slide type
