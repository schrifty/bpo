# Slide data gap analysis

This note explains **what “missing data” means** on customer-facing slides in **discrete manufacturing, inventory, supply chain, and ERP-adjacent** contexts, how to **isolate** requests per slide, and how to **prompt** models without fooling yourself or the reader.

The companion tool is `scripts/identify-data-gaps.py`: it extracts **text** from each Slides page, applies **heuristics** (brackets, lorem, hydrate-style tokens, embedded charts/images), and optionally runs an **LLM** pass framed for operations technology domains.

---

## Why this is hard

1. **Slides mix intents.** One page may combine editorial narrative (“alignment”, “priorities”), **named obligations** (“site CTB %”, “shortage dollars”), and **visual data** (charts, screenshots) that **text extraction never sees**.

2. **“Missing” is ambiguous.**
   - **Template gap:** `[Executive sponsor]`, `Lorem ipsum`, `TBD`, `[???]` — never substituted.
   - **Semantic gap:** polished prose that never commits to a **metric**, **time bucket**, or **system-of-record**.
   - **Structural gap:** chart or PNG exported from another tool; numbers exist visually but not in `replaceAllText` targets.

3. **Domain coupling.** In discrete manufacturing and supply chain, the same phrase (“**material availability**”) might imply **MRP**, **on-hand**, **supplier commits**, or **ATP** depending on company vocabulary. A generic LLM will happily blur those unless you **constrain** it.

4. **Compliance and trust.** Inventing “missing” KPIs or implying an ERP field exists when it does not destroys credibility. The goal is to **surface hypotheses** (“this slide *reads* like it wants shortage aging by site”) not **assert warehouse truth**.

---

## Systems-of-record framing

When interpreting gaps, it helps to classify **where data usually lives** (examples only — your tenant may differ):

| Concept area | Typical concepts | Common sources (examples) |
|--------------|------------------|---------------------------|
| Demand & fulfillment | Forecast, orders, ATP, allocations | ERP order mgmt, APS/IBP |
| Inventory & materials | On-hand, DOI, excess, shortage lists | ERP IM, WMS, planning spreadsheets |
| Procurement | Open POs, lead times, expedites | ERP procurement |
| Production | MO/WO status, scrap, downtime | MES, ERP shop floor |
| Logistics | Inbound/outbound, ASN | TMS, WMS |
| Financial / value | Savings, chargebacks, booked benefits | FP&A, project trackers, LeanDNA-style exports |
| Product usage | Adoption, champions | Product analytics (e.g. Pendo) |

Slides often **aggregate** across these without naming the source. Your job in analysis is to **tie vague bullets** to **candidate domains**, not to fabricate numbers.

---

## How to isolate and understand requests

### 1. Separate slide **role** from slide **payload**

Ask:

- Is this **agenda / divider / cover**? Often **no** numeric obligation — missing-data findings should be empty or “editorial only”.
- Is this **performance / KPI / operational review**? Expect **time period**, **scope** (site, plant, book), and **units**.
- Is this **program / transformation** (Lean, SIOP, ERP rollout)? Expect **milestones**, **benefits**, **risks** — still often underspecified in decks.

The script reports **element counts** (tables, charts, images). Heavy **chart/image** counts with little text usually mean **visual-only data** — flag as high structural uncertainty.

### 2. Read **what the template literally asks for**

Bracket tokens and hydrate-style markers (`[???]`, `[00%]`, …) are **explicit** asks. Map each to:

- A **business concept** (what decision does this support?)
- A **candidate metric name** (internal jargon allowed)
- Whether BPO’s **data dictionary / mappings** (`config/comprehensive_data_element_list.json`, `config/qbr_mappings.yaml`) already defines a path.

### 3. Classify **confidence**

| Level | Meaning |
|-------|---------|
| **High** | Explicit placeholder or obvious filler (brackets, lorem, duplicate “QBR highlight” slots). |
| **Medium** | Generic wording (“improvement”, “risk”) without numbers — likely needs a KPI but which one is ambiguous. |
| **Low** | Inference from tone only — treat as **hypothesis**, not backlog.

### 4. Use **data lineage** when automating fills

If you hydrate from APIs:

- Prefer **named paths** in your catalog over free-text guesses.
- When multiple sources could satisfy a phrase, **warn** rather than pick silently (see project rules on Salesforce vs Pendo authority).

---

## Prompting strategy (subtle and strict)

Use **two layers**: **heuristics** (deterministic) + **LLM** (interpretive). Never rely on the LLM alone for pass/fail without human review.

### System-level principles

1. **Role:** Analyst for **discrete manufacturing / supply chain / ERP ecosystems**, not creative writer.
2. **Evidence boundary:** Only reference **strings present** in the slide extract (and heuristic flags). No customer-specific facts not in text.
3. **Output shape:** JSON with **bounded fields** — `label`, `business_concept`, `likely_sources[]`, `confidence`, `notes`. Avoid prose essays inside cells.
4. **Hedging:** Require a `hedges` or `notes` field when the model infers across ambiguous domains (“could be IBP or spreadsheet”).
5. **Temperature:** Low (0–0.2) for gap extraction; creativity belongs in **wordsmithing**, not **metric detection**.

### User-level content

Provide:

- **Concatenated text preview** (truncated is fine).
- **Per-slide structure:** table vs bullet counts, chart/image markers.
- **Heuristic hits** from the script so the model **anchors** on known stubs.

### Failure modes to prompt against

- **Inventing ERP tables or fields** (“SAP table MARC”) without evidence.
- **Conflating** CTB, CTC, and fill-rate if the slide only says “supply performance”.
- **Declaring hydration complete** when only **placeholders** were removed but **semantics** are still empty.

### Iteration

- **Slide-type-conditioned prompts:** If you know `slide_type` from YAML, inject a short rubric (“for `platform_health` slides, expect CTB/CTC/shortage language”).
- **Vision (optional):** Thumbnails help **chart-heavy** slides; they add cost and privacy sensitivity — only when text is insufficient.

---

## Using `scripts/identify-data-gaps.py`

```bash
# Default: resolves the canonical QBR template on Drive (same name as ``QBR_TEMPLATE_FILE_NAME`` / QBR flows).
# JSON only — **never appends slides** (the template stays read-only on Drive).
python scripts/identify-data-gaps.py
python scripts/identify-data-gaps.py --out gap_report.json

# Scan an explicit presentation (e.g. a customer deck copy): **append** inventory table slides by default.
python scripts/identify-data-gaps.py --presentation 'https://docs.google.com/presentation/d/PRES_ID/edit'
python scripts/identify-data-gaps.py --presentation PRES_ID --out gap_report.json --max-slides 10

# Explicit deck + JSON only (no mutation)
python scripts/identify-data-gaps.py --presentation PRES_ID --no-write-summary-slide --out gap_report.json

# Add LLM interpretation (requires API keys per src/config); template scan
python scripts/identify-data-gaps.py --llm --out gap_report.json

# Verbose JSON (per-slide extracts + gap_inventory_rows) — default output is compact replacements only
python scripts/identify-data-gaps.py --presentation PRES_ID --verbose-json --no-write-summary-slide -o full.json
```

**Default JSON shape** (time-saving focus): `replacement_count`, `replacements[]` where each item has `slide`, `find` (text to substitute), `replace: { value (usually null), format, display? }`, optional `source`. There is no populated `value` unless you extend the tool to bind live data; format/display carry how the replacement should read once sourced.

**Slide mutation:** When `--presentation` points at **your** deck (not the resolved canonical template default), the script **appends** one or more slides titled **CSM lookup — data to source** at the **end** by default (`--write-summary-slide` / omit `--no-write-summary-slide`). The canonical QBR template is never modified: default runs resolve that file but only emit JSON. Rows intentionally **omit** placeholders a CSM can fill without lookups (today’s date, executive sponsor / CSM / AE / site roster, logos, housekeeping). The table highlights **performance and operations gaps** (KPI placeholders, hydrate tokens, vague metrics, consolidated chart/image cues). Columns: slide, field/signal, suggested datasource, data type, formatting, time/duration, accuracy/precision, context. Seven data rows per slide plus header; re-running appends another block unless `--no-write-summary-slide`.

**JSON output:**

- `gap_inventory_rows`: flattened rows matching the table (same columns as the slide).
- `heuristic_hits`: deterministic flags (`bracket_placeholder`, `embedded_chart`, …).
- `severity_hint`: coarse rollup (`high` / `medium` / `low`).
- `llm` (if `--llm`): structured `missing_data_items` with sourcing metadata — **review before acting**.

---

## Limitations

- **No OCR** on images; screenshots of ERP remain opaque to text-only scans.
- **Empty table cells** may not appear in extracts (only non-empty runs are collected).
- **Localization** and **custom fonts** do not change logic but may affect regex heuristics.
- **LLM output is advisory** — validate against real integrations and governance rules before publishing numbers.

---

## Related repo artifacts

- Hydrate text extraction: `src/hydrate_extract.py`
- Explicit QBR mappings: `config/qbr_mappings.yaml`, `docs/QBR_MAPPING_FIRST_ADAPTATION.md`
- Canonical data paths: `config/comprehensive_data_element_list.json`
