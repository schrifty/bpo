# QBR adaptation: mapping-first process (proposed)

This document describes the **intended** adaptation flow when Phase A no longer uses a parallel LLM pass and instead drives replacements **only** from `config/qbr_mappings.yaml`, then applies them through the existing Slides mutation path (“Phase B”).

Today’s code still runs **Phase A** as LLM-based reasoning (`adapt_custom_slides` in `src/evaluate.py`), with **`apply_explicit_qbr_mappings`** (`src/qbr_hydrate_mappings.py`) acting as a **post-pass** that adjusts LLM-produced replacement rows when explicit QBR mode is on. The process below is the **target design** for replacing that LLM Phase A for QBR runs.

## Scope

- Applies when the report marks **explicit QBR mappings** (see `REPORT_KEY_EXPLICIT_QBR_MAPPINGS` / `qbr_hydrate_mappings.py`) and `config/qbr_mappings.yaml` is the source of truth for **what** to find on each slide and **which data element** supplies the new value.
- **Phase B** remains responsible for mutating the Google Slides presentation (batch updates per slide, sequential over slides), updating speaker notes, summary slide behavior, and related QA—same *category* of work as today; only **how replacement rows are produced** changes.

## Inputs

1. **Presentation**: working copy of the QBR template (pages already resolved to `page_ids` / object ids).
2. **`qbr_mappings.yaml`**: flattened via `expand_mapping_rules()` into rows with at least:
   - `slide_number` (optional; 1-based index when scoped to one slide),
   - `slide_id` (optional; YAML slide type / id filter aligned with `_slide_plan`),
   - `source` (string to locate in slide text),
   - `target` (catalog-oriented name / dotted path into the hydrated **`data_summary`**),
   - optional `name` / `data_element_name` for logging.
3. **`data_summary`**: compact dict derived from the customer **`report`** (`_build_data_summary` in `evaluate.py`), used to resolve `target` to a scalar (today via `resolve_data_summary_target_path` + `data_summary_lookup` in `data_field_synonyms.py`).

## Per-slide algorithm

For each slide `page_id` in adapt order:

1. **Resolve slide context**
   - Determine **1-based slide index** in the presentation (`slide_number`).
   - If using slide-type filtering, resolve **`slide_type`** for this page (same idea as `build_adapt_page_slide_type_by_page_id` today).

2. **Load mapping rows for this slide**
   - Select all YAML rows where:
     - `slide_number` is **null/absent** (global) **or** equals this slide’s index, **and**
     - `slide_id` is **unset** or matches this page’s `slide_type`, **and**
     - `source` and `target` are both non-empty (rows with empty `target` are authoring stubs only).

3. **Extract slide text elements**
   - Parse the Slides API structure into **`text_elements`** (same representation Phase A uses today: boxes/shapes with extractable text and stable element identity for Phase B).

4. **For each mapping row (deterministic order, e.g. YAML order)**

   **4a. Find `source` on the slide**

   - Search slide text for the **`source`** string using rules consistent with authoring expectations, for example:
     - **Bracket placeholders**: treat `source` as an **exact** match on a text element’s content (e.g. `[???]`).
     - **Phrase sources**: treat `source` as a **substring** match within the normalized haystack for a candidate element (similar narrowing to `_narrow_synonym_haystack` / `_normalize_context` used in `apply_explicit_qbr_mappings`).
   - **Replace all**: Every text element that qualifies gets its **own** replacement row for Phase B (same formatted value). Multiple shapes/boxes may independently contain the same `source`; do **not** stop after the first hit. If a **single** element contains the `source` more than once, apply the replacement so **all** occurrences in that element are updated (substring replace-all semantics), unless the pipeline only supports whole-element replacement—in that case one row replacing the full element text is acceptable when it achieves the same visible result.
   - If **no** occurrence qualifies:
     - **Log a warning** (include slide number, optional element name, `source`, and `target`).
     - **Skip** this row (do not invent replacements).

   **4b. Resolve `target` to a replacement value**

   - Map `target` through the same resolution path as today’s explicit pass (human label → dotted path → lookup in `data_summary`).
   - If the value is **missing**, **empty**, or **not a scalar suitable for inline text** (e.g. raw dict/list):
     - **Log a warning** (slide number, `source`, `target`, resolved path if known).
     - **Skip** this row.

   **4c. Format for Slides**

   - Apply the same presentation formatting conventions as the explicit mapper where relevant (percent semantics, numeric suffixes on the original token, etc.—see `apply_explicit_qbr_mappings`).

   **4d. Record replacement(s)**

   - Append **one replacement record per matched text element** compatible with Phase B (`replacements` list: element identity, `original`, `new_value`, `mapped=True`, optional `field` / path for notes QA).

5. **Invoke Phase B for this slide**

   - After all rows for the slide are processed, run the **existing** adaptation application for that page: merge any non-YAML extras (if still needed), build Slides `batchUpdate` requests from `replacements`, execute sequentially with other slides unchanged.

## Logging expectations

| Situation | Action |
|-----------|--------|
| `source` not found on slide | **Warning**; skip row |
| `source` found, `target` cannot be resolved or value unusable | **Warning**; skip row |
| Replacement built | **Debug/info** (optional): slide, element label, path applied |

## Relationship to current modules

| Piece | Role |
|-------|------|
| `config/qbr_mappings.yaml` | Canonical list of `source` → `target` per slide / global |
| `expand_mapping_rules()` | Flatten YAML into iterable rows |
| `data_summary` + synonym/path helpers | Resolve `target` to a value |
| `_extract_slide_text_elements` / Phase B helpers | Locating text and applying updates |

## Implementation notes (open choices)

These are **not** blocking for documenting intent but matter when coding:

1. **Parallelism**: The proposed Phase A is cheap enough that per-slide sequential processing may suffice; any parallelism must not reorder Slides API writes (Phase B stays sequential per presentation).
2. **Non-QBR decks**: They may continue to use LLM Phase A + synonym table unless separately migrated to a mapping file.
3. **Caches**: Today’s adapt/analysis caches assume LLM fingerprints; a mapping-first path may need different cache keys or no adapt cache for QBR.

**Multiple matches (resolved):** **`source` matches → replace all** qualifying text elements on the slide (one Phase-B-ready row each).

## References

- `src/evaluate.py` — `adapt_custom_slides` (Phase A / Phase B split)
- `src/qbr_hydrate_mappings.py` — `load_qbr_mappings`, `expand_mapping_rules`, `apply_explicit_qbr_mappings`
- `src/data_field_synonyms.py` — `resolve_data_summary_target_path`, `data_summary_lookup`
- `config/qbr_mappings.yaml` — version 2 schema (`slides`, `global_elements`; see `bootstrap_qbr_mappings_from_slides` in `qbr_hydrate_mappings.py`)
