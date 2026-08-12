# Claude-Generated Deck Style Guide

Version 1.1 (Internal Use)

## Purpose

This guide governs **slides Claude designs end-to-end** and that Cortex renders from a small intermediate representation (IR) into Google Slides.

Today that path covers:

| Deck / command | When Claude designs slides |
|----------------|----------------------------|
| **Engineering Portfolio** (`engineering-portfolio`) | Default when `ANTHROPIC_API_KEY` is set (`CORTEX_ENG_PORTFOLIO_LLM_SLIDES` / `CORTEX_ENG_PORTFOLIO_CLAUDE_SLIDES`) |
| **Metrics decks** (`metrics-deck`, non-AKKR tags) | Default when Anthropic is configured (`--claude` / `--no-claude`) |

**AKKR metrics decks are out of scope here.** They use a fixed alphabetical KPI table and never call Claude.

The goal is the same as the hand-built guide: **clarity of signal for an executive reader**—dense enough to decide, never decorative or narrative filler.

Where this document and [`SLIDE_DESIGN_STANDARDS.md`](./SLIDE_DESIGN_STANDARDS.md) conflict for a Claude-IR slide, **follow this guide**. The hand-built guide remains authoritative for Python builders (`slides_client.py`, `slide_*.py`).

---

## Relationship to the hand-built standards

**Copy / reuse from** `SLIDE_DESIGN_STANDARDS.md`:

- One primary question and one primary takeaway per slide
- Title states the insight, not a topic label
- Executive / analytical density (not brochure layout)
- Brand color meanings (navy, blue accent, teal, light fills)
- KPI tiles: short labels, one primary value, shared accent in a row
- Bottom takeaway: implication, not restatement (here it is **required** on data slides, not optional)
- Text density: short bullets; no nested lists; no full-page prose
- Data integrity: only numbers present in the supplied digest
- Fail loud / say “no data” when a fact is missing—do not invent

**Do not apply** (Claude IR cannot do these today):

- Embedded Google Sheets charts, legends, or pie-slice overlays
- `_kpi_metric_card` / `createTable` Python helpers (IR uses `kpi_row` / `table`)
- `BODY_Y` / `BODY_BOTTOM` / scope-footer constants from builders
- Template-copy QBR hydration rules

---

## How Claude slides are built

1. Deck / slide YAML still defines **which** slides and **order**.
2. Cortex builds a **data digest** (scoped per slide for eng portfolio).
3. Claude returns one JSON **IR object** per slide.
4. `src/claude_slide_ir.py` maps IR → Slides API requests on a **720×405 pt** canvas.

Claude invents structure and copy within the IR vocabulary. Python does not apply LeanDNA builder layouts on this path.

Canonical schema text: `IR_SCHEMA_FOR_PROMPT` in `src/claude_slide_ir.py` (keep this guide and that string aligned when either changes).

---

## Hard constraints

### Slide intent

- Answer **one** primary question.
- Communicate **one** primary takeaway (title and/or `takeaway` element).
- Title states the takeaway (e.g. “Cycle time improved 18% QoQ”)—not “Cycle Time Overview”.
- Every data slide and every section divider carries a takeaway. A deck of data with no stated conclusions makes the reader do the analysis, which is the job the deck exists to do.

### Canvas and element budget

- Canvas: **720 × 405** points; coordinates `(x, y, w, h)` from top-left.
- **At most 10 elements** per slide.
- Keep strings short:
  - titles ≤ **60** characters
  - bullet lines ≤ **90** characters
  - takeaway ≤ **140** characters
  - `speaker_notes` ≤ **160** characters or omit
- No overlapping elements; nothing may clip past the canvas edge.
- Prefer **fewer, shorter strings** over long prose.
- Size boxes for the copy: a 140-character takeaway needs about **2 lines** at 11 pt across the content width. The renderer shrinks copy that would overflow its box (down to 8 pt), so an undersized box costs readability rather than clipping.

### Data integrity

- Use **only** facts and numbers present in the data digest / KPI facts.
- Never estimate, forecast, or invent KPIs, customers, or counts.
- If a metric is unavailable, say so plainly (or leave it out)—do not fabricate a value.
- Prefer concrete figures in titles and takeaways when the digest supplies them.

### Fail loud vs empty slides

- Required integrations that fail: surface the miss in copy if the digest includes an error; do not invent a healthy narrative.
- Optional integrations (e.g. Cursor) that are unconfigured: the deck plan should omit those slides entirely—Claude should not invent a “missing Cursor” placeholder page.

---

## Color: one meaning per hue

**Color is a data channel, not decoration.** A reader must be able to answer “why is that one a different color?” from the slide itself. If the answer is “so the row looks less flat”, the colors are wrong—five ordinary metrics get five identical tiles.

Use only these colors (hex), each for the meaning listed. Do not introduce gradients, glows, or off-brand hues.

| Meaning | Text / value | Fill | Where |
|---------|--------------|------|-------|
| **Structure** (not a signal) | `#FFFFFF` on navy | `#0B1F33` navy | Header bars, divider backgrounds, body text on light |
| **Ordinary metric** — the default | `#009AFF` | `#E8F4FC` | Every KPI value or emphasis with no status attached |
| **Needs attention** — off target, regressing, blocked, at risk | `#C0392B` | `#FDECEA` | Tiles, table rows, status words |
| **No data / excluded / N/A** | `#6B7280` | `#EEF0F3` | Missing metrics, excluded cohorts, takeaway band |
| **The one number to remember** | `#0B1F33` | `#AEFFF6` mint | At most **one** tile or callout per slide |
| **Series 2 / series 3** in an explicit comparison | `#38C0CE` teal, then `#7BC4FA` | — | Period-over-period or A/B pairs; teal also for `rule` |

**Rules**

- White page background by default (`"background": "#FFFFFF"`).
- Navy header bar with **white** title text is the default content-slide chrome.
- Hue encodes **status** or **series identity**—never rank, ordinal position, team, or variety. Do not give each tile in a row its own color.
- All tiles in a `kpi_row` share one fill/value pair unless a tile's **status** differs (attention, no data, or the single mint callout).
- At most **3** hues per slide beyond navy structure and white.
- Never rely on color alone; the label or copy must carry the same meaning.

`normalize_slide_ir` enforces the `kpi_row` part of this. Tiles claiming a status (red text or the attention fill, gray “no data” text) always keep their colors. A mint callout survives only in a row that is otherwise uniform; once the remaining tiles disagree, the row is treated as decoration and every non-status tile snaps to `#009AFF` on `#E8F4FC`.

---

## IR element vocabulary

Allowed `type` values only: `rect`, `text`, `kpi_row`, `bullets`, `table`, `takeaway`, `rule`.

### `rect`

Colored rectangle (header bars, tile backgrounds, section bands). Prefer navy for headers and light fills for cards—not decorative shapes without a job.

### `text`

Titles, section labels, short context lines, captions.

- Title on navy: ~20–24 pt bold white.
- Context under title: ~11–12 pt navy; **one line**; do not restate the title.
- Body / captions: ≥ **10 pt** when possible; never below **8 pt**.

### `kpi_row`

4–6 tiles across content width. Each item: short `label` + primary `value`.

- Row height **≥ 72** pt so label and value breathe.
- Labels ≤ **22** characters (long labels wrap and crush the number).
- Numbers must stay on **one line** (renderer shrinks type to fit).
- Use `fill` for tile background (`#E8F4FC`, `#EEF0F3`, `#AEFFF6`).
- Use `color` on the item for the value accent.
- Do not put methodology or caveats inside tiles—use bullets, takeaway, or speaker notes.

### `bullets`

Interpretation, implications, next actions.

- Prefer **2–4** bullets; never a wall of text.
- One short clause per bullet (~8–14 words when possible).
- No nested bullets.
- Do not restate the title without adding interpretation.

### `table`

Rankings, scorecards, off-target lists.

- Max **8** data rows (+ header); max **4** columns.
- Cells ≤ **22** characters when possible (wrapping grows row height).
- Tables grow **downward from `y`**; the requested `h` does not stop Google
  Slides from expanding wrapped rows.
- Calculate the table bottom before returning IR:
  `table_bottom = y + 26 × (header + data rows)`. Wrapped cells need more than
  26 pt, so avoid wrapping rather than relying on this minimum.
- Keep `table_bottom ≤ 395` when the table is the final element. If a rule,
  takeaway, bullets, or any other overlapping-width element follows, keep
  `table_bottom ≤ next_element_y - 12`.
- Default full-width placement: **`x=48`, `w=624`**. Start at **`y=72–140`**
  depending on whether a KPI row or context appears first.
- A table starting at `y=140` may contain at most **8 total rows** (header +
  data) when a bottom takeaway begins at `y=360`; use fewer rows if any cell
  may wrap.
- Side-by-side tables (`w≈300`) are only for **≤4 total rows** with very short
  cells. Narrow tables wrap sooner and are the most common cause of overflow.
- Never position a table under a KPI row without using the KPI row's actual
  bottom plus at least **12 pt** of spacing.
- If the rows do not fit, paginate or remove lower-priority rows. Never shrink,
  overlap, or place rows beyond the slide.
- Numeric columns should read as numbers (short, right-aligned intent via short cells).

### `rule`

Thin accent line (teal `#38C0CE` or navy). Use as a divider above a takeaway—not decoration.

### `takeaway`

Bottom “so what” band. **Every slide that shows data gets one.** A deck of 47 data slides with no takeaways has no message.

- One implication sentence with at least one concrete figure when available.
- Optional light fill (`#EEF0F3`).
- Only exempt: cover, agenda/TOC, the Takeaways slide itself (all takeaway), and appendix data-governance pages.
- If the digest genuinely supports no interpretation, say what is missing rather than padding.
- Banned filler (same spirit as eng takeaway prompts): “strategic review”, “investigate further”, “demands attention”, “closely monitor”, “leverage synergies”, vague “optimize”.

The eng portfolio generator logs a warning listing any non-exempt slide that came back without a takeaway element, so gaps are visible in the run output.

---

## Recommended slide archetypes (IR)

Claude may invent layout, but these patterns stay on-brand.

### 0. Takeaways (deck opener)

Navy header → 3–5 numbered `bullets`, each one sentence carrying its supporting number and its implication → optional single mint callout tile for the one figure that frames the review.

- Sits immediately after the cover; it is the deck's message on one page.
- Ordered by what the reader must act on first, not by deck section order.
- Spans the whole deck (delivery, quality, support pressure, AI spend vs output)—never a restatement of section titles.
- A reader who sees only this slide should know what is going well, what is at risk, and what decision is being asked of them.

### 1. Standing / snapshot

Navy header → optional one-line context → `kpi_row` (4–6) → optional short bullets → `takeaway`.

### 2. Attention / off-target

Navy header → `table` (KPI, Current, Target, Gap) worst-first → 1–3 bullets on top misses → `takeaway` naming the first fix.

### 3. Detail / reference scorecard

Navy header → full-width `table` of KPIs (paginate across slides if needed: “Scorecard (2 of 3)”) → optional short footer note via `text`, not a second takeaway.

### 4. Analytical insight (eng portfolio)

Navy header → title as insight → mix of `kpi_row` + `bullets` and/or a small `table` → bottom `takeaway`. One dominant idea; do not pack two unrelated analyses.

### 5. Section divider

Full navy background, large white section title, **plus a one-sentence takeaway** previewing that section's message with the number behind it. No KPI dump on dividers.

Dividers argue, they do not just announce: “Quality — bug backlog grew 18% while fix rate held flat” beats “Quality”. Section identity is carried by the title, not by a per-section background color.

No generic eyebrow labels (`SECTION`, `SECTION 3`). They add a line of text that carries no information.

### 6. Title / cover

Navy field, product/deck name as hero, one supporting line (audience + as-of). No KPI cards on the cover.

---

## Density and hierarchy

Default for Claude decks: **executive / analytical**—one dominant block, then supporting evidence.

Eye order:

1. Title  
2. KPI row or primary table  
3. Key numbers  
4. Bullets / takeaway  

Avoid:

- More than one dense table **and** a full KPI row unless the slide is explicitly a dashboard
- Decorative rects that do not structure content
- Repeating the same metric in title, KPI, bullets, and takeaway without adding meaning

---

## Continuation and pagination

- Prefer one slide when content fits.
- If paginating (e.g. long scorecard): label clearly — `Section Name (2 of 3)`.
- Cap continuation pages tightly (typically ≤ **3** for a metrics scorecard chunk; eng portfolio slides are usually one IR page each).
- Do not invent “missing from previous page” commentary—each page stands alone with its rows.

---

## Speaker notes

- Optional; ≤ **160** characters.
- Prefer methodology caveats or source/window notes that did not fit on the face.
- Do not dump raw JSON or long JQL into IR notes (hand-built decks have separate JQL tracing).

---

## Deck-specific notes

### Engineering Portfolio

- Audience: VP of Engineering.
- Deck shape: cover → **Takeaways** (`eng_takeaways`) → agenda → exec summary → sections, each opening with a divider that states the section takeaway.
- Slide YAML `prompt` + scoped digest are the brief—honor the slide’s job; do not wander into another chapter’s topic.
- Cursor / GitHub / AI correlation slides: only cite numbers in the digest; if Cursor is missing from the plan, Claude never sees that slide.
- Prefer operational insight over product marketing language.

### Metrics decks (non-AKKR)

- Standing first, then off-target, then detail tables.
- Off-target tiles / rows: use alert colors; on-target: blue/teal accents.
- Status wording: “Off target”, “On target”, or “No data”—no invented statuses.

### AKKR

- **Not Claude.** Alphabetical native tables only (KPI / Value / Target / Description). Do not apply Claude IR patterns to AKKR generation.

---

## Anti-patterns

Do not:

- Invent charts, logos, or element types outside the IR list
- Put long methodology paragraphs on the face
- Use microtext (&lt; 8 pt) to cram more content
- Color tiles, rows, or sections differently for variety, rank, or team identity
- Ship a data slide with no takeaway, or a divider that only names the topic
- Leave a takeaway label/band with empty or generic filler text
- Restate the title in every bullet
- Design a slide that would belong to another brand after removing LeanDNA navy/blue (weak branding)
- Quietly invent “healthy” narratives when the digest shows off-target or errors

---

## Maintenance

When changing Claude slide behavior:

1. Update **this guide** and `IR_SCHEMA_FOR_PROMPT` in the **same** change when palette, element types, or hard limits change.
2. Keep eng-portfolio / metrics system prompts consistent with the philosophy and banned-filler rules here.
3. Leave [`SLIDE_DESIGN_STANDARDS.md`](./SLIDE_DESIGN_STANDARDS.md) as the source of truth for **hand-built** builders; add a one-line pointer here when a shared rule changes for both paths.

---

## Quick checklist (per slide)

- [ ] One question, one takeaway; title carries the insight  
- [ ] ≤ 10 elements; strings within length caps  
- [ ] Only digest facts; no invented numbers  
- [ ] Takeaway present (data slides and dividers), and it states an implication  
- [ ] Every hue justified by status or series; shared KPI accent in a row  
- [ ] KPI labels short; table cells short; clearance under tables  
- [ ] Takeaway is a real implication—or omitted  
- [ ] Readable at presentation scale (no microtext crush)  
