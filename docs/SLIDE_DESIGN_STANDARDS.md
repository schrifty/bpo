# LeanDNA Slide Design and Data Visualization Standards
Version 1.5 (Internal Use)

## Purpose

This document defines how LeanDNA presentations should be structured, styled, and visualized **when those slides are built by this application** (programmatic layout via `slides_client.py` and related builders). It is intended for automated slide generation systems (such as Cursor agents) and for humans authoring deck YAML that drives those builders.

The goal is to produce slides that are:

- information-dense but readable
- visually consistent across decks
- analytical rather than narrative
- optimized for executive and customer decision-making

This version is written to be more directly usable by automated systems. Where possible, preferences are expressed as **explicit constraints, fallback orders, and implementation rules** rather than general guidance alone.

---

## Scope

### In scope

Slides **created or materially laid out by this codebase**—for example:

- slides assembled through `slides_client.py` helpers
- deck or slide YAML that drives those builders
- Python that positions or styles content the app owns
- automated layouts that create or materially change app-owned slide structure

### Out of scope

Slides or slide content that **come from an external Google Slides template, an existing deck, or copy/paste / duplicate operations**—including QBR or other flows that **copy** template slides and only **hydrate or adapt text**.

Automation must **not** use this document to redesign, re-layout, or “fix” those slides unless the user explicitly asks to change **app-owned** building behavior.

**Rule:** Template-origin slides keep their source layout and styling unless the requested change is explicitly about the application’s owned layout logic.

---

## Document Structure for Automation

Automation systems should treat this guide as five layers, in descending order of priority:

1. **Hard constraints** — rules that must be enforced unless technically impossible.
2. **Layout system** — safe bounds, spacing, pagination, density, and continuation behavior.
3. **Component standards** — KPI cards, title rows, chart blocks, legends, tables, and body text.
4. **Charting and data visualization standards** — chart selection, labeling, annotation, and color logic.
5. **Presentation philosophy** — clarity, signal, and executive usefulness.

If two rules appear to conflict, follow the rule from the **higher-priority layer**.

---

## Hard Constraints

The following rules are mandatory for automated generation.

### Slide intent

- Every slide must answer **one primary question**.
- Every slide must communicate **one primary takeaway**.
- Every slide title must state the takeaway, not merely name the topic.

### Layout safety

- No content may render outside the safe body bounds.
- No text, chart, KPI card, footer, or legend may overlap another slide element.
- No text may extend below `BODY_BOTTOM`.
- Footer and source text must remain visually separate from the body.

### Readability

- Title must be readable at presentation scale.
- Axis labels, category labels, legends, and KPI labels must remain readable at presentation scale.
- Do not shrink text below defined minimum sizes simply to avoid pagination.

### Quantitative visuals

- Quantitative charts should default to **embedded Google Sheets charts**.
- Do not hand-draw quantitative charts with ad hoc shapes unless there is no embedded-chart path available.
- Do not use 3D charts.
- Do not use decorative chart styling.

### KPI cards

- Any outlined KPI metric tile must be rendered only through `_kpi_metric_card` in `src/slides_client.py`.
- KPI labels inside cards must remain single-line.
- KPI values in the same row must use the same accent color unless a semantic exception is explicitly intended.

### Continuation behavior

- Continuation slides may be emitted only when content does not fit in the allotted body region.
- Continuation slides must be labeled clearly, e.g. `Section Name (2 of 3)`.
- Pagination must be bounded by a small fixed maximum, typically ten slides.

---

## Global Slide Philosophy

LeanDNA decks prioritize **clarity of signal over visual novelty**.

Slides should:

- answer a specific question
- present one idea or analytical takeaway
- include evidence such as a chart, KPI, metric, or structured comparison
- minimize filler language

Avoid slides that merely restate what the presenter will say.

### Title principle

Every slide should communicate the key takeaway in the title.

Bad title:

`Inventory Trends`

Good title:

`Inventory Turns Improved 18% QoQ Across 24 Sites`

---

## Density Profiles

To reduce ambiguity, automation should choose a density profile before laying out a slide.

### Executive deck density

Use for strategy, board, leadership, or high-level internal presentations.

- lower text density
- one dominant visual or KPI block
- minimal supporting bullets
- more whitespace

### QBR density

Use for customer QBRs and operational reviews.

- medium density
- one to two visuals
- three to five supporting bullets maximum
- explicit scope and source labeling

### Dashboard / analytical density

Use for internal performance dashboards, benchmark slides, and data-heavy operational review content.

- highest allowable density
- strong structure required
- tighter spacing allowed, but readability rules still apply
- pagination preferred over microtext

### Default

If slide type is unknown, default to **QBR density**.

---

## Slide Structure

Each slide should follow a predictable structure.

### Title

A full-sentence takeaway.

**Preferred rule:** titles should remain on a single line.

### Context line (optional)

A short subtitle explaining scope.

### Metric / context bar (optional)

A single-line summary directly below the title for scope, KPI totals, or timeframe.

When used, it should remain clearly readable at presentation size and typically use approximately **9–11 pt** text.

### Content area

Charts, diagrams, metrics, or structured bullets.

The content area must fit within the safe body bounds with explicit padding between sections.

### Footer

Source, timeframe, or dataset description when relevant.

---

## Title Fallback Order

Automation must use the following fallback order if a title does not fit on one line.

1. Shorten wording while preserving meaning.
2. Reduce title font size slightly.
3. Tighten title-area horizontal padding slightly if layout permits.
4. Rephrase for brevity.
5. Allow wrapping only as a last resort.

**Do not** reduce title size below the minimum readable threshold just to preserve a single line.

---

## Layout Spacing Rules

Use explicit block spacing between vertically stacked elements.

### Standard spacing

- Reserve clear separation between title area and body content.
- When a slide mixes KPI cards with charts, reserve at least **12–18 pt** between vertical blocks.
- When a slide includes a footer, leave enough clearance that footer content does not visually merge with the body.

### Block boundary rule

Each block must have a clear top, bottom, and padding allowance before layout proceeds to the next block.

Automation should not place body elements opportunistically into leftover gaps unless the resulting spacing still reads as intentional.

---

## Anti-Pattern Resolution Order

If a layout problem occurs, automation must resolve it in this order unless the slide type explicitly overrides the sequence.

1. Remove non-essential supporting bullets.
2. Paginate long lists or tables.
3. Reduce non-essential chart footprint if the slide contains more than one major visual.
4. Tighten spacing within allowed bounds.
5. Reduce font size within defined minimums.
6. Truncate low-priority text.
7. As a last resort, emit an explicit omission note or summary.

### Things automation should not do early

- shrink labels to unreadable sizes
- compress a dominant single chart into a corner
- remove the primary insight from the title
- silently drop rows or data categories without labeling the omission

---

## Canonical Slide Types

Automated systems should generate slides using a limited number of repeatable templates.

### Strategic Insight Slide

Purpose: highlight a major finding.

Structure:

- title states the insight
- large chart
- 2–3 bullets interpreting the data

Typical bullet roles:

- explanation of the trend
- operational implication
- next action

### KPI Dashboard Slide

Purpose: provide a snapshot of performance.

Layout:

- grid of KPI cards, typically 4–6 metrics

Each KPI card contains:

- metric name
- current value
- change versus previous period
- small sparkline if possible and space allows

### Trend Analysis Slide

Purpose: show change over time.

Layout:

- large time-series chart
- 2–3 explanatory bullets below chart

### Comparative Analysis Slide

Purpose: compare entities such as factories, sites, customers, or products.

Layout:

- ranked bar chart
- optional highlight for top or bottom performers

### Operational Process Slide

Purpose: explain system workflow or architecture.

Layout:

- simple diagram
- minimal text

### Initiative Status Slide

Purpose: report project progress.

Layout:

- short status summary
- milestones achieved
- next milestones
- risks or blockers

### Notable / Critical Signals Slide

Purpose: summarize the highest-priority account or portfolio actions.

Approved implementation:

- customer/account slides titled **Notable Signals** use the shared `signals` slide builder
- portfolio slides titled **Critical Signals** must use the same numbered-list visual treatment
- use a light background, standard slide title, 12 pt navy body text, and bold blue numbering
- show at most **8 numbered bullets** and emit **one slide only**; lower-priority signals are omitted from the slide body rather than continued
- optional trend/context banner may appear under the title on customer/account signals slides

Do not create alternate signal-list styles with severity dots, smaller 9 pt rows, or white-background dense lists unless the user explicitly asks for a different visual pattern.

---

## KPI Dashboard and Card Standards

### Universal card rule

Any automated slide that shows an outlined KPI metric tile must render it only via `_kpi_metric_card` in `src/slides_client.py`.

Do not hand-roll separate label and value text boxes.

### Shared KPI card styling

The shared KPI box pattern is:

- light fill `LIGHT`
- approximately 1 pt gray outline
- metric label at `KPI_METRIC_LABEL_PT` (10 pt) in `BLACK`
- primary value bold in caller-chosen accent
- default accent `NAVY` when `accent` is omitted

`_kpi_metric_card` applies styles with `textRange: ALL` so Slides theme defaults do not leak into the label or value.

### When to use KPI cards

Use KPI cards when:

- the slide’s main point is one to six headline numbers
- each metric fits one short label and one primary value
- the audience should read the metrics in parallel at a glance

### When not to use KPI cards

Do not use KPI card chrome for:

- narrative interpretation or recommendations in prose
- table cells
- chart axes or rank labels
- footnotes
- titles or section headers

### Label-fitting rule

Box labels must remain on a single line.

`_kpi_metric_card` enforces this structurally via `_fit_kpi_label`, which:

- calculates rendered text width from the card’s inner width and font size
- auto-shrinks font down to **8 pt** when necessary
- truncates with an ellipsis only as a last resort

If labels are prepared before calling the helper, `_truncate_kpi_card_label` may still be used as a static safety net, but it is no longer the primary guard.

### Universal accent rule

Every KPI value in a single row must use the same accent color, typically `BLUE` by default.

Do not mix `NAVY`, `BLUE`, and `TEAL` across cards in the same row unless there is a deliberate semantic reason.

### Compact KPI tiles in mixed layouts

On slides that pair KPI rows with charts, tables, or other body content, card height is typically tight.

For compact cards:

- use only two visual lines inside the box
- line 1: metric label
- line 2: primary value

Do not place explanatory or qualifying third lines inside short KPI cards unless card height is materially increased and the result is verified visually.

Put caveats, denominators, or definitions in:

- speaker notes
- a context bar under the title
- a separate plain-text callout

---

## Pagination and Continuation Slides

Some logical slide types emit more than one physical slide when lists, tables, or paired columns would otherwise overflow the safe body band.

### Principles

1. **Derive capacity from layout, not arbitrary caps.** Items per page should come from available height and body font size, using a consistent line-height model of approximately **font size × 1.22 pt per line** for multiline text boxes.
2. **Multi-column slides share one vertical budget.** If two columns use the same top and bottom bounds, they must agree on how many lines fit.
3. **Paginate only when necessary.** If all content fits in one body region, use one slide.
4. **Label continuations clearly.** Use `Section Name (2 of 3)` format.
5. **Bound runaway pagination.** Cap continuation pages at a small fixed maximum, typically ten.
6. **Charts vs lists.** Pagination rules apply to text and tables. Do not shrink a chart that is meant to be dominant merely to fit extra list rows.
7. **Tables must use empirical row fit.** Use `_table_rows_fit_span` in `slides_client.py` with an effective row height that matches rendered Slides table behavior.

### Table-specific rule

Google Slides applies default cell padding around table text, so rendered row height is greater than `fontSize × 1.22` alone.

For compact site tables, use an empirical effective row height of roughly **26 pt** at **7 pt** body text unless better calibrated values are available.

**Jira “recent opened / recent closed” tables** (HELP, CUSTOMER, LEAN): use `_table_rows_fit_span` with a **tight** `row_height_pt` (≈ **19 pt** in the current implementation), **`max_rows_cap=8`**, and text truncated with `_max_chars_one_line_for_table_col` per text column. A looser row pitch (e.g. 22 pt) plus up to 11–12 rows **looked** fine in layout math but **failed in Slides** when any cell still wrapped: wrapped rows are much taller than the nominal row height, so the table overflowed. Keep **8 data rows** and stricter one-line limits over increasing row count.

**Critical (overflow):** Truncate text with `_truncate_table_cell` using a **per-column** max length from `_max_chars_one_line_for_table_col(column_width_pt, font_pt)` (see `slides_client.py`). A single global max (e.g. 110 characters) in a ~236 pt Title column is unsafe: **Slides wraps** long strings, **row height grows past the `row_height_pt` budget**, and the table still overflows even when row *count* math looks correct.

### Implementation note

The codebase exposes `slide_type_may_paginate(slide_type)` and a registry of slide types that may emit multiple pages.

Prefer that registry for docs and tooling rather than duplicating a long slide-type list here.

---

## Visual Hierarchy

Slides should guide the viewer’s eye in this order:

1. Title
2. Primary chart or KPI block
3. Key numbers
4. Supporting bullets

### Chart dominance rules

- When multiple elements share a slide, the primary chart should typically occupy approximately **60% of the slide body area**.
- When a slide contains **one sole embedded chart**, that chart should dominate the body band.
- Do not leave a lone chart undersized with large unused whitespace around it.

---

## Embedded Chart Standards

Quantitative visuals should default to **Google Sheets charts embedded in Slides** rather than hand-drawn shapes.

### Use Slides-native shapes only for

- decorative accents
- diagrams or workflows
- simple non-quantitative visual structure

### Use embedded Sheets charts for

- metric comparisons
- time-series trends
- ranked category summaries
- operational dashboards

This improves consistency, reproducibility, and maintainability across decks.

### Single chart on a slide

When a slide shows exactly one embedded Sheets chart and no second chart, table, or dense text column competes for the body:

- use the full content width for bar, column, or line charts
- use the largest square that fits for pie or donut charts
- center the chart horizontally in the content area
- extend the chart vertically from just below the title or metric bar through `BODY_BOTTOM` minus a small bottom pad of about **10 pt**

When two charts share a slide:

- split the content width, for example about **58% / 40%** with a small gap
- give each chart full available height in the content band
- do not shrink a lone chart into a side column layout

### Chart title alignment

Chart titles and section headers should align to the visual unit represented by the chart:

- if the legend is below, center the title over the plot area
- if the legend is beside the chart, center the title over the combined chart-plus-legend block
- for two charts on one slide, each title should be centered over its own chart block

### Chart legend sizing and placement

The Google Sheets API does not expose a dedicated **legend** font size; Sheets-rendered legends (especially on pie/donut and **multi-series bar/column** charts) often look fine in the spreadsheet but are **unreadably small** once the chart is embedded and scaled on a slide.

Therefore:

- **Multi-series bar/column (and stacked) charts** must set `suppress_legend=True` in `src/charts.py` and render a slide-level legend via `_slide_chart_legend` in `slides_client.py`, using `BRAND_SERIES_COLORS` in the same order as the series. Do not rely on the embedded `BOTTOM_LEGEND` for these chart types in customer-facing decks.
- **Jira “ticket metrics breakdown” (two pies on one slide)** — the canonical pattern for *Unresolved by type* and *Unresolved by status* — is documented in **Pie charts: Jira ticket metrics breakdown** below. In short: **no** Sheets-rendered pie legend; **slide-level stacked** legend; large overlay pixels; chart background matches page tint.
- **Other pie / donut charts** (e.g. single pie, engagement): use `suppress_legend=True` in `add_pie_chart` when the embedded legend would be unreadable, and a slide-level swatch legend via `_slide_chart_legend` (horizontal) or vertical stacking where needed. Use **`CHART_PIE_OVERLAY_W_PX` / `CHART_PIE_OVERLAY_H_PX`** and **`maximized: true`** on the `ChartSpec` for sharp embeds. Do not rely on a shallow `embed_chart` box with `RIGHT_LEGEND` or `BOTTOM_LEGEND` for audience-facing slides.
- **Line / trend charts** (e.g. monthly created vs resolved): the **embedded box height** must not be very short (on the order of **80 pt** is too small—axis and month labels shrink to unreadable). Target roughly **100 pt** or more per chart in the support volume layout, with `show_legend=False` and a slide-level Created/Resolved key when needed.
- For slide-level legends, reserve about **24–28 pt** of vertical space below the chart (more for **stacked** pie legends; see below); label text at **`CHART_LEGEND_PT` (12 pt by default)** for default horizontal legends, with swatches at least **10×10 pt** where that helper is used.
- **Single-series** bar/column charts have no series legend; axis/category labels are handled separately.
- `ChartSpec.fontName` is set in `src/charts.py` to **`CHART_SPEC_FONT_NAME`** (Roboto) so chart text scales as a system font where the API applies it to titles/axes/legends.

### Pie charts: Jira ticket metrics breakdown (LEAN / CUSTOMER / HELP)

This is the **approved** pattern for composition-of-open-work slides that show two pies side by side. Implementations: `_project_ticket_metrics_breakdown_slide` in `src/slides_client.py` (used by e.g. `lean_project_ticket_metrics_breakdown`, `customer_project_ticket_metrics_breakdown`, and the HELP **Ticket Metrics Breakdown** slide via `customer_ticket_metrics`).

**Rationale:** Google Sheets does not expose a controllable legend font size for pie charts. Embedded `BOTTOM_LEGEND` / `RIGHT_LEGEND` legends are often **illegible** on slides. The fix is: **hide** the in-chart legend (`legendPosition` → **`NO_LEGEND`** when `suppress_legend=True` in `add_pie_chart`) and draw a **slide-native** legend with readable type.

**Layout**

- **Two columns** in the content band: equal width with a small gap (about **16 pt**); each column is one visual unit: centered **section title** + **pie** + **stacked legend** under the pie.
- **Section titles** (e.g. “Unresolved by type”, “Unresolved by status”): **13 pt** bold, `NAVY`, sans; **centered** over that column.
- **Vertical space:** Body height from the chart row through `BODY_BOTTOM` drives a shared `chart_h`. Each column reserves space for a **tall** pie and a **variable-height** legend band below. The pie height is `max(90, h_body - legend_h - 4)` pt so the pie never vanishes; legend height scales with slice count (capped, e.g. to ~**40%** of the column’s body height) so long legends do not obliterate the chart.

**Sheets chart (embedded pie)**

- Call `charts.add_pie_chart` with: **`suppress_legend=True`** (so **`NO_LEGEND`** in the `pieChart` spec), **`show_title=False`**, empty `title` string (so a hidden title does not steal layout from the pie), **`maximized=True`**, and **`background=`** the same rgb dict as the slide page (`_project_slide_bg(project)`) so the pie area is not a flat white box on a tinted slide.
- Use the shared **large** overlay: **`CHART_PIE_OVERLAY_W_PX`** / **`CHART_PIE_OVERLAY_H_PX`** (defaults **2560×1600** in `src/charts.py`) when creating the chart so the **bitmap** that Slides embeds is sharp at typical deck sizes.
- `embed_chart(..., linked=True)` is preferred for quality on this path.

**Slide-level legend (stacked, under each pie)**

- Use **`_slide_chart_legend_vertical`**, not the horizontal `_slide_chart_legend`, so multi-slice lists **wrap in a column** and stay readable.
- **Each row:** **10×10 pt** color swatch + label; **6 pt** gap; **~16 pt** row height; label **12 pt** (`CHART_LEGEND_PT`) navy sans — controlled by the parameters on `_embed_pie_plus_stacked_legend` in `slides_client.py`.
- **Label format (audience copy):**  
  `"{Category name}  —  {N} open"`  
  Use a **spaced em dash** between the category and the count (as generated in code: two spaces, em dash, two spaces) so the legend matches deck typography expectations.
- **Color alignment:** Swatch colors come from **`PIE_SLICE_COLORS[i % n]`** in **lockstep** with the **order of `labels` / `values`** passed into `add_pie_chart`. That order matches **Google Sheets’ default pie slice color sequence** for rows top-to-bottom, so the **swatch always matches the slice** without custom per-slice fill APIs. If you change sort order, bucket “Other,” or filter categories, keep **data order** and **legend order** identical.

**Data rules**

- Up to **6** type/status buckets per pie; if there are more, roll the tail into an **“Other”** sum so the pie stays scannable.
- Truncate long Jira names for chart labels with the same helper used for tables where noted in code (keeps the pie and legend stable).

**Regressions to avoid**

- Re-enabling any **Sheets** pie legend for this slide (too small, wrong layout).
- **Horizontal** legend that runs off the column for many categories.
- **Mismatched** `background` between `updatePageProperties` and `add_pie_chart` (visible rectangle around the pie).
- **Shallow** `embed_chart` height (shrinks the bitmap and muddies slice colors).

**Maintenance:** Any change to pie embed size, `suppress_legend` behavior, legend helpers, or `PIE_SLICE_COLORS` should be reflected **here** and in **`src/charts.py`** comments in the same change.

---

## Chart Font and Text Sizing

Sheets charts are created at an internal resolution and scaled down when embedded. Text that looks acceptable in the spreadsheet can become unreadably small on the slide.

All chart builders in `src/charts.py` must use shared constants.

### Standard chart constants

- `CHART_TITLE_PT` = **36 pt** — bold, `NAVY`
- `CHART_AXIS_PT` = **12 pt** — `GRAY` (minimum for category/axis labels at presentation scale; 10 pt was too small)
- `CHART_SPEC_FONT_NAME` = **Roboto** — `ChartSpec.fontName` for embedded charts
- `CHART_LEGEND_PT` = **12 pt** in `slides_client` — slide-level swatch legend labels (not a Sheets property)
- `CHART_PIE_OVERLAY_W_PX` / `CHART_PIE_OVERLAY_H_PX` in `src/charts.py` — **width/height in pixels** for the in-sheet `overlayPosition` when creating **pie** charts (larger = sharper bitmap when embedded in Slides at fixed pt). Change these only with visual QA; update this doc when you change the numbers.

**Maintenance:** when you change how pie/bar/line embeds, legend strategy (`LABELED_LEGEND`, `suppress_legend` + slide legend, etc.), or these constants, **update this file in the same PR/change** so the guide does not “revert” in practice.

Do not hard-code ad hoc chart font sizes in individual chart `spec` dicts; prefer these constants and `add_*` parameters.

Callers may pass a different `axis_font_size` only when category text genuinely needs more or less room (e.g. dense month labels on line charts), but not below **10 pt** for audience-facing slides.

---

## Charting Standards

Charts must communicate operational insight quickly and clearly.

### Preferred chart types

- line charts
- bar charts
- stacked bar charts
- scatter plots
- heatmaps
- small sparklines

### Avoid

- 3D charts
- decorative chart styles
- pie charts for general comparison or many categories — **exception:** the Jira **ticket metrics breakdown** two-up pies (capped categories, “Other” bucket, slide-stacked legend; see **Pie charts: Jira ticket metrics breakdown** under embedded chart standards)

### Time-series charts

Use for operational trends such as:

- inventory turns over time
- time-to-value reduction
- forecast accuracy

Rules:

- time on the x-axis
- metric value on the y-axis
- consistent time intervals
- strong series contrast when multiple lines are shown
- readable labels and legends
- annotate major events when relevant

Example annotation:

`Forecast model deployed`

### Bar charts

Use for comparisons such as:

- factory performance
- customer segments
- regional results

Rules:

- use **vertical column charts** by default for compact category comparisons and dashboard summaries
- use **horizontal bar charts** only when labels are long or ranking readability is the priority
- sort bars descending unless there is a clear chronological or categorical reason not to
- maximum of roughly **10–12 bars** per chart
- if horizontal bar charts are shown side by side, keep each chart to roughly **5–7 bars**
- all bar charts must have a visible border or outline
- highlight top or bottom performers when relevant

### Stacked bar charts

Use to show composition.

Avoid more than four segments unless there is a strong reason and the result remains legible.

### Scatter plots

Use to illustrate relationships between variables.

Add regression lines if meaningful.

### Heatmaps

Use for dense operational data.

Always include a clear color legend.

---

## Color Standards

Charts must follow consistent color logic.

### Primary meanings

- LeanDNA Blue → baseline metrics
- Green → improvement
- Red → deterioration
- Gray → baseline comparison

### Color rules

- never rely solely on color to communicate meaning
- include labels and legends
- preserve color meaning consistently within the deck

---

## Axis and Label Standards

Charts must always include:

- axis labels
- units
- timeframe
- label text sized for presentation readability

Do not shrink bar-chart category labels merely to fit more rows.

For embedded charts, prefer approximately **10–12 pt equivalent** axis and category label sizing at presentation scale.

Example:

`Inventory Turns (Monthly)`

### Engineering portfolio deck: Jira ticket rows (LEAN bugs, ER enhancements)

When a slide lists individual Jira issues (open bugs, blockers/criticals, open or shipped ER enhancements), use a **fixed three-band block per ticket** so readers get subject plus usable body context:

1. **Meta line** (9 pt): issue key (linked), status / priority / assignee or dates — compact, monospace for the key where used today.
2. **Subject** (9 pt, **bold**, navy): the Jira **summary** field, **one line**, width-truncated with `max_chars_one_line_for_table_col` for the content band (not a hard-coded character cap in the slide builder).
3. **Description** (8 pt, gray): **exactly two lines** of plain text from **`description_text`** (ADF description flattened in `jira_client`). Split word-aware into two lines using the same width helper at 8 pt. If there is no description, show an em dash on the first line and leave the second empty.

Do **not** stack a separate LLM “narrative” paragraph on these slides unless product asks for it; the Jira description is the source of truth for ticket body context. Pagination (e.g. open ER list) may reduce tickets per page so subject + two lines + meta fits above `BODY_BOTTOM`.

### Time units (minutes)

- Use **`min`** for minutes in axis labels, KPI text, tables, and legends (e.g. `15 min`, `Median time to first response (min)`).
- Do **not** use **`m`** for minutes. **`m`** is easy to read as meters, a casual million shorthand, or an ambiguous single letter.

---

## Data Annotation

Charts should include callouts when meaningful.

Examples:

- major system change
- algorithm deployment
- customer rollout

Annotations significantly improve interpretability and are preferred whenever a visible shift in the data is tied to a known event.

---

## Data Scope

All charts must clearly indicate scope.

Examples:

- `Across 24 Sites`
- `Top 100 SKUs`
- `Q1–Q4 2025`

Avoid ambiguous charts.

---

## Text Rules

Text must remain concise.

### Preferred bullet style

- one sentence
- roughly **8–14 words**

### Avoid

- nested bullet structures
- long paragraphs
- full-page text blocks
- filler bullets that restate the title without adding interpretation

### Typical text density

For most QBR and operational slides, **20–40 total words** is a reasonable default.

This is guidance, not a hard cap. Dense analytical slides may exceed it when warranted, but readability and hierarchy still govern.

---

## Data Integrity

Charts must use consistent definitions across the deck.

If a metric has a canonical definition, that definition must remain stable from slide to slide.

Example:

Inventory turns must always use the same calculation.

Data sources should be stable, repeatable, and documented.

Example source reference:

`LeanDNA analytics dataset v3`

---

## QBR Deck vs Executive Deck

### Executive decks emphasize

- strategy
- macro trends
- financial outcomes
- fewer, more dominant visuals

### QBR decks emphasize

- operational analytics
- site performance
- improvement opportunities
- benchmark and comparison context

Automation should choose layouts and density accordingly.

---

## Automation Guidance for Cursor

When generating slides automatically:

1. Identify the key insight from the dataset.
2. Generate a title that communicates the insight.
3. Select the appropriate slide type.
4. Select the appropriate chart type based on the data structure, defaulting to embedded Sheets charts for quantitative visuals.
5. Use vertical column charts by default and switch to horizontal bars only when label length or ranking readability requires it.
6. Apply LeanDNA color, labeling, and bar-outline standards.
7. Reserve explicit layout space for headers, charts, legends, and footers so embedded chart objects do not overlap surrounding text.
8. Add two or three explanatory bullets interpreting the data when the slide type calls for them.
9. For list- or table-heavy slides, follow pagination rules using the actual body band and font size.
10. For newly designed or materially changed app-owned layouts, export a thumbnail and validate it before considering the slide done.
11. For any new Jira-backed data fetch, record JQL with a data description and follow speaker-notes trace formatting.

Slides should **not include raw data tables** unless explicitly requested or unless the slide type is inherently tabular.

---

## Visual QA Checklist for Automation

Before marking a newly generated or materially changed slide as complete, automation should verify all of the following.

### Required checks

- no overlaps between any visible slide elements
- no text below `BODY_BOTTOM`
- title fully visible and readable at normal presentation scale
- chart axis labels and legends readable at thumbnail or presentation scale
- KPI labels remain single-line inside metric cards
- footer remains distinct from the body
- lone charts use dominant-body treatment rather than undersized placement
- continuation slides are labeled correctly
- continuation slides are not emitted unnecessarily
- no continuation slide has extremely low utilization unless required by data grouping rules

### Low-utilization continuation rule

A continuation slide with less than approximately **25% body utilization** should be treated as suspicious.

Automation should attempt to repack prior slides before accepting such an output, unless keeping groups together is more important than fill efficiency.

---

## Speaker Notes: JQL Trace

### Automated deck layout (`_build_slide_jql_speaker_notes`)

Speaker notes are plain text with **no titled section header**.

Order:

1. Timestamp
2. Blank line
3. `Slide: …` and `Slide type: …`
4. Blank line
5. One **bullet per trace** (`• description — source`), with the **query on the following line(s)** indented, and a **blank line** between separate traces so long JQL is scannable

Jira and SOQL rows use the same block shape as pipeline traces. There is no separate `JQL used:` block in automated output.

### Required Jira documentation format

When documenting or hand-writing Jira lines, each query can be described as:

`[Data description] - JQL`

Rules:

- **Data description** is a short phrase in square brackets naming what the query feeds
- use separator ` - ` between the description and the JQL text
- when multiple queries apply to the same slide, prefix with `1.`, `2.`, and so on

Example block:

```text
JQL used:
1. [HELP open issues for customer (non-done)] - project = HELP AND (…) AND statusCategory != Done ORDER BY updated DESC
2. [HELP customer issues created in last 365 days] - project = HELP AND (…) AND created >= -365d ORDER BY created DESC
```

### Implementation rules

- report payloads store `jql_queries` as a list of objects: `{"description": "…", "jql": "…"}`
- plain strings are still accepted for backward compatibility and shown with description `[Jira issue search]`
- new Jira fetches must supply a description at record time
- pipeline trace descriptions must match on-slide copy exactly
- automated output emits one bullet per trace, then the query (multi-line if needed); do not run `description: source - query` on a single unbroken line
- do not collapse multiple KPIs into a single trace block
- canonical builders in `_SLIDE_CANONICAL_PIPELINE_TRACES` must follow the same one-block-per-visible-metric rule
- non-Jira slides may attach `data_traces` with `description`, `source`, and `query` fields using the same output shape

---

## Common Mistakes to Avoid

Slides should not:

- repeat the same metric multiple times without adding meaning
- show charts without interpretation when interpretation is expected
- include more than two or three major visuals on a single slide
- include decorative graphics without analytical meaning
- contain excessive text
- silently omit overflowed data
- mix incompatible density patterns on the same slide
- use microtext to avoid pagination

---

## Example Slide (Conceptual)

Title:

`Inventory Turns Improved 18% After Forecast Model Deployment`

Content:

Line chart showing monthly inventory turns.

Annotation:

`Forecast model rollout`

Supporting bullets:

- turns increased from 5.1 to 6.0
- strongest improvement in high-volatility SKUs
- next step is rollout to remaining factories

---

## Future Direction: Prefer Executable Constraints Where Possible

The long-term goal is for operational datasets to generate slides automatically and for style rules to be increasingly machine-enforced.

Where a rule can be represented as configuration or code, prefer executable constraints over prose.

Examples include:

- minimum and maximum font sizes
- chart/body area ratios
- pagination limits
- density profile defaults
- KPI label fitting rules
- safe block spacing
- QA validation checks

This document should remain the human-readable policy layer, but the most important layout and validation rules should ultimately live in shared constants, helper functions, registries, or schema-driven configuration.
