# LeanDNA Slide Design and Data Visualization Standards
Version 1.0 (Internal Use)

## Purpose

This document defines how LeanDNA presentations should be structured, styled, and visualized. It is intended for automated slide generation systems (such as Cursor agents) and for humans building decks.

The goal is to produce slides that are:

- information-dense but readable  
- visually consistent across decks  
- analytical rather than narrative  
- optimized for executive and customer decision-making  

LeanDNA presentations typically combine three elements:

- operational narrative  
- quantitative performance metrics  
- product or initiative status updates  

Slides should emphasize **evidence, metrics, and clarity**, not decoration.

---

## Global Slide Philosophy

LeanDNA decks prioritize **clarity of signal over visual novelty**.

Slides should:

- answer a specific question  
- present one idea or analytical takeaway  
- include evidence (data, chart, KPI, metric)  
- minimize filler language  

Avoid slides that simply restate what the presenter will say.

Every slide should communicate **one key takeaway in the title**.

Example:

Bad title  
`Inventory Trends`

Good title  
`Inventory Turns Improved 18% QoQ Across 24 Sites`

---

## Slide Structure

Each slide should follow a predictable structure.

**Title**  
A full-sentence takeaway.
Titles should remain on a **single line**. If a draft title is too long, shorten the wording
or reduce title font size slightly rather than allowing the title to wrap.

**Context line (optional)**  
A short subtitle explaining scope.

**Metric / context bar (optional)**  
A single-line summary directly below the title for scope, KPI totals, or timeframe.
When used, it should be clearly readable at presentation size and generally use
approximately **9–11 pt** text, not microtext.

**Content area**  
Charts, diagrams, metrics, or structured bullets.
The content area must fit within the safe body bounds with explicit padding between sections.
Do not allow KPI cards, section headers, embedded charts, or footer content to overlap.
When a slide mixes KPI cards with charts, reserve at least **12–18 pt** of vertical space
between the bottom of one block and the top of the next.

**Footer**  
Source, timeframe, or dataset description when relevant.

---

## Slide Density

LeanDNA slides are moderately dense.

Target guidelines:

- 1–2 charts per slide  
- 3–5 supporting bullets maximum  
- 20–40 words total text typical  
- clear whitespace between elements

Avoid:

- long paragraphs  
- full-page text blocks  
- decorative icons unless meaningful

---

## Pagination and continuation slides

Some logical slide types emit **more than one physical slide** when lists, tables, or paired columns would otherwise overflow the safe body band (from the metric/context row through **`BODY_BOTTOM`**).

### Principles

1. **Derive capacity from layout, not arbitrary caps.** Items per page should come from the **available height** and **body font size**, using a consistent line-height model (approximately **font size × 1.22** pt per line for multiline text boxes). Reserve at least one line for a column header when measuring list capacity.

2. **Multi-column slides share one vertical budget.** If two columns use the **same** top and bottom bounds, they must agree on how many **lines** fit. When one column uses **multiple lines per logical row** (for example an email line plus a detail line), its row capacity is roughly **half** (or the appropriate fraction) of a single-line list in that same box—do not give one column a generous line-based budget and the other a smaller heuristic in pixels, or the deck will paginate **early** on one side and waste space on the other.

3. **Paginate only when necessary.** If all content for that slide type fits in one body region, use **one** slide. Continuation slides should be the exception, not the default.

4. **Label continuations clearly.** When there are multiple physical slides for one concept, titles should read like **`Section Name (2 of 3)`** so reviewers know nothing was dropped silently.

5. **Bound runaway pagination.** Cap continuation pages at a **small fixed maximum** (for example ten) so automation never produces enormous decks; if data exceeds that, truncate with an explicit omission or summary strategy rather than unbounded slides.

6. **Charts vs lists.** Pagination rules apply to **text and tables**. Embedded charts follow **Embedded Chart Standards** (including *Single chart on a slide*); do not shrink charts to “make room” for extra list rows when the design calls for a single dominant chart.

### Implementation note for automation

The codebase exposes **`slide_type_may_paginate(slide_type)`** and a registry of slide types that may emit multiple pages (for example long site lists, feature adoption, export behavior, signals, platform health, Jira- or Salesforce-backed tables). Prefer that registry for docs and tooling rather than duplicating the full list here; when adding a new paginating builder, update the registry and this document if the pattern is new.

---

## Title Rules

Titles must communicate insight.

Recommended structure:

`Observation + metric + scope`

Examples:

`Time-to-Value Reduced from 180 to 124 Days in 2025`  
`Writeback Adoption Increased to 42% of Sites`  
`Supply Coverage Errors Concentrated in 3 Factories`

Avoid neutral titles such as:

`QBR Metrics`  
`Operational KPIs`

---

## Canonical Slide Types

Automated systems such as Cursor should generate slides using a limited number of repeatable templates.

### Strategic Insight Slide

Purpose: highlight a major finding.

Structure:

- Title states the insight
- Large chart
- 2–3 bullets interpreting the data

Typical bullets:

- explanation of the trend  
- operational implication  
- next action  

---

### KPI Dashboard Slide

Purpose: provide a snapshot of performance.

Layout:

Grid of KPI cards (typically 4–6 metrics).

Each KPI card contains:

- metric name  
- current value  
- change versus previous period  
- small sparkline if possible

Example:

Inventory Turns  
6.2  
+0.8 YoY

#### When to use KPI boxes (outlined metric cards)

Use the shared **KPI box** pattern (light fill **``LIGHT``**, **~1 pt** gray outline via **``_bar_rect``** / **``_kpi_metric_card``** in code — same chrome as HELP ticket metrics) when:

- The slide’s main point is **one to six headline numbers** (rates, counts, scores, medians, dollars) that the audience should read **at a glance** in parallel.
- Each metric fits **one short label** (metric name or peer context, typically **≤ ~44 characters** on one line; truncate with an ellipsis if the cohort name is long) and **one primary value** (bold, accent color where hierarchy matters).
- You are **not** trying to fit a full sentence, bullet list, or multi-line caveat **inside** the same rectangle.

**Do not** put the KPI chrome on:

- Narrative interpretation, recommendations, or delta explanations in prose (use a **plain text box** below or beside the cards).
- Table cells, chart axes, rank labels, or footnotes.
- Titles and section headers (use **``_slide_title``** / dividers, not metric cards).

**Peer Benchmarks** slide: **This account** weekly active rate, **peer / cohort median**, and (when shown) **all-customer median** must use **KPI boxes** in one row; delta, account size, and qualitative bullets stay in a **separate** body text region under the row.

#### Compact KPI tiles (mixed layouts)

On slides that **pair KPI rows with charts, tables, or other body content**, card height is usually **tight** (on the order of **50–56 pt** tall). For those tiles:

- Use **only two visual lines inside the box**: **metric label** (small, secondary color) and **primary value** (large, emphasis color).  
- **Do not** add a third line of explanatory or qualifying text inside the same box unless you **increase card height** materially (for example **≥ 72 pt**), verify in a thumbnail export, and leave clearance above **``BODY_BOTTOM``**. Clipped footlines read as a bug, not a feature.  
- Put definitions, denominators (“21 of 26 met SLAs”), averages, and caveats in **speaker notes**, a **metric bar under the slide title**, or a **separate callout**—not inside a short KPI rectangle.

The full “name + value + delta + sparkline” pattern applies to **dedicated dashboard slides** where cards are given enough vertical space for all elements.

---

### Trend Analysis Slide

Purpose: show change over time.

Layout:

- large time-series chart  
- 2–3 explanatory bullets below chart

---

### Comparative Analysis Slide

Purpose: compare entities such as factories, sites, customers, or products.

Layout:

- ranked bar chart  
- optionally highlight top or bottom performers

Example title:

`Top 10 Factories by Inventory Reduction`

---

### Operational Process Slide

Purpose: explain system workflow or architecture.

Layout:

- simple diagram
- minimal text

---

### Initiative Status Slide

Purpose: report project progress.

Layout:

- short status summary  
- milestones achieved  
- next milestones  
- risks or blockers

---

## Charting Standards

Charts must communicate operational insight quickly and clearly.

Preferred chart types:

- line charts  
- bar charts  
- stacked bar charts  
- scatter plots  
- heatmaps  
- small sparklines

Avoid:

- 3D charts  
- decorative chart styles  
- pie charts (except when categories are extremely limited)

---

## Embedded Chart Standards

Quantitative visuals should default to **Google Sheets charts embedded in Slides** rather than hand-drawn shapes.

Use Slides-native shapes only for:

- decorative accents  
- diagrams or workflows  
- simple non-quantitative visual structure

Use embedded Sheets charts for:

- metric comparisons  
- time-series trends  
- ranked category summaries  
- operational dashboards

This improves consistency, reproducibility, and maintainability across decks.

### Single chart on a slide

When a slide shows **exactly one** embedded Sheets chart (no second chart, table, or dense text column competing for the body), that chart must:

- use the **full content width** for bar/column/line-style charts, or the **largest square that fits** the body band for pie/donut charts  
- be **horizontally centered** in the content area (between the standard left/right margins)  
- extend **vertically** from just below the title / metric bar through **``BODY_BOTTOM``** minus a small bottom pad (~10 pt), so it reads at presentation scale instead of sitting in a corner with empty space  

When **two** charts share a slide, split the content width (for example ~58% / ~40% with a small gap) and give each chart the **full available height** in the band—do not shrink a lone chart into the right column.

---

## Time-Series Charts

Used for operational trends.

Examples include:

- inventory turns over time  
- time-to-value reduction  
- forecast accuracy

Standards:

- time on the x-axis  
- metric value on the y-axis  
- consistent time intervals  
- when multiple lines appear on the same chart, use clearly distinctive series colors with strong contrast  
- axis labels and legends must be readable at presentation size; avoid undersized chart text  
- if an embedded chart legend becomes too small, suppress it and render a larger slide-level legend instead  
- major events annotated

Example annotation:

`Forecast model deployed`

---

## Bar Charts

Used for comparisons.

Examples:

- factory performance  
- customer segments  
- regional results

Rules:

- use **vertical column charts** by default for compact category comparisons and dashboard summaries  
- use **horizontal bar charts** only when labels are long or ranking readability is the priority  
- bars sorted descending  
- maximum of roughly 10–12 bars  
- when horizontal bar charts are shown side by side on a slide, keep each chart to roughly **5–7 bars** rather than shrinking label text  
- all bar charts must have a visible **border / outline**  
- highlight top or bottom performers

---

## Stacked Bar Charts

Used to show composition.

Examples:

- inventory categories  
- demand vs supply sources

Rule:

Avoid more than four segments.

---

## Scatter Plots

Used to illustrate relationships between variables.

Examples:

- factory complexity vs time-to-value  
- demand volatility vs stockouts

Add regression lines if meaningful.

---

## Heatmaps

Used for dense operational data.

Examples:

- factory vs metric performance  
- SKU vs demand volatility

Always include a clear color legend.

---

## Color Standards

Charts must follow consistent color logic.

Primary meanings:

- LeanDNA Blue → baseline metrics  
- Green → improvement  
- Red → deterioration  
- Gray → baseline comparison

Rules:

- never rely solely on color to communicate meaning  
- include labels and legends

---

## Axis and Label Standards

Charts must always include:

- axis labels  
- units  
- timeframe
- label text sized for presentation readability; do not shrink bar-chart category labels to fit more rows  

For embedded charts, prefer roughly **10–12 pt** equivalent axis / category label sizing at presentation scale.

Example:

`Inventory Turns (Monthly)`

Avoid charts without labels.

---

## Data Annotation

Charts should include callouts when meaningful.

Examples:

- major system change  
- algorithm deployment  
- customer rollout

Annotations significantly improve interpretability.

---

## Data Scope

All charts must clearly indicate scope.

Examples:

- `Across 24 Sites`  
- `Top 100 SKUs`  
- `Q1–Q4 2025`

Avoid ambiguous charts.

---

## QBR Charting Standards

QBR decks typically focus on operational performance metrics.

Common QBR metrics include:

- inventory turns  
- stockout rate  
- forecast accuracy  
- excess inventory  
- time-to-value  
- writeback usage  
- demand volatility

Charts should frequently compare:

- current quarter  
- previous quarter  
- customer benchmark

---

## Executive Deck vs QBR Deck

Executive decks emphasize:

- strategy  
- macro trends  
- financial outcomes

QBR decks emphasize:

- operational analytics  
- site performance  
- improvement opportunities

---

## Visual Hierarchy

Slides should guide the viewer’s eye.

Priority order:

1. Title  
2. Primary chart  
3. Key numbers  
4. Supporting bullets  

Charts should occupy approximately **60% of slide area** when multiple elements share the slide. A **sole** embedded chart in the body should dominate that band (see *Single chart on a slide* under Embedded Chart Standards).

---

## Whitespace

Whitespace should be used intentionally.

Do not fill the entire slide.

Whitespace improves readability and visual hierarchy.

---

## Text Rules

Text must remain concise.

Preferred bullet style:

- one sentence  
- roughly 8–14 words

Avoid nested bullet structures.

---

## Data Integrity

Charts must use consistent definitions across the deck.

Example:

Inventory turns must always use the same calculation.

Data sources should be stable and repeatable.

Example source reference:

`LeanDNA analytics dataset v3`

---

## Automation Guidance for Cursor

When generating slides automatically:

1. Identify the key insight from the dataset.  
2. Generate a title that communicates the insight.  
3. Select the appropriate chart type based on the data structure, defaulting to embedded Sheets charts for quantitative visuals.  
4. Use vertical column charts by default; switch to horizontal bars only when labels or ranking clarity require it.  
5. Apply LeanDNA color, labeling, and bar-outline standards.  
6. Reserve explicit layout space for headers, charts, and footers so embedded chart objects do not overlap surrounding text.  
7. Export a thumbnail for any newly designed or materially changed slide layout and fix collisions before considering the slide done.  
8. Add two or three explanatory bullets interpreting the data.  
9. For list- or table-heavy slides, follow **Pagination and continuation slides**: compute rows from the body band and font size; align multi-column line budgets; avoid unnecessary continuations.  
10. For any new Jira-backed data fetch, record JQL with a **data description** and follow **Speaker notes: JQL trace** formatting in speaker notes.

Slides should **not include raw data tables** unless explicitly requested.

---

## Speaker notes: JQL trace

Automated decks write **speaker notes** on each slide under a **Slide Query Trace** header. When the slide is backed by Jira, the notes include a **JQL used:** section.

### Required line format

Each Jira query must appear as:

`[Data description] - JQL`

Rules:

- **Data description** — Short phrase in **square brackets** naming what the query *feeds* (the slice of data), not the slide title. Example: `[HELP customer issues resolved in last 180 days]` not `[SED Ticket Metrics]`.  
- **Separator** — Space, hyphen, space: ` - ` between the closing `]` and the JQL text.  
- **Enumeration** — When multiple queries apply to the same slide, prefix with `1.`, `2.`, … so lists stay scannable.

Example block:

```text
JQL used:
1. [HELP open issues for customer (non-done)] - project = HELP AND (…) AND statusCategory != Done ORDER BY updated DESC
2. [HELP customer issues created in last 365 days] - project = HELP AND (…) AND created >= -365d ORDER BY created DESC
```

### Implementation

- Report payloads store ``jql_queries`` as a list of objects: ``{"description": "…", "jql": "…"}``. Plain strings are still accepted for backward compatibility and are shown with description `[Jira issue search]`.  
- New Jira fetches must supply a **description** at record time (e.g. ``_search(..., data_description="…")`` or ``_record_jql(jql, description="…")``).

---

## Common Mistakes to Avoid

Slides should not:

- repeat the same metric multiple times  
- show charts without interpretation  
- include more than two or three charts  
- include decorative graphics without meaning  
- contain excessive text

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

## Future Automation

The long-term goal is for operational datasets to generate slides automatically.

LeanDNA analytics systems and health-check datasets should ultimately feed automated reporting pipelines capable of generating QBR decks directly from data.
