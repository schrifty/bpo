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

**Context line (optional)**  
A short subtitle explaining scope.

**Content area**  
Charts, diagrams, metrics, or structured bullets.

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

- bars sorted descending  
- maximum of roughly 10–12 bars  
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

Charts should occupy approximately **60% of slide area**.

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
3. Select the appropriate chart type based on the data structure.  
4. Apply LeanDNA color and labeling standards.  
5. Add two or three explanatory bullets interpreting the data.

Slides should **not include raw data tables** unless explicitly requested.

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

LeanDNA analytics systems and health-check datasets should ultimately feed automated reporting pipelines capable of generating QBR decks directly from data. :contentReference[oaicite:0]{index=0}