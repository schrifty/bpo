# Slide Style Guide Review Skill

The Style Guide can be found in the project in the docs folder.

## Purpose

Review a slide deck against a style guide using a vision-capable model and return a structured audit of brand, layout, readability, and charting compliance.

This skill is intended for decks in PowerPoint, Google Slides export, or PDF form. It works best when given both:

* the deck rendered as slide images or a PDF
* a style guide written as concrete rules

This skill is a reviewer, not a final authority. It should identify likely violations, explain them clearly, and propose practical fixes.

---

## When to use

Use this skill when you need to:

* check whether a slide deck adheres to a company style guide
* review executive decks, board decks, QBRs, customer decks, and internal presentations for consistency
* find slides that are cluttered, off-brand, or visually weak
* generate a slide-by-slide remediation plan

Do not use this skill as the sole control for legal, financial, or factual review. It is for visual and stylistic compliance.

---

## Inputs

The skill expects:

1. **A slide deck**

   * preferred: PDF export of the deck
   * acceptable: folder of slide images, one image per slide
   * optional but helpful: original PPTX or Google Slides metadata if available

2. **A style guide**
   The style guide should be explicit and testable. Good examples:

   * title placement and size expectations
   * maximum lines per title
   * body text readability rules
   * logo placement
   * footer format
   * brand color palette
   * chart standards
   * whitespace and density expectations
   * slide archetypes such as title slide, agenda, section divider, chart slide, metric slide, comparison slide

3. **Optional context**

   * audience: board, customer executive, internal LT, product review
   * deck type: QBR, board deck, sales pitch, roadmap, diligence, support review
   * severity threshold: strict, normal, lenient

---

## Outputs

The skill should return:

1. **Executive summary**

   * overall adherence score from 0 to 100
   * number of slides reviewed
   * number of slides with major issues
   * top recurring problems
   * overall recommendation: pass, pass with fixes, or rework needed

2. **Slide-by-slide audit**
   For each
