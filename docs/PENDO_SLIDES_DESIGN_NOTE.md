# Pendo slides — design note

This note describes how **Pendo-backed slides** fit the deck narrative, what data each slide uses, and how **existing** usage slides were extended. It complements `docs/data-schema/PENDO_DATA_SCHEMA.md` and the slide YAML under `slides/`.

---

## New slide types (`slide_type`)

| `slide_type` | Builder | Report keys | Narrative role |
|--------------|---------|-------------|----------------|
| `pendo_localization` | `_pendo_localization_slide` | `visitor_languages` | Where users sit linguistically — rollout, training, locale issues. |
| `pendo_friction` | `_pendo_friction_slide` | `frustration` | Full friction dashboard (totals + top pages/features). Pair with **Feature Adoption**. |
| `pendo_sentiment` | `_pendo_sentiment_slide` | `poll_events` | Polls / NPS samples — pair with **Notable Signals** for exec-ready sentiment. |
| `pendo_track_analytics` | `_pendo_track_analytics_slide` | `track_events_breakdown` | All `pendo.track` names — complements **Kei** (chatbot-only). |
| `pendo_definitions_appendix` | `_pendo_definitions_appendix_slide` | `pendo_catalog_appendix` | **Definitions only** — sample names from Track Type / Segment / Report catalogs. Not funnel/path **results** (see schema gaps). |

Deck YAML references these by **slide id** (e.g. `pendo_localization`). Shared definitions live in `slides/pendo-*.yaml`.

---

## Existing slides — behavioral changes (builders)

| Slide | Change |
|-------|--------|
| **Feature adoption** (`features`) | Footer can include three bands: adoption narrative, friction summary, **ranked UX hotspots** (combined rage/dead/error/U-turn on features). |
| **Site comparison** (`sites`) | Added **Share** column — percent of account **total events** per row (helps spot skew without per-site frustration mapping). |
| **Behavioral depth** (`depth`) | Friction overlay line; if write ratio is low **and** friction is high, appends a **read-heavy + friction** coaching line. |
| **Champions** (`champions`) | Detail lines include **UI language** when `metadata.agent.language` is set on the visitor record. |
| **Kei AI** (`kei`) | Custom track listing moved to **`pendo_track_analytics`** so Kei stays chatbot-focused. |

---

## Deck placement (defaults)

- **Customer Success Health Review** (`decks/cs-health-review.yaml`): localization after engagement; friction after features; sentiment after depth; track analytics after Kei; definitions appendix immediately before Data Quality.
- **QBR** (`decks/qbr.yaml`): localization, friction, sentiment in the Usage block; definitions appendix before Data Quality (no Kei slide in this deck — track analytics not inserted).
- **Product Adoption** (`decks/product-adoption.yaml`): friction after features; sentiment after depth; track analytics after Kei; localization after engagement.

**Executive Summary** and **cohort review** decks intentionally omit these — keep slide count and audience fit.

---

## Operational notes

1. **`get_customer_health_report`** loads `visitor_languages`, `poll_events`, `frustration`, `track_events_breakdown`, and **`pendo_catalog_appendix`** (three REST catalog GETs). Deck runs cost more API calls than before.
2. **Definitions appendix** is a reference for authors and analysts — not a substitute for Pendo UI for saved report **results**.
3. **YAML-first**: prompts and deck order live in `slides/*.yaml` and `decks/*.yaml`; builders only layout and caps.

---

## Related identifiers

See `docs/data-schema/DATA_REGISTRY.md` (Pendo section) for registry IDs such as `PENDO-POLL-EVENTS-SOURCE` and frustration fields.
