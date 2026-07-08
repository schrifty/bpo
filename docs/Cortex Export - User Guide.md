# Cortex Export — User Guide

This guide explains **what’s in Cortex export files**, **how to read them**, and **how to ask an AI good questions** using the data. You do not need to know how exports are built to use them well.

Cortex produces two main kinds of markdown exports:

| Export | Who it’s for | What it covers |
|--------|----------------|----------------|
| **Portfolio LLM context** (`LLM-Context-Portfolio`) | Leadership, CS, AMs — whole book | Pendo headlines, Jira, Salesforce, CS Report, signals, risk — **all customers** in one file |
| **Per-customer Pendo export** (`Pendo Export  (Customer, Nd)`) | Account teams — one strategic customer | Deep **Pendo-only** usage: sites, features, people, trends — **one customer** per file (+ matching Sheet) |

Both use the same **Drive layout** (see [Where files live on Drive](#where-files-live-on-drive)).

---

## Portfolio LLM context export

### What is this file?

Cortex produces a portfolio snapshot whose bookmarkable name is **`LLM-Context-Portfolio-persistent.md`**. Think of it as a **single briefing packet** about your customer portfolio: who they are, how they use the product, support load, contract status, and health signals.

A same-day copy (plain name, no `-persistent`) is also saved under **Historical Data** for that run’s date. Each file has an **Exported (UTC)** date at the top so you know how old the data is. The nightly job refreshes the persistent copy on a schedule.

**Best use:** attach the file (or paste a section) to ChatGPT, Claude, or another assistant and ask questions in plain English. The assistant should **quote numbers from the file** and say when something isn’t in the snapshot.

---

## What’s inside (the numbered sections)

The portfolio export is split into numbered sections. When you ask the AI a question, it helps to name the section if you know it.

| Section | In plain English |
|---------|------------------|
| **Section 1 — Pendo** | Who’s using the product and how much (logins, active users, etc.). Portfolio-wide view. |
| **Section 2 — Jira (support)** | Support ticket volume and queue health for your **largest customers by revenue** — not every ticket detail. |
| **Section 3 — Salesforce (current customers)** | **Customers you should treat as “in the book today”** — active contracts **or** renewals in progress. Revenue and contract facts live here. **§3.1** is a ready-made table of every customer **ranked by current ARR** (read the `rank` column for “top N by revenue”); **§3.2** holds the contract-rollup and portfolio-total detail. |
| **Section 3b — Churned** | Customers who **left** (lost contracts, no renewal in flight). **Separate list — don’t mix with Section 3.** |
| **Section 3b-renewal — Renewal in progress** | Contracts that **expired** but a **renewal deal is still open**. These are **not** churn — sales is still working them. |
| **Section 3b-future — Future contracts** | Deals **signed but not started yet** (contract start date in the future). Not active today, not churn. |
| **Section 3c — Salesforce (detailed CRM)** | Extra Salesforce detail (opportunities, cases, contacts, etc.) for top customers. **`arr_by_ultimate_parent`** ranks **all** ultimate parents using the same contract-rollup math as the export (Carrier divisions collapse correctly). Sort by **`current_arr`**. Includes renewing and churned parents — not only the current book. |
| **Section 4 — CS Report** | Customer Success weekly health: platform health, supply chain, value metrics for large accounts. |
| **Section 4b — Slack** | Recent Slack conversations tied to customer names (when Slack is connected). |
| **Section 5 — Usage signals** | Product “flags” — e.g. low adoption, features not used, unusual usage patterns. |
| **Section 6 — Trend context** | Extra timing/trend notes when included. |
| **Section 7 — Risk insights** | Optional AI-written summary of account and churn risk. Scores use **Salesforce `commercial_status`** (not just Pendo usage). Customers in renewal negotiation are scored differently from churned accounts. |

At the top of the file you’ll also see **Snapshot coverage** — a short explanation of what was included, what was left out, and any limits on this run. **Read that first** if an answer looks incomplete.

---

## Customer status (who counts as what?)

Salesforce decides **who is a customer** and **whether they’re active, renewing, churned, or future**. Pendo shows **usage**; it does **not** decide if someone churned.

| Status | What it means |
|--------|----------------|
| **Active** | They have a current contract. |
| **Out of contract, renewing** | The old contract ended, but there’s an **open renewal** in Salesforce. Still part of the **current book** for revenue ranking. |
| **Churned** | No active contract and **no** renewal in progress. Listed in **Section 3b only**. |
| **Future** | Contract is **signed** but **hasn’t started** yet. Listed in **Section 3b-future only**. |

**Current book** = Active + Out of contract, renewing. That’s what Sections 1, 3, and 5 focus on for “who’s in the portfolio today.”

### Example prompts — customer status

- “List our top 10 customers by revenue from the Section 3.1 table (use the `rank` column) and say whether each is Active or Renewing.”
- “Who appears in Section 3b as churned? Don’t include anyone from Section 3.”
- “Is Acme Corp in the current book or in the churned section? Quote the status field.”
- “Which customers are in renewal negotiation (Section 3b-renewal) but might still show usage in Section 1?”

---

## Revenue (ARR) — what the numbers mean

**Two ranking lists (same rollup math, different scope):**

| List | Where | Who’s included |
|------|--------|----------------|
| **`selection_ranked`** | Coverage block; drives §2 Jira / §4 CS Report top-N | **Current book only** — ACTIVE + OUT_OF_CONTRACT_RENEWING. True churn (lost contracts, no renewal) is excluded on purpose. |
| **`arr_by_ultimate_parent`** | §3c Salesforce comprehensive | **All** ultimate parents from contract rollups — active, renewing, churned, future. Carrier divisions collapse to one row (~$1.1M). Ford appears here when `commercial_status` is OUT_OF_CONTRACT_RENEWING. |

If Ford shows **`CHURNED`** with no renewal pipeline in Salesforce, the export is reflecting CRM data — verify open opportunities on the parent account before expecting them in a current-ARR ranking.

**ARR** = Annual Recurring Revenue. The export breaks it down so you don’t double-count:

| Term | Plain meaning |
|------|----------------|
| **Current ARR** | The main number for **ranking big customers**. Active revenue **plus** renewal-in-progress revenue. |
| **Active ARR** | Revenue from contracts that are **currently active**. |
| **Renewal ARR** | Revenue tied to accounts **in renewal negotiation**. |
| **Historical ARR** | Total ARR on the account **including churned history** — useful for context, not for “who’s biggest today.” |

**Rule of thumb:** use **Current ARR** when asking “who are our largest customers?” Use **Section 3 totals** for the active book — **don’t add** Section 3b, 3b-renewal, or 3b-future on top.

### Example prompts — revenue

- “What is our total current-book ARR in Section 3?”
- “Who are the five largest customers by current ARR? Show the number for each.”
- “For customers in Section 3b-renewal, what is their renewal ARR vs active ARR?”
- “Compare historical ARR to current ARR for Beta Industries — what changed?”

---

## Prompts you might find valuable — Portfolio export

Copy/paste and adapt these. Start with “Use only the attached Cortex export; quote exact numbers and customer names, and say if something isn’t in the file.” See [Tips for talking to an AI](#tips-for-talking-to-an-ai) for more.

### Executive summary & QBR prep

- “Give me a one-page executive summary of the portfolio: top 10 customers by current ARR (Section 3 / 3c), the biggest usage risks (Sections 1, 5, 7), and anyone in renewal or churn (Sections 3b, 3b-renewal).”
- “Draft QBR talking points for **Carrier**: pull ARR (Section 3), support load (Section 2), CS health (Section 4), and usage trend (Section 1). Keep it to 5 bullets.”
- “Which accounts should leadership watch this week? Combine risk insights (Section 7) with declining usage (Section 5) and name the top 5 with a one-line reason each.”
- “Build a board-ready table: for our top 10 customers by current ARR, show ARR, status, support ticket trend, and CS health score.”

### Revenue & ARR

- “Rank all ultimate parents by **current ARR** from Section 3c (`arr_by_ultimate_parent`) and show `commercial_status` for each.”
- “What is total current-book ARR in Section 3, and what share sits in the top 5 accounts?”
- “Compare **historical ARR** to **current ARR** for Beta Industries — what changed and why?”
- “List current-book customers (Section 3) whose ARR is above $500K but whose login rate in Section 1 is below the portfolio median.”

### Renewals & churn risk

- “Which current-book customers have contracts ending in the next 90 days? (Sections 3 / 3b-renewal)”
- “Who is in renewal negotiation (Section 3b-renewal) and also flagged in Section 7 risk insights?”
- “Don’t count churned customers — who is most at risk among the current book? Use Sections 5 and 7.”
- “Who churned (Section 3b) that still showed usage in Section 1 in the last window? These may be win-back targets.”

### Support load (Section 2)

- “Which of our largest customers have the most open support tickets, and is volume rising or falling?”
- “Any signs of SLA or response-time issues for Carrier in Section 2?”
- “Cross-reference Section 2 support volume with Section 4 CS health — who has both high tickets and poor health?”

### Customer success health (Section 4)

- “For our top customers in Section 4, who has red or yellow platform-health scores?”
- “Give a one-paragraph health summary for Hussmann using Section 4 only.”

### Usage & adoption (Sections 1 & 5)

- “Which customers have the lowest login rate in the last 90 days?”
- “Summarize the usage signals in Section 5 for customers with declining engagement.”
- “Does Ford show up in Section 1? If yes, give active users and login percentage.”

### Cross-section “power” prompts (join the data)

- “Find **expansion candidates**: customers with strong usage (Section 1) and healthy CS scores (Section 4) whose contracts renew soon (Section 3).”
- “Find **hidden churn risk**: high current ARR (Section 3) but declining usage (Sections 1/5) and open support pressure (Section 2).”
- “Which customers appear in Section 7 risk insights but still have open renewals in Section 3b-renewal? Reconcile the two.”

### Trust & data quality

- “What was capped or omitted in this run? Read **Snapshot coverage** at the top and list any top-N limits.”
- “How fresh is the Salesforce data here? Quote the exported date and any cache note before I make a renewal decision.”

---

## Rules to avoid wrong answers

1. **Don’t mix churn with active customers.** Section 3b is churn only. Section 3 is the current book. Never add their revenue together.
2. **Trust Salesforce for customer status.** If someone looks “inactive” in Pendo but is Active in Section 3, **they’re still a customer**.
3. **Renewal ≠ churn.** Section 3b-renewal is for deals still being worked — treat them as part of the current book for revenue, not as lost accounts.
4. **Future contracts aren’t live yet.** Section 3b-future is for signed deals that haven’t started — don’t count them as active usage or support customers.
5. **Check the date.** Salesforce data may be up to ~48 hours cached. For contract or renewal decisions, confirm in Salesforce if the export is more than a day or two old.
6. **The file may be shortened.** On some runs, only the **top N customers** get full Jira, CS Report, or CRM detail. The coverage section says what was capped.

---

## Tips for talking to an AI

1. **Point to the file:** “Use only the attached Cortex export. If something isn’t there, say so.”
2. **Name the section:** “Answer from Section 3 only” avoids pulling churned accounts into a portfolio answer.
3. **Ask for quotes:** “Quote the exact numbers and customer names from the file.”
4. **Say what to exclude:** “Ignore Section 3b and 3b-future for this question.”
5. **One question at a time** works better than ten questions in one message for long files.

---

## Per-customer Pendo export

### What is this file?

For strategic accounts (e.g. Ford on a daily or weekly cadence), Cortex can export a **customer-scoped Pendo usage packet**: markdown plus a **Google Sheet** with the same tables. The bookmarkable markdown is named like **`Pendo Export  (Ford, 30d)-persistent.md`**; the companion spreadsheet uses the same stem with `-persistent` (no `.md`).

This export is **Pendo only** — no Jira, Salesforce, or CS Report sections. Use the **portfolio LLM context** export above when you need contract status, support load, or churn segmentation.

### Where it lives on Drive

Per-customer exports live under **`Output/Customer Exports/{Customer}/`**, not in the portfolio `Output/` root:

```
Output/Customer Exports/
  Ford/
    Pendo Export  (Ford, 30d)-persistent.md      ← bookmarkable current export
    Pendo Export  (Ford, 30d)-persistent         ← spreadsheet (same stem)
    Historical Data/
      2026-07-07/
        Pendo Export  (Ford, 30d).md
        Pendo Export  (Ford, 30d)                ← spreadsheet snapshot
```

Prior-month day folders under **Historical Data** are rolled into monthly buckets (`Historical Data/2026-06/…`) automatically at startup, same as portfolio exports.

### What’s inside (sections 1–12)

| Section | In plain English |
|---------|------------------|
| **1. Headline** | Active users, login rate, events, minutes — top-line health for the window. Site count reads **“N active of M provisioned”** (active = had usage in the window; provisioned = every site ever set up). |
| **2. Sites** | **Active sites only** (had events in the window), one row per site — Pendo’s internal “entity” duplicates are merged, so a plant appears once. Idle/never-used sites are excluded (counted in the headline instead). |
| **2.1 Business unit summary** | For big multi-division customers (e.g. Safran), active sites rolled up to **business unit** — sites, visitors, events, and the top site per unit. Only shown when a mapping exists for that customer. A **Confidence** note flags any sites mapped by a location/brand guess (`inferred`) or still `unmapped` — those need Customer Success confirmation (see `docs/DATA-GOVERNANCE/BUSINESS_UNIT_MAPPING_REVIEW.md`). Treat `inferred`/`unmapped` rows as provisional. |
| **3. Feature & page adoption** | Which product areas saw clicks/views |
| **4. Core feature checklist** | Expected capabilities vs observed usage |
| **5. Unused product features** | Features with no recent activity |
| **6. Behavioral depth** | How deeply users engage beyond logins |
| **7. People** | Champions and at-risk users (by recency) |
| **8. Export behavior** | Data export / download usage patterns |
| **9. Frustration signals** | Rage clicks and similar friction signals |
| **10. Kei AI** | Kei assistant usage for this customer |
| **11. Usage trends** | Weekly active users and period-over-period comparison |
| **12. Engagement context** | Cohort benchmarks and auto-detected usage signals |

**Detailed variant** (`--export-pendo-detailed`) adds **§13 Site detail** and **§14 User roster**:

- **§13.1 Site activity** — one **table** with every active site: business unit, visitors, 7d/30d/dormant, events, minutes, feature clicks, change vs prior period, and each site’s top page and top feature. Best for cross-site questions (“which sites are declining?”).
- **§13.2 Site user detail** — per-site user samples for the **busiest sites by events** only (the full user list is in §14).
- **§14 User roster** — per-user table across the account. For customers with a business-unit mapping, it includes a **Primary BU** column (the unit of each user’s most-used sites).

Every Pendo export also opens with a short **“How to read this export”** note that pins the key rules: it’s usage-only (no ARR/churn), “sites” means *active* sites (idle ones are counted in §1), and **per-site visitor counts overlap** so you shouldn’t add them up for unique headcount (use §1 total visitors).

The **top-ARR batch** (`--export-pendo-top-arr`) runs the detailed export for the largest Salesforce ultimate parents by current ARR.

### Prompts you might find valuable — Pendo export

Copy/paste and adapt. Start with “Use only the attached Pendo export for {Customer}; quote exact numbers and site/user names, and say if something isn’t in the file.” Replace `{Customer}` with the Pendo prefix in the filename.

**Account health at a glance**

- “Give me a health snapshot for {Customer}: headline metrics (Section 1), the weekly trend (Section 11), and the auto-detected signals (Section 12).”
- “Is engagement growing or shrinking? Use Section 11 weekly active users and the prior-period comparison, and quote the percentages.”
- “What are the three most important things to know about {Customer}’s usage this window? Cite the section for each.”

**Sites & business units**

- “How many active sites does {Customer} have vs how many are provisioned? (Sections 1 / 2)”
- “From Section 2.1, rank business units by active sites and events, and name the top site in each.”
- “In Section 13.1, which sites are declining most vs the prior period? Group them by business unit.”
- “Which active sites have many visitors but low events (possible adoption gaps)? (Section 2 / 13.1)”
- “Summarize {Customer}’s footprint by business unit for a QBR slide: sites, visitors, and events per unit (Section 2.1).”

**Adoption & whitespace**

- “From Section 4 (core feature checklist), which expected capabilities are not adopted or are declining?”
- “What are the top unused features in Section 5, and which look like real expansion or enablement opportunities?”
- “Which pages and features drive the most usage? (Section 3) What does that say about how they use the product?”
- “Given the write ratio in Section 6, are users running operations in the product or just reading dashboards?”

**People & champions**

- “List champions and at-risk users from Section 7 with their roles and last-visit dates.”
- “From Section 14, who are the 10 most active users, and what roles and business units are they in?”
- “Which business unit (Primary BU) do the most active users belong to? (Section 14)”
- “Who are the at-risk users we should re-engage, and which sites are they on? (Sections 7 / 13.2 / 14)”

**Friction & support risk**

- “Where is the most user friction? Summarize Section 9 (rage / dead / error / U-turn) by page.”
- “Does export behavior in Section 8 suggest users are working outside the product? Who are the heaviest exporters?”
- “Is there onboarding friction? Check the guide-dismiss signal in Section 12.”

**Detailed-variant power prompts (§13–§14)**

- “Build a per-site scorecard for {business unit}: from Section 13.1 list each site’s visitors, events, dormant count, and change vs prior period.”
- “Cross-reference Section 13.1 and Section 14: for the top declining sites, name the active users we should reach out to.”
- “For {Customer}, draft a QBR usage narrative using Sections 1, 2.1, 11, and 12 — highlight growth, decline, and business-unit spread.”

### Rules to avoid wrong answers (Pendo export)

1. **This file does not define churn or ARR.** For contract status or revenue, use the portfolio export’s Salesforce sections.
2. **Customer identity is the Pendo prefix** (e.g. `Ford`), not a Salesforce account name — aliases are resolved at export time.
3. **Check the lookback window** in the filename (`30d`, `7d`, etc.) before comparing numbers across files.

---

## Where files live on Drive

All Cortex exports under the QBR generator use the same pattern:

| Role | Location | Filename pattern |
|------|----------|------------------|
| **Bookmarkable “current” export** | Portfolio: `Output/` root · Per-customer Pendo: `Output/Customer Exports/{Customer}/` | `{stem}-persistent` (+ `.md` for markdown) |
| **Same-day historical snapshot** | `…/Historical Data/{YYYY-MM-DD}/` | Plain `{stem}` (no `-persistent`) |
| **Prior-month archives** | `…/Historical Data/{YYYY-MM}/{YYYY-MM-DD}/` | Rolled up at process startup |

**Portfolio exports** (`export-all`, engineering portfolio deck) use `Output/` as the persistent base. **Per-customer Pendo** uses each customer’s folder under `Customer Exports/`.

This user guide is also published to **`Output/Cortex Export - User Guide.md`** on Cortex startup when the repo copy is newer than Drive or missing there. It is not archived into `Historical Data/` with export snapshots.

Other artifacts (Jira cache JSON, chart spreadsheets) live under the generator root (`Cache/`, `chart-data/`) — not in this export layout.

---

## For operators (how exports are generated)

### Portfolio LLM context

```bash
cortex export-all
```

Common options: `--days 90` (lookback window), `--skip-risk-insights` (omit Section 7), filters to trim the customer list. The nightly `export-nightly` job uses a 90-day window by default.

Drive output: `Output/LLM-Context-Portfolio-persistent.md` and `Output/Historical Data/{today}/LLM-Context-Portfolio.md`.

### Per-customer Pendo

```bash
cortex --export-pendo --customer Ford --days 30
cortex --export-pendo-detailed --customer Ford --days 30
cortex --export-pendo-top-arr --top-n 5 --days 30
```

Scheduled jobs include `ford-pendo-7d`, `ford-pendo-30d`, `carrier-pendo-detailed-30d`, and `pendo-top-arr-30d`. Add `--no-drive` to write locally only; `-o` / `--out-dir` set local paths.

Business units for §2.1 / §13.1 come from `config/pendo_site_bu_map.yaml` (per Pendo prefix); customers with no entry simply omit the business-unit column and §2.1. Each rule carries a `confidence` (`high` = the site name self-labels its division; `inferred` = a location/brand guess); unmatched sites fall to the `default_business_unit` (`Unmapped — needs review`). Sites resolving to `inferred` or the default are surfaced every run (export log warning + §2.1 Confidence note) and collected in `docs/DATA-GOVERNANCE/BUSINESS_UNIT_MAPPING_REVIEW.md` for periodic CS review. For customers whose CS Report is split by division (e.g. Safran), `python scripts/build_csr_bu_map.py --customer <name> --live` joins Pendo sites to the CS Report factory list and prints an authoritative, CSR-confirmed rules fragment plus a coverage report to refresh the map. Safran is validated; **Carrier, Spirit, and Bombardier are provisional** (all rules `inferred`) pending CS-confirmed taxonomy. `CORTEX_PENDO_SITE_DETAIL_USER_SITES` (default 20) caps how many top sites get a per-site user table in §13.2.

Drive output (per customer): `Output/Customer Exports/{Customer}/` persistent markdown + spreadsheet, plus matching copies under `Historical Data/{today}/`.

For field definitions and integration details, see [`DATA_DICTIONARY.md`](./DATA-GOVERNANCE/DATA_DICTIONARY.md) and [`SALESFORCE_REVENUE_AND_ARR.md`](./DATA-GOVERNANCE/SALESFORCE_REVENUE_AND_ARR.md).
