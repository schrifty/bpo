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
| **Section 3 — Salesforce (current customers)** | **Customers you should treat as “in the book today”** — active contracts **or** renewals in progress. Revenue and contract facts live here. |
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

- “List our top 10 customers by revenue from Section 3 and say whether each is Active or Renewing.”
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

## Example prompts by topic

### Usage and adoption (Section 1 & 5)

- “Which customers have the lowest login rate in the last 90 days?”
- “Summarize the usage signals in Section 5 for customers with declining engagement.”
- “Does Ford show up in Section 1? If yes, give active users and login percentage.”

### Support (Section 2)

- “Which of our largest customers have the most open support tickets?”
- “Is support volume going up or down for our top 5 accounts?”
- “Any signs of SLA or response-time issues for Carrier?”

### Customer Success health (Section 4)

- “For our top customers in Section 4, who has red or yellow platform health scores?”
- “Give a one-paragraph health summary for Hussmann using Section 4 only.”

### Renewals and risk (Sections 3, 3b-renewal, 7)

- “Which current-book customers have contracts ending in the next 90 days?”
- “Who is in renewal negotiation and also flagged in Section 7 risk insights?”
- “Don’t count churned customers — who is most at risk of churn among the current book?”
- “In Section 7, who has OUT_OF_CONTRACT_RENEWING status and what does the risk score say about them?”

### Portfolio overview

- “Give me an executive summary: top customers by revenue, biggest usage concerns, and anyone in renewal or churn sections.”
- “What data is missing or truncated in this export? Check the coverage section at the top.”

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
| **2.1 Business unit summary** | For big multi-division customers (e.g. Safran), active sites rolled up to **business unit** — sites, visitors, events, and the top site per unit. Only shown when a mapping exists for that customer. |
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
- **§14 User roster** — per-user table across the account.

The **top-ARR batch** (`--export-pendo-top-arr`) runs the detailed export for the largest Salesforce ultimate parents by current ARR.

### Example prompts — Pendo export

- “Summarize Section 1 headline metrics for Ford over the last 30 days.”
- “How many active sites does this customer have vs how many are provisioned? (Section 1 / Section 2)”
- “Which sites in Section 2 have the lowest weekly active rate?”
- “From Section 2.1, which Safran business unit has the most active sites and the highest event volume?”
- “In Section 13.1, list the sites with the most negative change vs the prior period.”
- “List champions from Section 7 and any at-risk users.”
- “What unused features appear in Section 5? Should we be concerned?”
- “How did weekly active users trend in Section 11 vs the prior comparison window?”

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

Business units for §2.1 / §13.1 come from `config/pendo_site_bu_map.yaml` (per Pendo prefix); customers with no entry simply omit the business-unit column and §2.1. `CORTEX_PENDO_SITE_DETAIL_USER_SITES` (default 20) caps how many top sites get a per-site user table in §13.2.

Drive output (per customer): `Output/Customer Exports/{Customer}/` persistent markdown + spreadsheet, plus matching copies under `Historical Data/{today}/`.

For field definitions and integration details, see [`DATA_DICTIONARY.md`](./DATA-GOVERNANCE/DATA_DICTIONARY.md) and [`SALESFORCE_REVENUE_AND_ARR.md`](./DATA-GOVERNANCE/SALESFORCE_REVENUE_AND_ARR.md).
