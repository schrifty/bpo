# Cortex Export — User Guide

This guide explains **what’s in the Cortex customer export**, **how to read it**, and **how to ask an AI good questions** using the file. You do not need to know how the export is built to use it well.

## What is this file?

Cortex produces a snapshot called **`LLM-Context-Portfolio.md`**. Think of it as a **single briefing packet** about your customer portfolio: who they are, how they use the product, support load, contract status, and health signals.

The file is saved to **Google Drive** (under the Cortex generator folder → **Output**). A fresh copy is usually created on a schedule; each file has an **Exported (UTC)** date at the top so you know how old the data is.

**Best use:** attach the file (or paste a section) to ChatGPT, Claude, or another assistant and ask questions in plain English. The assistant should **quote numbers from the file** and say when something isn’t in the snapshot.

---

## What’s inside (the numbered sections)

The export is split into numbered sections. When you ask the AI a question, it helps to name the section if you know it.

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

## For operators (how the file is generated)

Technical teammates run:

```bash
cortex export-all
```

Common options: `--days 90` (lookback window), `--skip-risk-insights` (omit Section 7), filters to trim the customer list. The nightly job uses a 90-day window by default.

For field definitions and integration details, see [`DATA_DICTIONARY.md`](./DATA_DICTIONARY.md) and [`SALESFORCE_REVENUE_AND_ARR.md`](./SALESFORCE_REVENUE_AND_ARR.md).
