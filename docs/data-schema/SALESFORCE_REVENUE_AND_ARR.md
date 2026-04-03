# Salesforce: contracts, ARR, and where recurring value actually lives

This document goes **deeper** than [`SALESFORCE_DATA_SCHEMA.md`](./SALESFORCE_DATA_SCHEMA.md) §2–§7 on **how organizations typically model annual recurring revenue (ARR), monthly recurring revenue (MRR), and “contract value”** in Salesforce—and why a single `Contract` query rarely answers “what is ARR?”

**BPO today:** cohort/portfolio totals use **`Account.ARR__c`** on **`Type = 'Customer Entity'`** rows, matched to Pendo names by substring. That is one valid pattern; it is **not** the only pattern, and it may disagree with finance if their truth is on **subscriptions**, **order lines**, or **another rollup**.

---

## 1. The core confusion: “Contract” vs “contract value”

| Phrase | What people often mean | What Salesforce **standard** objects give you |
|--------|-------------------------|-----------------------------------------------|
| **Contract** | The **Contract** sObject: legal/commercial agreement record | `Contract` — **AccountId**, **Status**, **StartDate**, **EndDate**, **ContractTerm**, **ContractNumber**, owner, etc. |
| **Contract value / ARR** | Money: recurring or TCV | **Not** a universal standard currency field on `Contract`. Value is usually on **Account**, **Opportunity**, **Order/line**, **CPQ Subscription**, or **custom** fields—depending on the org. |

So: **having `Contract` rows does not imply you can read ARR from `Contract`** without org-specific fields or related objects.

---

## 2. Pattern A — Account-level ARR/MRR (simplest CRM model)

**Common in:** SMB/mid-market, single product line, finance OK with “one headline number per customer.”

- Custom fields on **Account**, e.g. `ARR__c`, `MRR__c`, `Renewal_Date__c`, sometimes maintained by:
  - **Integration** from billing (NetSuite, Zuora, Stripe metadata, etc.)
  - **Sales Ops** manual or spreadsheet load
  - **Flow / Process Builder** from winning Opportunity
  - **Roll-up summary** or **DLRS** from child objects (less common on vanilla Account without custom children)

**Pros:** Trivial to report and to match to a “customer” in Pendo. **Cons:** Can drift from source systems; substring name match (as in BPO) can **wrong-account** or **double-count** if matching is fuzzy.

**BPO alignment:** `get_arr_by_customer_names` → **`Account.ARR__c`** on Customer Entity accounts is this pattern.

---

## 3. Pattern B — Opportunity-centric value

**Common in:** Sales-led B2B; “ARR” often means **pipeline + closed** in different places.

- **Opportunity.Amount** — header deal value; often **TCV** or **first-year** value depending on process.
- Custom **Opportunity** fields: `ARR__c`, `TCV__c`, `ACV__c` — definitions are **org-specific**.
- **OpportunityLineItem** — line-level **Quantity**, **UnitPrice**, **TotalPrice**; recurring vs one-time is usually **product family**, **product code**, or **custom** line fields—not obvious from schema alone.

**Renewals:** Often a **new Opportunity** (Renewal record type) or **amendment** flows; summing “all open Opps” is **not** the same as booked ARR.

**Pros:** Tied to sales stages and forecasting. **Cons:** Not equal to “current entitlement ARR” unless process enforces it (e.g. closed-won snapshot to Account).

---

## 4. Pattern C — Order and OrderItem (quote-to-cash without CPQ)

**Common in:** Orgs using **Orders** as system of record for what was sold.

- **Order** — **TotalAmount**, **Status**, **EffectiveDate**, **AccountId**
- **OrderItem** — quantity, unit price, **TotalPrice**, link to **Product2**

Recurring ARR may be **derived** from:
- Products flagged as **subscription/recurring** (custom field or family)
- **Multiple orders** over time (need rules: active orders only, date range, etc.)

**Pros:** Closer to “what was booked.” **Cons:** Still need **business rules** to annualize and to exclude one-time SKUs.

---

## 5. Pattern D — Salesforce CPQ (legacy “Steelbrick” — `SBQQ__` namespace)

**Common in:** B2B SaaS with **guided selling**, **amendments**, **co-terming**, **subscriptions**.

Managed package objects (names are **typical**; always verify in **Setup → Schema** or installed package version):

| Concept | Typical API surface (CPQ) | Role in recurring revenue |
|--------|---------------------------|----------------------------|
| Quote | `SBQQ__Quote__c` (+ lines) | Negotiated commercial terms; list/net prices, discounts, **subscription terms** |
| Quote line | `SBQQ__QuoteLine__c` | **Product**, **quantity**, **term**, **billing frequency**, **proration**—often where **ARR is calculated or stored** before booking |
| Order / Order Product | Standard **Order**, **OrderItem** (often CPQ-populated) | What was **ordered**; feeds billing in some setups |
| Contract | Standard **Contract** | Often **activated** from CPQ; **links** subscription lifecycle to Account |
| Subscription | Typically **`SBQQ__Subscription__c`** | **Ongoing subscription instance**: product, dates, quantity, pricing fields—**common source for ARR/MRR reporting** in CPQ orgs |
| Asset | **Asset** (sometimes CPQ-driven) | Installed entitlement; can relate to subscriptions in asset-based models |

**ARR in CPQ** is often **not** “read `Contract.TotalSomething`” but:
- **Roll up** or **report** from **Subscription** (and quote line attributes), with formulas involving **term**, **prorate multiplier**, **billing frequency**, etc.
- Or a **custom rollup** to **Account** for executive dashboards

**Official references:** Search Salesforce Help for **CPQ subscription fields**, **quote line fields**, and your **installed package version** release notes—field-level help changes with package versions.

**Pros:** Rich subscription logic. **Cons:** **Heavy** integration surface; wrong to assume field names without Describe; **test in sandbox**.

---

## 6. Pattern E — Revenue Cloud, Subscription Management, Billing

Salesforce has been expanding **Revenue Cloud** (and related **Subscription Management**, **Billing**, CLM) with additional objects and flows—**Sales Contract Line**, **transaction lines**, **revenue schedules**, etc., depending on **what is licensed and enabled**.

**Characteristics:**
- Often the **system of truth** for **recognized revenue** or **contracted revenue** shifts toward these objects—not only `Account.ARR__c`.
- **Data model diagrams:** [Salesforce Contracts (developer data model gallery)](https://developer.salesforce.com/docs/platform/data-models/guide/salesforce-contracts.html) shows how **Contract** connects to a **large** ecosystem; your org may only use a subset.

**Pros:** Aligns CRM with revenue operations. **Cons:** Highest complexity; requires **Solution Architect**-level discovery.

---

## 7. Multi-currency and “whose ARR?”

- **Corporate vs transaction currency:** Amounts may be stored in **corporate currency** or **record currency** depending on **Multi-Currency** settings.
- **FX:** Summing ARR across regions without a **common currency** rule will not match finance.
- **Timing:** **Booking date** vs **service start** vs **invoice date**—three different answers to “ARR as of today.”

Any integration that sums floats from Salesforce should document **which currency and which date** apply.

---

## 8. Admin / engineer checklist: discover *your* org’s truth

Work in a **sandbox** with a **integration user** that has the same object access as production.

1. **Installed packages** — Setup → **Installed Packages**. Look for **Salesforce CPQ**, **Billing**, **Revenue Cloud**-related packages. Note **version**.
2. **Describe critical objects** — REST `GET /services/data/vXX.X/sobjects/Contract/describe` (and **Account**, **Order**, **`SBQQ__Subscription__c`** if it exists). Search describe for `ARR`, `MRR`, `ACV`, `TCV`, `Recurring`, `Net`, `Annual`.
3. **Report types** — See what **standard reports** finance uses for “ARR” or “bookings”; the **primary object** of those reports is a strong hint.
4. **Single customer drill** — Pick one Account; open **Related** lists: **Contracts**, **Orders**, **Opportunities**, **Subscriptions** (if present). See **where numbers agree** with finance’s spreadsheet.
5. **Matching keys** — Prefer **stable IDs** (Salesforce Account Id synced to Pendo or a data warehouse) over **name substring** once you know the ARR source object.

---

## 9. Implications for BPO (this repo)

| Topic | Current behavior | If ARR is “wrong” |
|-------|------------------|-------------------|
| Cohort **Total ARR** | Sum of **`Account.ARR__c`** for Customer Entity accounts matched by **name contains** | Finance may use **Subscription** or **Order** rollups; or **Account.ARR__c** is stale/wrong; or **matching** attributes ARR to the wrong account |
| Standard **Contract** in BPO | **`query_contracts`** / comprehensive **`contracts`** category — **no ARR** in default `MAINSTREAM_OBJECT_FIELDS` | Extend **`fields=`** with org ARR fields **if** they exist on `Contract`, **or** add a new query path (e.g. aggregate `SBQQ__Subscription__c` by Account) |
| Documentation | [`SALESFORCE_DATA_SCHEMA.md`](./SALESFORCE_DATA_SCHEMA.md) §2, §7 Contract, [`DATA_REGISTRY.md`](./DATA_REGISTRY.md) | Registry entries can be extended when a **canonical** ARR source object is chosen |

---

## 10. Further reading (external)

- [Salesforce Contracts — data model gallery](https://developer.salesforce.com/docs/platform/data-models/guide/salesforce-contracts.html) — relationship context (high level).
- Salesforce Help — search **CPQ subscription**, **quote line**, **contract activation** for your package version.
- [Subscription Management — Developers](https://developer.salesforce.com/docs/revenue/subscription-management/guide) — when Subscription Management is in scope.

---

*This file is descriptive reference, not legal or accounting advice. ARR/MRR definitions should match your **finance** policy.*
