# Usage Data Priorities: What Matters and What We Can Build

An analysis of what Customer Success, Product, and Marketing teams need from product usage data — mapped against what's actually available from our Pendo API.

---

## Table of Contents

1. [Customer Success Priorities](#1-customer-success-priorities)
2. [Product Team Priorities](#2-product-team-priorities)
3. [Marketing & Growth Priorities](#3-marketing--growth-priorities)
4. [Data Availability Matrix](#4-data-availability-matrix)
5. [Derived Metrics We Can Compute](#5-derived-metrics-we-can-compute)
6. [What We Cannot Get from Pendo Alone](#6-what-we-cannot-get-from-pendo-alone)
7. [Sources](#7-sources)

---

## 1. Customer Success Priorities

### 1.1 Churn Risk Detection

The #1 CS priority. Health scores that predict churn 30-90 days in advance reduce churn by 15-30% when properly implemented. The strongest predictors combine product usage with relationship signals.

**What predicts churn (ranked by impact):**

| Signal | Weight | What to Measure | Pendo Data Available? |
|--------|--------|-----------------|----------------------|
| **Declining login frequency** | High | DAU/WAU/MAU trending down over 4-8 weeks | **Yes** — `visitors.metadata.auto.lastvisit`, `pageEvents` by day, visitor counts per account over time |
| **Shrinking feature breadth** | High | Fewer distinct features used per period | **Yes** — `featureEvents.featureId` distinct count per visitor/account per period |
| **Falling engagement depth** | High | Fewer total events, less time spent | **Yes** — `numEvents`, `numMinutes` in pageEvents + featureEvents, aggregatable by day |
| **Seat utilization drop** | Medium | Active users / licensed users trending down | **Partial** — Active visitors per account from `visitors`; licensed seats NOT in Pendo (needs CRM/billing) |
| **Support ticket volume spike** | Medium | Increasing support load | **No** — Needs Zendesk/Intercom/etc. |
| **NPS score decline** | Medium | Sentiment drop | **Yes** — `pollEvents` has `pollResponse` (NPS score 0-10), `pollType: "NPSRating"` |
| **Guide dismissal patterns** | Low-Med | Users closing onboarding/help guides | **Yes** — `guideEvents` type `guideDismissed` by account |
| **Frustration signals** | Low-Med | Rage clicks, dead clicks, error clicks | **Yes** — `rageClickCount`, `deadClickCount`, `errorClickCount` on every page/feature event |

### 1.2 Account Health Scoring

Industry standard is a weighted composite (0-100 scale). Typical weights:

| Domain | Weight | Available from Pendo? |
|--------|--------|-----------------------|
| Product usage (login frequency, feature breadth, seat utilization) | 35% | **Mostly yes** (seat utilization needs license count from CRM) |
| Support health (ticket volume, resolution time) | 20% | **No** — external system |
| Engagement/Sentiment (NPS, communication) | 15% | **Partially** — NPS from pollEvents; communication sentiment not in Pendo |
| Commercial metrics (ARR, payment, renewal proximity) | 15% | **No** — CRM/billing system |
| Technical fit / value realization | 15% | **Partially** — feature adoption breadth as a proxy |

**Bottom line:** Pendo can supply roughly 40-50% of the inputs for a robust health score. The usage component (the most predictive single domain) is fully available.

### 1.3 QBR / Executive Business Review Decks

What CS teams present to customers quarterly. This is directly relevant to the slide decks we're generating.

**What belongs in a QBR deck:**

| Section | Data Needed | From Pendo? |
|---------|-------------|-------------|
| **Usage summary** | Total events, minutes, active users, trend vs. prior period | **Yes** |
| **Feature adoption** | Which features used, adoption % of core features | **Yes** — featureEvents + `/feature` catalog for names |
| **User engagement trend** | DAU/WAU/MAU over time, stickiness | **Yes** — visitors by day |
| **Top users / power users** | Most active visitors with names/roles | **Yes** — visitors with usage metrics, `emailaddress`, `role`, `profiletype` |
| **Underutilized features** | Features available but not used | **Yes** — catalog vs. actual usage |
| **NPS / sentiment** | Latest NPS score, trend | **Partially** — pollEvents (if surveys are active) |
| **Value realization** | ROI metrics, KPI achievement | **No** — business-specific, not in Pendo |
| **Recommendations** | Next steps, training suggestions | Derived from usage patterns |
| **Account contacts** | CSM name, executive sponsor | **Partially** — `ownername` (CSM) is in Pendo; executive sponsor is not |

### 1.4 Renewal & Expansion Signals

| Signal | Measurement | From Pendo? |
|--------|-------------|-------------|
| **Growing active users** | Account user count increasing over periods | **Yes** — visitor count per account over time |
| **New feature adoption** | Users trying features they hadn't used before | **Yes** — featureId tracking per visitor |
| **Cross-functional spread** | Multiple roles/departments using the product | **Yes** — `role`, `profiletype`, `division` in visitor metadata |
| **Approaching plan limits** | Nearing seat/usage caps | **No** — needs billing data |
| **Power user emergence** | Individual users with high engagement | **Yes** — visitor-level event aggregation |
| **Renewal date proximity** | Days until renewal | **No** — CRM data |

---

## 2. Product Team Priorities

### 2.1 Feature Adoption & Usage

The median feature adoption rate across SaaS is only 16.5%. Product teams need to understand what's being used, what's not, and why.

| Metric | How to Calculate | From Pendo? |
|--------|-----------------|-------------|
| **Feature adoption rate** | Visitors using feature / total active visitors | **Yes** — featureEvents + visitors |
| **Feature frequency** | Events per feature per period | **Yes** — `numEvents` grouped by `featureId` |
| **Feature retention** | % of users who used feature in period N who return in period N+1 | **Yes** — featureEvents over multiple periods |
| **Time on feature** | Minutes spent per feature | **Yes** — `numMinutes` on featureEvents |
| **Feature discovery** | Time from first login to first use of feature | **Yes** — `visitors.metadata.auto.firstvisit` vs. first featureEvent |
| **Unused features** | Features in catalog with zero events | **Yes** — `/feature` catalog vs. featureEvents |

### 2.2 User Journeys & Funnels

| Metric | How to Calculate | From Pendo? |
|--------|-----------------|-------------|
| **Page flow** | Sequence of pages visited in a session | **Partially** — pageEvents have `analyticsSessionId`, `firstTime`; can reconstruct paths |
| **Funnel conversion** | % of users completing a multi-step flow | **Yes** — pageEvents/featureEvents with session grouping |
| **Drop-off points** | Where in a flow users abandon | **Yes** — same data, identify missing next-step events |
| **Time to value** | Duration from signup to key activation event | **Yes** — `firstvisit` vs. first occurrence of defined activation event |

### 2.3 Frustration & UX Quality

Pendo captures frustration signals on every event row:

| Metric | Field | Status |
|--------|-------|--------|
| **Rage clicks** | `rageClickCount` | ✅ Available per page/feature event |
| **Dead clicks** | `deadClickCount` | ✅ |
| **Error clicks** | `errorClickCount` | ✅ |
| **U-turns** | `uTurnCount` | ✅ Quick navigation reversals |

These can be aggregated by page, feature, account, or visitor to identify problem areas.

### 2.4 Guide Effectiveness

| Metric | How to Calculate | From Pendo? |
|--------|-----------------|-------------|
| **Guide completion rate** | `guideAdvanced` count / `guideSeen` count | **Yes** — guideEvents by type |
| **Guide dismissal rate** | `guideDismissed` / `guideSeen` | **Yes** |
| **Guide-to-action** | Feature usage after guide seen (within session) | **Yes** — correlate guideEvents with featureEvents by session/visitor |
| **NPS response rate** | pollEvents count / guideSeen for NPS guide | **Yes** |

### 2.5 Retention & Stickiness

| Metric | Formula | From Pendo? |
|--------|---------|-------------|
| **DAU/MAU ratio** (stickiness) | Unique daily visitors / unique monthly visitors | **Yes** — visitors with pageEvents by day |
| **Retention by cohort** | % of users from signup cohort still active N days later | **Yes** — `firstvisit` as cohort key, activity by period |
| **Resurrection rate** | Previously inactive users who return | **Yes** — gap detection in visitor activity |

---

## 3. Marketing & Growth Priorities

### 3.1 Product-Led Growth Signals

| Signal | Measurement | From Pendo? |
|--------|-------------|-------------|
| **Product-Qualified Leads (PQLs)** | Users hitting activation thresholds | **Yes** — define threshold in events/features, query against it |
| **Viral/invite behavior** | Users inviting teammates | **No** — needs app-level invite tracking (could be a track event) |
| **Self-serve upgrade indicators** | Power users on basic plans | **Partial** — usage data yes; plan tier needs CRM |
| **Champion identification** | Most active users who could advocate | **Yes** — top visitors by usage, with email/role/name |
| **Expansion-ready accounts** | Accounts with growing user count + high engagement | **Yes** — visitor counts + event volumes over time |

### 3.2 Segmentation for Campaigns

Pendo visitor metadata enables rich segmentation:

| Segment Dimension | Field | Available? |
|--------------------|-------|------------|
| By role | `metadata.agent.role` | ✅ |
| By profile type | `metadata.agent.profiletype` | ✅ |
| By geography | `metadata.agent.viewercountry`, event `country` | ✅ |
| By business unit | `metadata.agent.businessunit` | ✅ |
| By account | `metadata.auto.accountid` | ✅ |
| By site | `metadata.agent.sitename` | ✅ |
| By language | `metadata.agent.language` | ✅ |
| By CSM/owner | `metadata.agent.ownername` | ✅ |
| By activity level | Derived from event counts | ✅ |
| By feature usage | Derived from featureEvents | ✅ |
| By account plan/tier | `metadata.agent.plan` (account-level) | ✅ (if populated) |
| By industry | Not in current metadata | ❌ |

### 3.3 Content & Adoption Marketing

| Need | Data Source | From Pendo? |
|------|------------|-------------|
| Feature awareness gaps | Features with low adoption vs. catalog | **Yes** |
| Training targeting | Users/accounts not using key features | **Yes** |
| Success stories | Accounts with high PES and growth | **Yes** |
| Webinar/event targeting | Users by role, region, feature usage | **Yes** |

---

## 4. Data Availability Matrix

Mapping the most-requested analytics against what our Pendo API provides today.

| # | Metric / Report | CS | Prod | Mktg | Available in Pendo API? | Data Source |
|---|-----------------|----|----- |------|------------------------|-------------|
| 1 | **Active users (DAU/WAU/MAU)** | ★★★ | ★★★ | ★★ | ✅ Full | `visitors` + `pageEvents` by day |
| 2 | **Feature adoption rate** | ★★★ | ★★★ | ★★ | ✅ Full | `featureEvents` + `/feature` catalog |
| 3 | **Feature breadth per user** | ★★★ | ★★★ | ★ | ✅ Full | Distinct `featureId` per visitor |
| 4 | **Engagement trend (events/minutes)** | ★★★ | ★★ | ★ | ✅ Full | `pageEvents` + `featureEvents` by period |
| 5 | **Usage by account/site** | ★★★ | ★★ | ★★ | ✅ Full | Events grouped by `accountId` or `sitename` |
| 6 | **Stickiness (DAU/MAU)** | ★★★ | ★★★ | ★ | ✅ Full | Visitor counts by day vs. month |
| 7 | **NPS scores** | ★★★ | ★★ | ★★ | ✅ Full | `pollEvents` with `pollResponse`, `pollType` |
| 8 | **Frustration signals** | ★★ | ★★★ | ★ | ✅ Full | `rageClickCount`, `deadClickCount`, etc. |
| 9 | **Guide effectiveness** | ★★ | ★★★ | ★★ | ✅ Full | `guideEvents` + `/guide` catalog |
| 10 | **User journey / page flow** | ★ | ★★★ | ★ | ✅ Partial | `pageEvents` with session ID; reconstruction needed |
| 11 | **Top users / champions** | ★★★ | ★ | ★★★ | ✅ Full | Visitor-level aggregation + metadata |
| 12 | **Seat utilization** | ★★★ | ★ | ★ | ⚠️ Half | Active visitors per account (yes); licensed seats (no — needs CRM) |
| 13 | **Product Engagement Score** | ★★★ | ★★★ | ★★ | ✅ API | Pendo PES API (separate from aggregation) |
| 14 | **Retention by cohort** | ★★ | ★★★ | ★ | ✅ Derivable | `firstvisit` cohort + activity by period |
| 15 | **Time to value** | ★★★ | ★★★ | ★★ | ✅ Derivable | `firstvisit` vs. first activation event |
| 16 | **Renewal date** | ★★★ | ★ | ★ | ❌ No | CRM/billing only |
| 17 | **ARR / revenue** | ★★★ | ★ | ★★ | ❌ No | CRM/billing only |
| 18 | **Support ticket health** | ★★★ | ★★ | ★ | ❌ No | Zendesk/Intercom/etc. |
| 19 | **Executive contacts** | ★★★ | ★ | ★★ | ⚠️ Partial | `ownername` (CSM); exec sponsor not in Pendo |
| 20 | **Account plan/tier** | ★★ | ★ | ★★ | ⚠️ Partial | `account.metadata.agent.plan` (if populated) |

**Legend:** ★ = nice-to-have, ★★ = important, ★★★ = critical

---

## 5. Derived Metrics We Can Compute

These don't exist as raw fields but can be calculated from available Pendo data:

### 5.1 Product Engagement Score (DIY version)

Pendo offers a PES API, but we can also compute a comparable score:

**Adoption** = Average distinct core features used per active visitor (last 30 days)
- Data: `featureEvents` → distinct `featureId` per `visitorId` per period
- Requires: Defining "core features" (subset of `/feature` catalog)

**Stickiness** = DAU / MAU
- Data: Distinct `visitorId` in `pageEvents` per day / per month

**Growth** = (New visitors + Returning after lapse) / Dropped visitors
- Data: `visitors.metadata.auto.firstvisit` for new; gap detection for returned/dropped

**PES** = (Adoption_normalized + Stickiness_normalized + Growth_normalized) / 3

### 5.2 Account Health Score (usage component)

Computable from Pendo data alone (the ~35-40% usage weight of a full health score):

| Component | Calculation | Weight |
|-----------|-------------|--------|
| Login frequency | Days with activity / days in period, per account | 30% |
| Feature breadth | Distinct features / total core features, per account | 25% |
| Engagement depth | Total events + minutes vs. peer benchmark | 20% |
| User growth | Active visitors this period vs. prior period | 15% |
| Frustration inverse | 1 - (frustration events / total events) | 10% |

### 5.3 Churn Risk Indicators

Flags derivable from Pendo data:

| Indicator | Logic | Severity |
|-----------|-------|----------|
| **No login in 14+ days** | `lastvisit` < now - 14 days for all account visitors | High |
| **50%+ drop in weekly events** | Compare 2-week rolling averages | High |
| **Feature breadth shrinking** | Distinct features this month < last month by >30% | Medium |
| **NPS detractor** | `pollResponse` <= 6 | Medium |
| **Rising frustration** | Rage/dead/error click rate increasing | Medium |
| **Single-user dependency** | >80% of account events from one visitor | Medium |
| **Guide avoidance** | High `guideDismissed` rate | Low |

### 5.4 Expansion Signals

| Signal | Logic |
|--------|-------|
| **Growing user base** | New visitors joining account this period > prior period |
| **Feature ceiling** | Account using >80% of available features |
| **Cross-role adoption** | Multiple distinct `role` values active on account |
| **Power users on basic plan** | High-usage accounts on plan != "Enterprise" (if plan is in metadata) |

---

## 6. What We Cannot Get from Pendo Alone

These are critical for a complete CS picture but require external data sources:

| Data | Why It Matters | Where to Get It |
|------|---------------|-----------------|
| **ARR / MRR** | Revenue health, expansion revenue | Salesforce, billing system |
| **Renewal date** | Timing urgency for at-risk accounts | Salesforce |
| **Licensed seat count** | Seat utilization denominator | Billing/license management |
| **Support tickets** | Support health domain of health score | Zendesk, Intercom, Jira |
| **CSAT scores** | Satisfaction beyond NPS | Support/survey tools |
| **Executive sponsor** | QBR audience, escalation path | CRM contacts |
| **Industry / vertical** | Segmentation, benchmarking | CRM, enrichment tools |
| **Contract terms** | Expansion vs. flat renewal | CRM |
| **Onboarding milestones** | Time-to-value tracking (beyond product events) | CS platform, CRM |
| **Communication history** | Email/call frequency, sentiment | CRM, email tools |

**Integration opportunities:** Pendo integrates with Salesforce (already connected — `metadata.salesforce` and `metadata.pendo_hubspot` are present in our data), ChurnZero, Gainsight, and supports Data Sync for warehouse export.

---

## 7. Sources

| Source | URL |
|--------|-----|
| Pendo: 3 data points to stay ahead of churn | https://www.pendo.io/pendo-blog/3-data-points-in-pendo-to-help-you-stay-ahead-of-customer-churn/ |
| Pendo: PES API introduction | https://www.pendo.io/pendo-blog/introducing-the-product-engagement-score-api-bring-the-power-of-pes-to-the-rest-of-your-business/ |
| Pendo: Product-led CS KPIs | https://www.pendo.io/product-led/the-kpis-of-product-led-customer-success-teams/ |
| Pendo: Using data to drive product-led CS | https://www.pendo.io/pendo-blog/how-to-use-pendo-data-to-drive-product-led-customer-success/ |
| Pendo: Why board decks should include PES | https://www.pendo.io/pendo-blog/why-every-board-presentation-should-include-this-product-usage-metric/ |
| Pendo: Core Events | https://support.pendo.io/hc/en-us/articles/360049089172-Core-Events |
| Pendo: PES calculation guide | https://www.pendo.io/pendo-blog/a-guide-to-calculating-your-product-engagement-score/ |
| Pendo: Churn prediction model (data science) | https://www.pendo.io/pendo-blog/churn-prediction-model/ |
| Pendo: Pendo Predict (AI health scoring) | https://www.pendo.io/pendo-blog/pendo-predict-customer-churn-health/ |
| Gainsight: CS Metrics 2026 | https://www.gainsight.com/blog/customer-success-metrics-what-to-track-in-2026/ |
| SaaStr: Top 10 CS metrics investors care about | https://www.saastr.com/the-top-10-customer-success-metrics-investors-really-care-about-in-2025-with-gainsights-ceo-nick-mehta/ |
| Fastenr: Health scoring that predicts churn | https://fastenr.co/blog/ultimate-guide-customer-health-scoring-2024 |
| Umbrex: Health scoring & analytics | https://umbrex.com/resources/customer-retention-playbook/customer-health-scoring-analytics/ |
| Product-Led Alliance: State of Product Analytics 2025 | https://www.productledalliance.com/state-of-product-analytics-report-2025/ |
| Gainsight: Product Analytics guide | https://www.gainsight.com/essential-guides/product-analytics/ |
| Pendo Help Center: Event properties | https://support.pendo.io/hc/en-us/articles/7710433678619 |
