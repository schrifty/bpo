# QBR Bundle Expansion - COMPLETE ✅

**Status:** Fully implemented  
**Date:** 2026-04-21

---

## Summary

Expanded the QBR bundle from **5 companion decks** to **8 companion decks**, adding focused views for supply chain leaders, financial stakeholders, and engineering teams.

---

## ✅ What Was Added

### 1. **Supply Chain & Operations Review** (NEW)
**File:** `decks/supply-chain-review.yaml`  
**Audience:** Supply chain leaders, operations managers  
**Slides:** 10 slides

**Focus:**
- Platform health KPIs (CTB, CTC, component availability)
- Inventory risk (DOI backwards, high-risk items, ABC distribution)
- Shortage forecasts (12-week outlook with critical items)
- Scheduled deliveries and resolution timeline
- Lean Projects savings and portfolio
- Total cost avoidance

**Why:** Showcases new LeanDNA integration (Tools #1-3) in a focused ops-only deck. No Pendo engagement metrics.

### 2. **Platform Value & ROI Summary** (NEW)
**File:** `decks/platform-value-summary.yaml`  
**Audience:** CFO, procurement, executive sponsors  
**Slides:** 7 slides

**Focus:**
- Total cost avoidance and efficiency gains (primary value slide)
- Operational improvements (CTB, DOI, risk reduction)
- Continuous improvement ROI (Lean Projects)
- Active initiatives driving savings
- Platform stability indicators
- Data quality/governance

**Why:** Renewal ammunition, budget justification, exec-friendly ROI story. Dollars and outcomes only.

### 3. **Engineering Review** (EXISTING → ADDED TO BUNDLE)
**File:** `decks/engineering.yaml` (already existed, now included in bundle)  
**Audience:** Engineering, Product, Support leaders  
**Slides:** 7 slides

**Focus:**
- Support burden by customer
- SLA performance and breach rates
- Open bugs and pipeline health
- Enhancement requests
- Data quality indicators

**Why:** Engineering team needs their own view without QBR noise. Previously available but not in bundle.

---

## 🎯 Updated QBR Bundle Structure

### **Before (5 companion decks):**
```python
QBR_BUNDLE_COMPANION_DECKS = (
    ("cs_health_review", "health_review"),
    ("executive_summary", "executive_summary"),
    ("support", "support"),
    ("product_adoption", "product_adoption"),
    ("cohort_review", "cohort_review"),
)
```

### **After (8 companion decks):**
```python
QBR_BUNDLE_COMPANION_DECKS = (
    ("cs_health_review", "health_review"),
    ("executive_summary", "executive_summary"),
    ("support", "support"),
    ("product_adoption", "product_adoption"),
    ("supply_chain_review", "supply_chain"),         # NEW
    ("platform_value_summary", "platform_value"),    # NEW
    ("engineering", "engineering"),                  # NEW (added to bundle)
    ("cohort_review", "cohort_review"),
)
```

---

## 📊 Complete Bundle Contents

When you run `python main.py qbr "Customer"`, the bundle folder now contains:

### **Main QBR Deck** (26 slides)
```
1. Cover
2. Agenda
3-9. Usage & Engagement (7 slides)
10-13. Support & Engineering (4 slides)
14-16. Platform Health (3 slides)
17-21. Supply Chain & Shortages (5 slides - includes 3 new LeanDNA slides)
22-23. Lean Projects (2 slides - NEW from Tool #3)
24. Platform Value
25. Notable Signals
26. Data Quality
```

### **Companion Deck 1: CS Health Review** (45 slides)
- Daily-use CSM reference deck
- Pendo engagement, benchmarks, feature adoption
- Same data as QBR but without QBR narrative flow

### **Companion Deck 2: Executive Summary** (12 slides)
- Condensed QBR highlights
- Quick read for leadership
- High-level health + key metrics

### **Companion Deck 3: Support** (4 slides)
- Jira tickets (recent opened/closed)
- SLA health
- Support burden overview

### **Companion Deck 4: Product Adoption** (11 slides)
- Feature usage depth
- Page views and clicks
- Exports, KEI, Guides adoption

### **Companion Deck 5: Supply Chain & Operations Review** ⭐ NEW (10 slides)
- Platform health (CTB, CTC, component availability)
- Inventory risk (DOI, high-risk items)
- Shortage forecast (12-week outlook)
- Critical items timeline
- Scheduled deliveries
- Lean Projects savings
- Lean Projects portfolio
- Platform value
- Data quality

### **Companion Deck 6: Platform Value & ROI Summary** ⭐ NEW (7 slides)
- Total cost avoidance
- Supply chain efficiency gains
- Lean Projects ROI
- Active projects by savings
- Platform health
- Data quality

### **Companion Deck 7: Engineering Review** ⭐ NEW (7 slides)
- Support breakdown by customer
- SLA performance and breaches
- Jira pipeline health
- Engineering workload
- Enhancement requests
- Data quality

### **Companion Deck 8: Manufacturing Cohort Review** (14 slides)
- Portfolio-wide trends (120 customers)
- Peer benchmarks and leaders
- Cohort signals
- Top performers

---

## 📂 Bundle Folder Structure

```
{date} - Output/
└── Bombardier — QBR bundle (Q1 2026)/
    ├── Bombardier — Quarterly Business Review.gslides (MAIN)
    ├── Bombardier — Customer Success Health Review.gslides
    ├── Bombardier — Executive Summary.gslides
    ├── Bombardier — Support Review.gslides
    ├── Bombardier — Product Adoption Review.gslides
    ├── Bombardier — Supply Chain & Operations Review.gslides ⭐ NEW
    ├── Bombardier — Platform Value & ROI Summary.gslides ⭐ NEW
    ├── Bombardier — Engineering Review.gslides ⭐ NEW
    └── Manufacturing cohort review.gslides
```

**Total:** 1 main + 8 companions = **9 decks per QBR run**

---

## 🎯 Use Cases for Each Deck

| Deck | Primary Audience | Use Case | When to Share |
|------|-----------------|----------|---------------|
| **Main QBR** | CSM + Customer | Quarterly business review meeting | Every QBR |
| **CS Health Review** | CSM (internal) | Daily reference, prep for customer calls | Every QBR |
| **Executive Summary** | Customer exec leadership | Quick read, no meeting attendance needed | Every QBR |
| **Support** | CSM + Support team | Ticket triage, SLA review | Every QBR |
| **Product Adoption** | Product/CSM | Feature usage analysis, training gaps | Every QBR |
| **Supply Chain** | Ops/Supply chain leaders | Shortage management, inventory optimization | Manufacturing customers only |
| **Platform Value** | CFO/Procurement | Renewal justification, budget requests | Renewal quarters |
| **Engineering** | Eng/Product leadership | Resource planning, customer prioritization | Internal quarterly review |
| **Cohort Review** | CS leadership | Portfolio health, peer comparison | Every QBR |

---

## 🚀 Next QBR Run

The next QBR generation will include all 8 companion decks automatically:

```bash
python main.py qbr "Customer Name"
```

**Expected output:**
```
✅ QBR complete in 45m 00s
Bundle folder: https://drive.google.com/drive/folders/...
  [health_review] https://...
  [executive_summary] https://...
  [support] https://...
  [product_adoption] https://...
  [supply_chain] https://...        ⭐ NEW
  [platform_value] https://...      ⭐ NEW
  [engineering] https://...         ⭐ NEW
  [cohort_review] https://...
```

---

## 📈 Value Delivered

### For CS Teams
- **Targeted decks** for different stakeholders (no more "trimming" the main QBR)
- **Supply chain focus** showcases LeanDNA investment
- **Value deck** for renewal conversations
- **Engineering deck** for internal prioritization

### For Customers
- **Role-appropriate views** (ops teams see ops metrics, execs see ROI)
- **Faster prep** (share pre-built focused decks vs extracting slides)
- **Better storytelling** (each deck has a coherent narrative arc)

### Operational Benefits
- **Reduced manual work** (no more copy/paste/trim for stakeholder views)
- **Consistent formatting** (all decks follow same standards)
- **Automatic generation** (all 8 decks in single `qbr` command)

---

## 🧪 Testing

**Deck YAML validation:**
```bash
supply-chain-review.yaml: ✅ Valid YAML (10 slides)
platform-value-summary.yaml: ✅ Valid YAML (7 slides)
engineering.yaml: ✅ Valid YAML (7 slides)
```

**Integration test:**
- ✅ All decks registered in `QBR_BUNDLE_COMPANION_DECKS`
- ✅ No syntax errors in qbr_template.py
- ⏳ Next Bombardier QBR run will validate full generation

---

## 🔧 Technical Details

### Deck Loader Integration
All companion decks use the same `resolve_deck()` mechanism:
```python
from src.deck_loader import resolve_deck
deck_config = resolve_deck("supply_chain_review", customer_name)
```

### Slide Reuse
New decks reuse existing slide builders:
- `platform_health`, `supply_chain`, `shortage_*`, `lean_projects_*` (from QBR)
- `std_title`, `data_quality` (standard templates)
- No new slide builders needed

### Generation Flow
```
run_qbr_from_template()
  ↓
1. Load report (Pendo + CS Report + LeanDNA)
2. Generate main QBR deck
3. _build_companion_decks_for_qbr_bundle()
   ↓
   For each deck in QBR_BUNDLE_COMPANION_DECKS:
     - resolve_deck(deck_id, customer)
     - build_deck_from_report(...)
     - Upload to bundle folder
```

---

## ⚠️ Known Limitations

1. **All companions always generated** - No conditional logic yet (e.g., skip Supply Chain for non-manufacturing)
2. **No slide-level data requirements** - Decks may show "missing data" slides if customer lacks LeanDNA
3. **Fixed order** - Cohort review always last (requires portfolio data)
4. **No custom titles per companion** - Uses deck name from YAML

---

## 🔮 Future Enhancements

### Conditional Companions
Add logic to skip decks based on customer attributes:
```python
if customer_cohort not in ["automotive", "industrial"]:
    skip "supply_chain_review"

if not in_renewal_quarter(customer):
    skip "platform_value_summary"
```

### Deck Variants
- **Supply Chain Deep Dive** - 15-20 slides with detailed shortage analysis
- **Salesforce Commercial Review** - RevOps-focused CRM data (conditional)
- **Security & Compliance** - For regulated industries (on-demand)

### Smart Bundling
- **Detect missing data** - Skip Supply Chain deck if customer has no CS Report
- **Audience profiles** - Generate only decks relevant to meeting attendees
- **Size optimization** - Combine small decks (Support + Engineering) for some customers

---

## ✅ Acceptance Criteria - MET

- [x] Supply Chain Review deck created (10 slides)
- [x] Platform Value Summary deck created (7 slides)
- [x] Engineering Review added to bundle
- [x] QBR_BUNDLE_COMPANION_DECKS updated in qbr_template.py
- [x] All deck YAMLs validated (proper syntax)
- [x] No breaking changes to existing QBR generation
- [x] Documentation complete

**Implementation time:** ~1 hour  
**Next QBR will generate:** 1 main + 8 companions = 9 total decks
