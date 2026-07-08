# Business Unit Mapping — Customer Success Review

**Audience:** Customer Success (account owners for the customers below)
**Owner of this doc:** Cortex / BPO analytics
**Config it drives:** `config/pendo_site_bu_map.yaml`
**Please return by:** _[set a date]_

---

## Why we're asking

Our large customers run their Pendo usage across many "sites" (individual
plants/factories). To let anyone ask an LLM *"how is Safran's Cabin & Seats
division doing?"* — instead of eyeballing 100+ site names — Cortex now rolls each
active site up to a **business unit** in the customer usage export (sections §2.1,
§13.1, §14).

We can map most sites automatically **when the site name contains its division**
(e.g. `Safran Electronics and Defense – Auxerre`). The rest we currently map by a
**location or brand guess**, or leave **unmapped**. Those guesses are the ones we
need you to confirm.

**What we need from you:** for each site in the "Please confirm" tables below, fill
in the **Correct business unit** column (or tick "guess is right"). If a whole
company's division taxonomy is easier to give us as a list, that's even better —
see the per-company asks.

**Confidence legend used in the export**

| Tag | Meaning | Needs your input? |
|---|---|---|
| `high` | Site name self-labels its division. We trust it. | No |
| `inferred` | Mapped by a location / brand / site-code guess. | **Yes — confirm** |
| `unmapped` | No rule matched; shows as "Unmapped — needs review". | **Yes — assign** |

> These sites are also flagged automatically every time the export runs (log
> warning + a "Confidence" note under §2.1), so drift shows up on its own.

---

## 1. Safran — mostly resolved via the CS Report

Safran's CS Report is delivered **split by division** (`Safran Cabin and Seats`,
`Safran Electrical and Power`, `Safran Aerosystems`, `Safran Electronics and
Defense`, `Safran Cabin Water and Waste`, `Safran SA`). We cross-referenced each
Pendo site against the CS Report factory list, which let us confirm most divisions
authoritatively (and it corrected two of our earlier guesses — `AMX` → Electrical &
Power, `Astronautics` → Cabin & Seats).

**a) Are these the right business units?**

- Electronics & Defense
- Electrical & Power
- Aerosystems
- Cabin & Seats  *(includes Cabin Water & Waste — confirm whether that should be its own unit)*
- Other / Corporate

**b) Please confirm the few we still can't source from the CS Report**

These Pendo sites don't cleanly match a CS Report factory and aren't
division-labeled in the site name:

| Site name pattern | Our guess (business unit) | Why we're unsure | Correct business unit? |
|---|---|---|---|
| `…Power Units…` (APU) | Electrical & Power | Auxiliary Power Units may sit under a different division; not seen in the CS Report | |
| `…Ventilation…` | Cabin & Seats | Could be Aerosystems | |
| `…Soliman…` | Electrical & Power | Appears under **both** Electrical & Power and Electronics & Defense in the CS Report — which owns the Pendo site? | |
| `…ICA…`, `…Slough…`, `…Loches…`, `…Issoudun…` | Cabin & Seats | Location-only, no CS Report match this run | |

> Everything else (Cabin, Seats, Electrical and Power, and the Garden Grove /
> Marysville / Montreal / Santa Maria / Chihuahua / Herborn / Newport plants) is
> either self-labeled or CS-Report-confirmed as **Cabin & Seats** — no action needed
> unless that looks wrong.

---

> **Note on the CS Report:** unlike Safran, the three companies below appear as a
> *single* customer in the CS Report (no division breakout), so we cannot source
> their business units automatically. We do have their real factory/site inventory,
> reflected below.

## 2. Carrier — PROVISIONAL, needs your taxonomy

The CS Report lists Carrier as one customer (~93 factories) with **no division
label**. Factory/entity names do carry brand tokens (e.g. `Athens, Transicold`,
`Aubagne, Profroid`, `Autronica`, `ALC`), so we seeded provisional brand buckets;
everything unmatched falls to "needs review".

**Provisional buckets (all `inferred`):**

- **Fire & Security** — Kidde, Marioff, Fireye, Det-Tronics, Autronica, Edwards, LenelS2
- **Refrigeration** — Transicold, Profroid, Sensitech
- **HVAC** — Automated Logic (ALC), Riello, Toshiba, Viessmann, Residential, Commercial

**What we need:**

1. The **official business-unit / segment list** you use for Carrier.
2. Which segment each of the ~93 plants belongs to (or a brand → segment key).

---

## 3. Spirit AeroSystems — confirm the rollup dimension

The CS Report lists Spirit as one customer (~13 factories), organized by
**location**: Belfast, Kinston, Morocco, Prestwick, Malaysia — plus a unit named
**Spirit Defense**. This confirms Spirit is essentially one aerostructures business
split by site.

**Provisional buckets (all `inferred`, by location):** Wichita · Tulsa / McAlester ·
Belfast / Prestwick · Kinston · Morocco · Malaysia · **Spirit Defense**

**What we need:**

1. Do you want this rolled up by **site/location** (what we have), by **program**
   (737, 787, A320, A350), or by a formal **business unit**?
2. Is **Spirit Defense** a separate unit we should always break out?

---

## 4. Bombardier Aviation — confirm the rollup dimension

The CS Report lists Bombardier as one customer (~28 factories) using site codes:
`Dorval`, `CoE`, `LBCC`, `Queretaro`. This confirms a site/program organization.

**Provisional buckets (all `inferred`, by location):** Montreal (Dorval / Mirabel /
St-Laurent) · Toronto (Downsview) · Queretaro · Red Oak · Wichita

**What we need:**

1. Preferred rollup: **site**, **aircraft program** (Global, Challenger, Learjet
   service), or **business unit**?
2. What do the site codes **CoE** and **LBCC** map to?

---

## How to return this

Reply inline in the tables (fill "Correct business unit"), or send us a simple list
of **division → site/brand keywords** per company. We translate it into
`config/pendo_site_bu_map.yaml` and re-run — confirmed rows become `high`
confidence and drop off the review list.

**Maintenance cadence:** we suggest a quick re-review each quarter, or whenever a
customer reorganizes / opens a plant. New or renamed sites automatically appear as
"needs review" in the export until mapped.
