# Customer Manufacturing Cohorts

Classification of LeanDNA customer accounts into manufacturing cohorts for benchmarking. The canonical source is `cohorts.yaml` at the project root; this document provides the research context and rationale.

Last updated: March 2026

---

## Data Sources

Customer names are derived from Pendo site-name prefixes (first word of each sitename). Site names themselves were the primary research input — they reveal divisions, locations, product lines, and ERP configurations that disambiguate otherwise generic company names.

Additional research was performed via web search for companies not immediately identifiable from site names alone (e.g., BJG, Harcosemco, BAYARD, Tri-State).

---

## Cohort Definitions

| Cohort ID | Display Name | Description | Count |
|-----------|-------------|-------------|-------|
| `aerospace_defense` | Aerospace & Defense | Airframe, aerostructures, engines, avionics, defense electronics, MRO | ~32 |
| `hvac_building` | HVAC & Building Systems | HVAC, refrigeration, fire/safety, building controls, lighting, plumbing | ~18 |
| `vehicles` | Automotive & Vehicles | Automotive OEM/Tier-1, specialty/commercial vehicles, heavy equipment | ~14 |
| `medical_devices` | Medical Devices | Medical device OEMs and contract manufacturers | 5 |
| `industrial_equipment` | Industrial Equipment | Pumps, valves, automation, material handling, process equipment, thermal | ~20 |
| `electronics` | Electronics & Electrical | Connectors, cable, semiconductors, photonics, test/analytical equipment | ~9 |
| `advanced_materials` | Advanced Materials | Specialty materials, ceramics, thermal, insulation, energy storage | 3 |
| `furniture` | Furniture & Office | Office/commercial furniture manufacturing | 2 |
| `consumer_products` | Consumer Products | Consumer-facing manufactured goods | 1 |

---

## Cohort Members

### Aerospace & Defense (~32 accounts)

The largest cohort, reflecting LeanDNA's strong position in aerospace supply chain.

**Aerostructures & Systems**

| Customer | Full Name | Size | Sites | Notes |
|----------|-----------|------|-------|-------|
| Safran | Safran SA | Enterprise | 381 | Cabin, Seats, Electrical & Power, Electronics & Defense, Aerosystems |
| Spirit | Spirit AeroSystems | Enterprise | 13 | Fuselages, nacelles. Wichita, Belfast, Morocco, Malaysia |
| Bombardier | Bombardier Aviation | Enterprise | 29 | Business jets. Dorval, Querétaro, Red Oak, Toronto |
| Kaman | Kaman Aerospace | Mid | 16 | Structures, bearings, precision products |
| Daher | Daher | Mid | 8 | Aerostructures, logistics. France, Morocco, US, Mexico |
| Qarbon | Qarbon Aerospace | Mid | 3 | Composites, metallic aerostructures (ex-Triumph). Red Oak, Thailand |
| LMI | LMI Aerospace (Sonaca) | Mid | 13 | Structures, assemblies. Acquired by Sonaca |
| Radius | Radius Aerospace | SMB | 8 | Sheet metal, machined parts. Fort Worth, Hot Springs, Phoenix, UK |
| Tighitco | TIGHITCO Inc | SMB | 7 | Composites, thermal insulation blankets. CT, SC, Mexico |
| GKN | GKN Aerospace | SMB | 1 | Aerostructures, engine components. El Cajon |
| NORDAM | NORDAM Group | SMB | 7 | Nacelles, thrust reversers, MRO. Tulsa |
| Triumph | Triumph Group | SMB | 7 | Structures, systems. Mexicali, Spokane |
| Signia | Signia Aerospace | SMB | 3 | Components. Montgomery, Piqua. Includes Onboard Systems |
| Enjet | Enjet Aero | SMB | 10 | Precision machining. Multiple US + Thailand |
| Allfast | Allfast Fastening Systems | SMB | 1 | Aerospace fasteners and tooling |
| Monogram | Monogram Aerospace Fasteners | SMB | 1 | Fasteners (TransDigm) |
| Kirkhill | Kirkhill Inc | SMB | 2 | Elastomers, seals, fuel cells. Brea, Tijuana |
| Jamco | Jamco America | SMB | 6 | Aircraft interiors — lavatories, galleys, seats |
| EZ | EZ Air Interior | Mid | 6 | Aircraft interior parts, finance |
| Erickson | Erickson Inc | SMB | 2 | Helicopters, aerial services |

**Avionics & Electronics**

| Customer | Full Name | Size | Notes |
|----------|-----------|------|-------|
| Harcosemco | HarcoSemco | SMB | Sensors, thermocouples, cable harnesses. TransDigm subsidiary |
| Thales | Thales Avionics | SMB | In-flight entertainment. Irvine, Orlando |
| Korry | Korry Electronics | Mid | Cockpit displays/switches. L3Harris/Esterline |
| CMC | CMC Electronics | SMB | Avionics, cockpit systems. Montreal |
| BJG | BJG Electronics | SMB | High-rel electronic components for aero/defense |
| Auxitrol | Auxitrol (Safran) | SMB | Aerospace sensors. Safran subsidiary |

**Defense Electronics**

| Customer | Full Name | Size | Notes |
|----------|-----------|------|-------|
| CAES | CAES (Cobham) | Mid | Defense electronics, RF/microwave |
| Curtiss | Curtiss-Wright | Mid | Defense electronics, actuation, sensors |
| Mercury | Mercury Systems | SMB | Defense processing subsystems |
| ITT | ITT Aerospace Controls | SMB | Connectors, controls, ECS |
| VACCO | VACCO Industries | Mid | Valves, filters for aerospace/defense |
| Special | Special Products & Mfg | SMB | Defense/aerospace specialty |

---

### HVAC & Building Systems (~18 accounts)

Anchored by the Carrier/JCI ecosystem plus other building-infrastructure manufacturers.

| Customer | Full Name | Size | Sub-cohort | Notes |
|----------|-----------|------|------------|-------|
| Carrier | Carrier Global | Enterprise | HVAC/Refrigeration | 102 sites globally. Also includes fire/security brands |
| JCI / Johnson | Johnson Controls | Enterprise | HVAC/Refrigeration | Building automation, HVAC, fire & security. ~66 sites combined |
| Daikin | Daikin Industries | Mid | HVAC/Refrigeration | San Luis Potosí, Staunton, Tijuana |
| Hussmann | Hussmann Corp | Mid | HVAC/Refrigeration | Commercial refrigeration (Panasonic). Chino, Monterrey |
| Modine | Modine Manufacturing | Enterprise | HVAC/Refrigeration | Thermal management. US, Brazil, Hungary, Netherlands, Mexico |
| MSA | MSA Safety | Enterprise | Fire/Safety | Gas detection, fall protection, firefighter gear. 59 global sites |
| Kidde | Kidde Global Solutions | Mid | Fire/Safety | Fire detection/suppression. Carrier subsidiary |
| Spectrum | Spectrum Safety | SMB | Fire/Safety | Autronica, Det-Tronics, Fireye, Marioff. Carrier subsidiary |
| Enviro | Enviro Systems | Mid | HVAC/Refrigeration | Environmental control systems |
| Current | Current Lighting | Mid | Building Products | Commercial/industrial lighting. Ex-GE Current. 24 sites |
| Chamberlain | Chamberlain Group | Mid | Building Products | Garage door openers, access solutions |
| Sloan | Sloan Valve | SMB | Building Products | Commercial plumbing/water management |
| HySecurity | HySecurity | SMB | Building Products | Gate operators, security |
| Turbochef | TurboChef Technologies | Mid | Building Products | High-speed commercial cooking |
| Refrigeration | Refrigeration Solutions | SMB | HVAC/Refrigeration | Aubagne France |

---

### Automotive & Vehicles (~14 accounts)

Mix of automotive OEMs/Tier-1 suppliers and specialty vehicle manufacturers.

**Automotive OEM / Tier-1**

| Customer | Full Name | Size | Notes |
|----------|-----------|------|-------|
| Ford | Ford Motor Company | SMB | Powertrain — Essex, Rawsonville, Sterling Axle, Van Dyke, Irapuato |
| Bosch | Robert Bosch | SMB | Mexico sites (Querétaro, Tizayuca, Toluca) |
| CAT | Caterpillar (Reman) | SMB | Remanufacturing. Mississippi |
| Gentex | Gentex Corporation | SMB | Auto-dimming mirrors. Carbondale |
| Control | Control Devices | SMB | Automotive sensors, switches |
| Midtronics | Midtronics | SMB | Battery testing for automotive |
| Fujikura | Fujikura Automotive | SMB | Wiring harnesses. Mexico |
| Tata | Tata AutoComp / TitanX | SMB | Automotive thermal. Saltillo Mexico |

**Specialty & Commercial Vehicles**

| Customer | Full Name | Size | Notes |
|----------|-----------|------|-------|
| REV | REV Group | Mid | Fire trucks, ambulances, buses, RVs. 22+ sites. Includes E-One |
| EnTrans | EnTrans International | Mid | Tank trailers. Athens, Juárez, Gatesville, Holdingford |
| CVG | Commercial Vehicle Group | SMB | Cab systems, trim, seating |
| Viking | Viking-Cives | SMB | Snow plows, truck equipment |
| Nelson | Nelson Global Products | SMB | Exhaust/emissions for commercial vehicles |
| USSC | USSC Group | SMB | Transportation seating. Exton PA, Sweden |

---

### Medical Devices (5 accounts)

| Customer | Full Name | Size | Notes |
|----------|-----------|------|-------|
| Terumo | Terumo Corporation | Enterprise | Cardiovascular, blood management. 31 global sites |
| MicroVention | MicroVention (Terumo) | SMB | Neurovascular devices. California, Costa Rica |
| Cirtec | Cirtec Medical | Mid | Contract manufacturing — implantable, interventional. 13 sites |
| Baxter | Baxter International | SMB | Medical devices, renal care |
| Artiflex | Artiflex Manufacturing | SMB | Contract manufacturing (medical focus) |

---

### Industrial Equipment (~20 accounts)

Broad category covering pumps, valves, automation, material handling, and process equipment.

| Customer | Full Name | Size | Sub-cohort | Notes |
|----------|-----------|------|------------|-------|
| Dover | Dover Corporation | Enterprise | Pumps/Flow | PSG, DFS, OPW, Belvac, Destaco. 46 sites |
| Flowserve | Flowserve Corporation | Mid | Pumps/Flow | Pumps, valves, seals. 37 global sites |
| Ingersoll | Ingersoll Rand | Mid | Pumps/Flow | Compressors, tools, fluid management |
| Milton | Milton Roy (IR) | Mid | Pumps/Flow | Metering pumps, mixing |
| Hydraforce | HydraForce Inc | Mid | Pumps/Flow | Hydraulic valves/manifolds. IL, UK, China |
| IDEX | IDEX Corporation | SMB | Pumps/Flow | Diversified industrial |
| Astec | Astec Industries | Mid | Heavy Machinery | Road construction, aggregate equipment |
| Columbus | Columbus McKinnon | SMB | Heavy Machinery | Hoists, rigging, motion control |
| Husky | Husky Injection Molding | Mid | Heavy Machinery | Injection molding systems |
| AGI | Ag Growth International | SMB | Heavy Machinery | Agricultural grain handling |
| CPM | CPM Holdings | SMB | Process Equipment | Food/animal feed processing |
| Duravant | Duravant LLC | SMB | Process Equipment | Food processing & packaging |
| Integrated | Integrated Packaging | SMB | Process Equipment | Packaging equipment |
| Convergix | Convergix Automation | Enterprise | Automation | Industrial automation, robotics |
| JR | JR Automation | Enterprise | Automation | Robotics (Hitachi subsidiary) |
| Destaco | DESTACO (Dover) | SMB | Automation | Clamping, gripping, transferring |
| OEM | OEM Controls | SMB | Automation | Industrial controls |
| Watlow | Watlow Electric | Mid | Thermal | Industrial heaters, temperature controllers |
| Jonathan | Jonathan Engineered Solutions | SMB | Hardware | Fasteners, hardware |
| Leco | LECO Corporation | SMB | Instruments | Analytical instruments, metallography |
| Nuvera | Nuvera Fuel Cells | SMB | Energy | Fuel cell engines for material handling |
| BAYARD | Bayards Aluminium | SMB | — | Custom aluminum structures |
| Tri-State | Tri-State Industries | SMB | — | Oil/gas fabrication |
| UCC | UCC Environmental | SMB | — | Environmental systems |

---

### Electronics & Electrical (~9 accounts)

| Customer | Full Name | Size | Sub-cohort | Notes |
|----------|-----------|------|------------|-------|
| Viakable | Viakable (Xignux) | Enterprise | Wire/Cable | 52 sites across Mexico |
| Volex | Volex plc | SMB | Wire/Cable | Power cords, cable assemblies |
| Interconnect | Interconnect Solutions | SMB | Connectors | Connectors, cable assemblies. 5 sites |
| Chatsworth | Chatsworth Products | SMB | Data Infrastructure | Data center cabinets. 5 sites |
| Excelitas | Excelitas Technologies | SMB | Photonics | Optoelectronics. 5 sites |
| Veeco | Veeco Instruments | Mid | Semiconductor Equip | Process equipment. San Jose, Somerset |
| Siemens | Siemens | SMB | Industrial Electronics | Querétaro, Wendell |
| Bluecrest | BlueCrest | SMB | Mail Equipment | Mail processing. Danbury |
| GE | GE Appliances (Haier) | SMB | Appliances | Louisville-area plants |

---

### Advanced Materials & Energy (3 accounts)

| Customer | Full Name | Size | Notes |
|----------|-----------|------|-------|
| Morgan | Morgan Advanced Materials | Mid | Ceramics, carbon, thermal management. 37 global sites |
| Aspen | Aspen Aerogels | Mid | Aerogel insulation. Statesboro, Monterrey |
| EnerSys | EnerSys | SMB | Industrial batteries, energy storage |

---

### Furniture & Office (2 accounts)

| Customer | Full Name | Size | Notes |
|----------|-----------|------|-------|
| HNI | HNI Corporation | Mid | Allsteel, HON, Kimball. 23 distribution sites |
| Knoll | Knoll (MillerKnoll) | SMB | Grand Rapids, Muskegon, Toronto |

---

### Consumer Products (1 account)

| Customer | Full Name | Notes |
|----------|-----------|-------|
| Fender | Fender Musical Instruments | Guitars, amplifiers. Corona CA |

---

## Excluded Accounts (~20)

These accounts are excluded from benchmarking as they represent test environments, internal tools, duplicates, or ambiguous entries.

| Prefix | Reason |
|--------|--------|
| Support | Internal LeanDNA support/test |
| Training | Training site |
| LeanProjectRecovery | Internal recovery |
| ISC | Training site |
| CI | Strategic projects — internal |
| USD | Currency conversion placeholder |
| Tijuana | Orphan site name |
| Jacksonville | Orphan site name |
| Buena | Orphan site name (Buena Park) |
| Primary | Upload factory — internal |
| SBO | Demo account |
| Stuart | Orphan — likely Daher migration artifact |
| Leach | Training site |
| TSA | Ambiguous single-site |
| AWB | Unknown single-site |
| AIP | Industrial distribution, not manufacturing |
| AST | Unclear (McCay, PMSI) |
| L2 | Likely software company or internal test |
| Beroun | Carrier Czech Republic site listed separately |
| Blythewood | Orphan — likely Stanadyne |
| CVI | Part of IDEX, single site |

---

## Aliases

Several companies appear under multiple Pendo prefixes due to typos, currency-conversion sites, or subsidiary structures:

| Alias | Canonical | Reason |
|-------|-----------|--------|
| Carrrier | Carrier | Typo |
| Johnson | JCI | Same company (Johnson Controls) |
| Daiking | Daikin | Currency conversion site |
| Hussman | Hussmann | Typo |
| Entrans | EnTrans | Alternate capitalization |
| Enersys | EnerSys | Alternate capitalization |
| Rev | REV | Alternate capitalization |
| Microvention | MicroVention | Alternate capitalization |
| SIemens | Siemens | Typo |
| Viakablemex | Viakable | Subsidiary |
| E-One | REV | REV Group brand |
| TitanX | Tata | Tata AutoComp brand |

---

## How Benchmarking Uses Cohorts

1. When generating a report for a customer, the system looks up their cohort from `cohorts.yaml`
2. It computes **cohort-specific median** active rates using only companies in the same cohort (minimum 3 members required)
3. Slides show the cohort median as the primary benchmark, with the all-customer median as secondary context
4. Excluded accounts are filtered out of all benchmark calculations
5. Signals (strong/low engagement) are evaluated against the cohort median rather than the global median

---

## Maintenance

- **Adding a new customer**: Add an entry to `cohorts.yaml` under the appropriate cohort
- **Reclassifying**: Change the `cohort` field in `cohorts.yaml`
- **New cohorts**: Add the cohort ID to `_COHORT_DISPLAY` in `src/pendo_client.py` for a clean display name
- The YAML is loaded once per process and cached; restart to pick up changes
