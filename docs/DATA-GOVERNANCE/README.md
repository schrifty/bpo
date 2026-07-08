# Data governance

This folder holds Cortex’s **data registry**, **dictionary**, **per-source schemas**, and related **governance / analysis** docs (gaps, priorities, cohorts, integration notes).

Start with **[`DATA_REGISTRY.md`](./DATA_REGISTRY.md)** for the cross-system identifier list; use the other files here for detailed schemas and supporting notes.

| Document | Description |
|----------|-------------|
| [`DATA_REGISTRY.md`](./DATA_REGISTRY.md) | Master registry: stable IDs, sources, where Cortex uses each item, status flags |
| [`DATA_DICTIONARY.md`](./DATA_DICTIONARY.md) | Human-readable mirror of the comprehensive data-element catalog (by source path) |
| [`JIRA_DATA_SCHEMA.md`](./JIRA_DATA_SCHEMA.md) | Jira / JSM fields, APIs, and custom fields |
| [`PENDO_DATA_SCHEMA.md`](./PENDO_DATA_SCHEMA.md) | Pendo aggregation sources, metadata, and events |
| [`CSR_DATA_SCHEMA.md`](./CSR_DATA_SCHEMA.md) | Customer Success Report (Drive XLSX) columns and KPI JSON |
| [`LEANDNA_DATA_API_SCHEMA.md`](./LEANDNA_DATA_API_SCHEMA.md) | LeanDNA Data API (REST): Item Master, shortages, Lean Projects, metrics, unused surfaces |
| [`LEANDNA_DATA_API_TOOLS.md`](./LEANDNA_DATA_API_TOOLS.md) | LeanDNA API integration opportunities and tooling notes |
| [`SALESFORCE_DATA_SCHEMA.md`](./SALESFORCE_DATA_SCHEMA.md) | Salesforce REST query surfaces, SOQL, Account, Opportunity, Contract list, comprehensive categories |
| [`SALESFORCE_REVENUE_AND_ARR.md`](./SALESFORCE_REVENUE_AND_ARR.md) | ARR/MRR storage patterns, CPQ `SBQQ__`, Orders, Revenue Cloud, multi-currency |
| [`USAGE_DATA_PRIORITIES.md`](./USAGE_DATA_PRIORITIES.md) | Usage data priorities and gaps |
| [`CUSTOMER_COHORTS.md`](./CUSTOMER_COHORTS.md) | Manufacturing cohort classifications (`config/cohorts.yaml`) |
| [`CONFIG_ALIASES.md`](./CONFIG_ALIASES.md) | Customer alias maps under `config/` |
| [`SLIDE_DATA_GAP_ANALYSIS.md`](./SLIDE_DATA_GAP_ANALYSIS.md) | Slide ↔ data coverage methodology / findings |

**Operational setup** (credentials, env vars) lives under [`../SETUP/`](../SETUP/). **Slide design & Pendo builders:** [`../PRESENTATION/`](../PRESENTATION/). **Export user guide:** [`../Cortex Export - User Guide.md`](../Cortex%20Export%20-%20User%20Guide.md). Other deck/product docs remain at [`../`](../).
