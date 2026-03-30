# Salesforce Data Schema

BPO uses the Salesforce **REST API** with **JWT Bearer** auth. Implementation: [`src/salesforce_client.py`](../../src/salesforce_client.py). Setup: [`../SALESFORCE_SETUP.md`](../SALESFORCE_SETUP.md). Registry: [`DATA_REGISTRY.md`](./DATA_REGISTRY.md) (Salesforce section).

## 1. HTTP

| Step | Method | URL |
|------|--------|-----|
| Token | POST | `{SF_LOGIN_URL}/services/oauth2/token` |
| Query | GET | `{instance}/services/data/{SF_REST_API_VERSION}/query` |
| Catalog | GET | `{instance}/services/data/{SF_REST_API_VERSION}/sobjects/` |

Version: `SF_REST_API_VERSION` in code (e.g. `v59.0`). Not used here: Bulk/Composite APIs. **`list_sobject_types()`** returns the global object list.

## 2. Deck pipeline (entity Account + Opportunity metrics)

**Account** — `Type = 'Customer Entity'` — custom fields used for customer match and contract context:

| Field | Role |
|-------|------|
| `Id` | PK; drives Opportunity filters |
| `Name`, `LeanDNA_Entity_Name__c` | Match to deck customer (substring, case-insensitive) |
| `US_Persons_Only_Customer__c` | Loaded; not on slides |
| `Contract_Status__c`, `Contract_Contract_Start_Date__c`, `Contract_Contract_End_Date__c`, `ARR__c` | Contract / ARR |

**Opportunity** — aggregates only (via `get_opportunity_creation_this_year`, `get_advanced_pipeline_arr`): filter by `Type` (New Business, New Expansion Business, Expansion Business, POC), `AccountId` in matched accounts, current **calendar year** on `CreatedDate` for counts; same types + `StageName` in (3-Business Validation, 4-Proposal, 5-Contracts) for **SUM(ARR__c)**.

**Comprehensive deck** (`get_customer_salesforce_comprehensive`) — after matching Customer Entity account(s), BPO walks the standard Account hierarchy: every account with `ParentId` pointing at an Id already in the set is added (breadth-first), up to depth 25 and 2000 Ids total, then all category SOQL uses that expanded Id list. Partner relationships and other non-`ParentId` links are not followed.

## 3. `get_customer_salesforce` output

| Key | Meaning |
|-----|---------|
| `customer` | Input name |
| `accounts` | Matched entity Account rows |
| `account_ids` | `Id` list |
| `opportunity_count_this_year` | Count query result |
| `pipeline_arr` | SUM query result |
| `matched` | Any account matched |

## 4. Mainstream helpers

`MAINSTREAM_OBJECT_FIELDS` plus **`query_leads`**, **`query_contacts`**, **`query_opportunities`**, … **`query_mainstream_object`**, **`query_soql`**, **`list_sobject_types`**. Not wired to decks by default. Override **`fields=`** if your org renames or omits a column.

## 5. Operations

Read access required on queried fields; org rate limits apply; no query retries in client. Hidden fields → SOQL errors or empty values.

## 6. Gaps

Per-object Describe not wrapped (use Workbench or `…/sobjects/{Name}/describe`). Custom `__c` objects: pass **`fields=`** or raw **`query_soql`**. Registry “missing” commercial fields stay aspirational until modeled.

## 7. Mainstream objects — default columns

Column meanings:

- **Type** — Salesforce field datatype as returned by the API.
- **Length / precision** — For text-like fields: **maximum length in characters**. For **Currency**: **precision 18, scale 2** means up to 18 digits total with **2 digits after the decimal** (standard Salesforce currency storage; amounts display in the org’s or record’s currency when multi-currency is on). For **Percent**: stored as a number where **100 means 100%** on Opportunity. **Reference** fields store an **18-character Salesforce Id**. **Picklist** values are **defined per org** (labels and API names can differ).
- **Description** — What the field represents in typical B2B use.

If anything disagrees with your org (custom fields, validation rules), use **`GET …/sobjects/{Object}/describe`** or the Object Manager UI.

### Lead

Prospect before qualification; **Lead conversion** can create **Account**, **Contact**, and optionally **Opportunity** in one step. After conversion, the lead row is **read-only** and **`IsConverted`** is true.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key for this lead record. |
| `FirstName` | Text | Up to 40 characters | Person’s given name. |
| `LastName` | Text | Up to 80 characters | Person’s family name (required in most orgs). |
| `Company` | Text | Up to 255 characters | Company name **as entered on the lead** (before an Account exists). |
| `Email` | Email | Up to 80 characters | Primary email; used for matching and outreach. |
| `Phone` | Phone | Up to 40 characters | Primary phone number (formatting rules depend on org). |
| `Status` | Picklist | Values defined in org | Where the lead sits in the **lead process** (e.g. Open, Qualified—values are customizable). |
| `LeadSource` | Picklist | Values defined in org | How the lead was acquired (e.g. Web, Partner); used for **attribution** and reporting. |
| `OwnerId` | Reference (User) | 18-character Id | Salesforce user who owns the lead (routing, queues, and list views use this). |
| `CreatedDate` | DateTime | UTC timestamp | When the lead record was created. |
| `LastModifiedDate` | DateTime | UTC timestamp | Last time any field on the row was saved. |
| `IsConverted` | Checkbox | true / false | **true** after **lead conversion**; the working copy of the person/company usually lives on Contact/Account afterward. |

### Account

**Company or site** you sell to—hierarchy anchor for **Contacts**, **Opportunities**, **Cases**, **Orders**, etc.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `Name` | Text | Up to 255 characters | Legal or operating name of the account. |
| `Type` | Picklist | Values defined in org | Segment such as Customer, Partner, Prospect (org-specific). |
| `Industry` | Picklist | Values defined in org | High-level industry classification for reporting. |
| `BillingCity` | Text | Up to 40 characters | Part of the **billing address** (city). |
| `BillingState` | Text | Up to 80 characters | State, province, or region on the billing address. |
| `BillingCountry` | Text | Up to 80 characters | Country on the billing address. |
| `Phone` | Phone | Up to 40 characters | Main company phone. |
| `Website` | URL | Up to 255 characters | Corporate website URL. |
| `OwnerId` | Reference (User) | 18-character Id | Account owner (often aligns with **Account Executive** in sales-led orgs). |
| `CreatedDate` | DateTime | UTC timestamp | When the account was created. |

### Contact

**Person** at a customer or partner; usually **`AccountId`** ties them to exactly one **Account** (B2B). Used for **stakeholder lists**, **case contact**, and **activity** targeting.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `FirstName` | Text | Up to 40 characters | Given name. |
| `LastName` | Text | Up to 80 characters | Family name. |
| `Email` | Email | Up to 80 characters | Work email (often used as a secondary match key in integrations). |
| `Phone` | Phone | Up to 40 characters | Direct or desk phone. |
| `AccountId` | Reference (Account) | 18-character Id | Company this person belongs to; **required** for typical B2B contact usage. |
| `Title` | Text | Up to 128 characters | Job title (e.g. VP Operations). |
| `MailingCity` | Text | Up to 40 characters | Mailing address — city (subset of full mailing address). |
| `MailingState` | Text | Up to 80 characters | Mailing address — state or region. |
| `MailingCountry` | Text | Up to 80 characters | Mailing address — country. |
| `OwnerId` | Reference (User) | 18-character Id | Owning user (may differ from Account owner). |
| `CreatedDate` | DateTime | UTC timestamp | When the contact was created. |

### Opportunity

**Deal in the pipeline**—revenue opportunity tied to an **Account**, with **stage**, **amount**, and **close date**. **OpportunityLineItem** rows are the **line-item** detail (products, quantities, prices).

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `Name` | Text | Up to 120 characters | Deal name (often “Company — Product” or similar). |
| `AccountId` | Reference (Account) | 18-character Id | Customer account this deal belongs to. |
| `StageName` | Picklist | Values defined in org | Current **sales stage** (e.g. Prospecting, Closed Won); drives pipeline and forecasting. |
| `Amount` | Currency | Precision 18, **scale 2** (two decimal places) | Total **deal value** in the opportunity’s currency (header-level; line items may roll up into this depending on org setup). |
| `Probability` | Percent | Stored 0–100 | **Win likelihood**; often **tied to stage** and used for **weighted pipeline** (Amount × Probability). |
| `CloseDate` | Date | Calendar date | **Expected close** date for forecasting and quarter planning. |
| `Type` | Picklist | Values defined in org | Deal category (e.g. New Business, Renewal); BPO’s Salesforce metrics filter on specific **Type** values. |
| `ForecastCategoryName` | Picklist | Values defined in org | Human-readable **forecast bucket** (e.g. Pipeline, Best Case, Closed); aligns with **forecasting** tools and is often **derived from stage** in standard setups. |
| `OwnerId` | Reference (User) | 18-character Id | Sales rep owning the opportunity. |
| `CreatedDate` | DateTime | UTC timestamp | When the opportunity was created. |
| `LastModifiedDate` | DateTime | UTC timestamp | Last modification time. |

### OpportunityLineItem

**Single product or service line** on an opportunity (SKU, quantity, price). **TotalPrice** is typically **Quantity × UnitPrice** before **header-level** discounts; CPQ and custom logic can change that.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `OpportunityId` | Reference (Opportunity) | 18-character Id | Parent opportunity. |
| `Product2Id` | Reference (Product2) | 18-character Id | Product from the **product catalog**. |
| `Quantity` | Double | Floating-point | Number of units (e.g. seats, licenses). |
| `UnitPrice` | Currency | Precision 18, scale 2 | Price **per unit** in the opportunity’s currency. |
| `TotalPrice` | Currency | Precision 18, scale 2 | Extended price for the line (often quantity × unit price). |
| `ServiceDate` | Date | Calendar date | Optional **service or revenue recognition** date for this line (used in some revenue and fulfillment flows). |

### Quote

Formal **price quote** (often CPQ) linked to an **Opportunity** and **Account**; may precede an **Order**. **`GrandTotal`** is the **rolled-up** quote total.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `QuoteNumber` | Auto Number | Format defined in org | Human-readable quote number (generated by Salesforce). |
| `Name` | Text | Up to 255 characters | Quote title or label. |
| `OpportunityId` | Reference (Opportunity) | 18-character Id | Deal this quote supports. |
| `AccountId` | Reference (Account) | 18-character Id | Customer account (often bill-to or sold-to). |
| `Status` | Picklist | Values defined in org | Quote lifecycle (e.g. Draft, Approved, Presented). |
| `ExpirationDate` | Date | Calendar date | Last day the quote terms are offered. |
| `GrandTotal` | Currency | Precision 18, scale 2 | **Total** amount for the quote after line rollups and quote-level adjustments (per org rules). |
| `OwnerId` | Reference (User) | 18-character Id | Owner of the quote record. |
| `CreatedDate` | DateTime | UTC timestamp | When the quote was created. |

### Order

**Customer order** to fulfill (products/services); ties to **Account** and often follows **Quote** in quote-to-cash flows.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `OrderNumber` | Text | Up to 30 characters | Order identifier (may be auto-numbered depending on org). |
| `AccountId` | Reference (Account) | 18-character Id | Customer placing the order. |
| `EffectiveDate` | Date | Calendar date | When the order **becomes effective** for fulfillment or billing (org-specific meaning). |
| `Status` | Picklist | Values defined in org | Order lifecycle (e.g. Draft, Activated). |
| `TotalAmount` | Currency | Precision 18, scale 2 | **Order total** in order currency. |
| `Type` | Picklist | Values defined in org | Order category if your org uses it. |
| `OwnerId` | Reference (User) | 18-character Id | Record owner. |
| `CreatedDate` | DateTime | UTC timestamp | When the order was created. |

### Contract

**Commercial agreement** (subscription, MSA, etc.) with **term** and **status**; often used for **renewal** and entitlement tracking.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `ContractNumber` | Text | Up to 30 characters | Contract identifier (may be auto-numbered). |
| `AccountId` | Reference (Account) | 18-character Id | Customer under contract. |
| `Status` | Picklist | Values defined in org | Contract state (e.g. Draft, Activated). |
| `StartDate` | Date | Calendar date | Agreement start. |
| `EndDate` | Date | Calendar date | Agreement end (often used as **renewal** anchor). |
| `ContractTerm` | Integer | Whole number | **Duration in months** in standard Salesforce (verify in your org’s field help). |
| `OwnerId` | Reference (User) | 18-character Id | Contract owner. |
| `CreatedDate` | DateTime | UTC timestamp | When the contract was created. |

### Case

**Support or service ticket**—tracks **issue**, **priority**, and **resolution**; **`ContactId`** is often **who reported**; **`AccountId`** is the **customer**.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `CaseNumber` | Auto Number | Format defined in org | Human-readable case number. |
| `Subject` | Text | Up to 255 characters | Short summary of the issue. |
| `Status` | Picklist | Values defined in org | Ticket state in your **support process** (e.g. New, Closed). |
| `Priority` | Picklist | Values defined in org | Urgency (e.g. High, Medium). |
| `Origin` | Picklist | Values defined in org | **Channel** where the case was created (Phone, Email, Web, etc.). |
| `AccountId` | Reference (Account) | 18-character Id | Customer account. |
| `ContactId` | Reference (Contact) | 18-character Id | Primary contact on the case (often the reporter). |
| `OwnerId` | Reference (User or Queue) | 18-character Id | Individual user or **queue** owning the case. |
| `CreatedDate` | DateTime | UTC timestamp | When the case was opened. |
| `ClosedDate` | DateTime | UTC timestamp | When the case was closed (if closed). |

### Task

**To-do** or logged call/email—**activities** use **`WhoId`** (“who”) and **`WhatId`** (“what”) to link to people and records.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `Subject` | Text | Up to 255 characters | Short description of the task. |
| `Status` | Picklist | Values defined in org | Task state (e.g. Not Started, Completed). |
| `Priority` | Picklist | Values defined in org | Urgency. |
| `ActivityDate` | Date | Calendar date | **Due date** or the date the activity occurred (task usage varies by org). |
| `WhoId` | Reference (Lead or Contact) | 18-character Id | **Person** side of the activity—points to a **Lead** or **Contact** (not both). |
| `WhatId` | Reference (polymorphic) | 18-character Id | **Related business record**—can be **Account**, **Opportunity**, **Case**, etc., depending on what the task is “about.” |
| `OwnerId` | Reference (User) | 18-character Id | User responsible for the task. |
| `IsClosed` | Checkbox | true / false | **true** when the task is completed. |
| `CreatedDate` | DateTime | UTC timestamp | When the task was created. |

### Event

**Calendar event** (meeting); same **`WhoId`** / **`WhatId`** pattern as Task for linking people and records.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `Subject` | Text | Up to 255 characters | Meeting title. |
| `StartDateTime` | DateTime | UTC timestamp | Meeting start (timezone handling follows Salesforce/user settings). |
| `EndDateTime` | DateTime | UTC timestamp | Meeting end. |
| `Location` | Text | Up to 255 characters | Physical location or meeting link text. |
| `WhoId` | Reference (Lead or Contact) | 18-character Id | Primary person (Lead or Contact). |
| `WhatId` | Reference (polymorphic) | 18-character Id | Related record the meeting is about. |
| `OwnerId` | Reference (User) | 18-character Id | Organizer / owner. |
| `CreatedDate` | DateTime | UTC timestamp | When the event was created. |

### Campaign

**Marketing program** (email, webinar, trade show) with optional **budget** and **date range**; **CampaignMember** links **Leads** and **Contacts** to the campaign.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `Name` | Text | Up to 80 characters | Campaign name. |
| `Status` | Picklist | Values defined in org | Campaign lifecycle (e.g. Planned, In Progress). |
| `Type` | Picklist | Values defined in org | Kind of campaign (e.g. Email, Event). |
| `StartDate` | Date | Calendar date | Campaign start. |
| `EndDate` | Date | Calendar date | Campaign end. |
| `BudgetedCost` | Currency | Precision 18, scale 2 | Planned spend. |
| `ActualCost` | Currency | Precision 18, scale 2 | Recorded spend to date. |
| `OwnerId` | Reference (User) | 18-character Id | Marketing owner. |

### CampaignMember

**Junction** between a **Campaign** and a **Lead** or **Contact**—one row per person in the campaign; typically **either** **`LeadId`** **or** **`ContactId`** is set.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `CampaignId` | Reference (Campaign) | 18-character Id | Campaign. |
| `LeadId` | Reference (Lead) | 18-character Id | Set when the member is a **lead**. |
| `ContactId` | Reference (Contact) | 18-character Id | Set when the member is a **contact**. |
| `Status` | Picklist | Values defined in org | Member outcome (e.g. Sent, Responded); labels vary by org. |
| `CreatedDate` | DateTime | UTC timestamp | When the person was added to the campaign. |

### User

**Login** that can **own records** and appear in **`OwnerId`**; **`ProfileId`** controls **permissions**.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key (integration users are Users too). |
| `Name` | Text (compound) | Display value | **Full name** as shown in Salesforce (backed by FirstName + LastName + salutation logic). |
| `Username` | Text | Up to 80 characters | **Unique** login identifier (often email-like). |
| `Email` | Email | Up to 128 characters | User’s email. |
| `IsActive` | Checkbox | true / false | **false** disables login but may retain historical ownership on records. |
| `ProfileId` | Reference (Profile) | 18-character Id | **Profile** controlling object and field access. |
| `UserType` | Picklist | Values defined in org | Kind of user (e.g. Standard, CsnOnly for Experience Cloud); used to filter real humans vs. special users. |

### Product2

**Product catalog** SKU—what you sell on **OpportunityLineItem**, **OrderItem**, and **Asset**.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `Name` | Text | Up to 255 characters | Product name. |
| `ProductCode` | Text | Up to 255 characters | SKU or internal code. |
| `Description` | Long Text Area | Max length is field-specific (Describe shows bytes/chars) | Marketing or technical product copy; long text areas can be tens of thousands of characters depending on org. |
| `IsActive` | Checkbox | true / false | **false** hides the product from most selection UIs. |
| `Family` | Picklist | Values defined in org | Product grouping for reporting. |
| `CreatedDate` | DateTime | UTC timestamp | When the product was created. |

### Pricebook2

**Price book**—container for **PricebookEntry** rows (product + list price per currency). **`IsStandard`** marks the org’s **standard** price book.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `Name` | Text | Up to 255 characters | Price book name (e.g. “USD List 2025”). |
| `IsActive` | Checkbox | true / false | Inactive price books are usually hidden from selection. |
| `IsStandard` | Checkbox | true / false | **true** for the **standard** price book (one per org in simple setups). |
| `Description` | Text | Up to 255 characters | Internal notes. |

### Asset

**Sold or installed unit** at a customer—often **serial**, **warranty**, and **support** context; links **Account** and **Product2**.

| Field | Type | Length / precision | Description |
|-------|------|--------------------|-------------|
| `Id` | Id (reference) | 18-character Salesforce Id | Primary key. |
| `Name` | Text | Up to 255 characters | Label for the asset (e.g. server name at customer site). |
| `AccountId` | Reference (Account) | 18-character Id | Customer who owns the asset. |
| `SerialNumber` | Text | Up to 80 characters | Manufacturer serial or asset tag. |
| `Status` | Picklist | Values defined in org | Installed, Shipped, Registered, etc. |
| `Product2Id` | Reference (Product2) | 18-character Id | Product definition this asset represents. |
| `InstallDate` | Date | Calendar date | When the asset went in service. |
| `OwnerId` | Reference (User) | 18-character Id | Record owner. |
| `CreatedDate` | DateTime | UTC timestamp | When the asset was created. |

## 8. Related

- [`../SALESFORCE_SETUP.md`](../SALESFORCE_SETUP.md)
- [`DATA_REGISTRY.md`](./DATA_REGISTRY.md)
