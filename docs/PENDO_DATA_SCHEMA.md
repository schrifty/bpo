# Pendo Data Schema

Comprehensive schema of data available from the Pendo APIs. Every field is labeled with its verification status:

- ✅ **Verified** — Observed in actual API responses from this subscription (March 2026)
- 📄 **Documented** — From Pendo Help Center or official sources, not yet observed in our data
- ❓ **Inferred** — Observed in third-party tools (tap-pendo, pendo-ETL-API-calls) or community sources

---

## Table of Contents

1. [API Surface](#1-api-surface)
2. [Aggregation API — Sources](#2-aggregation-api--sources)
   - [visitors](#21-visitors)
   - [accounts](#22-accounts)
   - [pageEvents](#23-pageevents)
   - [featureEvents](#24-featureevents)
   - [events (Track Events)](#25-events-track-events)
   - [guideEvents](#26-guideevents)
   - [pollEvents](#27-pollevents)
3. [Aggregation API — Pipeline Operations](#3-aggregation-api--pipeline-operations)
4. [REST API — Catalog Endpoints](#4-rest-api--catalog-endpoints)
   - [GET /page](#41-get-page)
   - [GET /feature](#42-get-feature)
   - [GET /guide](#43-get-guide)
   - [GET /tracktype](#44-get-tracktype)
   - [GET /report](#45-get-report)
   - [GET /segment](#46-get-segment)
5. [REST API — Entity Endpoints](#5-rest-api--entity-endpoints)
   - [GET /visitor/:id](#51-get-visitorid)
   - [GET /account/:id](#52-get-accountid)
6. [REST API — Schema Endpoints](#6-rest-api--schema-endpoints)
   - [GET /metadata/schema/visitor](#61-get-metadataschemavisitor)
   - [GET /metadata/schema/account](#62-get-metadataschemaaccount)
7. [Subscription-Specific Metadata (This Instance)](#7-subscription-specific-metadata-this-instance)
8. [Cross-Referencing IDs](#8-cross-referencing-ids)
9. [Volumes & Rate Limits](#9-volumes--rate-limits)
10. [Gaps & Open Questions](#10-gaps--open-questions)
11. [References](#11-references)

---

## 1. API Surface

Pendo exposes two complementary APIs through the same base URL and auth:

| API | Method | Path | Purpose |
|-----|--------|------|---------|
| **Aggregation** | POST | `/api/v1/aggregation` | Pipeline queries over event and entity data |
| **REST** | GET | `/api/v1/{resource}` | Catalog metadata (pages, features, guides, etc.) and entity lookup |

**Base URLs:** `https://app.pendo.io` (US) ✅, `https://app.eu.pendo.io` (EU) ✅

**Auth:** `X-Pendo-Integration-Key` header on every request ✅

**Aggregation request envelope:**
```json
{
  "response": { "mimeType": "application/json" },
  "request": {
    "requestId": "<string>",
    "pipeline": [ ... ]
  }
}
```

**Aggregation response envelope:** `{ "results": [ ... ] }` ✅

---

## 2. Aggregation API — Sources

Every pipeline starts with a `source` step. The following sources have been verified or documented.

### 2.1 `visitors`

All visitors in the subscription. Returns one record per visitor.

**Source params:** ✅
```json
{ "visitors": { "startTime": <epoch_ms>, "endTime": <epoch_ms> } }
```
Also accepts `null` for unbounded query. ✅

**Record count (3-day window):** 34,068 ✅

**Top-level fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `visitorId` | string | ✅ | Pendo visitor ID (your app's user ID) |
| `metadata` | object | ✅ | Nested metadata groups |

**`metadata` groups observed:**

| Group | Status | Description |
|-------|--------|-------------|
| `metadata.agent` | ✅ | Custom metadata sent via your install script or integrations |
| `metadata.auto` | ✅ | Auto-captured by Pendo |
| `metadata.auto__323232` | ✅ | Per-app variant of auto (app ID suffix); subset of auto fields |
| `metadata.salesforce` | ✅ | Salesforce integration fields |
| `metadata.pendo` | ✅ | Pendo system fields (usually empty in aggregation, populated in REST) |

**`metadata.agent` fields (subscription-specific):**

| Field | Type | Status | Example |
|-------|------|--------|---------|
| `sitename` | string | ✅ | "AGI Clay Center" |
| `sitenames` | list | ✅ | ["AGI Omaha", "AGI Sentinal", "AGI Clay Center", "AGI Training Site"] |
| `siteid` | integer | ✅ | 3684 |
| `siteids` | list | ✅ | [3499, 3500, 3684, 3687] |
| `entity` | string | ✅ | "Clay Center" |
| `entitynames` | string | ✅ | "[Omaha Albion Clay Center AGI Training Site]" |
| `businessunit` | string | ✅ | "AGI" |
| `businessunitnames` | list | ✅ | ["AGI"] |
| `division` | string | ✅ | "AGI" |
| `divisionnames` | list | ✅ | ["AGI"] |
| `emailaddress` | string | ✅ | "deniz.balci@aggrowth.com" |
| `ownername` | string | ✅ | "Josh Fox" |
| `ownernames` | list | ✅ | ["Josh Fox", "<nil>"] |
| `ownerndx` | integer | ✅ | 33596 |
| `ownerndxs` | list | ✅ | [33596, 0] |
| `profiletype` | string | ✅ | "SystemAdministrator" |
| `profiletypes` | list | ✅ | ["SystemAdministrator"] |
| `role` | string | ✅ | "ExecutiveVP" |
| `viewercountry` | string | ✅ | "US" |
| `language` | string | ✅ | "en_US" |
| `isimpersonated` | boolean | ✅ | false |
| `isinternaluser` | boolean | ✅ | false |
| `issmecertified` | boolean | ✅ | false |

> **Note:** `metadata.agent` fields are **subscription-specific**. They come from your metadata configuration (Settings > Metadata) and the data your app sends via the Pendo install script. Other subscriptions will have different fields. See [Section 7](#7-subscription-specific-metadata-this-instance) for the full schema.

**`metadata.auto` fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `accountid` | string | ✅ | Account ID this visitor belongs to |
| `accountids` | list | ✅ | All account IDs |
| `id` | string | ✅ | Same as visitorId |
| `nid` | integer | ✅ | Numeric visitor ID |
| `idhash` | integer | ✅ | Hash of visitor ID |
| `createdat` | integer | ✅ | First seen (epoch ms) |
| `firstvisit` | integer | ✅ | First visit (epoch ms) |
| `lastvisit` | integer | ✅ | Last visit (epoch ms) |
| `lastupdated` | integer | ✅ | Last metadata update (epoch ms) |
| `lastbrowsername` | string | ✅ | e.g. "Edge", "Chrome" |
| `lastbrowserversion` | string | ✅ | e.g. "145.0.0" |
| `lastoperatingsystem` | string | ✅ | e.g. "Windows", "Mac OS X" |
| `lastservername` | string | ✅ | e.g. "app.leandna.com", "app.eu.leandna.com" |
| `lastuseragent` | string | ✅ | Full UA string |

> The REST endpoint `GET /visitor/:id` returns an additional field `firstidentifiedvisit` in `metadata.auto` that does not appear in aggregation results. ✅

---

### 2.2 `accounts`

All accounts. Returns one record per account.

**Source params:** ✅
```json
{ "accounts": null }
```

**Record count:** 227 ✅

**Top-level fields:**

| Field | Type | Status |
|-------|------|--------|
| `accountId` | string | ✅ |
| `metadata` | object | ✅ |

**`metadata` groups observed:**

| Group | Status | Fields |
|-------|--------|--------|
| `metadata.agent` | ✅ | `name` (string, e.g. "AGI"), `region` (string, e.g. "US") |
| `metadata.auto` | ✅ | `id`, `nid`, `idhash`, `firstvisit`, `lastvisit`, `lastupdated` |
| `metadata.auto__323232` | ✅ | Per-app variant: `firstvisit`, `lastupdated`, `lastvisit` |
| `metadata.pendo_hubspot` | ✅ | `record_id` (string) |
| `metadata.salesforce` | ✅ | `account_id_pendo__c` (string) |

> **Note:** `metadata.agent` for accounts has different fields than for visitors. The schema endpoint (Section 6) lists `name`, `plan`, `region`. ✅

---

### 2.3 `pageEvents`

Page view events. Each row represents one visitor's interaction with one page in one time bucket.

**Source params:** ✅
```json
{ "pageEvents": null, "timeSeries": { "period": "dayRange", "first": "now()", "count": -30 } }
```
- `period`: `"dayRange"` ✅ or `"hourRange"` ✅
- With `"dayRange"`, rows have a `day` field. With `"hourRange"`, rows have an `hour` field instead. ✅
- Without `timeSeries`, returns all time (large dataset). ✅

**Record count (1 day, dayRange):** ~14,500 ✅

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `visitorId` | string | ✅ | |
| `accountId` | string | ✅ | |
| `pageId` | string | ✅ | Opaque Pendo page ID. Map to name via `GET /page`. |
| `appId` | integer | ✅ | Pendo app ID |
| `numEvents` | integer | ✅ | Number of page views in this bucket |
| `numMinutes` | integer | ✅ | Engagement minutes |
| `day` | integer | ✅ | Day bucket start (epoch ms). **Only present with `dayRange`.** |
| `hour` | integer | ✅ | Hour bucket start (epoch ms). **Only present with `hourRange`.** |
| `week` | integer | ✅ | Week bucket start |
| `month` | integer | ✅ | Month bucket start |
| `quarter` | integer | ✅ | Quarter bucket start |
| `firstTime` | integer | ✅ | First event timestamp (epoch ms) |
| `lastTime` | integer | ✅ | Last event timestamp (epoch ms) |
| `lastKeyFrameTimestamp` | integer | ✅ | |
| `server` | string | ✅ | Server name (e.g. "app.leandna.com") |
| `userAgent` | string | ✅ | Browser user agent |
| `remoteIp` | string | ✅ | IP address (requires "Allow logging remote addresses" in subscription settings) |
| `country` | string | ✅ | ISO country code |
| `region` | string | ✅ | Region/state code |
| `latitude` | float | ✅ | |
| `longitude` | float | ✅ | |
| `analyticsSessionId` | string | ✅ | Session ID |
| `recordingId` | string | ✅ | Session replay recording ID (if applicable) |
| `recordingSessionId` | string | ✅ | |
| `tabId` | string | ✅ | Browser tab ID |
| `signatureAgent` | string | ✅ | |
| `errorClickCount` | integer | ✅ | Clicks that produced errors |
| `rageClickCount` | integer | ✅ | Rapid repeated clicks (frustration signal) |
| `uTurnCount` | integer | ✅ | Quick navigation reversals |
| `deadClickCount` | integer | ✅ | Clicks with no response |
| `parameters` | object | ✅ | Page parameters from URL rules (e.g. `{"parameter": "excess-and-obsolete-inventory"}`) |
| `properties` | object | ✅ | Event properties (see below) |

**`properties` structure:**

| Path | Type | Status | Description |
|------|------|--------|-------------|
| `__sg__.visitormetadata.agent__sitename` | string | ✅ | Visitor's site name at event time |
| `__sg__.visitormetadata.agent__entity` | string | ✅ | Visitor's entity at event time |
| `__sg__.visitormetadata.agent__viewercountry` | string | ✅ | Visitor's country at event time |
| `__utm__.channel` | string | ✅ | UTM channel (e.g. "Referral", "Organic Search") |
| `__utm__.referrer` | string | ✅ | Referrer URL |

> `__sg__` contains **historical metadata** — the visitor's metadata snapshot at event time, not the current value. This is distinct from visitor `metadata` which shows the latest state. 📄 [Event properties docs](https://support.pendo.io/hc/en-us/articles/7710433678619)

---

### 2.4 `featureEvents`

Feature click/interaction events. Same structure as `pageEvents` with these differences:

**Source params:** ✅
```json
{ "featureEvents": null, "timeSeries": { "period": "dayRange", "first": "now()", "count": -30 } }
```

**Record count (1 day):** ~24,600 ✅

**Field differences from `pageEvents`:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `featureId` | string | ✅ | Opaque Pendo feature ID. Map to name via `GET /feature`. **Replaces `pageId`.** |
| `parameters` | — | ✅ | **Not present** on feature events (page-only field) |

**Custom event properties observed in `properties`:**

| Key | Type | Status | Description |
|-----|------|--------|-------------|
| `widgetname` | string | ✅ | Widget/feature display name (click event property) |
| `report` | string | ✅ | Report name (click event property) |
| `date` | string | ✅ | Date context |
| `comparator` | string | ✅ | Comparison type |
| `report_filter_factory_ndx` | string | ✅ | Filter factory index |
| `report_row_factory_ndx` | string | ✅ | Row factory index |
| `inprogress_reason` | string | ✅ | In-progress reason |
| `item_drilldown_factory_ndx` | string | ✅ | Drilldown factory index |

> Custom event properties are configured per-Feature in the Visual Design Studio. They vary by feature. 📄

---

### 2.5 `events` (Track Events)

Custom events sent via `pendo.track()` in your application code.

**Source params:** ✅
```json
{ "events": { "eventClass": ["web"] }, "timeSeries": { "period": "dayRange", "first": "now()", "count": -7 } }
```

**`eventClass` values:** `"web"` ✅, `"ios"` ✅, `"android"` 📄

**Record count (7 days, web):** 57,251 ✅

**Field structure:** Identical to `pageEvents` (has `pageId`, not `featureId`). ✅ The `pageId` field contains the track event type name (e.g. `"allevents"`). ✅

**Note:** In our subscription, querying with `eventClass: ["ios"]` returns the same count as `["web"]` (57,251). This may indicate the filter is ignored or all events share a class. ✅

---

### 2.6 `guideEvents`

In-app guide interactions (tooltips, modals, walkthroughs, banners, etc.).

**Source params:** ✅
```json
{ "guideEvents": null, "timeSeries": { "period": "dayRange", "first": "now()", "count": -7 } }
```

**Record count (7 days):** 29,507 ✅

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `visitorId` | string | ✅ | |
| `accountId` | string | ✅ | |
| `accountIds` | list | ✅ | All account IDs for this visitor |
| `appId` | integer | ✅ | |
| `type` | string | ✅ | Event type (see values below) |
| `guideId` | string | ✅ | Pendo guide ID. Map to name via `GET /guide`. |
| `guideStepId` | string | ✅ | Step within the guide |
| `guideTimestamp` | integer | ✅ | Guide event timestamp (epoch ms) |
| `guideSeenReason` | string | ✅ | Why guide was shown (see values below) |
| `browserTime` | integer | ✅ | Client-side timestamp |
| `receivedTime` | integer | ✅ | Server-received timestamp |
| `loadTime` | integer | ✅ | Guide load time (ms) |
| `url` | string | ✅ | Page URL when guide was shown |
| `title` | string | ✅ | Encoded guide/step title |
| `language` | string | ✅ | Browser language (e.g. "en-US", "fr") |
| `userAgent` | string | ✅ | |
| `remoteIp` | string | ✅ | |
| `country` | string | ✅ | |
| `region` | string | ✅ | |
| `latitude` | float | ✅ | |
| `longitude` | float | ✅ | |
| `elementPath` | string | ✅ | DOM element path (e.g. "BODY") |
| `eventId` | string | ✅ | |
| `fragment` | string | ✅ | |
| `analyticsSessionId` | string | ✅ | |
| `tabId` | string | ✅ | |
| `serverName` | string | ✅ | Note: field name differs from event sources (`server` vs `serverName`) |
| `oldVisitorId` | string | ✅ | Overloaded field — contains metadata context, not always a visitor ID |
| `uiElementId` | string | ✅ | UI element that triggered the guide |
| `uiElementType` | string | ✅ | e.g. "BUTTON" |
| `uiElementActions` | string | ✅ | JSON-encoded actions (e.g. `[{"action":"showGuide","guideId":"..."}]`) |
| `properties` | object | ✅ | Same `__sg__` and `__utm__` structure as other event sources |

**Distinct `type` values observed:** ✅

| Value | Description |
|-------|-------------|
| `guideSeen` | Guide was displayed to the visitor |
| `guideAdvanced` | Visitor advanced to the next step |
| `guideDismissed` | Visitor dismissed the guide |
| `guideActivity` | Visitor interacted with a guide element |

**Distinct `guideSeenReason` values observed:** ✅

| Value | Description |
|-------|-------------|
| `auto` | Automatically triggered by targeting rules |
| `launcher` | Opened from the Resource Center launcher |
| `badge` | Triggered by a badge |
| `dom` | Triggered by DOM element appearance |
| `whatsnew` | Triggered from What's New section |
| `advanced` | Triggered by advancing from another guide step |
| `continue` | Continuation of a multi-step guide |

---

### 2.7 `pollEvents`

NPS and poll/survey response events.

**Source params:** ✅
```json
{ "pollEvents": null, "timeSeries": { "period": "dayRange", "first": "now()", "count": -7 } }
```

**Record count (7 days):** 14 ✅

**Fields:** Largely the same as `guideEvents`, plus:

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `pollId` | string | ✅ | Poll identifier |
| `pollResponse` | integer | ✅ | Numeric response (e.g. NPS score 0-10) |
| `pollType` | string | ✅ | e.g. "NPSRating" |
| `type` | string | ✅ | e.g. "ui:web:pollResponse" |

---

## 3. Aggregation API — Pipeline Operations

After the `source` step, pipelines can chain these operations:

### `timeSeries` (on source step)

Controls the time window for event sources. ✅

```json
"timeSeries": {
  "period": "dayRange" | "hourRange",
  "first": "now()" | <epoch_ms>,
  "count": -N,
  "last": "now()"
}
```

- `count` negative = look back from `first`. ✅
- `"dayRange"` produces `day` field on results; `"hourRange"` produces `hour` field instead. ✅

### `select`

Choose and rename fields. ✅

```json
{ "select": { "alias": "sourceField", "nested": "properties.__sg__.visitormetadata.agent__sitename" } }
```

### `group`

Aggregate with grouping. ✅

```json
{
  "group": {
    "group": ["field1", "field2"],
    "fields": {
      "totalEvents": { "sum": "numEvents" },
      "totalMinutes": { "sum": "numMinutes" }
    }
  }
}
```

### `filter`

Filter expression string. 📄 ❓

```json
{ "filter": "day>=1772258400000" }
```

> Filter syntax is sparsely documented. The `>=`, `==`, `!=` operators appear in tap-pendo and pendo-ETL-API-calls.

### `sort`

Sort by fields. ✅

```json
{ "sort": ["field1", "field2"] }
```

### `limit`

Limit result count. 📄

```json
{ "limit": 1000 }
```

### `identified`

Filter to identified visitors only. 📄

```json
{ "identified": "visitorId" }
```

---

## 4. REST API — Catalog Endpoints

These return metadata about tagged Pages, Features, Guides, etc. — the "catalog" that gives meaning to the opaque IDs in event data.

### 4.1 `GET /page`

Returns all tagged pages. ✅

**Count:** 157 ✅

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `id` | string | ✅ | Page ID (matches `pageId` in pageEvents) |
| `name` | string | ✅ | Human-readable name (e.g. "Achievements", "Analytics index") |
| `appId` | integer | ✅ | |
| `appIds` | list | ✅ | |
| `kind` | string | ✅ | Always "Page" |
| `group` | object | ✅ | Page group: `{id, name, description, color}` |
| `group.name` | string | ✅ | Group name (e.g. "Supplier", "Analytics (i.e. Reports)", "Misc") |
| `rules` | list | ✅ | URL matching rules. Each: `{rule, designerHint, parsedRule}` |
| `rulesjson` | list | ✅ | Alternative rules format |
| `description` | string | ✅ | |
| `createdAt` | integer | ✅ | Epoch ms |
| `lastUpdatedAt` | integer | ✅ | |
| `createdByUser` | object | ✅ | `{id, username, first, last, role}` |
| `lastUpdatedByUser` | object | ✅ | |
| `isCoreEvent` | boolean | ✅ | |
| `isSuggested` | boolean | ✅ | |
| `suggestedName` | string | ✅ | |
| `validThrough` | integer | ✅ | |
| `dirty` | boolean | ✅ | |
| `dailyMergeFirst` | integer | ✅ | |
| `dailyRollupFirst` | integer | ✅ | |
| `rootVersionId` | string | ✅ | |
| `stableVersionId` | string | ✅ | |
| `ignoredFrustrationTypes` | list | ✅ | |
| `tagMaintenanceExclude` | boolean | ✅ | |

---

### 4.2 `GET /feature`

Returns all tagged features. ✅

**Count:** 277 ✅

**Fields:** Same as `/page` plus:

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `id` | string | ✅ | Feature ID (matches `featureId` in featureEvents) |
| `name` | string | ✅ | e.g. "3 Line LOB UI Toggle", "Accept supplier alternative button" |
| `kind` | string | ✅ | Always "Feature" |
| `elementPathRules` | list | ✅ | CSS selectors for matching (e.g. `['[data-testid="btn_accept-supplier-request"]']`) |
| `elementInitialTag` | string | ✅ | |
| `elementSelectionType` | string | ✅ | |
| `eventPropertyConfigurations` | list | ✅ | Click event property configs |
| `createdDesignerVersion` | string | ✅ | |
| `appWide` | boolean | ✅ | |
| `suggestedMatch` | string | ✅ | |

---

### 4.3 `GET /guide`

Returns all guides. ✅

**Count:** 668 ✅

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `id` | string | ✅ | Guide ID (matches `guideId` in guideEvents) |
| `name` | string | ✅ | e.g. "11/12/20 Release Notes" |
| `kind` | string | ✅ | Always "Guide" |
| `state` | string | ✅ | e.g. "disabled", "public", "draft" |
| `launchMethod` | string | ✅ | e.g. "launcher", "auto", "badge" |
| `isMultiStep` | boolean | ✅ | |
| `isTraining` | boolean | ✅ | |
| `isModule` | boolean | ✅ | |
| `isTopLevel` | boolean | ✅ | |
| `steps` | list | ✅ | Guide steps, each with `{id, guideId, templateId, type, contentType, buildingBlocksUrl, domUrl, ...}` |
| `attributes` | object | ✅ | Guide attributes (e.g. `dates`, `device.type`, `isAnnouncement`, `priority`) |
| `publishedAt` | integer | ✅ | |
| `publishedEver` | boolean | ✅ | |
| `recurrence` | integer | ✅ | |
| `recurrenceEligibilityWindow` | integer | ✅ | |
| `resetAt` | integer | ✅ | |
| `editorType` | string | ✅ | |
| `emailState` | string | ✅ | |
| `authoredLanguage` | string | ✅ | |
| `autoCreateFeedback` | boolean | ✅ | |
| `dependentMetadata` | object | ✅ | |
| `audienceUiHint` | object | ✅ | |
| `currentFirstEligibleToBeSeenAt` | integer | ✅ | |
| `createdAt` | integer | ✅ | |
| `lastUpdatedAt` | integer | ✅ | |
| `createdByUser` | object | ✅ | |
| `lastUpdatedByUser` | object | ✅ | |

---

### 4.4 `GET /tracktype`

Returns all Track Event types. ✅

**Count:** 326 ✅

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `id` | string | ✅ | Track type ID |
| `name` | string | ✅ | Display name |
| `trackTypeName` | string | ✅ | Internal name (matches the string passed to `pendo.track()`) |
| `trackTypeRules` | list | ✅ | Matching rules (e.g. `["addDashboard"]`) |
| `eventPropertyNameList` | list\|null | ✅ | Custom property names (e.g. `["name", "parentname", "structure"]`) |
| `kind` | string | ✅ | Always "TrackType" |
| `group` | object | ✅ | Group `{id, name}` |
| `appId` | integer | ✅ | |
| `createdAt` | integer | ✅ | |
| `lastUpdatedAt` | integer | ✅ | |

---

### 4.5 `GET /report`

Returns saved Pendo reports (Data Explorer, funnels, etc.). ✅

**Count:** 564 ✅

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `id` | string | ✅ | |
| `name` | string | ✅ | e.g. "Arnav LeanDNA Engagement" |
| `type` | string | ✅ | e.g. "Saved Reports" |
| `kind` | string | ✅ | "Report" |
| `definition` | object | ✅ | Report query definition |
| `aggregation` | object | ✅ | |
| `aggregationList` | list | ✅ | |
| `shared` | boolean | ✅ | |
| `level` | string | ✅ | |
| `scope` | string | ✅ | |
| `lastRunAt` | integer | ✅ | |

> **Note:** Per Pendo docs, the API does not support exporting report *results* for paths, funnels, workflows, retention, or Data Explorer. This endpoint returns report *definitions* only. 📄

---

### 4.6 `GET /segment`

Returns all segments. ✅

**Count:** 1,272 ✅

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `id` | string | ✅ | |
| `name` | string | ✅ | |
| `kind` | string | ✅ | "Segment" |
| `definition` | object | ✅ | Segment rules |
| `pipeline` | list | ✅ | Aggregation pipeline equivalent of the segment |
| `shared` | boolean | ✅ | |
| `createdByApi` | boolean | ✅ | |
| `dependentMetadata` | list | ✅ | |

---

## 5. REST API — Entity Endpoints

### 5.1 `GET /visitor/:id`

Returns a single visitor with full metadata. ✅

```
GET /api/v1/visitor/73000
```

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `id` | string | ✅ | Visitor ID |
| `accountIds` | list | ✅ | e.g. ["298"] |
| `lastStartTime` | integer | ✅ | |
| `metadata` | object | ✅ | Same structure as aggregation, plus `firstidentifiedvisit` in `metadata.auto` |
| `account` | object | ✅ | `{id: ""}` (lightweight reference) |

> **Note:** `GET /visitor` (no ID) returns 404. You must query by specific visitor ID. ✅

### 5.2 `GET /account/:id`

Returns a single account with full metadata. ✅

```
GET /api/v1/account/298
```

**Fields:**

| Field | Type | Status | Description |
|-------|------|--------|-------------|
| `id` | string | ✅ | Account ID |
| `metadata` | object | ✅ | Same groups as aggregation `accounts` source |

> **Note:** `GET /account` (no ID) returns 404. ✅

---

## 6. REST API — Schema Endpoints

### 6.1 `GET /metadata/schema/visitor`

Returns the metadata schema definition for visitors. Describes all configured fields, their types, and display names. ✅

**Response structure:** `{ "<group>": { "<field>": { "Type", "DisplayName", "sample", "isHidden", "isDeleted", ... } } }`

**Groups:** `agent`, `auto`, `custom`, `pendo`, `salesforce` ✅

See [Section 7](#7-subscription-specific-metadata-this-instance) for the full field listing from this endpoint.

### 6.2 `GET /metadata/schema/account`

Same structure as visitor schema. ✅

**Groups:** `agent`, `auto`, `pendo`, `pendo_hubspot`, `salesforce` ✅

---

## 7. Subscription-Specific Metadata (This Instance)

From `GET /metadata/schema/visitor` and `GET /metadata/schema/account`. These fields are specific to the LeanDNA Pendo subscription. Other subscriptions will differ.

### Visitor Metadata Schema

**Group: `agent`** (sent by your app)

| Field | Type | DisplayName |
|-------|------|-------------|
| `businessunit` | string | businessUnit |
| `businessunitnames` | (list) | Business Unit Names |
| `businessunits` | (list) | businessUnits |
| `division` | string | (none) |
| `divisionnames` | (list) | Division Names |
| `divisions` | (list) | (none) |
| `email` | string | (none) |
| `emailaddress` | string | Email Address |
| `entities` | (list) | (none) |
| `entity` | string | Entity |
| `entitynames` | string | Entity Names |
| `isimpersonated` | boolean | Impersonated |
| `isinternaluser` | boolean | Internal User |
| `issmecertified` | boolean | isSmeCertified |
| `language` | string | Language |
| `ownername` | string | CSM |
| `ownernames` | list | ownerNames |
| `ownerndx` | integer | ownerNdx |
| `ownerndxs` | list | ownerNdxs |
| `profiletype` | string | Profile Type |
| `profiletypes` | list | profileTypes |
| `role` | string | Role |
| `siteid` | integer | Site ID |
| `siteids` | list | Site Ids |
| `sitename` | string | Site Name |
| `sitenames` | list | Site Names |
| `viewercountry` | string | viewerCountry |

**Group: `auto`** (captured by Pendo)

| Field | Type | DisplayName |
|-------|------|-------------|
| `accountid` | string | Account ID |
| `accountids` | list | Account IDs |
| `createdat` | time | Created At |
| `firstvisit` | time | First Visit |
| `id` | string | Visitor ID |
| `lastbrowsername` | string | Most recent browser name |
| `lastbrowserversion` | string | Most recent browser version |
| `lastoperatingsystem` | string | Most recent operating system |
| `lastservername` | string | Most recent server name |
| `lastvisit` | time | Last Visit |

**Group: `pendo`**

| Field | Type | DisplayName |
|-------|------|-------------|
| `designerenabled` | boolean | Designer Enabled |
| `donotprocess` | boolean | Do Not Process |
| `integrationsource` | string | Integration Source |

**Group: `salesforce`**

| Field | Type | DisplayName |
|-------|------|-------------|
| `email` | string | Email |

### Account Metadata Schema

**Group: `agent`**

| Field | Type | DisplayName |
|-------|------|-------------|
| `name` | string | Account Name |
| `plan` | string | (none) |
| `region` | string | Region |

**Group: `auto`**

| Field | Type | DisplayName |
|-------|------|-------------|
| `firstvisit` | time | First Visit |
| `id` | string | Account ID |
| `lastvisit` | time | Last Visit |

**Group: `pendo`**

| Field | Type | DisplayName |
|-------|------|-------------|
| `description` | string | Description |
| `donotprocess` | boolean | Do Not Process |
| `integrationsource` | string | Integration Source |

**Group: `pendo_hubspot`**

| Field | Type | DisplayName |
|-------|------|-------------|
| `record_id` | string | Record ID |

**Group: `salesforce`**

| Field | Type | DisplayName |
|-------|------|-------------|
| `account_id_pendo__c` | string | Pendo Account ID |

---

## 8. Cross-Referencing IDs

Event data uses opaque IDs. Here's how to resolve them:

| ID Field | Found In | Resolve Via |
|----------|----------|-------------|
| `pageId` | pageEvents, events | `GET /page` → match `id` → get `name`, `rules`, `group` |
| `featureId` | featureEvents | `GET /feature` → match `id` → get `name`, `elementPathRules`, `group` |
| `guideId` | guideEvents | `GET /guide` → match `id` → get `name`, `state`, `steps` |
| `guideStepId` | guideEvents | `GET /guide` → match guide → `steps[].id` |
| `visitorId` | all events | `GET /visitor/:id` or aggregation `visitors` source |
| `accountId` | all events, visitors | `GET /account/:id` or aggregation `accounts` source |
| `appId` | all records | Subscription-level constant (-323232 for this instance) |

**Practical note:** The `/page`, `/feature`, `/guide` endpoints return the full catalog (157 pages, 277 features, 668 guides). Build a local lookup map once, then join against event data.

---

## 9. Volumes & Rate Limits

Observed data volumes (this subscription, single queries):

| Source | Time Window | Row Count |
|--------|-------------|-----------|
| `visitors` | 3 days | 34,068 |
| `accounts` | all | 227 |
| `pageEvents` | 1 day (dayRange) | ~14,500 |
| `pageEvents` | 1 day (hourRange) | ~28,500 |
| `featureEvents` | 1 day | ~24,600 |
| `events` (track) | 7 days | ~57,000 |
| `guideEvents` | 7 days | ~29,500 |
| `pollEvents` | 7 days | 14 |
| Pages catalog | — | 157 |
| Features catalog | — | 277 |
| Guides catalog | — | 668 |
| Track types catalog | — | 326 |
| Reports catalog | — | 564 |
| Segments catalog | — | 1,272 |
| Sites (derived from visitors) | 7 days | 492 |

**Rate limits:** Pendo returns HTTP 429 or sometimes 403 with reason `rateLimitExceeded`. The tap-pendo connector sleeps 30s on "Too Many Requests". No published rate limit numbers; empirically ~60 requests/minute appears safe. 📄 ❓

---

## 10. Gaps & Open Questions

| # | Gap | Notes |
|---|-----|-------|
| 1 | **Visitor history** | `GET /visitor/:id/history?starttime=<ms>` exists but returns 422 — may require different param format or be deprecated. tap-pendo uses it. |
| 2 | **Single page/feature/guide by ID** | `GET /page/:id`, `GET /feature/:id`, `GET /guide/:id` all return 404. Use the catalog list endpoints and filter client-side. |
| 3 | **Event filtering in aggregation** | The `filter` step syntax is poorly documented. Only `field>=value` pattern confirmed. Boolean operators, string matching are unknown. |
| 4 | **`auto__323232` metadata** | Per-app metadata variant (the number is the app ID). Contains a subset of `auto` fields plus `lastmetadataupdate_agent`. Purpose unclear. |
| 5 | **`oldVisitorId` in guide/poll events** | Overloaded field. Sometimes contains context strings (e.g. "lang:en-US reason:auto") rather than actual visitor IDs. |
| 6 | **Track events vs pageEvents** | Track events use the `events` source with `eventClass`, but return `pageId` field with the track type name (not an actual page ID). |
| 7 | **Report results export** | Pendo docs explicitly state the API cannot export results for paths, funnels, workflows, retention, or Data Explorer. Recommends Data Sync instead. |
| 8 | **Data Sync** | A separate Pendo export mechanism (not API-based). Not explored here. |
| 9 | **`eventClass` filtering** | `"web"` and `"ios"` return identical counts (57,251). May not actually filter, or all events may share a class in this subscription. |
| 10 | **Pendo MCP server** | Pendo offers MCP query tools (`visitorMetadataSchema`, `accountMetadataSchema`, usage queries). May provide additional access not tested here. |

---

## 11. References

| Source | URL | Trust Level |
|--------|-----|-------------|
| Pendo Help Center: Event properties | https://support.pendo.io/hc/en-us/articles/7710433678619 | 📄 Official |
| Pendo Help Center: Metadata configuration | https://support.pendo.io/hc/en-us/articles/360031832072 | 📄 Official |
| Pendo Help Center: Developer documentation | https://support.pendo.io/hc/en-us/articles/38099922926875 | 📄 Official |
| Pendo Engage API (interactive docs) | https://engageapi.pendo.io/ | 📄 Official |
| pendo-io/pendo-ETL-API-calls | https://github.com/pendo-io/pendo-ETL-API-calls | ❓ Official but sparse |
| singer-io/tap-pendo | https://github.com/singer-io/tap-pendo | ❓ Community, may be outdated |
| Pendo Web SDK docs | https://web-sdk.pendo.io/ | 📄 Official (client-side) |
