# Jira / JSM Data Schema

Comprehensive schema of data available from the Jira Cloud and Jira Service Management APIs at `leandna.atlassian.net`. Every field is labeled with its verification status:

- ✅ **Verified** — Observed in actual API responses from this instance (March 2026)
- 📄 **Documented** — From Atlassian documentation, not yet observed in our data
- ❓ **Inferred** — Observed in field definitions but no sample value seen

---

## Table of Contents

1. [API Surface](#1-api-surface)
2. [Authentication & Rate Limits](#2-authentication--rate-limits)
3. [Projects](#3-projects)
4. [Issue Search (JQL)](#4-issue-search-jql)
5. [Issue Schema](#5-issue-schema)
   - [Top-Level Keys](#51-top-level-keys)
   - [Standard Fields](#52-standard-fields)
   - [Custom Fields (Active in BPO)](#53-custom-fields-active-in-bpo)
   - [Custom Fields (Available but Unused)](#54-custom-fields-available-but-unused)
6. [JSM-Specific Structures](#6-jsm-specific-structures)
   - [SLA Fields](#61-sla-fields)
   - [Request Type](#62-request-type)
   - [Organizations](#63-organizations)
   - [Satisfaction / Sentiment](#64-satisfaction--sentiment)
7. [Issue Types](#7-issue-types)
8. [Statuses & Categories](#8-statuses--categories)
9. [Priorities](#9-priorities)
10. [Resolutions](#10-resolutions)
11. [Organizations (JSM)](#11-organizations-jsm)
12. [CMDB Object References](#12-cmdb-object-references)
13. [How BPO Uses This Data](#13-how-bpo-uses-this-data)
14. [Volumes & Limits](#14-volumes--limits)
15. [Gaps & Open Questions](#15-gaps--open-questions)
16. [References](#16-references)

---

## 1. API Surface

| API | Method | Path | Purpose |
|-----|--------|------|---------|
| **REST v3** ✅ | GET | `/rest/api/3/{resource}` | Field definitions, projects, priorities, statuses, resolutions, users |
| **JQL Search** ✅ | POST | `/rest/api/3/search/jql` | Paginated issue search with field selection |
| **Service Desk** ✅ | GET | `/rest/servicedeskapi/{resource}` | Organizations, request types, SLA data, customer portals |
| **CMDB (Assets)** ❓ | GET | `/rest/assets/1.0/{resource}` | Asset / configuration item lookups (Site, Entity objects) |

**Base URL:** `https://leandna.atlassian.net` ✅

---

## 2. Authentication & Rate Limits

**Auth:** HTTP Basic with `email:api_token`, base64-encoded in the `Authorization` header. ✅

**Rate limits** (observed in response headers): ✅

| Header | Value |
|--------|-------|
| `X-Ratelimit-Limit` | 350 requests/window |
| `X-Ratelimit-Remaining` | Decrements per request |

Per Atlassian documentation, the window is a sliding 1-minute window for Jira Cloud. 📄

---

## 3. Projects

22 projects observed. The ones relevant to customer-facing data: ✅

| Key | Name | Type | Purpose |
|-----|------|------|---------|
| **HELP** | HELP | `service_desk` | Customer support tickets (JSM portal) |
| **LEAN** | LeanDNA | `software` | Engineering/product development |
| **ER** | Enhancement Requests | `software` | Customer enhancement requests |
| **PS** | Professional Services | `service_desk` | Implementation & professional services |
| **IQ** | Implementation Queue | `business` | Customer onboarding queue |
| **IS** | Internal Support | `business` | Internal support requests |
| **KB** | Knowledge Base | `software` | Documentation |

Other projects (not customer-facing): AM, AUTO, CUSTOMER, CTEST, EV, IT, KBR, LEANTEST, MDP, PM, PSPT, PMD, IMPROVE, UX, WEB ✅

---

## 4. Issue Search (JQL)

**Endpoint:** `POST /rest/api/3/search/jql` ✅

**Request body:**
```json
{
  "jql": "project = HELP AND Organizations = \"Carrier\" ORDER BY created DESC",
  "maxResults": 100,
  "fields": ["summary", "status", "issuetype", "priority", "created", ...],
  "nextPageToken": null
}
```

**Response envelope:**
```json
{
  "expand": "...",
  "startAt": 0,
  "maxResults": 100,
  "total": 42,
  "issues": [ ... ],
  "isLast": true,
  "nextPageToken": "..."
}
```

**Pagination:** Token-based via `nextPageToken` (not offset-based). `isLast: true` signals the final page. ✅

**Field selection:** The `fields` array controls which fields are returned per issue. Use `["*all"]` for everything, or list specific field IDs. ✅

---

## 5. Issue Schema

### 5.1 Top-Level Keys

Every issue object: ✅

| Key | Type | Example |
|-----|------|---------|
| `id` | string | `"112640"` |
| `key` | string | `"HELP-34848"` |
| `self` | string (URL) | `"https://leandna.atlassian.net/rest/api/3/issue/112640"` |
| `expand` | string | Expansion hints |
| `fields` | object | All field data (see below) |

### 5.2 Standard Fields

All standard Jira fields observed in API responses: ✅

| Field | Type | Description |
|-------|------|-------------|
| `summary` | string | Issue title |
| `description` | ADF object | Atlassian Document Format (rich text) |
| `issuetype` | object | `{id, name, subtask, hierarchyLevel}` |
| `project` | object | `{id, key, name, projectTypeKey}` |
| `status` | object | `{id, name, description, statusCategory}` |
| `statusCategory` | object | `{id, key, name, colorName}` |
| `priority` | object | `{id, name, iconUrl}` |
| `resolution` | object \| null | `{id, name, description}` — null if unresolved |
| `assignee` | object \| null | `{accountId, displayName, emailAddress, active}` |
| `reporter` | object | `{accountId, displayName, emailAddress, active, timeZone, accountType}` |
| `creator` | object | Same shape as reporter |
| `created` | string (ISO 8601) | `"2026-03-09T09:44:10.509-0500"` |
| `updated` | string (ISO 8601) | `"2026-03-09T11:39:08.792-0500"` |
| `resolutiondate` | string \| null | ISO 8601 when resolved |
| `statuscategorychangedate` | string | ISO 8601 of last status category change |
| `duedate` | string \| null | Date string |
| `labels` | string[] | `["jira_escalated"]` |
| `components` | object[] | Usually empty in this instance |
| `fixVersions` | object[] | Release versions |
| `versions` | object[] | Affected versions |
| `issuelinks` | object[] | Linked issues |
| `subtasks` | object[] | Child subtasks |
| `attachment` | object[] | File attachments |
| `comment` | object | `{comments[], total, maxResults}` |
| `worklog` | object | `{worklogs[], total, maxResults}` |
| `votes` | object | `{votes: int, hasVoted: bool}` |
| `watches` | object | `{watchCount: int, isWatching: bool}` |
| `timetracking` | object | Time tracking data (usually empty) |
| `timeestimate` | int \| null | Remaining estimate (seconds) |
| `timeoriginalestimate` | int \| null | Original estimate (seconds) |
| `timespent` | int \| null | Time logged (seconds) |
| `workratio` | int | `-1` if no estimate set |
| `progress` | object | `{progress: int, total: int}` |
| `aggregateprogress` | object | Same, including subtasks |
| `security` | object \| null | `{id, name, description}` — security level |
| `environment` | string \| null | Environment description |

### 5.3 Custom Fields (Active in BPO)

These custom fields are actively used by `jira_client.py`: ✅

| Field ID | Name | Type | Shape |
|----------|------|------|-------|
| `customfield_10100` | **Customer** | array\<option\> | `[{value: "Carrier"}]` — multi-select |
| `customfield_10502` | **Organizations** | array\<org\> | `[{id, name, created, _links}]` — JSM organizations |
| `customfield_10613` | **Site IDs** | string \| null | Free text site identifier |
| `customfield_10629` | **Bug Severity** | object \| null | `{value: "S1", id: "..."}` — radio button |
| `customfield_10665` | **Time to resolution** | SLA object | See [SLA Fields](#61-sla-fields) |
| `customfield_10666` | **Time to first response** | SLA object | See [SLA Fields](#61-sla-fields) |

### 5.4 Custom Fields (Available but Unused)

Notable custom fields available in the instance that BPO does not currently consume:

| Field ID | Name | Type | Potential Value |
|----------|------|------|-----------------|
| `customfield_10102` | Service Activity | multi-select | Categorization of support activities |
| `customfield_10200` | Flagged | checkboxes | Impediment/blocker flags |
| `customfield_10204` | Sprint | json array | Agile sprint associations (LEAN project) |
| `customfield_10501` | Team | team | Atlassian team assignment |
| `customfield_10602` | Hours Spent | float | Manual time tracking |
| `customfield_10604` | Request Type | JSM object | Portal request type with links (see [6.2](#62-request-type)) |
| `customfield_10609` | Satisfaction | feedback | CSAT rating from customers |
| `customfield_10624` | Entity | select | Entity categorization |
| `customfield_10630` | Improvement Importance | radio | Enhancement prioritization |
| `customfield_10631` | Customer Contact and Role | text | Contact info on the ticket |
| `customfield_10633` | Agile Team | select | Engineering team assignment |
| `customfield_10639` | Urgency | select | ITIL urgency |
| `customfield_10640` | Impact | select | ITIL impact |
| `customfield_10641` | Pending reason | select | Why ticket is waiting |
| `customfield_10642` | Product categorization | cascading select | Product area taxonomy |
| `customfield_10676` | Ease of Effort | select | Effort estimation |
| `customfield_10677` | Level of Benefit | select | Benefit scoring |
| `customfield_10679` | Resolution Type | select | How the issue was resolved |
| `customfield_10685` | Sentiment | array | `[{id, name}]` — AI-detected sentiment (e.g. "Neutral") |
| `customfield_10815` | Time to done | SLA object | Additional SLA metric |
| `customfield_10881` | Lean Connect | CMDB object | Asset reference |
| `customfield_11046` | New Support Component | cascading select | Support categorization |
| `customfield_11079` | Closed date | datetime | When ticket was closed |
| `customfield_11080` | First response time | datetime | When first response occurred |
| `customfield_11081` | Zendesk ID | text | Legacy Zendesk cross-reference |
| `customfield_11083` | Sub-Type | select | Issue sub-categorization |
| `customfield_11084` | Component | select | Product component |
| `customfield_11121` | Site | CMDB object | `[{workspaceId, id, objectId}]` — CMDB site ref |
| `customfield_11154` | Entity | CMDB object | `[{workspaceId, id, objectId}]` — CMDB entity ref |
| `customfield_11220` | CSM | text | CSM name(s) as free text |

---

## 6. JSM-Specific Structures

### 6.1 SLA Fields

Both `customfield_10665` (Time to resolution) and `customfield_10666` (Time to first response) share the same structure: ✅

```json
{
  "id": "17",
  "name": "Time to resolution",
  "_links": { "self": "https://leandna.atlassian.net/rest/servicedeskapi/request/{issueId}/sla/17" },
  "completedCycles": [
    {
      "startTime":     { "iso8601": "...", "epochMillis": 1771312211507 },
      "stopTime":      { "iso8601": "...", "epochMillis": 1773077127192 },
      "breachTime":    { "iso8601": "...", "epochMillis": 1773698400000 },
      "breached": false,
      "goalDuration":  { "millis": 576000000, "friendly": "160h" },
      "elapsedTime":   { "millis": 415527192, "friendly": "115h 25m" },
      "remainingTime": { "millis": 160472808, "friendly": "44h 34m" }
    }
  ],
  "ongoingCycle": null,
  "slaDisplayFormat": "NEW_SLA_FORMAT"
}
```

**Key behaviors:**

| State | `completedCycles` | `ongoingCycle` |
|-------|-------------------|----------------|
| SLA completed | 1+ entries | null |
| SLA in progress | empty | object with `elapsedTime`, `remainingTime`, `breached`, `paused` |
| No SLA | empty | null |

**Time objects** always include `iso8601`, `jira` (Jira format), `friendly`, and `epochMillis`. ✅

**SLA goals** (from observed data): ✅

| SLA | Goal |
|-----|------|
| Time to first response | 48h |
| Time to resolution | 160h |

### 6.2 Request Type

`customfield_10604` — present on JSM issues: ✅

```json
{
  "_links": {
    "jiraRest": "https://leandna.atlassian.net/rest/api/2/issue/106930",
    "web": "https://leandna.atlassian.net/servicedesk/customer/portal/135/HELP-34438",
    "agent": "https://leandna.atlassian.net/browse/HELP-34438"
  },
  "requestType": {
    "id": "369",
    "name": "Help",
    "description": "Welcome! Raise a Support Engineering request...",
    "issueTypeId": "10512",
    "serviceDeskId": "135",
    "portalId": "135"
  },
  "currentStatus": {
    "status": "Closed",
    "statusCategory": "DONE",
    "statusDate": { "iso8601": "...", "epochMillis": ... }
  }
}
```

**Request types** in the HELP service desk: ✅

| ID | Name | Description |
|----|------|-------------|
| 369 | Help | General support request |
| 303 | Emailed request | Inbound email |
| 435 | Get developer support | Developer escalation |
| 402 | Health Check Confirmation | Pipeline health check |

### 6.3 Organizations

`customfield_10502` — links JSM tickets to customer organizations: ✅

```json
[
  {
    "id": "146",
    "uuid": "",
    "name": "Carrier",
    "created": { "iso8601": "2026-02-01T18:51:13-0600", "epochMillis": 1769993473544 },
    "_links": { "self": "https://leandna.atlassian.net/rest/servicedeskapi/organization/146" },
    "scimManaged": false
  }
]
```

186 organizations exist in the instance. ✅

### 6.4 Satisfaction / Sentiment

**Satisfaction** (`customfield_10609`) — CSAT feedback from customers via the JSM portal: ❓
- Observed as `null` on all sampled issues
- Structure (per docs): `{rating: int, comment: string}` 📄

**Sentiment** (`customfield_10685`) — AI-detected sentiment: ✅

```json
[{ "id": "2000", "name": "Neutral" }]
```

Known values: Positive, Neutral, Negative 📄

---

## 7. Issue Types

Issue types observed across all projects: ✅

**Customer-facing (HELP project):**

| Name | Subtask | Notes |
|------|---------|-------|
| Help | No | Primary support request type |
| Emailed request | No | From email channel |
| Developer escalation | No | Escalated to engineering |
| Data Sync Escalation | No | Data pipeline issues |
| Support | No | General support |
| Hypercare | No | Post-go-live intensive support |
| Ingestion Data | No | Data ingestion issues |
| Data Access | No | Data access requests |
| Request for Information | No | Customer info requests |
| Security Incident | No | Security events |
| Enablement | No | Customer enablement |
| SUT | No | Support utility |

**Engineering (LEAN project):**

| Name | Subtask | Notes |
|------|---------|-------|
| Bug | No | Product defects |
| New Feature | No | New functionality |
| Improvement | No | Enhancements |
| Task | No | Work items |
| Story | No | User stories |
| Epic | No | Large initiatives |
| Test | No | Test tickets |
| Enhancement | No | Enhancement requests |
| Missing Documentation | No | Doc gaps |

---

## 8. Statuses & Categories

Statuses are grouped into Jira status categories: ✅

| Category | Statuses |
|----------|----------|
| **To Do** | Backlog, Clarifying Requirements, New, On Hold, Open, Parking lot, Pending/On Hold, Review, Scheduled, Selected for Development, To Do, Under review, Waiting for approval, Waiting for IT |
| **In Progress** | Accepted, Authorize, Awaiting approval, Awaiting implementation, Code Review, Delivery, Design, Development In Progress, Discovery, Draft, Escalated, Handoff, Impact, Implementing, In Engineering Queue, In Progress, In Review, Investigating, Pending, Planning, Production Deploy, Production Verify, Ready for delivery, Remediate Issue On Production, Reopened, Requirements, Research, Staging Deploy, Staging Verify, Tech review, Under investigation, Waiting for customer, Waiting for support, Waiting on Data Governance, Work in progress |
| **Done** | Canceled, Closed, Completed, Declined, Done, Failed, Moved to Forum, Not Taken, Published, Resolved |
| **No Category** | Waiting for developers |

**Note:** "Waiting for customer" is categorized as In Progress (not paused/stopped), which means SLA clocks continue running for these tickets. ✅

---

## 9. Priorities

5 priority levels, from highest to lowest: ✅

| ID | Name | Description |
|----|------|-------------|
| 1 | Blocker | The platform is completely down |
| 2 | Critical | Significant operational impact |
| 3 | Major | Workaround available, not essential |
| 4 | Minor | Impairs non-essential functionality |
| 5 | Not Prioritized | Administrative or cosmetic requests |

---

## 10. Resolutions

| ID | Name | Notes |
|----|------|-------|
| 1 | Fixed | Standard fix |
| 2 | Won't Fix | Declined |
| 3 | Duplicate | Duplicate issue |
| 4 | Incomplete | Insufficient info |
| 5 | Cannot Reproduce | Not reproducible |
| 10000 | Done | Generic completion |
| 10100 | Not A Bug | Working as designed |
| 10200 | Won't Do | Not planned |
| 10300 | Staged for Testing | In test pipeline |
| 10301 | Future Consideration | Deferred |
| 10302 | Declined | Rejected |
| 10303 | Known Error | Acknowledged known issue |
| 10304 | Hardware failure | Hardware root cause |
| 10305 | Software failure | Software root cause |

---

## 11. Organizations (JSM)

**Endpoint:** `GET /rest/servicedeskapi/organization` ✅

186 total organizations. These map to customer accounts and are used for:
- Routing tickets to the right support team
- Filtering issues by customer (`Organizations = "Carrier"` in JQL)
- Associating SLA targets with customer tiers

**Organization object:**
```json
{
  "id": "146",
  "uuid": "",
  "name": "Carrier",
  "created": { "iso8601": "...", "epochMillis": ... },
  "_links": { "self": "https://leandna.atlassian.net/rest/servicedeskapi/organization/146" },
  "scimManaged": false
}
```

**Pagination:** `start` and `limit` params; `isLastPage` flag. ✅

---

## 12. CMDB Object References

Two custom fields reference Jira Assets (CMDB) objects: ✅

| Field ID | Name | Purpose |
|----------|------|---------|
| `customfield_11121` | Site | Links ticket to a customer site in CMDB |
| `customfield_11154` | Entity | Links ticket to an entity/system in CMDB |

**Shape:**
```json
[{
  "workspaceId": "f81d4850-9b29-464b-be3c-e77e0daeb146",
  "id": "f81d4850-9b29-464b-be3c-e77e0daeb146:219",
  "objectId": "219"
}]
```

These IDs can be resolved via the Assets API (`/rest/assets/1.0/object/{objectId}`) to get site names, locations, and other metadata. ❓ Not currently used by BPO.

---

## 13. How BPO Uses This Data

`src/jira_client.py` exposes two main methods:

### `get_customer_jira(customer_name, days)`

Searches HELP project by Organization name and summary prefix. Returns:

| Key | Type | Description |
|-----|------|-------------|
| `total_issues` | int | Total matching issues |
| `open_issues` | int | Unresolved count |
| `resolved_issues` | int | Resolved count |
| `escalated` | int | Issues with `jira_escalated` label or `Developer escalation` type |
| `open_bugs` | int | Open bugs |
| `by_status` | dict | Issue counts per status |
| `by_type` | dict | Issue counts per type |
| `by_priority` | dict | Issue counts per priority |
| `recent_issues` | list | Up to 8 most recent issues (key, summary, type, status, priority, created) |
| `escalated_issues` | list | Up to 5 escalated issues |
| `engineering` | dict | Related LEAN project tickets (see below) |
| `ttfr` | dict | Time to First Response SLA statistics |
| `ttr` | dict | Time to Resolution SLA statistics |

### `_get_engineering_tickets(customer_name)`

Searches LEAN project for tickets mentioning the customer. Returns open and recently closed engineering tickets.

### SLA Statistics

Both `ttfr` and `ttr` are computed from JSM SLA cycle data:

| Key | Type | Description |
|-----|------|-------------|
| `tickets` | int | HELP project tickets in set |
| `measured` | int | Tickets with completed SLA cycles |
| `waiting` | int | Tickets with ongoing (unmeasured) cycles |
| `breached` | int | Tickets that breached the SLA goal |
| `avg_ms` / `avg` | int / string | Average elapsed time |
| `median_ms` / `median` | int / string | Median elapsed time |
| `min_ms` / `min` | int / string | Fastest response/resolution |
| `max_ms` / `max` | int / string | Slowest response/resolution |

### QA Checks

`jira_client.py` runs cross-validation checks:
- Status breakdown sums to total
- Priority breakdown sums to total
- Type breakdown sums to total
- Open + resolved equals total
- SLA measured + waiting does not exceed HELP ticket count

---

## 14. Volumes & Limits

| Metric | Value |
|--------|-------|
| Rate limit | 350 requests/minute ✅ |
| Max results per page | 100 (JQL search) ✅ |
| Pagination | Token-based (`nextPageToken`) ✅ |
| HELP project | ~35,000 total issues ✅ |
| LEAN project | ~27,500 total issues ✅ |
| Organizations | 186 ✅ |
| Custom fields | 191 total field definitions ✅ |
| BPO max results per search | 200 (customer) / 50 (engineering) ✅ |

---

## 15. Gaps & Open Questions

| Gap | Impact | Mitigation |
|-----|--------|------------|
| `customfield_10100` (Customer multi-select) is rarely populated | Cannot reliably match tickets to customers via this field | BPO uses Organizations field + summary text search instead ✅ |
| CMDB Site/Entity objects (`customfield_11121`, `11154`) are opaque IDs | Cannot resolve site names from ticket data alone | Would need Assets API integration ❓ |
| Satisfaction (`customfield_10609`) appears always null | No CSAT data available from API | May require JSM portal configuration or different API endpoint |
| CSM field (`customfield_11220`) has inconsistent casing | e.g. "Bartlomiej Grabowy, Bartlomiej Grabowy" | Known Pendo-origin issue; same dedup logic needed |
| `customfield_10815` (Time to done) appears null on sampled issues | Additional SLA metric not in use | May be configured for specific request types only |
| ER and PS project data not consumed | Enhancement requests and professional services tickets available | Could add to customer picture when relevant |
| No changelog/history in current queries | Cannot compute time-in-status or status transition patterns | Would need `expand=changelog` or the `/issue/{key}/changelog` endpoint 📄 |
| Bug Severity (`customfield_10629`) often null | Severity data sparse | May only be populated for Bug issue types in HELP |

---

## 16. References

- [Jira Cloud REST API v3](https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/) 📄
- [JQL Reference](https://support.atlassian.com/jira-software-cloud/docs/use-advanced-search-with-jql/) 📄
- [JSM REST API](https://developer.atlassian.com/cloud/jira/service-desk/rest/intro/) 📄
- [Jira Assets (CMDB) API](https://developer.atlassian.com/cloud/assets/rest/) 📄
- [Rate Limiting](https://developer.atlassian.com/cloud/jira/platform/rate-limiting/) 📄
- BPO client: `src/jira_client.py` ✅
