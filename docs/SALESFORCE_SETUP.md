# Salesforce integration (JWT Bearer Flow)

The app can pull **Entity Contract** (Account) and **Opportunity** data from Salesforce for customer health reports and deck generation. Authentication uses the **JWT Bearer Flow** with a Connected App and a certificate-signed assertion (no password or MFA).

## The app doesn’t include a certificate — you create one

The BPO app does **not** ship with a certificate or private key. For JWT Bearer Flow, **you** (or your team) generate a **key pair** once:

- **Private key** (e.g. `server.key`) — stays with you; the app uses it to sign every JWT. You configure it via `SF_PRIVATE_KEY` or `SF_PRIVATE_KEY_PATH`. Never commit it or send it to Salesforce.
- **Certificate** (e.g. `server.crt`) — public half of the same pair. You give this file to your Salesforce admin to **upload** into the Connected App (Setup → App Manager → your app → Use digital signatures → upload). Salesforce uses it only to verify that JWTs signed with your private key are legitimate.

**Generate the pair (one-time):**

```bash
openssl req -x509 -sha256 -nodes -days 3650 -newkey rsa:2048 -keyout server.key -out server.crt -subj "/CN=LeanDNA-Salesforce-Integration"
```

- Keep `server.key` secure (local file, secret manager, or env var `SF_PRIVATE_KEY` with the PEM contents). **Do not commit it** — `server.key` is in `.gitignore`; never add it to the repo.
- Send `server.crt` to your Salesforce admin so they can upload it to the Connected App. Committing `server.crt` is optional (it’s public); many teams don’t, since only the admin needs it for upload.

After that, the admin creates the Connected App and integration user; you get the Consumer Key and username and point the app at the private key. No cert is bundled with the app — you create it and own it.

## What to do now that you have the cert

1. **Send `server.crt` to your Salesforce admin.**  
   They upload it in Salesforce: **Setup → App Manager** → open the Connected App (e.g. “LeanDNA Data Export”) → **Edit** → under **API (Enable OAuth Settings)** check **Use digital signatures** → **Choose File** and select `server.crt` → **Save**.  
   They do **not** need `server.key`; never send the private key.

2. **Keep `server.key` on your machine (or in a secret store).**  
   Don’t commit it. The app will read it from a path or from an env var (see step 4).

3. **Get from the admin when the Connected App is ready:**  
   **Consumer Key** and **integration user username**. Confirm with them that the cert is uploaded, “Admin approved users” is set, the integration user has access to the app, and that user has Read on Account and Opportunity (and the required fields). See **What you need from your admin** below.

4. **Configure the app.**  
   In `.env` (or your secret manager), set:
   - `SF_LOGIN_URL=https://login.salesforce.com` (or `https://test.salesforce.com` for sandbox)
   - `SF_CONSUMER_KEY=<Consumer Key from admin>`
   - `SF_USERNAME=<integration user username>`
   - Either **`SF_PRIVATE_KEY_PATH=server.key`** (path to `server.key`, e.g. in project root) or **`SF_PRIVATE_KEY=<paste PEM contents of server.key>`** (for CI/production secrets).

5. **Run a deck or health report.**  
   If everything is set up, the app will get a token from Salesforce and pull Account/Opportunity data; the Data Quality slide will show Salesforce as **ok**. If something’s wrong, see **Troubleshooting** at the bottom of this doc.

## What the app uses

- **Account** (Type = `Customer Entity`): contract fields (e.g. `LeanDNA_Entity_Name__c`, `Contract_Status__c`, `ARR__c`, start/end dates).
- **Opportunity**: count of new/expansion opportunities created this year; sum of `ARR__c` for pipeline stages (e.g. 3-Business Validation, 4-Proposal, 5-Contracts).

Customer matching is done by name: Account `Name` or `LeanDNA_Entity_Name__c` is matched (case-insensitive) to the Pendo/deck customer name.

## What you need from your admin (and why)

Ask your Salesforce admin to complete the integration setup (see **Admin setup** below) and then give you the following. Each item is something you’ll plug into your app (env vars or secrets); the admin doesn’t need to know your app’s internals, only that “engineering needs these to connect.”

---

### 1. **Consumer Key (Client ID)** → you set `SF_CONSUMER_KEY`

**What it is:** A long alphanumeric string that identifies the **Connected App** in Salesforce (e.g. `3MVG9...`). It’s not secret; it’s the “app ID” Salesforce uses to know which app is asking for access.

**Where the admin finds it:** Setup → App Manager → open the Connected App (e.g. “LeanDNA Data Export”) → **Consumer Key** is shown in the API (Enable OAuth Settings) section.

**Why you need it:** The app includes this in the JWT it sends to Salesforce. Salesforce uses it to look up the Connected App, verify the JWT signature with the certificate the admin uploaded, and then issue an access token. Without it, Salesforce doesn’t know which app is connecting.

---

### 2. **Confirmation: certificate uploaded and “Use digital signatures” enabled**

**What it is:** The admin must have uploaded the **public** certificate (e.g. `server.crt`) to the Connected App and checked **Use digital signatures**. You never see the certificate file yourself; you only need confirmation that this step is done.

**Why you need it:** The app signs each JWT with the **private** key (you hold that). Salesforce verifies the signature using the **public** certificate. If the certificate isn’t uploaded or digital signatures aren’t enabled, Salesforce will reject the JWT and you’ll get errors like “invalid_grant” or “audience is invalid.”

---

### 3. **Confirmation: “Admin approved users are pre-authorized” and integration user has access**

**What it is:** The Connected App’s **Permitted Users** must be set to *Admin approved users are pre-authorized*. The **integration user** (the dedicated user that represents the app, e.g. `leandna.integration@leandna.com`) must be explicitly granted access to that Connected App (via a Permission Set or the app’s “Manage Profiles” / “Manage Permission Sets”).

**Why you need it:** Otherwise Salesforce returns “user hasn’t approved this consumer.” The integration user is the identity (`sub` in the JWT) that the app uses for every API call; that user must be pre-authorized to use this Connected App.

---

### 4. **Integration user username** → you set `SF_USERNAME`

**What it is:** The **Username** of the dedicated integration user (e.g. `leandna.integration@leandna.com`). This is the same user that must be approved to use the Connected App (see #3).

**Where the admin finds it:** Setup → Users → open the integration user → **Username** is at the top.

**Why you need it:** The app puts this in the JWT as the “subject” (`sub`). Salesforce then issues an access token that acts as that user. All API calls (Account, Opportunity, etc.) run with this user’s permissions, so the admin must give this user Read on the right objects and fields.

---

### 5. **Confirmation: integration user has Read on Account and Opportunity (and the right fields)**

**What it is:** The integration user’s profile or permission set must allow **Read** on **Account** and **Opportunity**, and Read on the specific fields the app queries (e.g. Account: `Name`, `LeanDNA_Entity_Name__c`, `Contract_Status__c`, contract dates, `ARR__c`; Opportunity: `AccountId`, `Type`, `StageName`, `ARR__c`, `CreatedDate`, etc.).

**Why you need it:** If any object or field is missing, the SOQL queries will fail with “insufficient access rights.” The admin doesn’t need to run the app; they just need to grant the permissions listed in the setup guide (or in `salesforce_client.py` / the admin doc).

---

### 6. **Private key (server.key)** → you set `SF_PRIVATE_KEY` or `SF_PRIVATE_KEY_PATH`

**What it is:** The **private** key from the key pair you generated (see **The app doesn’t include a certificate** above). It’s the file `server.key` from the same `openssl` command that produced `server.crt`. The admin never sees or uploads this; only the certificate (`server.crt`) goes to Salesforce.

**Who provides it:** You (or whoever ran the `openssl` command) keep `server.key` and either pass its path to the app (`SF_PRIVATE_KEY_PATH`) or put the PEM contents in a secret and set `SF_PRIVATE_KEY`. The admin only receives `server.crt` to upload to the Connected App.

**Why you need it:** The app uses this key to **sign** every JWT. Salesforce uses the public certificate (already uploaded) to verify the signature. No private key → no valid JWT → no access token.

---

### Summary: what to ask the admin for

| You need from admin | You use it as | Why |
|---------------------|---------------|-----|
| **Consumer Key** | `SF_CONSUMER_KEY` | So Salesforce knows which Connected App is connecting and can verify the JWT. |
| **Confirmation: cert uploaded + digital signatures on** | (no env var) | So Salesforce can verify the JWT signature. |
| **Confirmation: admin-approved users + integration user has app access** | (no env var) | So the app isn’t rejected with “user hasn’t approved this consumer.” |
| **Integration user username** | `SF_USERNAME` | So the JWT identifies the correct user and the token has the right permissions. |
| **Confirmation: integration user can Read Account & Opportunity (and fields)** | (no env var) | So the app’s SOQL queries don’t fail with “insufficient access rights.” |
| **Private key (server.key)** | `SF_PRIVATE_KEY` or `SF_PRIVATE_KEY_PATH` | So the app can sign the JWT; you generate it (see above) and keep it; admin never gets it. |

---

## Admin setup

Your Salesforce admin should follow the full guide (e.g. **Salesforce: Integration User + OAuth Connected App (JWT Bearer) Setup**) to:

1. Create an **Integration User** and an **OAuth Connected App** with JWT (digital signatures).
2. Upload the **certificate** (`server.crt`) to the Connected App; keep **private key** (`server.key`) in a secure secret store.
3. Set **Permitted Users** to *Admin approved users are pre-authorized* and grant the integration user access (e.g. via Permission Set).
4. Grant the integration user **Read** on Account (and required fields) and on Opportunity (and required fields) as needed for the SOQL used in the app.

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SF_LOGIN_URL` | Yes | `https://login.salesforce.com` (prod) or `https://test.salesforce.com` (sandbox) |
| `SF_CONSUMER_KEY` | Yes | Connected App Consumer Key (Client ID) |
| `SF_USERNAME` | Yes | Integration user username (e.g. `leandna.integration@leandna.com`) |
| `SF_PRIVATE_KEY` | Or path | PEM string of the private key (e.g. contents of `server.key`) |
| `SF_PRIVATE_KEY_PATH` | Or key | Path to the private key file (e.g. `path/to/server.key`) |

Provide either `SF_PRIVATE_KEY` (PEM string, e.g. from a secret manager) or `SF_PRIVATE_KEY_PATH` (file path). Do not commit the private key to the repo.

## Behavior

- If any of the required Salesforce env vars are missing or invalid, the health report still runs; Salesforce is skipped and the **Data Quality** slide shows Salesforce as *unavailable*.
- If the integration user lacks object/field permissions, the SOQL calls fail and the same “unavailable” behavior applies.
- When Salesforce is configured and queries succeed, the report includes `salesforce` (accounts, `opportunity_count_this_year`, `pipeline_arr`, `matched`) and the Data Quality slide shows Salesforce as *ok*.

## Troubleshooting

- **"client identifier invalid"** → The Consumer Key in the JWT doesn’t match a Connected App in this org. Check `SF_CONSUMER_KEY`: get the exact **Consumer Key** from Setup → App Manager → your Connected App (no spaces, no Consumer Secret). Sandbox and production have different keys.
- **"user hasn't approved this consumer"** → Set Permitted Users to *Admin approved users* and assign the Connected App to the integration user (Permission Set or profile).
- **"invalid_grant" / "audience is invalid"** → Use the correct `SF_LOGIN_URL` for prod vs sandbox.
- **"insufficient access rights on object id"** → Grant the integration user Read on Account and Opportunity and on the fields used in SOQL (see `salesforce_client.py` and the admin doc).
