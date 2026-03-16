# Running BPO on AWS

BPO today only responds to **synchronous** requests (you run the script). Soon it will also:

- Run **every night** (cron).
- React to **Google Drive events** (file added or changed in a certain folder, e.g. `new-slides`).

This doc describes one architecture that supports all three: sync now, cron and Drive-triggered soon.

---

## How BPO is triggered

| Trigger        | Today | Soon   | What runs                          |
|----------------|-------|--------|------------------------------------|
| You run it     | ✅    | ✅     | Sync: CLI or API call              |
| Nightly        | —     | ✅     | Cron (e.g. “health review for all customers”) |
| Drive activity | —     | ✅     | File added/changed in a folder → e.g. hydrate or evaluate |

To support Drive, you need an **HTTP endpoint** that receives notifications and starts the right job. So the shape is:

- **One place that runs the script:** EC2, or Lambda (short jobs) + ECS (long jobs).
- **Sync:** You invoke that place (SSH + `decks.py`, or POST to an API).
- **Cron:** EventBridge (or cron on EC2) invokes the same runner on a schedule.
- **Drive:** Google sends a webhook to your endpoint; the endpoint triggers the runner (e.g. “hydrate” when something lands in `new-slides`).

---

## Recommended: EC2 + webhook + cron

Single box that does everything: run the script, run cron, and accept a webhook for Drive (and optional sync API).

### 1. One EC2 instance

- **AMI:** Amazon Linux 2023 or Ubuntu 22.04.
- **Type:** e.g. `t3.small` (or `t3.medium` for “all customers”).
- **IAM role:** So it can read Secrets Manager (or use keys).
- **Security group:** Outbound HTTPS; **inbound** only what you need for the webhook (e.g. 443 from 0.0.0.0/0 if behind a load balancer, or a small range if you use a fixed IP for Google).

### 2. Install app and secrets

Clone repo, create venv, `pip install -r requirements.txt`. Then either:

- **Quick:** Copy your laptop `.env` to the server and the Google service account JSON to e.g. `/opt/bpo/keys/`; set `GOOGLE_APPLICATION_CREDENTIALS` to that path.
- **Production:** Store env vars and the Google SA JSON in **AWS Secrets Manager**. Before each run, load them (e.g. a small script that fetches secrets, writes the JSON to `/tmp/sa.json`, exports env, then runs `python decks.py "$@"`). Grant the EC2 instance role permission to read those secrets.

### 3. Synchronous (today)

Run the script when you want:

```bash
# SSH into EC2, or run via SSM Run Command
cd /opt/bpo && ./scripts/load_secrets.sh "health review for Carrier"
cd /opt/bpo && ./scripts/load_secrets.sh hydrate
```

Optional: add a tiny API (e.g. Flask/FastAPI) that accepts `POST /run` with `{"prompt": "health review for all customers"}` and runs `decks.py` in a subprocess or background thread, so you can call it from your laptop without SSH.

### 4. Nightly cron (soon)

```bash
crontab -e
# Every night at 2am UTC
0 2 * * * cd /opt/bpo && ./scripts/load_secrets.sh "health review for all customers" >> /var/log/bpo.log 2>&1
```

Or use **EventBridge** with a cron rule to trigger the same command (e.g. via SSM Run Command on the EC2 instance), so you keep scheduling in AWS.

### 5. Google Drive events (soon)

Google Drive can notify an URL when a file or folder changes (**Push Notifications**). You expose a small **webhook** that receives that POST and then runs the right `decks` command.

**Flow:**

1. You register a **watch** on the Drive folder (e.g. the folder that contains `new-slides`, or the folder where you drop “run hydrate” triggers). Registration is done once (e.g. via a script or at app startup) and returns a `resourceId` and `expiration`; you renew before expiration.
2. When something changes, Drive sends a POST to your **webhook URL** with a small payload (e.g. `X-Goog-Resource-State: update`, `X-Goog-Channel-ID`, etc.). The body is often empty; you identify the change by headers.
3. Your webhook handler verifies the request (see below), then triggers the job: e.g. run `decks hydrate` or `decks evaluate` (or enqueue a job that does that).

**Webhook URL:** Must be **HTTPS** and publicly reachable by Google. Options:

- **API Gateway + Lambda:** Lambda receives the POST, verifies, then invokes the runner (e.g. send SQS message to an EC2 that runs the script, or run a short Lambda that starts an ECS task for “hydrate”). No need to open EC2 to the internet if you use Lambda as the only public endpoint.
- **EC2 with a small HTTP server:** e.g. Flask/FastAPI on 443 (or behind ALB). Handler receives POST, verifies, runs `decks hydrate` in background (subprocess or queue). Simpler if the script already runs on EC2.

**Verifying Drive notifications:** Google does not sign the webhook POST. Common approach: use a **shared secret** in the channel (e.g. as a query param or in a custom header) and check it in the handler, or only accept POSTs from Google’s IP ranges. For full details: [Drive API Push Notifications](https://developers.google.com/drive/api/guides/push).

**What to run on “file added/changed”:**

- If the watched folder is **new-slides**: on any change, run `decks hydrate` (and optionally `decks evaluate`). You may want to debounce (e.g. wait 30s and run once) so one upload doesn’t trigger many runs.
- If you have another folder (e.g. “run requests”), you could have the webhook look at the change and call different commands.

**Implementing the watch:** Use the Drive API `files.watch` on the folder ID (the one that contains `new-slides`, or the Drive folder ID you already use). You need to do this with credentials that have Drive scope; your existing service account or a user OAuth can do it. Store `expiration` and renew the watch before it expires (Google recommends renewing before 24h for reliability).

---

## Alternative: Lambda + ECS for long jobs

If you prefer not to manage EC2:

- **Sync + webhook:** API Gateway + Lambda. Lambda receives your sync request or the Drive webhook, then either runs a short `decks` command (if you fit in 15 min) or starts an **ECS Fargate task** that runs `decks.py` (for “all customers”, hydrate, etc.).
- **Cron:** EventBridge rule triggers the same Lambda (or triggers ECS RunTask directly) to run the nightly job.

Trade-off: more moving parts (Lambda, ECS, IAM, task definitions), but no server to patch. The Drive webhook is still “HTTP endpoint → run job”; the endpoint is Lambda instead of a process on EC2.

---

## Summary

| Your need              | Approach                                              |
|------------------------|--------------------------------------------------------|
| Sync (now)             | Run `decks.py` on EC2 via SSH (or POST to small API). |
| Nightly run (soon)     | Cron on EC2, or EventBridge → Lambda/ECS.             |
| Drive events (soon)    | Public HTTPS webhook → verify → run `decks hydrate` (or evaluate). Webhook = Lambda or small server on EC2. |

**Minimal path:** One EC2, cron for nightly, and a tiny webhook server (or API Gateway + Lambda) that runs `decks hydrate` when Drive notifies you of changes in the target folder. Sync stays “you run the script” (or one POST to that same server).

If you want, next step can be a minimal Flask/FastAPI webhook stub in the repo (e.g. `scripts/drive_webhook.py`) that checks a shared secret and runs `decks hydrate` in the background, plus a small script to register the Drive watch on your folder.

---

## Possible future: Bedrock Agents for the interactive agent

The **batch flows** (decks, hydrate, evaluate, cron, Drive webhook) don’t use LangChain; they use the OpenAI client for prompt parsing and your own Python for the rest. LangChain is only used for the **interactive agent** in `main.py` (conversational “ask about customers / generate a deck” with tools).

If you later expose that as a chat or API on AWS, you could move that agent from **LangChain to Bedrock Agents**: host the conversational agent in AWS, use Claude on Bedrock, and re-express the Pendo/deck tools as Bedrock action groups (e.g. Lambda per tool). That would give you an AWS-native agent and one less process to run for the chat path—but it’s optional; the sync/cron/Drive deployment above doesn’t depend on it.
