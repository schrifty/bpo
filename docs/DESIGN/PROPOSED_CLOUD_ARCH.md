# Proposed cloud architecture (AWS)

Design note for running Cortex in the cloud. **Operational artifacts** live in `Dockerfile`, `scripts/run_job.sh`, `scripts/bootstrap_aws_env.py`, and `infra/` (ECS task definition, EventBridge, CloudWatch alarm templates).

## Operational runbook (nightly decks)

**Infrastructure:** use Terraform (`infra/terraform/`). Manual JSON templates in `infra/*.json` are legacy reference.

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init && terraform apply
terraform output -raw run_task_engineering_portfolio | bash
```

See `infra/terraform/README.md` for ECR push, secrets upload, and schedules.

### Local parity

```bash
cp .env.example .env   # fill keys
python3 cortex.py run-job --job nightly-core --dry-run
python3 cortex.py run-job --job engineering-portfolio
```

Optional unattended strictness (matches AWS default):

```bash
export CORTEX_FAIL_ON_INTEGRATION_WARNINGS=1
export CORTEX_LOG_FORMAT=json
python3 cortex.py run-job --job export-weekly
```

### AWS (ECS Fargate + EventBridge)

1. Build/push image to ECR; register `infra/ecs-task-definition.json` (replace `ACCOUNT_ID`, `REGION`, EFS ids).
2. Create Secrets Manager secret (JSON keys mirror `.env.example`; include `GOOGLE_SERVICE_ACCOUNT_JSON`).
3. Mount EFS at `/var/cortex/cache`; set `CORTEX_CACHE_DIR=/var/cortex/cache`.
4. CloudWatch log group `/cortex/decks` with `awslogs` driver (see task definition).
5. EventBridge rules in `infra/eventbridge-rules.json` — **one rule per job** so failures are isolated.
6. Alarms on `Cortex.RunSuccess` EMF metric or log filter `{ $.event = "run_complete" && $.success = false }`.

Each task emits:

- JSON logs (`CORTEX_LOG_FORMAT=json`, automatic when `ECS_CONTAINER_METADATA_URI` is set)
- `CORTEX_RUN_SUMMARY={"success":…,"failures":[…],…}` on stdout
- EMF line with `Cortex.RunSuccess`, `Cortex.RunDurationSeconds`, `Cortex.DeckFailures`, `Cortex.IntegrationWarnings`

On failure, `failures-<job>-<run_id>.json` is uploaded to Drive Output when configured.

---

Cortex today only responds to **synchronous** requests (you run the script). Soon it will also:

- Run **every night** (cron).
- React to **Google Drive events** or a **schedule** (e.g. run `decks hydrate` after decks are shared with the intake group).

This doc describes one architecture that supports all three: sync now, cron and Drive-triggered soon.

---

## How Cortex is triggered

| Trigger        | Today | Soon   | What runs                          |
|----------------|-------|--------|------------------------------------|
| You run it     | ✅    | ✅     | Sync: CLI or API call              |
| Nightly        | —     | ✅     | Cron (e.g. “health review for all customers”) |
| Drive / schedule | —     | ✅     | e.g. cron or webhook → `decks hydrate` / `decks evaluate` (group intake) |

To support Drive, you need an **HTTP endpoint** that receives notifications and starts the right job. So the shape is:

- **One place that runs the script:** EC2, or Lambda (short jobs) + ECS (long jobs).
- **Sync:** You invoke that place (SSH + `cortex.py`, or POST to an API).
- **Cron:** EventBridge (or cron on EC2) invokes the same runner on a schedule.
- **Drive:** Google can send a webhook to your endpoint; the handler triggers the runner (e.g. `decks hydrate` for group intake).

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

- **Quick:** Copy your laptop `.env` to the server and the Google service account JSON to e.g. `/opt/cortex/keys/`; set `GOOGLE_APPLICATION_CREDENTIALS` to that path.
- **Production:** Store env vars and the Google SA JSON in **AWS Secrets Manager**. Before each run, load them (e.g. a small script that fetches secrets, writes the JSON to `/tmp/sa.json`, exports env, then runs `python cortex.py "$@"`). Grant the EC2 instance role permission to read those secrets.

### 3. Synchronous (today)

Run the script when you want:

```bash
# SSH into EC2, or run via SSM Run Command
# There is no committed load_secrets.sh; use your own wrapper that loads AWS Secrets Manager
# (or .env), exports env vars, then runs the CLI, for example:
cd /opt/cortex && python cortex.py "health review for Carrier"
cd /opt/cortex && python cortex.py hydrate
```

Optional: add a tiny API (e.g. Flask/FastAPI) that accepts `POST /run` with `{"prompt": "health review for all customers"}` and runs `cortex.py` in a subprocess or background thread, so you can call it from your laptop without SSH.

### 4. Nightly cron (soon)

```bash
crontab -e
# Every night at 2am UTC
0 2 * * * cd /opt/cortex && python cortex.py "health review for all customers" >> /var/log/cortex.log 2>&1
```

Or use **EventBridge** with a cron rule to trigger the same command (e.g. via SSM Run Command on the EC2 instance), so you keep scheduling in AWS.

### 5. Google Drive events (soon)

Google Drive can notify an URL when a file or folder changes (**Push Notifications**). You expose a small **webhook** that receives that POST and then runs the right `decks` command.

**Flow:**

1. You register a **watch** on a relevant Drive resource, or use **cron** to run hydrate on a schedule. Registration is done once (e.g. via a script or at app startup) and returns a `resourceId` and `expiration`; you renew before expiration.
2. When something changes, Drive sends a POST to your **webhook URL** with a small payload (e.g. `X-Goog-Resource-State: update`, `X-Goog-Channel-ID`, etc.). The body is often empty; you identify the change by headers.
3. Your webhook handler verifies the request (see below), then triggers the job: e.g. run `decks hydrate` or `decks evaluate` (or enqueue a job that does that).

**Webhook URL:** Must be **HTTPS** and publicly reachable by Google. Options:

- **API Gateway + Lambda:** Lambda receives the POST, verifies, then invokes the runner (e.g. send SQS message to an EC2 that runs the script, or run a short Lambda that starts an ECS task for “hydrate”). No need to open EC2 to the internet if you use Lambda as the only public endpoint.
- **EC2 with a small HTTP server:** e.g. Flask/FastAPI on 443 (or behind ALB). Handler receives POST, verifies, runs `decks hydrate` in background (subprocess or queue). Simpler if the script already runs on EC2.

**Verifying Drive notifications:** Google does not sign the webhook POST. Common approach: use a **shared secret** in the channel (e.g. as a query param or in a custom header) and check it in the handler, or only accept POSTs from Google’s IP ranges. For full details: [Drive API Push Notifications](https://developers.google.com/drive/api/guides/push).

**What to run on “file added/changed”:**

- On a **timer or webhook**, run `decks hydrate` (and optionally `decks evaluate`) to process decks shared with `GOOGLE_HYDRATE_INTAKE_GROUP`. Debounce if needed (e.g. wait 30s and run once).
- If you have another folder (e.g. “run requests”), you could have the webhook look at the change and call different commands.

**Implementing the watch:** Use the Drive API `files.watch` if you want push notifications, or rely on **cron** (e.g. every 15 minutes) calling `decks hydrate`. You need credentials with Drive scope; your existing service account or a user OAuth can do it. Store `expiration` and renew the watch before it expires (Google recommends renewing before 24h for reliability).

---

## Alternative: Lambda + ECS for long jobs

If you prefer not to manage EC2:

- **Sync + webhook:** API Gateway + Lambda. Lambda receives your sync request or the Drive webhook, then either runs a short `cortex` command (if you fit in 15 min) or starts an **ECS Fargate task** that runs `cortex.py` (for “all customers”, hydrate, etc.).
- **Cron:** EventBridge rule triggers the same Lambda (or triggers ECS RunTask directly) to run the nightly job.

Trade-off: more moving parts (Lambda, ECS, IAM, task definitions), but no server to patch. The Drive webhook is still “HTTP endpoint → run job”; the endpoint is Lambda instead of a process on EC2.

---

## Summary

| Your need              | Approach                                              |
|------------------------|--------------------------------------------------------|
| Sync (now)             | Run `cortex.py` on EC2 via SSH (or POST to small API). |
| Nightly run (soon)     | Cron on EC2, or EventBridge → Lambda/ECS.             |
| Drive events (soon)    | Public HTTPS webhook → verify → run `decks hydrate` (or evaluate). Webhook = Lambda or small server on EC2. |

**Minimal path:** One EC2, cron for nightly health runs, and cron (or a webhook) that runs `decks hydrate` on a schedule so group-shared decks are picked up. Sync stays “you run the script” (or one POST to that same server).

If you want, next step can be a minimal Flask/FastAPI webhook stub in the repo (e.g. `scripts/drive_webhook.py`) that checks a shared secret and runs `decks hydrate` in the background, plus a small script to register a Drive watch if you use push notifications.

---

## Possible future: Bedrock Agents for the interactive agent

The **batch flows** (decks, hydrate, evaluate, cron, Drive webhook) don’t use LangChain; they use the OpenAI client for prompt parsing and your own Python for the rest. LangChain is only used for the **interactive agent** in `main.py` (conversational “ask about customers / generate a deck” with tools).

If you later expose that as a chat or API on AWS, you could move that agent from **LangChain to Bedrock Agents**: host the conversational agent in AWS, use Claude on Bedrock, and re-express the Pendo/deck tools as Bedrock action groups (e.g. Lambda per tool). That would give you an AWS-native agent and one less process to run for the chat path—but it’s optional; the sync/cron/Drive deployment above doesn’t depend on it.
