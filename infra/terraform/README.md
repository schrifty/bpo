# Cortex AWS infrastructure (Terraform)

Idempotent replacement for manual IAM / EFS / ECS / EventBridge setup.

## What this creates

| Resource | Name (default) |
|----------|----------------|
| ECR repository | `cortex-decks` |
| Secrets Manager secret | `cortex/prod/env` |
| CloudWatch log group | `/cortex/decks` |
| EFS + access point | `cortex-cache` (uid/gid 1000) |
| IAM roles | `cortex-ecs-execution`, `cortex-ecs-task`, `cortex-eventbridge-ecs` (if schedules on) |
| ECS cluster | `cortex` |
| ECS task definition | `cortex-decks` |
| EventBridge rules | optional (`enable_schedules`) |

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5 (`brew install terraform`)
- AWS CLI configured (`aws sts get-caller-identity`)
- Docker (build/push image)

## Quick start

```bash
cd infra/terraform

cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set secrets_json_file when ready

# Build secrets JSON from laptop .env (gitignored output)
cd ../..
python3 scripts/build_secrets_manager_json.py

cd infra/terraform
terraform init
terraform plan
terraform apply
```

### Push container image

After first `apply` (creates ECR):

```bash
eval "$(terraform output -raw ecr_login_command)"

cd ../..   # repo root
docker build --platform linux/amd64 -t cortex-decks .
docker tag cortex-decks:latest $(terraform -chdir=infra/terraform output -raw ecr_repository_url):latest
docker push $(terraform -chdir=infra/terraform output -raw ecr_repository_url):latest
```

If you changed only the image, force new task revision:

```bash
cd infra/terraform
terraform apply -var="image_tag=latest"
# Or taint task definition if image URI unchanged but digest changed:
# aws ecs update-service ... OR change image_tag to a git sha
```

ECS uses `:latest` tag — after push, new tasks pull the new image. Re-run `run-task` for smoke test.

### Smoke test (manual run)

```bash
terraform output -raw run_task_engineering_portfolio | bash
```

Watch logs:

```bash
aws logs tail $(terraform output -raw cloudwatch_log_group) --follow
```

Look for `CORTEX_RUN_SUMMARY={"success":true,...}`.

### Enable nightly cron

In `terraform.tfvars`:

```hcl
enable_schedules = true
```

```bash
terraform apply
```

Jobs are defined in `variables.tf` → `scheduled_jobs` (default: `decks-engineering-portfolio` 02:00 UTC, `export-nightly` 03:00 UTC, `ford-pendo-daily` 04:00 UTC). Override `rule_name` on a job when the EventBridge rule should not use `{name_prefix}-{job_key}`.

## Variables (common)

| Variable | Default | Notes |
|----------|---------|-------|
| `use_default_vpc` | `true` | Easiest first deploy |
| `vpc_id` / `subnet_ids` | empty | Override for custom VPC |
| `assign_public_ip` | `true` | Set `false` with private subnets + NAT |
| `secrets_json_file` | empty | Path to `output/cortex-secrets-manager.json` |
| `enable_schedules` | `false` | EventBridge → ECS |
| `enable_schedule_alarms` | `true` | FailedInvocations + run-summary-failed + ECS task-failure SNS |
| `alarm_sns_topic_arn` | empty | Uses `${name_prefix}-cortex-schedule-alarms` when alarms enabled |
| `name_prefix` | `cortex` | Change if importing existing manual roles |

## Importing existing manual resources

If you created resources by hand before Terraform (common), import them once:

```bash
chmod +x ../../scripts/terraform_import_existing.sh
../../scripts/terraform_import_existing.sh
terraform plan
terraform apply
```

Or manually:

```bash
terraform import aws_cloudwatch_log_group.decks /cortex/decks
terraform import aws_ecr_repository.decks cortex-decks
terraform import aws_iam_role.ecs_execution cortex-ecs-execution
terraform import aws_iam_role.ecs_task cortex-ecs-task
terraform apply
```

If you prefer a clean slate instead, delete the manual ECR repo, log group, and IAM roles in the console, then `terraform apply` (only if nothing depends on them).

## Importing existing manual resources (alternate: new name prefix)

If you already created `cortex-ecs-execution` by hand, either:

1. Delete manual roles and `terraform apply`, or  
2. Set `name_prefix = "cortex-tf"` in tfvars to avoid name clash, or  
3. `terraform import` each resource (see AWS docs).

## Secrets without Terraform

Create the secret shell with Terraform, upload JSON once:

```bash
aws secretsmanager put-secret-value \
  --secret-id cortex/prod/env \
  --secret-string file://../../output/cortex-secrets-manager.json
```

## Destroy

```bash
terraform destroy
```

EFS and secrets have retention/recovery windows — read the plan before confirming.

## Legacy JSON templates

`infra/ecs-task-definition.json` and siblings are superseded by this module but kept as reference.
