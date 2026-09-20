variable "aws_region" {
  description = "AWS region for all Cortex resources."
  type        = string
  default     = "us-east-2"
}

variable "name_prefix" {
  description = "Prefix for resource names (e.g. cortex, cortex-prod)."
  type        = string
  default     = "cortex"
}

variable "environment" {
  description = "Environment tag (prod, staging)."
  type        = string
  default     = "prod"
}

variable "ses_identity" {
  description = "Verified SES identity (domain or email) that morning-report may send as. IAM SendEmail is limited to this identity ARN."
  type        = string
  default     = "leandna.com"
}

variable "ses_from_address" {
  description = "Optional exact From address for ses:FromAddress. Empty allows any address on ses_identity. Must match CORTEX_METRICS_DIGEST_FROM when set."
  type        = string
  default     = ""
}

# --- Network ---

variable "use_default_vpc" {
  description = "When true and vpc_id is empty, use the account default VPC and its subnets."
  type        = bool
  default     = true
}

variable "vpc_id" {
  description = "VPC for ECS and EFS. Leave empty to use default VPC when use_default_vpc is true."
  type        = string
  default     = ""
}

variable "subnet_ids" {
  description = "Subnets for Fargate tasks and EFS mount targets. Empty = all subnets in the chosen VPC."
  type        = list(string)
  default     = []
}

variable "assign_public_ip" {
  description = "Assign a public IP to Fargate tasks (simplest path without NAT). Use false with private subnets + NAT."
  type        = bool
  default     = true
}

# --- Container ---

variable "image_tag" {
  description = "ECR image tag for cortex-decks (push before apply or after first apply + push)."
  type        = string
  default     = "latest"
}

variable "default_job_command" {
  description = "Default container command passed to scripts/run_job.sh (ECS task definition default)."
  type        = list(string)
  default     = ["engineering-portfolio"]
}

variable "task_cpu" {
  type    = number
  default = 4096
}

variable "task_memory" {
  type    = number
  default = 16384
}

variable "job_timeout_seconds" {
  description = "Wall-clock limit for a scheduled job step (CORTEX_JOB_TIMEOUT_SECONDS)."
  type        = number
  default     = 14400
}

variable "enable_job_retries" {
  description = "When schedules are enabled, allow failed jobs to create one-shot EventBridge Scheduler retries."
  type        = bool
  default     = true
}

variable "job_retry_delay_minutes" {
  description = "Minutes to wait before a one-shot retry of a failed scheduled job."
  type        = number
  default     = 15
}

variable "job_retry_max_attempts" {
  description = "Max retry attempts after the original run (1 = one delayed re-run)."
  type        = number
  default     = 1
}

variable "job_retry_max_elapsed_seconds" {
  description = "Skip one-shot retries when a failed step ran at least this long (start/quick failures still retry)."
  type        = number
  default     = 300
}

variable "fail_on_integration_warnings" {
  type    = bool
  default = false
}

variable "enable_schedule_alarms" {
  description = "CloudWatch alarms + SNS notifications for scheduled job failures (requires enable_schedules)."
  type        = bool
  default     = true
}

variable "alarm_sns_topic_arn" {
  description = "Optional existing SNS topic for schedule alarms. When empty and enable_schedule_alarms is true, Terraform creates a topic named {name_prefix}-cortex-schedule-alarms."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  type    = number
  default = 30
}

# --- Secrets ---

variable "secrets_json_file" {
  description = "Optional combined Secrets Manager JSON. When secrets_json_dir is empty, uploaded to cortex/prod/env (legacy) and the integrations bundle."
  type        = string
  default     = ""
  sensitive   = true
}

variable "secrets_json_dir" {
  description = "Directory of split JSON files from scripts/build_secrets_manager_json.py (google.json, integrations.json, llm.json, slack.json)."
  type        = string
  default     = ""
}

variable "secret_recovery_window_days" {
  description = "Days before Secrets Manager permanently deletes a secret."
  type        = number
  default     = 7
}

# --- Schedules ---

variable "kpi_store_s3_uri" {
  description = <<-EOT
    Optional s3://bucket/key for the KPI SQLite store (CORTEX_KPI_STORE_S3_URI).
    When set, Terraform injects the env var on the task definition and grants
    s3:GetObject/PutObject on that object to ECS task roles (full + metrics + decks).
    Leave empty to persist on EFS only (config/jobs/kpi-snapshot.yaml uses skip_s3).
  EOT
  type        = string
  default     = ""

  validation {
    condition = (
      trimspace(var.kpi_store_s3_uri) == "" ||
      can(regex("^s3://[^/]+/.+$", trimspace(var.kpi_store_s3_uri)))
    )
    error_message = "kpi_store_s3_uri must be empty or s3://bucket/key."
  }
}

variable "enable_schedules" {
  description = "Create EventBridge rules that run ECS Fargate tasks on a cron."
  type        = bool
  default     = false
}

variable "scheduled_jobs" {
  description = "Map of scheduled deck jobs (command = args to run_job.sh)."
  type = map(object({
    schedule_expression = string
    command             = list(string)
    enabled             = bool
    rule_name           = optional(string)
  }))
  default = {
    # EventBridge cron is UTC. Shared Pendo ingest runs first (03:00). Snapshot
    # consumers start at 07:00 so they sit after EventBridge's 2h RunTask retry
    # window plus snapshot runtime (~20m). Remaining Pendo transforms stay 30
    # minutes apart. CSR dumps stay locked to CDT wall-clock slots.
    pendo-snapshot-refresh = {
      schedule_expression = "cron(0 3 * * ? *)"
      command             = ["pendo-snapshot-refresh"]
      enabled             = true
      rule_name           = "cortex-pendo-snapshot-refresh"
    }
    llm-context-portfolio-daily = {
      schedule_expression = "cron(0 7 * * ? *)"
      command             = ["llm-context-portfolio-daily"]
      enabled             = true
      rule_name           = "cortex-llm-context-portfolio-daily"
    }
    engineering-portfolio = {
      schedule_expression = "cron(30 7 * * ? *)"
      command             = ["engineering-portfolio"]
      enabled             = true
      rule_name           = "cortex-engineering-portfolio"
    }
    engineering-kpis = {
      schedule_expression = "cron(45 7 * * ? *)"
      command             = ["engineering-kpis"]
      enabled             = true
      rule_name           = "cortex-engineering-kpis"
    }
    # Persist registry KPIs to SQLite (EFS; optional S3 when CORTEX_KPI_STORE_S3_URI set).
    # Runs before engineering-kpis so history charts can read today's rows.
    kpi-snapshot = {
      schedule_expression = "cron(15 7 * * ? *)"
      command             = ["kpi-snapshot"]
      enabled             = true
      rule_name           = "cortex-kpi-snapshot"
    }
    pendo-ford-7d = {
      schedule_expression = "cron(0 8 * * ? *)"
      command             = ["pendo-ford-7d"]
      enabled             = true
      rule_name           = "cortex-pendo-ford-7d"
    }
    pendo-ford-30d = {
      schedule_expression = "cron(30 8 * * ? *)"
      command             = ["pendo-ford-30d"]
      enabled             = true
      rule_name           = "cortex-pendo-ford-30d"
    }
    pendo-top-arr-detailed = {
      schedule_expression = "cron(0 9 * * ? *)"
      command             = ["pendo-top-arr-detailed"]
      enabled             = true
      rule_name           = "cortex-pendo-top-arr-detailed"
    }
    # CSR full dumps: UTC crons locked to current CDT hours
    # (midnight / 6am / noon / 6pm Chicago). After CST, these UTC hours shift
    # one hour earlier on the Chicago clock.
    csr-dump-0000 = {
      schedule_expression = "cron(0 5 * * ? *)"
      command             = ["csr-dump-0000"]
      enabled             = true
      rule_name           = "cortex-csr-dump-0000"
    }
    csr-dump-0600 = {
      schedule_expression = "cron(0 11 * * ? *)"
      command             = ["csr-dump-0600"]
      enabled             = true
      rule_name           = "cortex-csr-dump-0600"
    }
    csr-dump-1200 = {
      schedule_expression = "cron(0 17 * * ? *)"
      command             = ["csr-dump-1200"]
      enabled             = true
      rule_name           = "cortex-csr-dump-1200"
    }
    csr-dump-1800 = {
      schedule_expression = "cron(0 23 * * ? *)"
      command             = ["csr-dump-1800"]
      enabled             = true
      rule_name           = "cortex-csr-dump-1800"
    }
    # DISABLED until SES domain DKIM/DNS + CORTEX_METRICS_DIGEST_* secrets.
    # Enable path: docs/SETUP/KPI_OPS.md (set enabled = true after Marc AWS SES action).
    morning-report = {
      schedule_expression = "cron(0 12 * * ? *)"
      command             = ["morning-report"]
      enabled             = false
      rule_name           = "cortex-morning-report"
    }
  }
}

variable "tags" {
  description = "Additional tags for all resources."
  type        = map(string)
  default     = {}
}
