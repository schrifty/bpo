variable "aws_region" {
  description = "AWS region for all Cortex resources."
  type        = string
  default     = "us-east-1"
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
  type    = number
  default = 7200
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
  description = "Optional path to Secrets Manager JSON (from scripts/build_secrets_manager_json.py). Creates/updates secret version when set."
  type        = string
  default     = ""
  sensitive   = true
}

variable "secret_recovery_window_days" {
  description = "Days before Secrets Manager permanently deletes a secret."
  type        = number
  default     = 7
}

# --- Schedules ---

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
    # EventBridge cron is UTC. Jobs run daily, 30 minutes apart starting 01:00 UTC.
    export-nightly = {
      schedule_expression = "cron(0 1 * * ? *)"
      command             = ["export-nightly"]
      enabled             = true
      rule_name           = "cortex-export-nightly"
    }
    engineering-portfolio = {
      schedule_expression = "cron(30 1 * * ? *)"
      command             = ["engineering-portfolio"]
      enabled             = true
      rule_name           = "cortex-engineering-portfolio"
    }
    ford-pendo-7d = {
      schedule_expression = "cron(0 2 * * ? *)"
      command             = ["ford-pendo-7d"]
      enabled             = true
      rule_name           = "cortex-ford-pendo-7d"
    }
    ford-pendo-30d = {
      schedule_expression = "cron(30 2 * * ? *)"
      command             = ["ford-pendo-30d"]
      enabled             = true
      rule_name           = "cortex-ford-pendo-30d"
    }
    # Daily 3:00 AM US/Central (CST) = 09:00 UTC; 4:00 AM during CDT.
    pendo-top-arr-30d = {
      schedule_expression = "cron(0 9 * * ? *)"
      command             = ["pendo-top-arr-30d"]
      enabled             = true
      rule_name           = "cortex-pendo-top-arr-30d"
    }
    # Sunday 11:00 PM US/Central (CST) = Monday 05:00 UTC; 10:00 PM during CDT.
    metrics-eng-cycle-lead-weekly = {
      schedule_expression = "cron(0 5 ? * MON *)"
      command             = ["metrics-eng-cycle-lead-weekly"]
      enabled             = true
      rule_name           = "cortex-metrics-eng-cycle-lead-weekly"
    }
  }
}

variable "tags" {
  description = "Additional tags for all resources."
  type        = map(string)
  default     = {}
}
