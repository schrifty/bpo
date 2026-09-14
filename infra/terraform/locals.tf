locals {
  account_id       = data.aws_caller_identity.current.account_id
  ses_identity     = trimspace(var.ses_identity)
  ses_from_address = trimspace(var.ses_from_address)
  ses_identity_arn = "arn:aws:ses:${var.aws_region}:${local.account_id}:identity/${local.ses_identity}"

  vpc_id = var.vpc_id != "" ? var.vpc_id : (
    var.use_default_vpc ? data.aws_vpc.default[0].id : ""
  )

  subnet_ids = length(var.subnet_ids) > 0 ? var.subnet_ids : (
    local.vpc_id != "" ? data.aws_subnets.selected[0].ids : []
  )

  common_tags = merge(
    {
      Project     = "cortex"
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags,
  )

  ecr_repository_name = "${var.name_prefix}-decks"
  log_group_name      = "/${var.name_prefix}/decks"
  secret_name         = "${var.name_prefix}/${var.environment}/env"
  secret_bundles      = ["google", "integrations", "llm", "slack"]
  # Path only; var is marked sensitive so plans hide it — for_each needs a non-sensitive map.
  secrets_json_file_path = nonsensitive(var.secrets_json_file)
  bundle_secret_files = {
    for b in local.secret_bundles :
    b => (
      var.secrets_json_dir != "" && fileexists("${var.secrets_json_dir}/${b}.json") ? "${var.secrets_json_dir}/${b}.json" :
      (b == "integrations" && local.secrets_json_file_path != "" ? local.secrets_json_file_path : null)
    )
    if(
      (var.secrets_json_dir != "" && fileexists("${var.secrets_json_dir}/${b}.json")) ||
      (b == "integrations" && local.secrets_json_file_path != "")
    )
  }
  secrets_arns_csv = join(",", concat(
    [aws_secretsmanager_secret.cortex.arn],
    [for b in local.secret_bundles : aws_secretsmanager_secret.bundle[b].arn],
  ))

  # Job-family secret profiles. Keep in sync with src/secrets_bundles.py JOB_SECRET_PROFILE.
  secret_profile_bundles = {
    llm     = ["google", "integrations", "llm", "slack"]
    decks   = ["google", "integrations"]
    metrics = ["integrations"]
  }
  job_secret_profile = {
    "llm-context-portfolio-daily" = "llm"
    "engineering-portfolio"       = "llm"
    "engineering-kpis"            = "decks"
    "pendo-snapshot-refresh"      = "decks"
    "pendo-ford-7d"               = "decks"
    "pendo-ford-30d"              = "decks"
    "pendo-top-arr-detailed"      = "decks"
    "csr-dump-0000"               = "decks"
    "csr-dump-0600"               = "decks"
    "csr-dump-1200"               = "decks"
    "csr-dump-1800"               = "decks"
    "morning-report"              = "metrics"
  }
  secret_profile_arns = {
    for profile, bundles in local.secret_profile_bundles :
    profile => [for b in bundles : aws_secretsmanager_secret.bundle[b].arn]
  }
  secret_profile_arns_csv = {
    for profile, arns in local.secret_profile_arns : profile => join(",", arns)
  }
  task_role_arn_by_profile = merge(
    { full = aws_iam_role.ecs_task.arn },
    { for profile, role in aws_iam_role.ecs_task_profile : profile => role.arn },
  )
  cluster_name = var.name_prefix
  task_family  = "${var.name_prefix}-decks"

  execution_role_name = "${var.name_prefix}-ecs-execution"
  task_role_name      = "${var.name_prefix}-ecs-task"
  events_role_name    = "${var.name_prefix}-eventbridge-ecs"

  ecr_image = "${aws_ecr_repository.decks.repository_url}:${var.image_tag}"

  container_environment = concat(
    [
      { name = "CORTEX_SKIP_DOTENV", value = "1" },
      { name = "CORTEX_CACHE_DIR", value = "/var/cortex/cache" },
      { name = "CORTEX_LOG_FORMAT", value = "json" },
      { name = "CORTEX_FAIL_ON_INTEGRATION_WARNINGS", value = var.fail_on_integration_warnings ? "1" : "0" },
      { name = "CORTEX_JOB_TIMEOUT_SECONDS", value = tostring(var.job_timeout_seconds) },
      { name = "CORTEX_SECRETS_ARNS", value = local.secrets_arns_csv },
      { name = "CORTEX_SECRETS_ARN", value = aws_secretsmanager_secret.bundle["integrations"].arn },
      { name = "CORTEX_ECS_TASK_ROLE_ARN", value = aws_iam_role.ecs_task.arn },
      # Finance constant (not a secret); SM blob should match so local/.env and ECS agree.
      { name = "CORTEX_MONTHLY_SPEND_USD_ENGINEERING", value = "436000" },
    ],
    var.enable_schedules && var.enable_job_retries ? [
      { name = "CORTEX_JOB_RETRY_ENABLED", value = "1" },
      { name = "CORTEX_JOB_RETRY_DELAY_MINUTES", value = tostring(var.job_retry_delay_minutes) },
      { name = "CORTEX_JOB_RETRY_MAX_ATTEMPTS", value = tostring(var.job_retry_max_attempts) },
      { name = "CORTEX_JOB_RETRY_MAX_ELAPSED_SECONDS", value = tostring(var.job_retry_max_elapsed_seconds) },
      { name = "CORTEX_JOB_RETRY_SCHEDULE_GROUP", value = "${var.name_prefix}-job-retries" },
      { name = "CORTEX_ECS_CLUSTER_ARN", value = aws_ecs_cluster.cortex.arn },
      { name = "CORTEX_ECS_TASK_DEFINITION_ARN", value = "arn:aws:ecs:${var.aws_region}:${local.account_id}:task-definition/${local.task_family}" },
      { name = "CORTEX_ECS_SUBNETS", value = join(",", local.subnet_ids) },
      { name = "CORTEX_ECS_SECURITY_GROUPS", value = aws_security_group.ecs_tasks.id },
      { name = "CORTEX_ECS_ASSIGN_PUBLIC_IP", value = var.assign_public_ip ? "ENABLED" : "DISABLED" },
      { name = "CORTEX_ECS_CONTAINER_NAME", value = "cortex-decks" },
      { name = "CORTEX_SCHEDULER_ROLE_ARN", value = aws_iam_role.scheduler_ecs[0].arn },
    ] : [],
  )

  scheduled_jobs_enabled = {
    for k, v in var.scheduled_jobs : k => v if var.enable_schedules && v.enabled
  }
}
