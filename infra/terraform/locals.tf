locals {
  account_id = data.aws_caller_identity.current.account_id

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
  cluster_name        = var.name_prefix
  task_family         = "${var.name_prefix}-decks"

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
      { name = "CORTEX_SECRETS_ARN", value = aws_secretsmanager_secret.cortex.arn },
      { name = "CORTEX_ALLOW_PRODUCTION_MUTATIONS", value = "true" },
    ],
    var.enable_schedules && var.enable_job_retries ? [
      { name = "CORTEX_JOB_RETRY_ENABLED", value = "1" },
      { name = "CORTEX_JOB_RETRY_DELAY_MINUTES", value = tostring(var.job_retry_delay_minutes) },
      { name = "CORTEX_JOB_RETRY_MAX_ATTEMPTS", value = tostring(var.job_retry_max_attempts) },
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
