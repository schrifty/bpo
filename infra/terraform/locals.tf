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
      Project     = "bpo"
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

  container_environment = [
    { name = "BPO_SKIP_DOTENV", value = "1" },
    { name = "BPO_CACHE_DIR", value = "/var/bpo/cache" },
    { name = "BPO_LOG_FORMAT", value = "json" },
    { name = "BPO_FAIL_ON_INTEGRATION_WARNINGS", value = var.fail_on_integration_warnings ? "1" : "0" },
    { name = "BPO_JOB_TIMEOUT_SECONDS", value = tostring(var.job_timeout_seconds) },
    { name = "BPO_SECRETS_ARN", value = aws_secretsmanager_secret.bpo.arn },
  ]

  scheduled_jobs_enabled = {
    for k, v in var.scheduled_jobs : k => v if var.enable_schedules && v.enabled
  }
}
