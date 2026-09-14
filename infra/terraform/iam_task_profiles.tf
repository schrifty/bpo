# Job-family ECS task roles: GetSecretValue only for that profile's bundles.
# cortex-ecs-task (full) stays the default task-definition role for ad-hoc run-task.

resource "aws_iam_role" "ecs_task_profile" {
  for_each = local.secret_profile_bundles

  name               = "${local.task_role_name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = merge(local.common_tags, { CortexSecretProfile = each.key })
}

data "aws_iam_policy_document" "ecs_task_profile" {
  for_each = local.secret_profile_bundles

  statement {
    sid    = "ReadProfileSecrets"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = local.secret_profile_arns[each.key]
  }

  statement {
    sid    = "MountEfsCache"
    effect = "Allow"
    actions = [
      "elasticfilesystem:ClientMount",
      "elasticfilesystem:ClientWrite",
    ]
    resources = [aws_efs_file_system.cache.arn]
  }

  dynamic "statement" {
    for_each = each.key == "metrics" ? [1] : []
    content {
      sid    = "SesSendMetricsDigest"
      effect = "Allow"
      actions = [
        "ses:SendEmail",
        "ses:SendRawEmail",
      ]
      resources = ["*"]
    }
  }

  dynamic "statement" {
    for_each = var.enable_schedules && var.enable_job_retries ? [1] : []
    content {
      sid    = "CreateJobRetrySchedules"
      effect = "Allow"
      actions = [
        "scheduler:CreateSchedule",
        "scheduler:GetSchedule",
        "scheduler:UpdateSchedule",
        "scheduler:DeleteSchedule",
      ]
      resources = [
        "arn:aws:scheduler:${var.aws_region}:${local.account_id}:schedule/${var.name_prefix}-job-retries/*",
      ]
    }
  }

  dynamic "statement" {
    for_each = var.enable_schedules && var.enable_job_retries ? [1] : []
    content {
      sid    = "PassSchedulerEcsRole"
      effect = "Allow"
      actions = [
        "iam:PassRole",
      ]
      resources = [
        "arn:aws:iam::${local.account_id}:role/${var.name_prefix}-scheduler-ecs",
      ]
    }
  }
}

resource "aws_iam_role_policy" "ecs_task_profile" {
  for_each = local.secret_profile_bundles

  name   = "${local.task_role_name}-${each.key}-policy"
  role   = aws_iam_role.ecs_task_profile[each.key].id
  policy = data.aws_iam_policy_document.ecs_task_profile[each.key].json
}
