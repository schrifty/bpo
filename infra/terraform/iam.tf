data "aws_iam_policy_document" "ecs_task_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name               = local.execution_role_name
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "ecs_task" {
  statement {
    sid    = "ReadBpoSecrets"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = concat(
      [for b in local.secret_bundles : aws_secretsmanager_secret.bundle[b].arn],
      [aws_secretsmanager_secret.cortex.arn],
    )
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
        # Created in scheduler.tf; reference by name pattern when count is 0 is avoided via dynamic.
        "arn:aws:iam::${local.account_id}:role/${var.name_prefix}-scheduler-ecs",
      ]
    }
  }

  dynamic "statement" {
    for_each = local.kpi_store_s3_enabled ? [1] : []
    content {
      sid    = "KpiStoreS3Object"
      effect = "Allow"
      actions = [
        "s3:GetObject",
        "s3:PutObject",
      ]
      resources = [local.kpi_store_s3_object_arn]
    }
  }
}

resource "aws_iam_role" "ecs_task" {
  name               = local.task_role_name
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "ecs_task" {
  name   = "${local.task_role_name}-policy"
  role   = aws_iam_role.ecs_task.id
  policy = data.aws_iam_policy_document.ecs_task.json
}

data "aws_iam_policy_document" "ses_send_metrics_digest" {
  statement {
    sid       = "SesSendMetricsDigest"
    effect    = "Allow"
    actions   = ["ses:SendEmail"]
    resources = [local.ses_identity_arn]

    dynamic "condition" {
      for_each = local.ses_from_address != "" ? [1] : []
      content {
        test     = "StringEquals"
        variable = "ses:FromAddress"
        values   = [local.ses_from_address]
      }
    }
  }
}

resource "aws_iam_role_policy" "ecs_task_ses" {
  name   = "${local.task_role_name}-ses"
  role   = aws_iam_role.ecs_task.id
  policy = data.aws_iam_policy_document.ses_send_metrics_digest.json
}

resource "aws_iam_role_policy" "ecs_task_metrics_ses" {
  name   = "${local.task_role_name}-metrics-ses"
  role   = aws_iam_role.ecs_task_profile["metrics"].id
  policy = data.aws_iam_policy_document.ses_send_metrics_digest.json
}

# EventBridge → ECS RunTask

data "aws_iam_policy_document" "eventbridge_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eventbridge_ecs" {
  count              = var.enable_schedules ? 1 : 0
  name               = local.events_role_name
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "eventbridge_ecs" {
  count = var.enable_schedules ? 1 : 0

  statement {
    effect    = "Allow"
    actions   = ["ecs:RunTask"]
    resources = ["arn:aws:ecs:${var.aws_region}:${local.account_id}:task-definition/${local.task_family}:*"]
  }

  statement {
    effect  = "Allow"
    actions = ["iam:PassRole"]
    resources = concat(
      [
        aws_iam_role.ecs_execution.arn,
        aws_iam_role.ecs_task.arn,
      ],
      [for role in aws_iam_role.ecs_task_profile : role.arn],
    )
  }
}

resource "aws_iam_role_policy" "eventbridge_ecs" {
  count  = var.enable_schedules ? 1 : 0
  name   = "${local.events_role_name}-policy"
  role   = aws_iam_role.eventbridge_ecs[0].id
  policy = data.aws_iam_policy_document.eventbridge_ecs[0].json
}
