# One-shot job retries (EventBridge Scheduler → ECS) after retryable overnight failures.

resource "aws_scheduler_schedule_group" "job_retries" {
  count = var.enable_schedules && var.enable_job_retries ? 1 : 0

  name = "${var.name_prefix}-job-retries"
  tags = local.common_tags
}

data "aws_iam_policy_document" "scheduler_assume" {
  count = var.enable_schedules && var.enable_job_retries ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler_ecs" {
  count              = var.enable_schedules && var.enable_job_retries ? 1 : 0
  name               = "${var.name_prefix}-scheduler-ecs"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume[0].json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "scheduler_ecs" {
  count = var.enable_schedules && var.enable_job_retries ? 1 : 0

  statement {
    effect    = "Allow"
    actions   = ["ecs:RunTask"]
    resources = ["arn:aws:ecs:${var.aws_region}:${local.account_id}:task-definition/${local.task_family}:*"]
  }

  statement {
    effect = "Allow"
    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.ecs_execution.arn,
      aws_iam_role.ecs_task.arn,
    ]
  }
}

resource "aws_iam_role_policy" "scheduler_ecs" {
  count  = var.enable_schedules && var.enable_job_retries ? 1 : 0
  name   = "${var.name_prefix}-scheduler-ecs-policy"
  role   = aws_iam_role.scheduler_ecs[0].id
  policy = data.aws_iam_policy_document.scheduler_ecs[0].json
}
