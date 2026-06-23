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
    resources = [aws_secretsmanager_secret.bpo.arn]
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
    effect = "Allow"
    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.ecs_execution.arn,
      aws_iam_role.ecs_task.arn,
    ]
  }
}

resource "aws_iam_role_policy" "eventbridge_ecs" {
  count  = var.enable_schedules ? 1 : 0
  name   = "${local.events_role_name}-policy"
  role   = aws_iam_role.eventbridge_ecs[0].id
  policy = data.aws_iam_policy_document.eventbridge_ecs[0].json
}
