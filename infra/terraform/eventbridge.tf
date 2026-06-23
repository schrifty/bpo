resource "aws_cloudwatch_event_rule" "job" {
  for_each = local.scheduled_jobs_enabled

  name                = "${var.name_prefix}-${each.key}"
  description         = "BPO scheduled job: ${each.key}"
  schedule_expression = each.value.schedule_expression
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "job" {
  for_each = local.scheduled_jobs_enabled

  rule      = aws_cloudwatch_event_rule.job[each.key].name
  target_id = "${var.name_prefix}-${each.key}"
  arn       = aws_ecs_cluster.bpo.arn
  role_arn  = aws_iam_role.eventbridge_ecs[0].arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.decks.arn
    launch_type         = "FARGATE"
    platform_version    = "LATEST"

    network_configuration {
      subnets          = local.subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = var.assign_public_ip
    }
  }

  input = jsonencode({
    containerOverrides = [{
      name    = "bpo-decks"
      command = each.value.command
    }]
  })
}
