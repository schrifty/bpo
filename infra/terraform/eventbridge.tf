resource "aws_cloudwatch_event_rule" "job" {
  for_each = local.scheduled_jobs_enabled

  name                = coalesce(each.value.rule_name, "${var.name_prefix}-${each.key}")
  description         = "Cortex scheduled job: ${each.key}"
  schedule_expression = each.value.schedule_expression
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "job" {
  for_each = local.scheduled_jobs_enabled

  rule      = aws_cloudwatch_event_rule.job[each.key].name
  target_id = coalesce(each.value.rule_name, "${var.name_prefix}-${each.key}")
  arn       = aws_ecs_cluster.cortex.arn
  role_arn  = aws_iam_role.eventbridge_ecs[0].arn

  ecs_target {
    task_count          = 1
    task_definition_arn = aws_ecs_task_definition.decks.arn
    launch_type         = "FARGATE"
    platform_version    = "LATEST"
    task_role_arn       = local.task_role_arn_by_profile[lookup(local.job_secret_profile, each.key, "full")]

    network_configuration {
      subnets          = local.subnet_ids
      security_groups  = [aws_security_group.ecs_tasks.id]
      assign_public_ip = var.assign_public_ip
    }
  }

  input = jsonencode({
    containerOverrides = [{
      name    = "cortex-decks"
      command = each.value.command
      environment = [
        {
          name  = "CORTEX_SECRETS_ARNS"
          value = lookup(local.secret_profile_arns_csv, lookup(local.job_secret_profile, each.key, "full"), local.secrets_arns_csv)
        },
        {
          name  = "CORTEX_ECS_TASK_ROLE_ARN"
          value = local.task_role_arn_by_profile[lookup(local.job_secret_profile, each.key, "full")]
        },
      ]
    }]
  })

  retry_policy {
    maximum_event_age_in_seconds = 7200
    maximum_retry_attempts       = 2
  }
}
