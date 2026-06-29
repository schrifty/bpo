# Monitoring for scheduled ECS batch jobs (EventBridge → Fargate).

locals {
  alarm_actions = var.alarm_sns_topic_arn != "" ? [var.alarm_sns_topic_arn] : (
    var.enable_schedule_alarms && length(aws_sns_topic.schedule_alarms) > 0 ? [aws_sns_topic.schedule_alarms[0].arn] : []
  )
}

resource "aws_sns_topic" "schedule_alarms" {
  count = var.enable_schedules && var.enable_schedule_alarms && var.alarm_sns_topic_arn == "" ? 1 : 0
  name  = "${var.name_prefix}-cortex-schedule-alarms"
  tags  = local.common_tags
}

resource "aws_cloudwatch_log_metric_filter" "run_summary_failed" {
  count          = var.enable_schedule_alarms ? 1 : 0
  name           = "${var.name_prefix}-run-summary-failed"
  log_group_name = aws_cloudwatch_log_group.decks.name
  pattern        = "\"CORTEX_RUN_SUMMARY\" \"\\\"success\\\":false\""

  metric_transformation {
    name      = "RunSummaryFailed"
    namespace = "Cortex/Schedules"
    value     = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "run_summary_failed" {
  count               = var.enable_schedule_alarms ? 1 : 0
  alarm_name          = "${var.name_prefix}-run-summary-failed"
  alarm_description   = "CORTEX_RUN_SUMMARY reported success=false in ${local.log_group_name}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "RunSummaryFailed"
  namespace           = "Cortex/Schedules"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  tags                = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "eventbridge_failed_invocations" {
  for_each = var.enable_schedules && var.enable_schedule_alarms ? local.scheduled_jobs_enabled : {}

  alarm_name          = "${var.name_prefix}-eb-failed-${each.key}"
  alarm_description   = "EventBridge failed to invoke ECS for scheduled job ${each.key} (${coalesce(each.value.rule_name, "${var.name_prefix}-${each.key}")})"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "FailedInvocations"
  namespace           = "AWS/Events"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  tags                = local.common_tags

  dimensions = {
    RuleName = aws_cloudwatch_event_rule.job[each.key].name
  }
}

# ECS tasks started by EventBridge that stop with a non-zero exit code.
resource "aws_cloudwatch_event_rule" "ecs_scheduled_task_bad_exit" {
  count          = var.enable_schedules && var.enable_schedule_alarms ? 1 : 0
  name           = "${var.name_prefix}-ecs-scheduled-task-bad-exit"
  description    = "Scheduled Cortex ECS tasks that stopped with a non-zero container exit code"
  event_bus_name = "default"
  tags           = local.common_tags

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      clusterArn = [aws_ecs_cluster.cortex.arn]
      lastStatus = ["STOPPED"]
      group      = ["family:${local.task_family}"]
      startedBy  = [{ "prefix" = "events.amazonaws.com" }]
      containers = {
        exitCode = [{ "anything-but" = [0] }]
      }
    }
  })
}

# Tasks that never start the container (no app logs): TaskFailedToStart, CannotPullContainerError, etc.
resource "aws_cloudwatch_event_rule" "ecs_scheduled_task_failed_to_start" {
  count          = var.enable_schedules && var.enable_schedule_alarms ? 1 : 0
  name           = "${var.name_prefix}-ecs-scheduled-task-failed-to-start"
  description    = "Scheduled Cortex ECS tasks that failed before the container ran"
  event_bus_name = "default"
  tags           = local.common_tags

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      clusterArn = [aws_ecs_cluster.cortex.arn]
      lastStatus = ["STOPPED"]
      group      = ["family:${local.task_family}"]
      startedBy  = [{ "prefix" = "events.amazonaws.com" }]
      stoppedReason = [
        { "prefix" = "Task failed to start" },
        { "prefix" = "CannotPullContainerError" },
        { "prefix" = "ResourceInitializationError" },
      ]
    }
  })
}

resource "aws_cloudwatch_event_target" "ecs_scheduled_task_bad_exit_sns" {
  count     = var.enable_schedules && var.enable_schedule_alarms && length(local.alarm_actions) > 0 ? 1 : 0
  rule      = aws_cloudwatch_event_rule.ecs_scheduled_task_bad_exit[0].name
  target_id = "sns"
  arn       = local.alarm_actions[0]
  input_transformer {
    input_paths = {
      task   = "$.detail.taskArn"
      reason = "$.detail.stoppedReason"
      group  = "$.detail.group"
      exit   = "$.detail.containers[0].exitCode"
    }
    input_template = "\"Cortex scheduled ECS task failed (non-zero exit). task=<task> group=<group> exit=<exit> reason=<reason>\""
  }
}

resource "aws_cloudwatch_event_target" "ecs_scheduled_task_failed_to_start_sns" {
  count     = var.enable_schedules && var.enable_schedule_alarms && length(local.alarm_actions) > 0 ? 1 : 0
  rule      = aws_cloudwatch_event_rule.ecs_scheduled_task_failed_to_start[0].name
  target_id = "sns"
  arn       = local.alarm_actions[0]
  input_transformer {
    input_paths = {
      task   = "$.detail.taskArn"
      reason = "$.detail.stoppedReason"
      group  = "$.detail.group"
    }
    input_template = "\"Cortex scheduled ECS task failed to start. task=<task> group=<group> reason=<reason>\""
  }
}

resource "aws_sns_topic_policy" "schedule_alarms_events" {
  count = var.enable_schedules && var.enable_schedule_alarms && length(local.alarm_actions) > 0 ? 1 : 0
  arn   = local.alarm_actions[0]

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridgePublish"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = local.alarm_actions[0]
      Condition = {
        ArnLike = {
          "aws:SourceArn" = [
            aws_cloudwatch_event_rule.ecs_scheduled_task_bad_exit[0].arn,
            aws_cloudwatch_event_rule.ecs_scheduled_task_failed_to_start[0].arn,
          ]
        }
      }
    }]
  })
}
