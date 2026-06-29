resource "aws_ecs_cluster" "cortex" {
  name = local.cluster_name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = local.common_tags
}

resource "aws_ecs_task_definition" "decks" {
  family                   = local.task_family
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  volume {
    name = "cortex-cache"
    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.cache.id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.cache.id
        iam             = "ENABLED"
      }
    }
  }

  container_definitions = jsonencode([
    {
      name      = "cortex-decks"
      image     = local.ecr_image
      essential = true
      entryPoint = [
        "/app/scripts/run_job.sh",
      ]
      command = var.default_job_command
      environment = local.container_environment
      mountPoints = [
        {
          sourceVolume  = "cortex-cache"
          containerPath = "/var/cortex/cache"
          readOnly      = false
        },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.decks.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    },
  ])

  tags = local.common_tags
}
