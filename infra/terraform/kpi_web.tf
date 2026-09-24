# KPI catalog UI: Fargate service + ALB (HTTP origin) + CloudFront (HTTPS).
# Google OAuth redirect is the CloudFront URL; add it in the Workspace OAuth client.

locals {
  kpi_web_port = 8080
  kpi_web_name = "${var.name_prefix}-kpi-web"
}

data "aws_ec2_managed_prefix_list" "cloudfront_origin" {
  count = var.enable_kpi_web ? 1 : 0
  name  = "com.amazonaws.global.cloudfront.origin-facing"
}

data "aws_cloudfront_cache_policy" "caching_disabled" {
  count = var.enable_kpi_web ? 1 : 0
  name  = "Managed-CachingDisabled"
}

data "aws_cloudfront_origin_request_policy" "all_viewer_except_host" {
  count = var.enable_kpi_web ? 1 : 0
  name  = "Managed-AllViewerExceptHostHeader"
}

resource "random_password" "kpi_web_session" {
  count   = var.enable_kpi_web ? 1 : 0
  length  = 48
  special = false
}

resource "aws_secretsmanager_secret" "kpi_web" {
  count                   = var.enable_kpi_web ? 1 : 0
  name                    = "${var.name_prefix}/${var.environment}/kpi-web"
  recovery_window_in_days = var.secret_recovery_window_days
  tags                    = merge(local.common_tags, { CortexSecretBundle = "kpi-web" })
}

resource "aws_secretsmanager_secret_version" "kpi_web" {
  count     = var.enable_kpi_web ? 1 : 0
  secret_id = aws_secretsmanager_secret.kpi_web[0].id
  secret_string = jsonencode({
    CORTEX_KPI_WEB_SESSION_SECRET  = random_password.kpi_web_session[0].result
    CORTEX_KPI_WEB_ALLOWED_DOMAINS = var.kpi_web_allowed_domains
  })

  lifecycle {
    # Google OAuth client id/secret are added with put-secret-value; do not revert.
    ignore_changes = [secret_string]
  }
}

resource "aws_security_group" "kpi_web_alb" {
  count       = var.enable_kpi_web ? 1 : 0
  name        = "${local.kpi_web_name}-alb"
  description = "ALB for Cortex KPI web (CloudFront origin only)"
  vpc_id      = local.vpc_id

  ingress {
    description     = "HTTP from CloudFront"
    from_port       = 80
    to_port         = 80
    protocol        = "tcp"
    prefix_list_ids = [data.aws_ec2_managed_prefix_list.cloudfront_origin[0].id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, { Name = "${local.kpi_web_name}-alb" })
}

resource "aws_security_group" "kpi_web_tasks" {
  count       = var.enable_kpi_web ? 1 : 0
  name        = "${local.kpi_web_name}-tasks"
  description = "KPI web Fargate tasks (ALB to container)"
  vpc_id      = local.vpc_id

  ingress {
    description     = "App from ALB"
    from_port       = local.kpi_web_port
    to_port         = local.kpi_web_port
    protocol        = "tcp"
    security_groups = [aws_security_group.kpi_web_alb[0].id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.common_tags, { Name = "${local.kpi_web_name}-tasks" })
}

resource "aws_lb" "kpi_web" {
  count              = var.enable_kpi_web ? 1 : 0
  name               = local.kpi_web_name
  load_balancer_type = "application"
  internal           = false
  security_groups    = [aws_security_group.kpi_web_alb[0].id]
  subnets            = local.subnet_ids
  idle_timeout       = 60
  tags               = merge(local.common_tags, { Name = local.kpi_web_name })
}

resource "aws_lb_target_group" "kpi_web" {
  count       = var.enable_kpi_web ? 1 : 0
  name        = local.kpi_web_name
  port        = local.kpi_web_port
  protocol    = "HTTP"
  vpc_id      = local.vpc_id
  target_type = "ip"

  health_check {
    enabled             = true
    path                = "/api/health"
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  deregistration_delay = 30
  tags                 = merge(local.common_tags, { Name = local.kpi_web_name })
}

resource "aws_lb_listener" "kpi_web" {
  count             = var.enable_kpi_web ? 1 : 0
  load_balancer_arn = aws_lb.kpi_web[0].arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.kpi_web[0].arn
  }
}

resource "aws_cloudfront_distribution" "kpi_web" {
  count           = var.enable_kpi_web ? 1 : 0
  enabled         = true
  is_ipv6_enabled = true
  comment         = "Cortex KPI web"
  price_class     = "PriceClass_100"
  http_version    = "http2and3"

  origin {
    domain_name = aws_lb.kpi_web[0].dns_name
    origin_id   = "kpi-web-alb"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "http-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id         = "kpi-web-alb"
    viewer_protocol_policy   = "redirect-to-https"
    allowed_methods          = ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"]
    cached_methods           = ["GET", "HEAD"]
    compress                 = true
    cache_policy_id          = data.aws_cloudfront_cache_policy.caching_disabled[0].id
    origin_request_policy_id = data.aws_cloudfront_origin_request_policy.all_viewer_except_host[0].id
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = merge(local.common_tags, { Name = local.kpi_web_name })
}

data "aws_iam_policy_document" "ecs_task_kpi_web" {
  count = var.enable_kpi_web ? 1 : 0

  statement {
    sid    = "ReadKpiWebAndIntegrations"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "secretsmanager:DescribeSecret",
    ]
    resources = [
      aws_secretsmanager_secret.bundle["integrations"].arn,
      aws_secretsmanager_secret.kpi_web[0].arn,
    ]
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
    for_each = local.kpi_store_s3_enabled ? [1] : []
    content {
      sid    = "KpiStoreS3Read"
      effect = "Allow"
      actions = [
        "s3:GetObject",
      ]
      resources = [local.kpi_store_s3_object_arn]
    }
  }
}

resource "aws_iam_role" "ecs_task_kpi_web" {
  count              = var.enable_kpi_web ? 1 : 0
  name               = "${local.task_role_name}-kpi-web"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume.json
  tags               = merge(local.common_tags, { CortexSecretProfile = "kpi-web" })
}

resource "aws_iam_role_policy" "ecs_task_kpi_web" {
  count  = var.enable_kpi_web ? 1 : 0
  name   = "${local.task_role_name}-kpi-web-policy"
  role   = aws_iam_role.ecs_task_kpi_web[0].id
  policy = data.aws_iam_policy_document.ecs_task_kpi_web[0].json
}

resource "aws_ecs_task_definition" "kpi_web" {
  count                    = var.enable_kpi_web ? 1 : 0
  family                   = local.kpi_web_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.kpi_web_cpu
  memory                   = var.kpi_web_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task_kpi_web[0].arn

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
      name      = "cortex-kpi-web"
      image     = local.ecr_image
      essential = true
      entryPoint = [
        "/app/scripts/run_kpi_web.sh",
      ]
      portMappings = [
        {
          containerPort = local.kpi_web_port
          protocol      = "tcp"
        },
      ]
      environment = concat(
        [
          { name = "CORTEX_SKIP_DOTENV", value = "1" },
          { name = "CORTEX_CACHE_DIR", value = "/var/cortex/cache" },
          { name = "CORTEX_LOG_FORMAT", value = "json" },
          { name = "CORTEX_SECRETS_ARNS", value = join(",", [
            aws_secretsmanager_secret.bundle["integrations"].arn,
            aws_secretsmanager_secret.kpi_web[0].arn,
          ]) },
          { name = "CORTEX_SECRETS_ARN", value = aws_secretsmanager_secret.kpi_web[0].arn },
          { name = "CORTEX_KPI_WEB_HOST", value = "0.0.0.0" },
          { name = "CORTEX_KPI_WEB_PORT", value = tostring(local.kpi_web_port) },
          { name = "CORTEX_KPI_WEB_BASE_URL", value = "https://${aws_cloudfront_distribution.kpi_web[0].domain_name}" },
          { name = "CORTEX_KPI_WEB_ALLOWED_DOMAINS", value = var.kpi_web_allowed_domains },
        ],
        local.kpi_store_s3_enabled ? [
          { name = "CORTEX_KPI_STORE_S3_URI", value = local.kpi_store_s3_uri },
        ] : [],
      )
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
          "awslogs-stream-prefix" = "kpi-web"
        }
      }
      healthCheck = {
        command     = ["CMD-SHELL", "python3 -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:${local.kpi_web_port}/api/health')\" || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    },
  ])

  tags = local.common_tags
}

resource "aws_ecs_service" "kpi_web" {
  count           = var.enable_kpi_web ? 1 : 0
  name            = local.kpi_web_name
  cluster         = aws_ecs_cluster.cortex.id
  task_definition = aws_ecs_task_definition.kpi_web[0].arn
  desired_count   = var.kpi_web_desired_count
  launch_type     = "FARGATE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets = local.subnet_ids
    security_groups = [
      aws_security_group.ecs_tasks.id,
      aws_security_group.kpi_web_tasks[0].id,
    ]
    assign_public_ip = var.assign_public_ip
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.kpi_web[0].arn
    container_name   = "cortex-kpi-web"
    container_port   = local.kpi_web_port
  }

  depends_on = [aws_lb_listener.kpi_web]

  tags = local.common_tags
}

check "kpi_web_subnets_for_alb" {
  assert {
    condition     = !var.enable_kpi_web || length(local.subnet_ids) >= 2
    error_message = "KPI web ALB needs at least two subnets. Set subnet_ids or use the default VPC."
  }
}
