output "aws_region" {
  value = var.aws_region
}

output "account_id" {
  value = local.account_id
}

output "ecr_repository_url" {
  description = "docker tag/push target (append :tag)"
  value       = aws_ecr_repository.decks.repository_url
}

output "ecr_login_command" {
  description = "Authenticate Docker to ECR"
  value       = "aws ecr get-login-password --region ${var.aws_region} | docker login --username AWS --password-stdin ${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}

output "docker_build_push_commands" {
  description = "Build and push after terraform apply"
  value       = <<-EOT
    cd /path/to/cortex
    docker build --platform linux/amd64 -t ${local.ecr_repository_name} .
    docker tag ${local.ecr_repository_name}:latest ${aws_ecr_repository.decks.repository_url}:${var.image_tag}
    docker push ${aws_ecr_repository.decks.repository_url}:${var.image_tag}
  EOT
}

output "secrets_manager_arn" {
  value = aws_secretsmanager_secret.cortex.arn
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.cortex.name
}

output "ecs_task_definition_family" {
  value = aws_ecs_task_definition.decks.family
}

output "ecs_task_definition_arn" {
  value = aws_ecs_task_definition.decks.arn
}

output "execution_role_arn" {
  value = aws_iam_role.ecs_execution.arn
}

output "task_role_arn" {
  value = aws_iam_role.ecs_task.arn
}

output "ecs_tasks_security_group_id" {
  value = aws_security_group.ecs_tasks.id
}

output "subnet_ids" {
  value = local.subnet_ids
}

output "vpc_id" {
  value = local.vpc_id
}

output "cloudwatch_log_group" {
  value = aws_cloudwatch_log_group.decks.name
}

output "efs_file_system_id" {
  value = aws_efs_file_system.cache.id
}

output "efs_access_point_id" {
  value = aws_efs_access_point.cache.id
}

output "run_task_engineering_portfolio" {
  description = "One-off smoke test (copy/paste after image push + secret upload)"
  value       = <<-EOT
    aws ecs run-task \
      --cluster ${aws_ecs_cluster.cortex.name} \
      --launch-type FARGATE \
      --task-definition ${aws_ecs_task_definition.decks.family} \
      --network-configuration "awsvpcConfiguration={subnets=[${join(",", local.subnet_ids)}],securityGroups=[${aws_security_group.ecs_tasks.id}],assignPublicIp=${var.assign_public_ip ? "ENABLED" : "DISABLED"}}" \
      --overrides '{"containerOverrides":[{"name":"cortex-decks","command":["engineering-portfolio"]}]}' \
      --region ${var.aws_region}
  EOT
}

output "scheduled_job_rules" {
  value = [for k, r in aws_cloudwatch_event_rule.job : r.name]
}
