resource "aws_secretsmanager_secret" "cortex" {
  name                    = local.secret_name
  recovery_window_in_days = var.secret_recovery_window_days
  tags                    = local.common_tags
}

resource "aws_secretsmanager_secret_version" "cortex" {
  count = var.secrets_json_file != "" ? 1 : 0

  secret_id     = aws_secretsmanager_secret.cortex.id
  secret_string = file(var.secrets_json_file)
}
