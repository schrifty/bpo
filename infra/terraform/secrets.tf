resource "aws_secretsmanager_secret" "cortex" {
  # Legacy combined blob (`cortex/prod/env`). Kept so apply does not delete it.
  # New tasks load the split bundles below, not this ARN.
  name                    = local.secret_name
  recovery_window_in_days = var.secret_recovery_window_days
  tags                    = local.common_tags
}

resource "aws_secretsmanager_secret_version" "cortex" {
  count = local.secrets_json_file_path != "" && var.secrets_json_dir == "" ? 1 : 0

  secret_id     = aws_secretsmanager_secret.cortex.id
  secret_string = file(local.secrets_json_file_path)
}

resource "aws_secretsmanager_secret" "bundle" {
  for_each = toset(local.secret_bundles)

  name                    = "${var.name_prefix}/${var.environment}/${each.key}"
  recovery_window_in_days = var.secret_recovery_window_days
  tags                    = merge(local.common_tags, { CortexSecretBundle = each.key })
}

resource "aws_secretsmanager_secret_version" "bundle" {
  for_each = local.bundle_secret_files

  secret_id     = aws_secretsmanager_secret.bundle[each.key].id
  secret_string = file(each.value)
}

# Empty JSON so GetSecretValue succeeds before the first put-secret-value.
# ignore_changes keeps later CLI/console uploads from being reverted to {}.
resource "aws_secretsmanager_secret_version" "bundle_placeholder" {
  for_each = {
    for b in local.secret_bundles : b => b
    if lookup(local.bundle_secret_files, b, null) == null
  }

  secret_id     = aws_secretsmanager_secret.bundle[each.key].id
  secret_string = "{}"

  lifecycle {
    ignore_changes = [secret_string]
  }
}
