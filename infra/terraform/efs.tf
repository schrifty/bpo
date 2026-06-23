resource "aws_efs_file_system" "cache" {
  creation_token   = "${var.name_prefix}-cache"
  performance_mode = "generalPurpose"
  throughput_mode  = "bursting"
  encrypted        = true

  tags = merge(local.common_tags, { Name = "${var.name_prefix}-cache" })
}

resource "aws_efs_mount_target" "cache" {
  for_each = toset(local.subnet_ids)

  file_system_id  = aws_efs_file_system.cache.id
  subnet_id       = each.value
  security_groups = [aws_security_group.efs.id]
}

resource "aws_efs_access_point" "cache" {
  file_system_id = aws_efs_file_system.cache.id

  posix_user {
    uid = 1000
    gid = 1000
  }

  root_directory {
    path = "/cache"
    creation_info {
      owner_uid   = 1000
      owner_gid   = 1000
      permissions = "755"
    }
  }

  tags = merge(local.common_tags, { Name = "${var.name_prefix}-cache-ap" })
}
