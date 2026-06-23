data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  count   = var.use_default_vpc && var.vpc_id == "" ? 1 : 0
  default = true
}

data "aws_subnets" "selected" {
  count = local.vpc_id != "" && length(var.subnet_ids) == 0 ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [local.vpc_id]
  }
}
