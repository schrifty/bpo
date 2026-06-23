check "vpc_configured" {
  assert {
    condition     = local.vpc_id != "" && length(local.subnet_ids) > 0
    error_message = "No VPC/subnets resolved. Set vpc_id + subnet_ids or use_default_vpc = true."
  }
}
