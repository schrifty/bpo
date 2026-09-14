check "vpc_configured" {
  assert {
    condition     = local.vpc_id != "" && length(local.subnet_ids) > 0
    error_message = "No VPC/subnets resolved. Set vpc_id + subnet_ids or use_default_vpc = true."
  }
}

check "scheduled_jobs_have_secret_profiles" {
  assert {
    condition = alltrue([
      for k in keys(var.scheduled_jobs) : contains(keys(local.job_secret_profile), k)
    ])
    error_message = "Every scheduled_jobs key must have an entry in local.job_secret_profile (llm/decks/metrics)."
  }
}
