#!/usr/bin/env bash
# Import manually created Cortex AWS resources into Terraform state.
# Run from infra/terraform after: terraform init
set -euo pipefail
cd "$(dirname "$0")/../infra/terraform"

echo "Importing existing resources (safe to re-run if already in state)..."

import_if_missing() {
  local addr=$1
  local id=$2
  if terraform state show "$addr" >/dev/null 2>&1; then
    echo "  skip (in state): $addr"
  else
    echo "  import: $addr <- $id"
    terraform import "$addr" "$id"
  fi
}

import_if_missing 'aws_cloudwatch_log_group.decks' '/cortex/decks'
import_if_missing 'aws_ecr_repository.decks' 'cortex-decks'
import_if_missing 'aws_iam_role.ecs_execution' 'cortex-ecs-execution'
import_if_missing 'aws_iam_role.ecs_task' 'cortex-ecs-task'

echo ""
echo "Done. Run: terraform plan && terraform apply"
