#!/usr/bin/env bash
# Import manually created Cortex AWS resources into Terraform state.
# Run from repo root or infra/terraform after: terraform init
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/infra/terraform"

PREFIX="${TF_NAME_PREFIX:-cortex}"
ENV="${TF_ENV:-prod}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

echo "Importing existing ${PREFIX} resources in ${REGION} (safe to re-run if already in state)..."

import_if_missing() {
  local addr=$1
  local id=$2
  if [[ -z "${id}" || "${id}" == "None" || "${id}" == "null" ]]; then
    echo "  skip (no id): $addr"
    return 0
  fi
  if terraform state show "$addr" >/dev/null 2>&1; then
    echo "  skip (in state): $addr"
  else
    echo "  import: $addr <- $id"
    terraform import "$addr" "$id"
  fi
}

VPC_ID="$(aws ec2 describe-vpcs --region "$REGION" \
  --filters Name=isDefault,Values=true \
  --query 'Vpcs[0].VpcId' --output text 2>/dev/null || true)"

import_if_missing 'aws_cloudwatch_log_group.decks' "/${PREFIX}/decks"
import_if_missing 'aws_ecr_repository.decks' "${PREFIX}-decks"
import_if_missing 'aws_iam_role.ecs_execution' "${PREFIX}-ecs-execution"
import_if_missing 'aws_iam_role.ecs_task' "${PREFIX}-ecs-task"

SECRET_ARN="$(aws secretsmanager describe-secret --region "$REGION" \
  --secret-id "${PREFIX}/${ENV}/env" \
  --query 'ARN' --output text 2>/dev/null || true)"
import_if_missing 'aws_secretsmanager_secret.cortex' "$SECRET_ARN"

if [[ -n "${VPC_ID}" && "${VPC_ID}" != "None" ]]; then
  ECS_SG="$(aws ec2 describe-security-groups --region "$REGION" \
    --filters "Name=group-name,Values=${PREFIX}-ecs-tasks" "Name=vpc-id,Values=${VPC_ID}" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
  EFS_SG="$(aws ec2 describe-security-groups --region "$REGION" \
    --filters "Name=group-name,Values=${PREFIX}-efs" "Name=vpc-id,Values=${VPC_ID}" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)"
  import_if_missing 'aws_security_group.ecs_tasks' "$ECS_SG"
  import_if_missing 'aws_security_group.efs' "$EFS_SG"
fi

FS_ID="$(aws efs describe-file-systems --region "$REGION" \
  --query "FileSystems[?CreationToken=='${PREFIX}-cache'].FileSystemId | [0]" --output text 2>/dev/null || true)"
import_if_missing 'aws_efs_file_system.cache' "$FS_ID"

if [[ -n "${FS_ID}" && "${FS_ID}" != "None" ]]; then
  AP_ID="$(aws efs describe-access-points --region "$REGION" \
    --query "AccessPoints[?FileSystemId=='${FS_ID}'].AccessPointId | [0]" --output text 2>/dev/null || true)"
  import_if_missing 'aws_efs_access_point.cache' "$AP_ID"

  while IFS=$'\t' read -r mt_id subnet_id; do
    [[ -z "${mt_id}" || "${mt_id}" == "None" ]] && continue
    [[ -z "${subnet_id}" || "${subnet_id}" == "None" ]] && continue
    import_if_missing "aws_efs_mount_target.cache[\"${subnet_id}\"]" "$mt_id"
  done < <(
    aws efs describe-mount-targets --region "$REGION" --file-system-id "$FS_ID" \
      --query 'MountTargets[].[MountTargetId,SubnetId]' --output text 2>/dev/null || true
  )
fi

if aws iam get-role --role-name "${PREFIX}-eventbridge-ecs" >/dev/null 2>&1; then
  if grep -qE '^[[:space:]]*enable_schedules[[:space:]]*=[[:space:]]*true' terraform.tfvars 2>/dev/null; then
    import_if_missing 'aws_iam_role.eventbridge_ecs[0]' "${PREFIX}-eventbridge-ecs"
  else
    echo "  skip: ${PREFIX}-eventbridge-ecs (enable_schedules=false in terraform.tfvars)"
  fi
fi

# EventBridge rules (only when schedules are enabled in tfvars)
if grep -qE '^[[:space:]]*enable_schedules[[:space:]]*=[[:space:]]*true' terraform.tfvars 2>/dev/null; then
  for job_key in engineering-portfolio export-nightly ford-pendo-daily; do
    case "$job_key" in
      engineering-portfolio) rule_name="cortex-engineering-portfolio" ;;
      export-nightly) rule_name="cortex-export-nightly" ;;
      ford-pendo-daily) rule_name="cortex-ford-pendo-daily" ;;
      *) rule_name="${PREFIX}-${job_key}" ;;
    esac
    if aws events describe-rule --region "$REGION" --name "$rule_name" >/dev/null 2>&1; then
      import_if_missing "aws_cloudwatch_event_rule.job[\"${job_key}\"]" "$rule_name"
      target_id="$rule_name"
      import_if_missing "aws_cloudwatch_event_target.job[\"${job_key}\"]" "${rule_name}/${target_id}"
    fi
  done
fi

echo ""
echo "Done. Run: terraform plan && terraform apply"
