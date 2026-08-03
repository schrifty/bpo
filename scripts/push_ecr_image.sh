#!/usr/bin/env bash
# Build cortex-decks for linux/amd64 and push to AWS ECR.
#
# Uses terraform outputs from infra/terraform (ECR must exist after terraform apply).
#
# Usage:
#   scripts/push_ecr_image.sh
#   scripts/push_ecr_image.sh --tag latest
#   scripts/push_ecr_image.sh --tag "$(git rev-parse --short HEAD)"
#   bin/push-ecr --no-cache
set -euo pipefail

SOURCE="${BASH_SOURCE[0]}"
while [ -L "$SOURCE" ]; do
  DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  [[ "$SOURCE" != /* ]] && SOURCE="$DIR/$SOURCE"
done
ROOT="$(cd -P "$(dirname "$SOURCE")/.." && pwd)"
TF_DIR="${ROOT}/infra/terraform"

IMAGE_NAME="cortex-decks"
TAG="latest"
NO_CACHE=0
DRY_RUN=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Build the repo Docker image (linux/amd64) and push to ECR.

Options:
  --tag TAG       Image tag (default: latest)
  --no-cache      Pass --no-cache to docker build
  --dry-run       Print commands without running build/push
  -h, --help      Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      TAG="${2:?missing value for --tag}"
      shift 2
      ;;
    --no-cache)
      NO_CACHE=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but not found in PATH" >&2
  exit 1
fi
if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required but not found in PATH" >&2
  exit 1
fi
if ! command -v terraform >/dev/null 2>&1; then
  echo "terraform is required (for ECR URL/login); install terraform or set ECR_REPOSITORY_URL" >&2
  exit 1
fi

ECR_URL="$(terraform -chdir="$TF_DIR" output -raw ecr_repository_url 2>/dev/null || true)"
if [[ -z "$ECR_URL" ]]; then
  echo "Could not read ecr_repository_url from ${TF_DIR}; run terraform apply first." >&2
  exit 1
fi

ECR_LOGIN="$(terraform -chdir="$TF_DIR" output -raw ecr_login_command)"
REMOTE="${ECR_URL}:${TAG}"

BUILD_ARGS=(--platform linux/amd64 -t "${IMAGE_NAME}:latest" "${ROOT}")
if [[ "$NO_CACHE" -eq 1 ]]; then
  BUILD_ARGS=(--no-cache "${BUILD_ARGS[@]}")
fi

run() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

echo "ECR target: ${REMOTE}"
run bash -c "$ECR_LOGIN"
run docker build "${BUILD_ARGS[@]}"
run docker tag "${IMAGE_NAME}:latest" "$REMOTE"
run docker push "$REMOTE"

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "Dry run — image not built or pushed."
else
  echo "Pushed ${REMOTE}"
  echo "New ECS tasks will pull :${TAG} on next run-task or scheduled job."
fi
