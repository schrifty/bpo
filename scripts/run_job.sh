#!/usr/bin/env bash
# Entrypoint for ECS/Fargate nightly deck jobs.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export CORTEX_SKIP_DOTENV="${CORTEX_SKIP_DOTENV:-1}"
export CORTEX_CACHE_DIR="${CORTEX_CACHE_DIR:-/var/cortex/cache}"

if [[ -n "${CORTEX_SECRETS_ARN:-}" ]]; then
  eval "$(python3 scripts/bootstrap_aws_env.py --shell-export)"
  echo "bootstrap_aws_env: loaded secrets from CORTEX_SECRETS_ARN" >&2
fi

JOB="${1:-${CORTEX_JOB:-nightly-core}}"
shift || true

exec python3 cortex.py run-job --job "$JOB" "$@"
