#!/usr/bin/env bash
# Entrypoint for the Cortex KPI web ECS service (not a one-shot job).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export CORTEX_SKIP_DOTENV="${CORTEX_SKIP_DOTENV:-1}"
export CORTEX_CACHE_DIR="${CORTEX_CACHE_DIR:-/var/cortex/cache}"
export CORTEX_KPI_WEB_HOST="${CORTEX_KPI_WEB_HOST:-0.0.0.0}"
export CORTEX_KPI_WEB_PORT="${CORTEX_KPI_WEB_PORT:-8080}"

if [[ -n "${CORTEX_SECRETS_ARNS:-}" || -n "${CORTEX_SECRETS_ARN:-}" ]]; then
  eval "$(python3 scripts/bootstrap_aws_env.py --shell-export)"
  echo "bootstrap_aws_env: loaded secrets from CORTEX_SECRETS_ARNS" >&2
fi

if [[ "${CORTEX_KPI_WEB_ALLOW_DEV_AUTH:-}" =~ ^(1|true|yes|on)$ ]]; then
  echo "CORTEX_KPI_WEB_ALLOW_DEV_AUTH must not be enabled on ECS" >&2
  exit 1
fi

exec python3 -m src.kpi_web
