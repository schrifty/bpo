#!/usr/bin/env bash
# Entrypoint for ECS/Fargate nightly deck jobs.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export BPO_SKIP_DOTENV="${BPO_SKIP_DOTENV:-1}"
export BPO_CACHE_DIR="${BPO_CACHE_DIR:-/var/bpo/cache}"

if [[ -n "${BPO_SECRETS_ARN:-}" ]]; then
  python3 scripts/bootstrap_aws_env.py
fi

JOB="${1:-${BPO_JOB:-nightly-core}}"
shift || true

exec python3 decks.py run-job --job "$JOB" "$@"
