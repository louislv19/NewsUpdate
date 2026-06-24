#!/usr/bin/env bash
# Wrapper for cron: loads .env, runs the binary, logs output.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

exec ./news-digest "$@"
