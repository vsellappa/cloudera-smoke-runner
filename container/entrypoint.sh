#!/usr/bin/env bash
set -euo pipefail

# Accept CM_PASS via file (preferred, -v /path/to/pass:/run/cm_pass:ro) or env.
if [[ -z "${CM_PASS:-}" && -r /run/cm_pass ]]; then
  CM_PASS="$(cat /run/cm_pass)"
  export CM_PASS
fi

mkdir -p "${OUTPUT_DIR:-/output}"
exec python3 /app/smoke.py "$@"
