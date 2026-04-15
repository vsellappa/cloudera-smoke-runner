#!/usr/bin/env bash
# Run the cloudera-smoke-runner container with podman or docker.
#
# Parameters may be supplied via (precedence highest → lowest):
#   1. command-line flags     ( --region ap-southeast-1 )
#   2. environment variables  ( AWS_REGION=... )
#   3. config file            ( KEY=VALUE, sourced )
#   4. defaults
#   5. interactive prompt (for anything still unset)
#
# The CM password is NEVER accepted on the command line (visible in `ps`).
# Provide it via:  CM_PASS env var | --cm-pass-file PATH | --cm-pass-stdin | prompt

set -euo pipefail

IMAGE_NAME_DEFAULT="cloudera-smoke-runner:latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR_DEFAULT="${SCRIPT_DIR}/output"

# ---------------------------------------------------------------------------
# usage
# ---------------------------------------------------------------------------
usage() {
  cat <<'EOF'
Usage: run.sh [OPTIONS] [-- EXTRA_ARGS_TO_SMOKE_PY...]

Target / AWS:
  --env-name NAME          logical env label  (ENV_NAME)
  --region REGION          AWS region         (AWS_REGION)
  --profile NAME           AWS named profile  (AWS_PROFILE)
  --vpc VPC_ID             VPC id             (VPC_ID)
  --subnet SUBNET_ID       subnet id          (SUBNET_ID)

Cloudera Manager:
  --cm-host HOST           CM private IP/DNS  (CM_HOST)
  --cm-port PORT           default 7183       (CM_PORT)
  --cm-user USER           default admin      (CM_USER)
  --cm-pass-file PATH      read CM password from file (chmod 600)
  --cm-pass-stdin          read CM password from stdin
                           [CM_PASS env var also accepted]

Scope / output:
  --cluster NAME           Cloudera cluster name   (CLUSTER_NAME)
  --services LIST          comma list or 'all'     (SERVICES_TO_TEST)
  --formats LIST           nagios,prom,json,html   (OUTPUT_FORMATS)
  --output-dir PATH        host directory for results (default: ./output)

Config / runtime:
  -c, --config FILE        source a KEY=VALUE config file
  --image NAME[:TAG]       container image name (default: cloudera-smoke-runner:latest)
  --runtime {podman|docker}  force runtime (default: autodetect)
  --build                  build the image before running
  -q, --quiet              suppress interactive prompts; fail if any value is missing
  -h, --help               this help

Config file lookup (first match wins, overridable by --config):
  $CLOUDERA_SMOKE_CONFIG
  ./.smoke.env
  ${XDG_CONFIG_HOME:-$HOME/.config}/cloudera-smoke/config
  /etc/cloudera-smoke/config

Exit codes: Nagios convention — 0 OK, 1 WARN, 2 CRIT, 3 UNKNOWN.
EOF
}

# ---------------------------------------------------------------------------
# arg parsing
# ---------------------------------------------------------------------------
BUILD=0
QUIET=0
CONFIG_FILE=""
IMAGE_NAME=""
FORCED_RUNTIME=""
CM_PASS_FILE=""
CM_PASS_STDIN=0
OUTPUT_DIR=""
EXTRA_ARGS=()

die() { echo "ERROR: $*" >&2; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name)       ENV_NAME="$2"; shift 2 ;;
    --region)         AWS_REGION="$2"; shift 2 ;;
    --profile)        AWS_PROFILE="$2"; shift 2 ;;
    --vpc)            VPC_ID="$2"; shift 2 ;;
    --subnet)         SUBNET_ID="$2"; shift 2 ;;
    --cm-host)        CM_HOST="$2"; shift 2 ;;
    --cm-port)        CM_PORT="$2"; shift 2 ;;
    --cm-user)        CM_USER="$2"; shift 2 ;;
    --cm-pass-file)   CM_PASS_FILE="$2"; shift 2 ;;
    --cm-pass-stdin)  CM_PASS_STDIN=1; shift ;;
    --cluster)        CLUSTER_NAME="$2"; shift 2 ;;
    --services)       SERVICES_TO_TEST="$2"; shift 2 ;;
    --formats)        OUTPUT_FORMATS="$2"; shift 2 ;;
    --output-dir)     OUTPUT_DIR="$2"; shift 2 ;;
    -c|--config)      CONFIG_FILE="$2"; shift 2 ;;
    --image)          IMAGE_NAME="$2"; shift 2 ;;
    --runtime)        FORCED_RUNTIME="$2"; shift 2 ;;
    --build)          BUILD=1; shift ;;
    -q|--quiet)       QUIET=1; shift ;;
    -h|--help)        usage; exit 0 ;;
    --cm-pass=*|--cm-password=*)
                      die "passwords must not be on the command line; use --cm-pass-file, --cm-pass-stdin, or the CM_PASS env var" ;;
    --)               shift; EXTRA_ARGS=("$@"); break ;;
    -*)               die "unknown option: $1" ;;
    *)                EXTRA_ARGS+=("$1"); shift ;;
  esac
done

# ---------------------------------------------------------------------------
# config file
# ---------------------------------------------------------------------------
# Explicit --config wins; otherwise pick the first that exists.
if [[ -z "${CONFIG_FILE}" ]]; then
  for candidate in \
      "${CLOUDERA_SMOKE_CONFIG:-}" \
      "${SCRIPT_DIR}/.smoke.env" \
      "./.smoke.env" \
      "${XDG_CONFIG_HOME:-$HOME/.config}/cloudera-smoke/config" \
      "/etc/cloudera-smoke/config" ; do
    if [[ -n "${candidate}" && -r "${candidate}" ]]; then CONFIG_FILE="${candidate}"; break; fi
  done
fi

if [[ -n "${CONFIG_FILE}" ]]; then
  [[ -r "${CONFIG_FILE}" ]] || die "config file not readable: ${CONFIG_FILE}"
  # Refuse world/group-readable config files that contain CM_PASS
  if grep -qE '^[[:space:]]*CM_PASS[[:space:]]*=' "${CONFIG_FILE}"; then
    perms=$(stat -c '%a' "${CONFIG_FILE}" 2>/dev/null || stat -f '%Lp' "${CONFIG_FILE}")
    if [[ "${perms}" != "600" && "${perms}" != "400" ]]; then
      die "config file ${CONFIG_FILE} contains CM_PASS but has mode ${perms}; chmod 600 first"
    fi
  fi
  # Snapshot any values already set (CLI flags + host env) so the config file
  # cannot override them. Precedence: CLI flag > env var > config file.
  KEYS=( ENV_NAME AWS_REGION AWS_PROFILE VPC_ID SUBNET_ID \
         CM_HOST CM_PORT CM_USER CM_PASS \
         CLUSTER_NAME SERVICES_TO_TEST OUTPUT_FORMATS )
  declare -A _PRE
  for k in "${KEYS[@]}"; do _PRE[$k]="${!k-}"; done
  # Source the file
  set -a
  # shellcheck disable=SC1090
  source "${CONFIG_FILE}"
  set +a
  # Restore any pre-existing values (higher precedence wins)
  for k in "${KEYS[@]}"; do
    if [[ -n "${_PRE[$k]:-}" ]]; then export "$k=${_PRE[$k]}"; fi
  done
  unset _PRE KEYS
  echo ">> Loaded config from ${CONFIG_FILE}"
fi

# ---------------------------------------------------------------------------
# password sourcing
# ---------------------------------------------------------------------------
if [[ -z "${CM_PASS:-}" && -n "${CM_PASS_FILE}" ]]; then
  [[ -r "${CM_PASS_FILE}" ]] || die "cannot read password file: ${CM_PASS_FILE}"
  perms=$(stat -c '%a' "${CM_PASS_FILE}" 2>/dev/null || stat -f '%Lp' "${CM_PASS_FILE}")
  if [[ "${perms}" != "600" && "${perms}" != "400" ]]; then
    echo "WARNING: ${CM_PASS_FILE} mode is ${perms} (expected 600/400)" >&2
  fi
  CM_PASS="$(<"${CM_PASS_FILE}")"
  CM_PASS="${CM_PASS%$'\n'}"  # strip trailing newline
fi
if [[ -z "${CM_PASS:-}" && "${CM_PASS_STDIN}" -eq 1 ]]; then
  IFS= read -rs CM_PASS
  echo
fi

# ---------------------------------------------------------------------------
# runtime detection
# ---------------------------------------------------------------------------
RUNTIME="${FORCED_RUNTIME:-${CONTAINER_RUNTIME:-}}"
if [[ -z "${RUNTIME}" ]]; then
  if command -v podman >/dev/null 2>&1; then RUNTIME=podman
  elif command -v docker >/dev/null 2>&1; then RUNTIME=docker
  else die "neither podman nor docker found on PATH"
  fi
fi
command -v "${RUNTIME}" >/dev/null 2>&1 || die "runtime '${RUNTIME}' not on PATH"

: "${IMAGE_NAME:=${IMAGE_NAME_DEFAULT}}"
: "${OUTPUT_DIR:=${OUTPUT_DIR_DEFAULT}}"

# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------
if [[ "${BUILD}" -eq 1 ]]; then
  echo ">> Building ${IMAGE_NAME} with ${RUNTIME}"
  "${RUNTIME}" build \
    --build-arg IMAGE_CREATED="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --build-arg IMAGE_REVISION="$(git -C "${SCRIPT_DIR}" rev-parse --short HEAD 2>/dev/null || echo local)" \
    -t "${IMAGE_NAME}" \
    "${SCRIPT_DIR}"
fi

# ---------------------------------------------------------------------------
# fill remaining required values (prompt unless --quiet)
# ---------------------------------------------------------------------------
need() { # VAR_NAME  [default]
  local name="$1" def="${2:-}"
  if [[ -n "${!name:-}" ]]; then return 0; fi
  if [[ -n "${def}" ]]; then export "${name}=${def}"; return 0; fi
  if [[ "${QUIET}" -eq 1 ]]; then die "required parameter ${name} not set (and --quiet given)"; fi
  local val
  read -r -p "${name}: " val
  [[ -n "${val}" ]] || die "${name} is required"
  export "${name}=${val}"
}
need_secret() { # VAR_NAME
  local name="$1" val
  if [[ -n "${!name:-}" ]]; then return 0; fi
  if [[ "${QUIET}" -eq 1 ]]; then die "${name} not set (and --quiet given)"; fi
  read -r -s -p "${name}: " val; echo
  [[ -n "${val}" ]] || die "${name} is required"
  export "${name}=${val}"
}

need ENV_NAME
need AWS_REGION
need AWS_PROFILE
need VPC_ID
need SUBNET_ID
need CM_HOST
need CM_PORT "7183"
need CM_USER "admin"
need_secret CM_PASS
need CLUSTER_NAME
need SERVICES_TO_TEST "all"
: "${OUTPUT_FORMATS:=nagios,prom,json,html}"

mkdir -p "${OUTPUT_DIR}" || die "cannot create ${OUTPUT_DIR}"

# ---------------------------------------------------------------------------
# assemble runtime flags
# ---------------------------------------------------------------------------
# Pass password through an --env-file so it does not show up in `ps`.
ENV_FILE="$(mktemp -t smoke-env.XXXXXX)"
chmod 600 "${ENV_FILE}"
trap 'rm -f "${ENV_FILE}"' EXIT
printf 'CM_PASS=%s\n' "${CM_PASS}" > "${ENV_FILE}"

RUN_FLAGS=(
  --rm
  --env-file "${ENV_FILE}"
  -e "ENV_NAME=${ENV_NAME}"
  -e "AWS_REGION=${AWS_REGION}"
  -e "AWS_DEFAULT_REGION=${AWS_REGION}"
  -e "AWS_PROFILE=${AWS_PROFILE}"
  -e "VPC_ID=${VPC_ID}"
  -e "SUBNET_ID=${SUBNET_ID}"
  -e "CM_HOST=${CM_HOST}"
  -e "CM_PORT=${CM_PORT}"
  -e "CM_USER=${CM_USER}"
  -e "CLUSTER_NAME=${CLUSTER_NAME}"
  -e "SERVICES_TO_TEST=${SERVICES_TO_TEST}"
  -e "OUTPUT_FORMATS=${OUTPUT_FORMATS}"
  -v "${HOME}/.aws:/home/smoke/.aws:rw"
  -v "${OUTPUT_DIR}:/output:rw"
)

HOST_UID="$(id -u)"
HOST_GID="$(id -g)"
if [[ "${RUNTIME}" == "podman" ]]; then
  if podman run --rm --userns=keep-id:uid=10001,gid=10001 alpine true >/dev/null 2>&1; then
    RUN_FLAGS+=( --userns=keep-id:uid=10001,gid=10001 )
  else
    echo "NOTE: podman < 4.3 detected; falling back to --user ${HOST_UID}:${HOST_GID}" >&2
    RUN_FLAGS+=( --user "${HOST_UID}:${HOST_GID}" )
  fi
else
  RUN_FLAGS+=( --user "${HOST_UID}:${HOST_GID}" )
fi

[[ -t 0 && -t 1 ]] && RUN_FLAGS+=( -it )

echo ">> Runtime: ${RUNTIME}"
echo ">> Image:   ${IMAGE_NAME}"
echo ">> Output:  ${OUTPUT_DIR}"
echo ">> Env:     ${ENV_NAME}   Cluster: ${CLUSTER_NAME}   CM: ${CM_HOST}:${CM_PORT}"
echo

set +e
"${RUNTIME}" run "${RUN_FLAGS[@]}" "${IMAGE_NAME}" "${EXTRA_ARGS[@]}"
RC=$?
set -e

echo
case "${RC}" in
  0) echo "Exit: 0 (OK)" ;;
  1) echo "Exit: 1 (WARNING)" ;;
  2) echo "Exit: 2 (CRITICAL)" ;;
  3) echo "Exit: 3 (UNKNOWN)" ;;
  *) echo "Exit: ${RC}" ;;
esac
exit "${RC}"
