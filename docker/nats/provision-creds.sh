#!/usr/bin/env bash
# =============================================================================
# PROJECT HEDGE — NATS operator/account/user provisioning.
#
# Generates the operator → account → user tree required by the design's
# Authority Hierarchy (R21.3 / R21.4 / R30.6) using `nsc`, the NATS Operator
# CLI. Each user's `.creds` file is exported to `docker/nats/creds/`.
#
# Usage:
#   docker/nats/provision-creds.sh           # provision (idempotent)
#   docker/nats/provision-creds.sh rotate    # rotate user nkeys + creds
#   docker/nats/provision-creds.sh status    # show current operator/account
#
# Prerequisites:
#   * `nsc` >= 2.8.0  — install via `go install github.com/nats-io/nsc/v2@latest`
#                       or download from https://github.com/nats-io/nsc/releases
#   * `nats` CLI      — optional, used for the smoke-test recipe in README.md
#
# The script is idempotent: re-running detects existing operator and account
# entries and skips creation. Re-running after a deletion (`nsc delete user
# warm_ai -A HEDGE`) recreates only the missing pieces.
# =============================================================================

set -euo pipefail

OP_NAME="hedge_op"
ACCOUNT_NAME="HEDGE"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CREDS_DIR="${SCRIPT_DIR}/creds"

# Five user names + the ACL profile each must be granted. The profiles are
# the source of truth that `nats-server.conf` mirrors. Whitespace separator
# inside the permission lists; pipe character separates publish-allow,
# publish-deny, subscribe-allow.
#
# Format per row: <user>|<pub_allow>|<pub_deny>|<sub_allow>
USERS=(
  "hot_path|md.> of.> feat.> sig.> risk.> exec.> pos.> obs.> ops.>||md.> of.> feat.> sig.> risk.> exec.> pos.> obs.> ops.> ai.> mem.> trader.>"
  "warm_ai|ai.> mem.> obs.>|risk.> exec.> trader.>|md.> sig.> exec.> pos.> mem.> ops.> ai.>"
  "ui_gateway|trader.> obs.>||md.> of.> feat.> sig.> risk.> exec.> pos.> ai.> mem.> ops.> obs.>"
  "supervisor|ops.action.> obs.>||obs.> md.connection.> cache.redis.> broker.metric.> obs.latency.> obs.budget.breach.> obs.error.> ai.ollama.degraded exec.broker.failover"
  "obs_collector|||>"
)

# --- helpers -----------------------------------------------------------------

log()  { printf "[provision-creds] %s\n" "$*"; }
warn() { printf "[provision-creds] WARN: %s\n" "$*" >&2; }
die()  { printf "[provision-creds] ERROR: %s\n" "$*" >&2; exit 1; }

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "missing required tool: $1"
}

ensure_operator() {
  if nsc list operators 2>/dev/null | grep -qE "^[[:space:]]*\\|[[:space:]]*${OP_NAME}[[:space:]]"; then
    log "operator '${OP_NAME}' already exists, skipping"
  else
    log "creating operator '${OP_NAME}'"
    nsc add operator --generate-signing-key --sys "${OP_NAME}"
  fi
  nsc env --operator "${OP_NAME}" >/dev/null
}

ensure_account() {
  if nsc list accounts -o "${OP_NAME}" 2>/dev/null | grep -qE "^[[:space:]]*\\|[[:space:]]*${ACCOUNT_NAME}[[:space:]]"; then
    log "account '${ACCOUNT_NAME}' already exists, skipping"
  else
    log "creating account '${ACCOUNT_NAME}'"
    nsc add account --name "${ACCOUNT_NAME}"
  fi
}

# Build the `nsc add user` argument list from the pipe-separated profile.
build_user_args() {
  local pub_allow="$1" pub_deny="$2" sub_allow="$3"
  local args=()

  if [[ -n "${pub_allow// }" ]]; then
    for s in ${pub_allow}; do args+=(--allow-pub "${s}"); done
  fi
  if [[ -n "${pub_deny// }" ]]; then
    for s in ${pub_deny}; do args+=(--deny-pub "${s}"); done
  fi
  if [[ -n "${sub_allow// }" ]]; then
    for s in ${sub_allow}; do args+=(--allow-sub "${s}"); done
  fi

  # Read-only collector: no publish surface at all.
  if [[ -z "${pub_allow// }" && -z "${pub_deny// }" ]]; then
    args+=(--deny-pub ">")
  fi

  printf "%s\n" "${args[@]}"
}

ensure_user() {
  local user="$1" pub_allow="$2" pub_deny="$3" sub_allow="$4"

  if nsc list users -a "${ACCOUNT_NAME}" 2>/dev/null | grep -qE "^[[:space:]]*\\|[[:space:]]*${user}[[:space:]]"; then
    log "user '${user}' already exists; reapplying permissions"
    # `nsc edit user` is the idempotent permission-update path.
    local args
    mapfile -t args < <(build_user_args "${pub_allow}" "${pub_deny}" "${sub_allow}")
    nsc edit user --name "${user}" --account "${ACCOUNT_NAME}" "${args[@]}"
  else
    log "creating user '${user}'"
    local args
    mapfile -t args < <(build_user_args "${pub_allow}" "${pub_deny}" "${sub_allow}")
    nsc add user --name "${user}" --account "${ACCOUNT_NAME}" "${args[@]}"
  fi

  mkdir -p "${CREDS_DIR}"
  local out="${CREDS_DIR}/${user}.creds"
  log "exporting credentials to ${out}"
  nsc generate creds --name "${user}" --account "${ACCOUNT_NAME}" > "${out}"
  chmod 0600 "${out}"
}

cmd_provision() {
  require_tool nsc
  ensure_operator
  ensure_account
  for row in "${USERS[@]}"; do
    IFS='|' read -r user pub_allow pub_deny sub_allow <<<"${row}"
    ensure_user "${user}" "${pub_allow}" "${pub_deny}" "${sub_allow}"
  done
  emit_resolver_snippet
  log "done. Five .creds files refreshed under ${CREDS_DIR}"
  log "next: include docker/nats/generated/accounts.conf in nats-server.conf"
  log "      and restart the nats container"
}

emit_resolver_snippet() {
  local out_dir="${SCRIPT_DIR}/generated"
  local out="${out_dir}/accounts.conf"
  mkdir -p "${out_dir}"

  local account_jwt account_pub op_jwt sys_pub sys_jwt
  account_jwt="$(nsc describe account --name "${ACCOUNT_NAME}" --raw 2>/dev/null || true)"
  account_pub="$(nsc describe account --name "${ACCOUNT_NAME}" --field sub 2>/dev/null || true)"
  op_jwt="$(nsc describe operator --raw 2>/dev/null || true)"
  sys_pub="$(nsc describe account --name SYS --field sub 2>/dev/null || true)"
  sys_jwt="$(nsc describe account --name SYS --raw 2>/dev/null || true)"

  if [[ -z "${account_jwt}" || -z "${account_pub}" ]]; then
    warn "could not extract account JWT/pub key — skipping accounts.conf emission"
    return 0
  fi

  log "writing resolver snippet to ${out}"
  {
    echo "# Auto-generated by provision-creds.sh — do not edit by hand."
    echo "# Re-run docker/nats/provision-creds.sh to refresh."
    echo
    echo "operator: \"${op_jwt}\""
    if [[ -n "${sys_pub}" ]]; then
      echo "system_account: ${sys_pub}"
    fi
    echo "resolver: MEMORY"
    echo "resolver_preload: {"
    echo "  ${account_pub}: \"${account_jwt}\""
    if [[ -n "${sys_pub}" && -n "${sys_jwt}" ]]; then
      echo "  ${sys_pub}: \"${sys_jwt}\""
    fi
    echo "}"
  } > "${out}"
  chmod 0600 "${out}"
}

cmd_rotate() {
  require_tool nsc
  for row in "${USERS[@]}"; do
    IFS='|' read -r user _ _ _ <<<"${row}"
    log "rotating nkey for user '${user}'"
    nsc rotate user --name "${user}" --account "${ACCOUNT_NAME}"
    nsc generate creds --name "${user}" --account "${ACCOUNT_NAME}" > "${CREDS_DIR}/${user}.creds"
    chmod 0600 "${CREDS_DIR}/${user}.creds"
  done
  log "rotation complete; restart consumer services to pick up new creds"
}

cmd_status() {
  require_tool nsc
  printf "Operator: %s\n" "${OP_NAME}"
  nsc list accounts -o "${OP_NAME}" || true
  nsc list users -a "${ACCOUNT_NAME}" || true
}

main() {
  case "${1:-provision}" in
    provision) cmd_provision ;;
    rotate)    cmd_rotate ;;
    status)    cmd_status ;;
    *) die "unknown command: $1 (use: provision | rotate | status)" ;;
  esac
}

main "$@"
