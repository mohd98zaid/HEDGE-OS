#!/usr/bin/env bash
#
# install-egress-firewall.sh
#
# Idempotently installs the PROJECT HEDGE egress deny ruleset on the host
# running the Ollama_Infrastructure containers (R10.8). Re-running the
# script is safe: it flushes and re-loads its own nftables table, and
# rewrites its own dnsmasq snippet.
#
# Usage:
#   sudo ./install-egress-firewall.sh [--dry-run]
#
# Requirements:
#   * Ubuntu 22.04+ or Debian 12+ on the Ollama host (R29.5).
#   * `nftables` and `dnsmasq` packages installed.
#   * Run as root (or with sudo). The script refuses to run otherwise.
#
# What it does:
#   1. Validates dependencies and root privilege.
#   2. Resolves every domain in `cloud-llm-domains.txt` and adds the
#      resulting IPv4 / IPv6 addresses to the `cloud_llm_v4` /
#      `cloud_llm_v6` nftables sets defined in `egress-deny.rules`.
#   3. Loads `egress-deny.rules` into the running kernel.
#   4. Writes a `/etc/dnsmasq.d/hedge-cloud-llm.conf` snippet that returns
#      `0.0.0.0` / `::` for every blocked domain (DNS-layer fail-closed).
#   5. Restarts dnsmasq.
#   6. Persists the nftables ruleset so it reloads on boot.
#
# What it does NOT do:
#   * Manage the Ollama containers. Use `docker-compose.ollama.yml`.
#   * Install or update GPU drivers.  # TODO: production protocol
#   * Configure NATS ACLs (covered by task 7.1).

set -Eeuo pipefail

# ---------------------------------------------------------------------------
# Constants -----------------------------------------------------------------
# ---------------------------------------------------------------------------

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly RULES_FILE="${SCRIPT_DIR}/egress-deny.rules"
readonly DOMAINS_FILE="${SCRIPT_DIR}/cloud-llm-domains.txt"
readonly DNSMASQ_SNIPPET="/etc/dnsmasq.d/hedge-cloud-llm.conf"
readonly NFT_PERSIST_FILE="/etc/nftables.d/hedge-egress-deny.nft"
readonly NFT_TABLE="hedge_egress"

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
fi

# ---------------------------------------------------------------------------
# Helpers -------------------------------------------------------------------
# ---------------------------------------------------------------------------

log()  { printf '[hedge-firewall] %s\n' "$*" >&2; }
fail() { printf '[hedge-firewall] FATAL: %s\n' "$*" >&2; exit 1; }

run() {
    if (( DRY_RUN )); then
        printf '+ %s\n' "$*"
    else
        eval "$@"
    fi
}

require_root() {
    if (( EUID != 0 )); then
        fail "must run as root (try: sudo $0)"
    fi
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 \
        || fail "missing required command: $1 (install with: apt-get install $2)"
}

# ---------------------------------------------------------------------------
# Step 1: preflight ---------------------------------------------------------
# ---------------------------------------------------------------------------

(( DRY_RUN )) || require_root

require_cmd nft       nftables
require_cmd dnsmasq   dnsmasq
require_cmd getent    libc-bin

[[ -f "${RULES_FILE}"   ]] || fail "missing ruleset: ${RULES_FILE}"
[[ -f "${DOMAINS_FILE}" ]] || fail "missing denylist: ${DOMAINS_FILE}"

log "rules:   ${RULES_FILE}"
log "domains: ${DOMAINS_FILE}"

# ---------------------------------------------------------------------------
# Step 2: resolve domains → IPv4 / IPv6 sets --------------------------------
# ---------------------------------------------------------------------------

declare -a v4_addrs=()
declare -a v6_addrs=()
declare -a domains=()

# Strip comments and blank lines.
while IFS= read -r line; do
    line="${line%%#*}"
    line="${line//[$'\t\r\n ']/}"
    [[ -z "${line}" ]] && continue
    domains+=("${line}")
done < "${DOMAINS_FILE}"

log "resolving ${#domains[@]} domain(s)…"

for d in "${domains[@]}"; do
    # IPv4
    while IFS= read -r addr; do
        [[ -n "${addr}" ]] && v4_addrs+=("${addr}")
    done < <(getent ahostsv4 "${d}" 2>/dev/null | awk '{print $1}' | sort -u || true)

    # IPv6
    while IFS= read -r addr; do
        [[ -n "${addr}" ]] && v6_addrs+=("${addr}")
    done < <(getent ahostsv6 "${d}" 2>/dev/null | awk '{print $1}' | sort -u || true)
done

# Deduplicate.
mapfile -t v4_addrs < <(printf '%s\n' "${v4_addrs[@]:-}" | sort -u | grep -v '^$' || true)
mapfile -t v6_addrs < <(printf '%s\n' "${v6_addrs[@]:-}" | sort -u | grep -v '^$' || true)

log "resolved ${#v4_addrs[@]} IPv4 address(es) and ${#v6_addrs[@]} IPv6 address(es)"

# ---------------------------------------------------------------------------
# Step 3: load nftables ruleset --------------------------------------------
# ---------------------------------------------------------------------------

log "loading nftables ruleset → table inet ${NFT_TABLE}"

run "nft -f '${RULES_FILE}'"

if (( ${#v4_addrs[@]} > 0 )); then
    elem_v4="$(IFS=,; echo "${v4_addrs[*]}")"
    run "nft add element inet ${NFT_TABLE} cloud_llm_v4 { ${elem_v4} }"
fi

if (( ${#v6_addrs[@]} > 0 )); then
    elem_v6="$(IFS=,; echo "${v6_addrs[*]}")"
    run "nft add element inet ${NFT_TABLE} cloud_llm_v6 { ${elem_v6} }"
fi

# Persist ruleset for boot reload.
run "mkdir -p '$(dirname "${NFT_PERSIST_FILE}")'"
run "cp '${RULES_FILE}' '${NFT_PERSIST_FILE}'"

# ---------------------------------------------------------------------------
# Step 4: write dnsmasq snippet (DNS-layer fail-closed) ---------------------
# ---------------------------------------------------------------------------

log "writing dnsmasq snippet → ${DNSMASQ_SNIPPET}"

snippet="$(mktemp)"
trap 'rm -f "${snippet}"' EXIT

{
    printf '# Auto-generated by install-egress-firewall.sh — do not edit.\n'
    printf '# Returns 0.0.0.0 / :: for every PROJECT HEDGE cloud LLM denylist entry.\n'
    printf '# Source of truth: %s\n\n' "${DOMAINS_FILE}"
    for d in "${domains[@]}"; do
        printf 'address=/%s/0.0.0.0\n'  "${d}"
        printf 'address=/%s/::\n'        "${d}"
    done
} > "${snippet}"

run "install -m 0644 '${snippet}' '${DNSMASQ_SNIPPET}'"

# ---------------------------------------------------------------------------
# Step 5: restart dnsmasq ---------------------------------------------------
# ---------------------------------------------------------------------------

if systemctl list-unit-files dnsmasq.service >/dev/null 2>&1; then
    run "systemctl restart dnsmasq"
else
    log "dnsmasq.service not present — skipping restart (configure manually)"
fi

# ---------------------------------------------------------------------------
# Step 6: enable nftables on boot ------------------------------------------
# ---------------------------------------------------------------------------

if systemctl list-unit-files nftables.service >/dev/null 2>&1; then
    run "systemctl enable --now nftables"
fi

log "egress firewall installed."
log "verify with:"
log "  nft list table inet ${NFT_TABLE}"
log "  dig +short api.openai.com   # should return 0.0.0.0"
log "  curl -sS https://api.openai.com/  # should fail (no route or refused)"
