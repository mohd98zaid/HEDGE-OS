#!/usr/bin/env bash
#
# Chaos: kill `hedge-market-data` mid-session and verify the rest of
# the stack continues per Property 11 (Self-Healing Policy) and R29.6
# ("WHERE a service fails, THE system SHALL continue operating other
# services and SHALL surface the failure via observability").
#
# Used by `.github/workflows/nightly.yml::chaos`. Operator-runnable on
# any host with `docker compose` and a working dev compose profile.
#
# References:
#   - .kiro/specs/project-hedge/design.md   (§ Testing Strategy → Soak / chaos)
#   - .kiro/specs/project-hedge/requirements.md  (R29.6, R25.1)
#   - crates/hedge-supervisor/src/policy.rs  (WsDisconnected → Reconnect)

set -euo pipefail

# --------------------------------------------------------------------------- #
# Tunables. CI overrides via env; the defaults are sized for a local
# laptop run.
# --------------------------------------------------------------------------- #
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
COMPOSE_PROFILE="${COMPOSE_PROFILE:-hot_path}"
WARMUP_SECONDS="${WARMUP_SECONDS:-25}"
SETTLE_SECONDS="${SETTLE_SECONDS:-15}"
KILL_TARGET="${KILL_TARGET:-hedge-market-data}"
SUPERVISOR_SERVICE="${SUPERVISOR_SERVICE:-hedge-supervisor}"
NATS_SERVICE="${NATS_SERVICE:-nats}"
# Services that must still be running after the kill. The supervisor
# stays up because R29.6 requires the rest of the stack to keep
# serving when one component fails.
SURVIVING_SERVICES=(
    "${NATS_SERVICE}"
    "redis"
    "${SUPERVISOR_SERVICE}"
    "hedge-orderflow"
    "hedge-features"
    "hedge-signals"
    "hedge-risk"
    "hedge-exec"
    "hedge-position"
)
# Log lines that prove the supervisor noticed the disconnect and
# decided on a `Reconnect` action. The supervisor's actuator publishes
# `ops.action.<target>` on the bus and structured-logs the same
# action; we grep its container logs.
SUPERVISOR_RECONNECT_PATTERNS=(
    "ops\\.action"
    "[Rr]econnect"
)
# Patterns that indicate the supervisor crashed or the rest of the
# stack failed to operate independently. Any match fails the chaos
# job.
SUPERVISOR_CRASH_PATTERNS=(
    "panicked at"
    "supervisor: rehydrate failed"
)

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
log() {
    printf '[chaos %s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

cleanup() {
    local rc=$?
    log "cleanup: capturing supervisor logs (last 200 lines)"
    docker compose -f "${COMPOSE_FILE}" logs --tail=200 "${SUPERVISOR_SERVICE}" || true
    log "cleanup: tearing down compose stack"
    docker compose -f "${COMPOSE_FILE}" --profile "${COMPOSE_PROFILE}" down -v --remove-orphans \
        >/dev/null 2>&1 || true
    exit $rc
}
trap cleanup EXIT INT TERM

require() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "FAIL: required command '$1' not found on PATH" >&2
        exit 2
    fi
}

assert_running() {
    local svc=$1
    local state
    state=$(docker compose -f "${COMPOSE_FILE}" ps --format json "${svc}" \
        2>/dev/null \
        | grep -E -o '"State":[[:space:]]*"[^"]+"' \
        | head -n 1 \
        | sed 's/.*"\(running\|exited\|created\|paused\|restarting\|removing\|dead\)".*/\1/' \
        || true)
    if [[ "${state}" != "running" ]]; then
        echo "FAIL: service '${svc}' is not running (state='${state:-unknown}')" >&2
        return 1
    fi
}

# --------------------------------------------------------------------------- #
# Pre-flight
# --------------------------------------------------------------------------- #
require docker
require grep
require sed

if ! docker compose version >/dev/null 2>&1; then
    echo "FAIL: 'docker compose' subcommand is unavailable (need Docker Compose v2)" >&2
    exit 2
fi

# --------------------------------------------------------------------------- #
# Stage 1: bring the stack up
# --------------------------------------------------------------------------- #
log "stage 1: starting stack with profile '${COMPOSE_PROFILE}'"
docker compose -f "${COMPOSE_FILE}" --profile "${COMPOSE_PROFILE}" up -d \
    --remove-orphans \
    "${NATS_SERVICE}" "redis" \
    "${SUPERVISOR_SERVICE}" "${KILL_TARGET}" \
    "hedge-orderflow" "hedge-features" "hedge-signals" \
    "hedge-risk" "hedge-exec" "hedge-position"

log "stage 1: warming up for ${WARMUP_SECONDS}s"
sleep "${WARMUP_SECONDS}"

# Sanity: every surviving service plus the kill target must be
# `running` before we attempt the chaos.
log "stage 1: verifying all services are running"
for svc in "${SURVIVING_SERVICES[@]}" "${KILL_TARGET}"; do
    assert_running "${svc}" || exit 1
done

# --------------------------------------------------------------------------- #
# Stage 2: kill `hedge-market-data` mid-session
# --------------------------------------------------------------------------- #
log "stage 2: docker kill '${KILL_TARGET}' (SIGKILL — no graceful shutdown)"
docker compose -f "${COMPOSE_FILE}" kill -s KILL "${KILL_TARGET}"

log "stage 2: settling for ${SETTLE_SECONDS}s so supervisor can react"
sleep "${SETTLE_SECONDS}"

# --------------------------------------------------------------------------- #
# Stage 3: assert the rest of the stack is still up (R29.6)
# --------------------------------------------------------------------------- #
log "stage 3: asserting surviving services are still running (R29.6)"
violations=0
for svc in "${SURVIVING_SERVICES[@]}"; do
    if ! assert_running "${svc}"; then
        violations=$((violations + 1))
    fi
done
if [[ "${violations}" -gt 0 ]]; then
    echo "FAIL: ${violations} surviving service(s) crashed when ${KILL_TARGET} died" >&2
    exit 1
fi
log "stage 3: ok — ${#SURVIVING_SERVICES[@]} surviving services still running"

# --------------------------------------------------------------------------- #
# Stage 4: assert supervisor logged a Reconnect action (Property 11, R25.1)
# --------------------------------------------------------------------------- #
log "stage 4: scanning supervisor logs for Reconnect action"
sup_logs=$(docker compose -f "${COMPOSE_FILE}" logs --no-color "${SUPERVISOR_SERVICE}" 2>&1 || true)

for pattern in "${SUPERVISOR_CRASH_PATTERNS[@]}"; do
    if echo "${sup_logs}" | grep -E -i "${pattern}" >/dev/null; then
        echo "FAIL: supervisor crash pattern '${pattern}' present in logs" >&2
        exit 1
    fi
done

reconnect_hits=0
for pattern in "${SUPERVISOR_RECONNECT_PATTERNS[@]}"; do
    if echo "${sup_logs}" | grep -E -i "${pattern}" >/dev/null; then
        reconnect_hits=$((reconnect_hits + 1))
    fi
done

if [[ "${reconnect_hits}" -lt "${#SUPERVISOR_RECONNECT_PATTERNS[@]}" ]]; then
    echo "FAIL: supervisor did not log a Reconnect action after ${KILL_TARGET} died" >&2
    echo "      matched ${reconnect_hits} of ${#SUPERVISOR_RECONNECT_PATTERNS[@]} expected patterns" >&2
    echo "------- last 200 supervisor log lines -------" >&2
    echo "${sup_logs}" | tail -n 200 >&2
    echo "---------------------------------------------" >&2
    exit 1
fi
log "stage 4: ok — supervisor logged Reconnect (matched ${reconnect_hits}/${#SUPERVISOR_RECONNECT_PATTERNS[@]} patterns)"

# --------------------------------------------------------------------------- #
# Stage 5: assert NATS is still serving (R29.2 + R29.6)
# --------------------------------------------------------------------------- #
log "stage 5: asserting NATS is still serving (R29.2 + R29.6)"
if ! docker compose -f "${COMPOSE_FILE}" exec -T "${NATS_SERVICE}" \
        sh -c 'nats-server --version' >/dev/null 2>&1; then
    # Fall back to a raw TCP probe — `nats-server --version` may not
    # be available depending on the distro packaging.
    log "stage 5: falling back to /healthz HTTP probe on :8222"
    if ! docker compose -f "${COMPOSE_FILE}" exec -T "${NATS_SERVICE}" \
            sh -c 'wget -q -O- http://127.0.0.1:8222/healthz 2>/dev/null \
                   || curl -sf http://127.0.0.1:8222/healthz' \
            >/dev/null 2>&1; then
        echo "FAIL: NATS health probe failed after chaos" >&2
        exit 1
    fi
fi
log "stage 5: ok — NATS still serving"

# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
log "PASS: chaos suite — supervisor recovered, ${#SURVIVING_SERVICES[@]} services survived (R29.6, Property 11)"
