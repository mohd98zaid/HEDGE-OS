# NATS Account ACLs

Operator-facing reference for the broker-level enforcement of the
PROJECT HEDGE Authority Hierarchy (R21) and architectural prohibitions
(R30.6).

The bus is the structural enforcement point. Even if a Warm_AI_Pipeline
component is misconfigured or compromised, it cannot publish on `risk.*`,
`exec.*`, or `trader.*` because the NATS server rejects the publish at
the wire layer. This is one half of Property 2 ("Authority Hierarchy and
Hot_Path Purity"); the other half — Hot_Path purity at compile time — is
enforced by the CI dependency-forbid check (task 8.1).

## Authority hierarchy enforcement narrative

Per `design.md § Authority Hierarchy and Decision Flow`, the precedence
order is:

```
Risk_Engine  ▶  Execution_Engine  ▶  Signal_Engine  ▶  Warm_AI_Pipeline  ▶  Trader_Input
```

Each level is enforced by a complementary mechanism:

| Level | Enforcement | Mechanism |
|---|---|---|
| Risk_Engine over Execution_Engine | HMAC `ApprovalToken` | Single-use signed token; only Risk_Engine holds the signing key. |
| Risk_Engine and Execution_Engine over Warm_AI | Subject ACL | `warm_ai` account denied publish on `risk.*`, `exec.*`. |
| UI as sole publisher of trader intents | Subject ACL | `ui_gateway` is the only account allowed to publish `trader.>`. |
| Warm_AI restricted to recommendations | Subject ACL | `warm_ai` allow-list is `ai.>`, `mem.>`, `obs.>` only. |
| Trader_Input lowest precedence | Risk_Engine logic | Risk_Engine evaluates `trader.intent.*` against all higher-authority gates before approving. |

The ACL table below is the canonical machine-readable encoding of rows 2,
3, and 4. It is mirrored byte-for-byte in
`docker/nats/nats-server.conf` and reapplied by
`docker/nats/provision-creds.sh`.

## ACL table

| Account | Consumer services | Publish allow | Publish deny | Subscribe allow |
|---|---|---|---|---|
| `hot_path` | `hedge-market-data`, `hedge-orderflow`, `hedge-features`, `hedge-signals`, `hedge-risk`, `hedge-exec`, `hedge-position`, `hedge-replay`, `hedge-session` | `md.>` `of.>` `feat.>` `sig.>` `risk.>` `exec.>` `pos.>` `obs.>` `ops.>` | — | `md.>` `of.>` `feat.>` `sig.>` `risk.>` `exec.>` `pos.>` `obs.>` `ops.>` `ai.>` `mem.>` `trader.>` |
| `warm_ai` | `hedge-news`, `hedge-regime`, `hedge-priority`, `hedge-prevday`, `hedge-psych`, `hedge-rank`, `hedge-journal`, `hedge-governance`, `hedge-shadow`, `hedge-rag` | `ai.>` `mem.>` `obs.>` | **`risk.>` `exec.>` `trader.>`** | `md.>` `sig.>` `exec.>` `pos.>` `mem.>` `ops.>` `ai.>` |
| `ui_gateway` | `hedge-ui-gateway` | `trader.>` `obs.>` | — | `md.>` `of.>` `feat.>` `sig.>` `risk.>` `exec.>` `pos.>` `ai.>` `mem.>` `ops.>` `obs.>` |
| `supervisor` | `hedge-supervisor` | `ops.action.>` `obs.>` | — | `obs.>` `md.connection.>` `cache.redis.>` `broker.metric.>` `obs.latency.>` `obs.budget.breach.>` `obs.error.>` `ai.ollama.degraded` `exec.broker.failover` |
| `obs_collector` | Prometheus exporter, Loki shipper, Jaeger collector | — (denied on `>`) | `>` | `>` |

The bold deny clause on `warm_ai` is the structural enforcement of R21.3
and R30.6: the broker rejects every publish on those subjects, the
publisher receives a permission-violation error, and the supervisor sees
the violation in the broker log stream.

## Smoke test recipe (R21.3, R30.6)

> Assumes the operator+JWT profile is active (`provision-creds.sh` has
> been run). For the dev/password profile use `--user`/`--password` in
> place of `--creds`.

```bash
# Bring up the NATS service.
docker compose --profile infra up -d nats

# These three commands MUST be rejected by the broker.
for subj in risk.decision.approved exec.order.submitted trader.intent.killswitch; do
  nats --creds docker/nats/creds/warm_ai.creds \
       --server nats://localhost:4222 \
       pub "$subj" 'spoofed' \
    && { echo "FAIL: warm_ai allowed to publish $subj"; exit 1; } \
    || echo "OK: $subj denied"
done

# Positive control: a warm_ai publish on ai.* MUST succeed.
nats --creds docker/nats/creds/warm_ai.creds \
     --server nats://localhost:4222 \
     pub ai.regime.changed '{"regime":"trending_up"}' \
  && echo "OK: ai.* allowed"
```

Task 7.2 wraps this recipe in a Rust integration test using the
`async_nats` test harness so the property is verified on every CI run.

## Cross-references

* Requirements — R21.1, R21.2, R21.3, R21.4, R30.6.
* Design — `design.md § Authority Hierarchy and Decision Flow`,
  `design.md § Data Models § NATS Subject Naming Convention`.
* Property — Property 2 ("Authority Hierarchy and Hot_Path Purity"),
  `design.md § Correctness Properties`.
* Provisioning — `docker/nats/README.md` for the operator workflow,
  `docker/nats/provision-creds.sh` for the idempotent script.
