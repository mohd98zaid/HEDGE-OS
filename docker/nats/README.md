# PROJECT HEDGE — NATS account provisioning and ACLs

This directory configures the NATS broker that backs the Hot_Path message
bus (R29.3, R30.6) and enforces the Authority Hierarchy at the broker
level (R21.1, R21.3, R21.4).

| File | Purpose |
|---|---|
| `nats-server.conf` | Server config consumed by the `nats` container. Defines five accounts and their per-subject permissions. Ships with environment-variable password placeholders for the dev profile; operator+JWT mode is documented inline for production. |
| `provision-creds.sh` | Idempotent operator/account/user provisioning via `nsc`. Generates the operator JWT, account JWTs, and per-user `.creds` files for the production profile. |
| `creds/*.creds` | Per-account credentials mounted into consumer containers at `/etc/hedge/nats/<account>.creds`. The committed files are placeholders; real credentials are minted by `provision-creds.sh`. |

## Two profiles, one ACL table

The same five-account ACL table is enforced under both profiles:

* **Dev profile** (default in `nats-server.conf`): inline accounts with
  `password: "$VAR"` per user. The compose file injects each password via
  environment variables; consumers connect with
  `nats://<user>:<pass>@nats:4222`.
* **Production profile**: operator + JWT model. `provision-creds.sh`
  generates the operator JWT, account JWTs, and per-user `.creds` files.
  The `.creds` files are mounted into consumer containers at
  `/etc/hedge/nats/<account>.creds` and the Rust client opens them via
  [`NatsClient::connect_with_creds`](../../crates/hedge-bus/src/nats.rs).
  The password placeholders in `nats-server.conf` are removed and
  replaced by an `include ./generated/accounts.conf` directive.

The choice between profiles is a single edit to `nats-server.conf`; the
account/permission topology never changes.

## ACL table (canonical)

The five accounts and their subject ACLs are derived from
`design.md § Authority Hierarchy and Decision Flow` and
`design.md § Data Models § NATS Subject Naming Convention`:

| Account | Publish allow | Publish deny | Subscribe allow | Spec link |
|---|---|---|---|---|
| `hot_path` | `md.>` `of.>` `feat.>` `sig.>` `risk.>` `exec.>` `pos.>` `obs.>` `ops.>` | — | `md.>` `of.>` `feat.>` `sig.>` `risk.>` `exec.>` `pos.>` `obs.>` `ops.>` `ai.>` `mem.>` `trader.>` | R21.1 |
| `warm_ai` | `ai.>` `mem.>` `obs.>` | `risk.>` `exec.>` `trader.>` | `md.>` `sig.>` `exec.>` `pos.>` `mem.>` `ops.>` `ai.>` | **R21.3, R30.6** |
| `ui_gateway` | `trader.>` `obs.>` | — | `md.>` `of.>` `feat.>` `sig.>` `risk.>` `exec.>` `pos.>` `ai.>` `mem.>` `ops.>` `obs.>` | R21.1, R21.4 |
| `supervisor` | `ops.action.>` `obs.>` | — | `obs.>` `md.connection.>` `cache.redis.>` `broker.metric.>` `obs.latency.>` `obs.budget.breach.>` `obs.error.>` `ai.ollama.degraded` `exec.broker.failover` | R25.1–R25.5, R27 |
| `obs_collector` | — (denied on `>`) | `>` | `>` | R27, R30.6 |

The deny clause on `warm_ai` is the structural enforcement of R21.3: even
with valid credentials, the NATS server rejects any publish on `risk.>`,
`exec.>`, or `trader.>`. This is the "no order without risk approval"
guarantee at the transport layer (Property 2).

## Installing `nsc`

`nsc` is the official NATS Operator CLI used to mint the operator,
accounts, and users.

```bash
# macOS / Linux
curl -L https://github.com/nats-io/nsc/releases/latest/download/nsc-${OS}-${ARCH}.zip -o nsc.zip
unzip nsc.zip && sudo mv nsc /usr/local/bin/

# Or with Go
go install github.com/nats-io/nsc/v2@latest

# Verify
nsc --version   # expect >= 2.8.0
```

## Provisioning real credentials

```bash
# From the repo root.
docker/nats/provision-creds.sh provision
```

> If `provision-creds.sh` was added on a Windows clone, mark it executable
> before pushing:
>
> ```bash
> git update-index --chmod=+x docker/nats/provision-creds.sh
> ```

This will:

1. Create operator `hedge_op` (skipped if it already exists).
2. Create account `HEDGE` under `hedge_op` (skipped if it exists).
3. Create users `hot_path`, `warm_ai`, `ui_gateway`, `supervisor`,
   `obs_collector` with the per-user permissions from the ACL table.
4. Export each user's credentials to `docker/nats/creds/<user>.creds`
   (mode 0600).
5. Print the next manual step: substituting the operator/account/user
   public NKEYs into `nats-server.conf` (or wiring up the JWT memory
   resolver for production).

`nsc list users -a HEDGE` then shows the live permissions; compare
against the ACL table above.

## Rotating credentials

```bash
docker/nats/provision-creds.sh rotate
```

Rotates the user nkeys, regenerates the `*.creds` files, and chmod 0600s
them. After rotation, restart each consumer service so it picks up the
new credentials:

```bash
docker compose --profile hot_path restart hedge-market-data hedge-orderflow \
  hedge-features hedge-signals hedge-risk hedge-exec hedge-position \
  hedge-replay hedge-session
docker compose --profile warm_ai restart hedge-news hedge-regime \
  hedge-priority hedge-prevday hedge-psych hedge-rank hedge-journal \
  hedge-governance hedge-shadow hedge-rag
docker compose restart hedge-ui-gateway hedge-supervisor
```

## Validating the ACL: smoke test for R21.3 / R30.6

> **Profile prerequisite**: this recipe assumes the operator+JWT profile
> is active (i.e. `provision-creds.sh` has been run and
> `nats-server.conf` is using the `include ./generated/accounts.conf`
> directive). The dev/password profile uses
> `nats --user <user> --password <pw>` instead of `--creds`.

The `warm_ai` account MUST be denied publish on `risk.>`, `exec.>`, and
`trader.>`. Verify it with the `nats` CLI:

```bash
# Bring the NATS service up.
docker compose --profile infra up -d nats

# Negative test 1: warm_ai publishing on risk.* must be rejected.
nats --creds docker/nats/creds/warm_ai.creds \
     --server nats://localhost:4222 \
     pub risk.decision.approved 'spoofed' || echo "denied (expected)"

# Negative test 2: warm_ai publishing on exec.* must be rejected.
nats --creds docker/nats/creds/warm_ai.creds \
     --server nats://localhost:4222 \
     pub exec.order.submitted 'spoofed' || echo "denied (expected)"

# Negative test 3: warm_ai publishing on trader.* must be rejected.
nats --creds docker/nats/creds/warm_ai.creds \
     --server nats://localhost:4222 \
     pub trader.intent.killswitch 'spoofed' || echo "denied (expected)"

# Positive test: warm_ai publishing on ai.* succeeds.
nats --creds docker/nats/creds/warm_ai.creds \
     --server nats://localhost:4222 \
     pub ai.regime.changed '{"regime":"trending_up"}'
```

In the negative cases the server logs:

```
[ERR] Publish Violation - User "warm_ai", Subject "risk.decision.approved"
```

Task 7.2 turns these into automated integration tests.

## Activating the operator + JWT profile

Once `provision-creds.sh` has run, switch `nats-server.conf` to the
operator+JWT profile:

1. Comment out the `accounts: { … }` and `system_account: SYS` block at the
   bottom of `nats-server.conf`.
2. Uncomment the `operator:`, `system_account:`, `resolver:`, and
   `resolver_preload:` lines and substitute the values from the file
   `docker/nats/generated/accounts.conf` written by the script.
3. Optionally `include ./generated/accounts.conf` instead of inlining
   the substitutions.
4. Restart the `nats` container.

In production the ACL table is the same; only the authentication transport
changes (password ↔ JWT). Reference docs:
<https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/nkey_auth_jwt>.

## Cross-references

* Spec — `.kiro/specs/project-hedge/requirements.md` Requirement 21,
  Requirement 30, Requirement 32.
* Design — `design.md § Authority Hierarchy and Decision Flow`,
  `design.md § Data Models § NATS Subject Naming Convention`.
* Property — Property 2: Authority Hierarchy and Hot_Path Purity
  (`design.md § Correctness Properties`).
* Companion docs — `docs/nats-acls.md` for the operator-facing ACL summary.
