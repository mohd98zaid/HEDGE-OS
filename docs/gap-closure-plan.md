# PROJECT HEDGE — Gap Closure Plan (Phase-wise Task List)

**Generated:** 2026-06-12
**Purpose:** Systematic closure of all gaps identified in `docs/gap-analysis-report.md`
**Total estimated work:** ~80 discrete tasks across 6 phases

---

## Phase 1: Foundation Test Completeness (Rust)
**Goal:** Complete all unchecked component-level proptest sub-tasks for the Hot_Path crates.
**Depends on:** Nothing (Foundation is done)
**Estimated tasks:** 10

### Wave 1A — Core primitives
- [x] **T1.1** — `hedge-core` proptests (Task 2.2): Px round-trip arithmetic, RingWindow push/pop no-alloc, LatencyTimer monotonic deltas — **16 tests passing**
- [x] **T1.2** — `hedge-schemas` round-trip proptests (Task 4.2): field preservation, equality, enum round-trips — **9 tests passing**
- [x] **T1.3** — `hedge-config` default/schema tests (Task 6.2): Unit test default values (base_inr=20000, targets, session times), schema-violation exits non-zero — **12 tests passing**

### Wave 1B — Transport + observability
- [ ] **T1.4** — `hedge-bus` subscriber delivery proptests (Task 3.2): Exact-once delivery per subject, Redis Stream consumer-group ack, zero-copy receive
- [x] **T1.5** — `hedge-obs` budget-breach proptests (Task 5.2): Every budget breach emits exactly one event with matching correlation_id, LatencyRecord per stage — **6 tests passing** (BoundedRingLogBuffer properties)
- [ ] **T1.6** — NATS ACL integration test (Task 7.2): `warm_ai` credential publish on `risk.*` and `exec.*` rejected by NATS

### Wave 1C — Hot_Path engine proptests
- [ ] **T1.7** — `hedge-market-data` proptests (Task 10.2): tick ingest p99 < 2ms, per-symbol exact-once distribution, disconnect event emission
- [x] **T1.8** — `hedge-orderflow` proptests (Task 11.2): liquidity_pressure ∈ [-1, 1], zero heap alloc steady-state, spoofing detection exact emission — **8 tests passing**
- [x] **T1.9** — `hedge-features` proptests (Task 12.2): VWAP bounds, EMA bounds, ATR non-negative, compression_zone bounded, breakout_pressure bounded — **7 tests passing**
- [ ] **T1.10** — `hedge-signals` proptests (Task 13.2): zero signals from disabled/blocked strategies, base_probability and confidence ∈ [0, 1]

### Wave 1D — Risk, Execution, Position, Broker tests
- [ ] **T1.11** — `hedge-risk` proptests (Task 14.2): risk check p99 < 2ms, post-approval limit invariant, Adaptive_Risk formula, session-time gate, daily-profit-target
- [ ] **T1.12** — `hedge-exec` proptests (Task 15.2): execution routing p99 < 5ms, HMAC verify, FSM validity, failover emission
- [ ] **T1.13** — `hedge-position` proptests (Task 16.2): position update p99 < 5ms, partial-fill aggregation equivalence
- [ ] **T1.14** — Broker adapter tests (Task 17.2): FSM substitutability across all adapters, broker.metric emission, credential rejection

---

## Phase 2: Python PBT Suites (hypothesis)
**Goal:** Implement all 15 hypothesis-based test suites for Warm_AI_Pipeline and Memory_RAG_Layer.
**Depends on:** Phase 1 (some schemas need Rust round-trip parity)
**Estimated tasks:** 15

### Wave 2A — Ollama + ONNX + News
- [ ] **T2.1** — Ollama fallback hypothesis test (Task 19.2): unresponsive model triggers exactly one `ai.ollama.degraded`, fallback routing works
- [ ] **T2.2** — ONNX latency hypothesis test (Task 20.2): fast NLP p95 < 10ms, classical ML within budget
- [x] **T2.3** — News Intelligence hypothesis tests (Task 21.2): fast-path p95 < 10ms, sentiment ∈ [-1, 1], impact_magnitude ∈ [0, 1], slow-path non-blocking — **7 tests passing**

### Wave 2B — Regime, Priority, Prev_Day, Psychology
- [x] **T2.4** — Market Regime hypothesis test (Task 22.2): regime classification priority, liquidity crisis override, valid regime output — **8 tests passing**
- [x] **T2.5** — Symbol Priority hypothesis tests (Task 23.2): totality (exactly one tier), edge-triggered emission count — **6 tests passing**
- [x] **T2.6** — Previous Day Memory hypothesis test (Task 24.2): encode-decode round-trip structural equality, one record per symbol per day — **4 tests passing**
- [x] **T2.7** — Trader Psychology hypothesis tests (Task 25.2): stability score formula exact, bounded output, monotonicity — **6 tests passing**

### Wave 2C — Ranking, Journal, Governance, Shadow
- [x] **T2.8** — AI Trade Ranking hypothesis tests (Task 26.2): score formula exact, bounded output, weights sum to 1.0 — **6 tests passing**
- [ ] **T2.9** — AI Trade Journal hypothesis tests (Task 27.2): round-trip through Memory_RAG_Layer, exactly one event per closed trade
- [x] **T2.10** — AI Governance hypothesis tests (Task 28.2): governance level change events == adjacent-pair changes, critical threshold triggers shadow mode — **6 tests passing**
- [x] **T2.11** — AI Shadow Mode hypothesis tests (Task 29.2): shadow outputs persisted with timestamp, UI channel never delivers shadowed output — **9 tests passing**

### Wave 2D — Memory_RAG_Layer
- [ ] **T2.12** — Qdrant round-trip hypothesis tests (Task 31.2): encode-decode and write-read structural equality for all entity types
- [ ] **T2.13** — Timescale round-trip hypothesis tests (Task 32.2): write-read structural equality with correct time ordering
- [ ] **T2.14** — Redis cache hypothesis tests (Task 33.2): most-recent value within staleness window, Redis unavailability triggers reconnection + `cache.redis.degraded`
- [ ] **T2.15** — Retrieval pipeline hypothesis tests (Task 34.2): every trader-event produces exactly one recommendation, no Hot_Path synchronous call

---

## Phase 3: UI Test Coverage
**Goal:** Add component tests for React panels and integration tests for trader controls.
**Depends on:** Task 36.1 and 38.1 being complete
**Estimated tasks:** 4

- [ ] **T3.1** — UI component tests: Alerts panel (critical-above-non-critical ordering) (Task 37.2)
- [ ] **T3.2** — UI component tests: Latency Dashboard renders per-stage histograms (Task 37.2)
- [ ] **T3.3** — UI component tests: High-volatility mode increases refresh rate (Task 37.2)
- [ ] **T3.4** — Trader controls integration test: Kill Switch → `trader.intent.killswitch` → Risk_Engine emits `risk.killswitch.activated`; strategy toggle and priority change reach respective engines (Task 38.2)

---

## Phase 4: Complete In-Progress Tasks
**Goal:** Finish the 3 tasks marked `[-]` (in-progress) and 4 tasks marked `[~]` (partial).
**Depends on:** Phase 1-3 (tests validate completeness)
**Estimated tasks:** 7

- [ ] **T4.1** — Complete Task 36.1: UI gateway — verify shadowed AI filtering on `/signals` channel, high-volatility presentation mode implementation
- [ ] **T4.2** — Complete Task 37.1: Human_Control_UI — verify all 16 panels render correctly, WebSocket subscription protocol works for all channels
- [ ] **T4.3** — Complete Task 38.1: Trader controls — verify Kill_Switch, Strategy Toggles, Priority publish to NATS via `/control` channel
- [ ] **T4.4** — Complete Task 40.1: Replay_Engine — verify Recorder, Player, SimulatedBroker binding, `/replay` UI control plane
- [ ] **T4.5** — Complete Task 41.1: Self_Healing_Supervisor — verify Failure_Detector, Recovery_Policy rules, Recovery_Actuator, systemd/docker bring-up
- [ ] **T4.6** — Complete Task 42.1: Market_Open_War_Mode — verify WarModeController emits ops.warmode.start/end, Hot_Path applies profile, UI applies reduced-clutter
- [ ] **T4.7** — Complete Task 43.1: Session manager — verify ops.session.start/end emission, Risk_Engine session-time gate, cancel open orders on session.end
- [ ] **T4.8** — Complete Task 45.1: Grafana dashboards — verify all 5 dashboards load at startup, all required panels present

---

## Phase 5: End-to-End PBT Suites (Group G)
**Goal:** Wire all 12 Correctness Property end-to-end suites plus replay regression.
**Depends on:** Phase 1-4 (all components complete and tested individually)
**Estimated tasks:** 14

### Wave 5A — Properties 1-4
- [ ] **T5.1** — Property 1: Risk Limit Invariant suite (Task 47.1) — generate signals/fills/ticks/news against wired Risk+Position+Session+WarmCache, assert no limit breach
- [ ] **T5.2** — Property 2: Authority Hierarchy + Hot_Path Purity suite (Task 48.1) — HMAC verify, forbid_modules + grep gates, no LLM/blocking/RAG in Hot_Path
- [ ] **T5.3** — Property 3: Latency Budget Compliance suite (Task 49.1) — tick→signal→approval→submit chains, per-stage p99 assertions, e2e p99 < 50ms
- [ ] **T5.4** — Property 4: Score Formula Equivalence suite (Task 50.1) — Adaptive_Risk, Trader_Stability_Score, Trade_Confidence_Score, liquidity_pressure bounds

### Wave 5B — Properties 5-8
- [ ] **T5.5** — Property 5: Serialization Round-Trip suite (Task 51.1) — all FlatBuffers + JSON + CBOR encode-decode, Memory_RAG write-read
- [ ] **T5.6** — Property 6: Incremental Feature Computation suite (Task 52.1) — incremental == window-based reference for all 18 features
- [ ] **T5.7** — Property 7: Strategy Gating suite (Task 53.1) — zero signals from disabled/blocked/below-threshold strategies
- [ ] **T5.8** — Property 8: Edge-Triggered Emission suite (Task 54.1) — regime/priority/war-mode/session/governance/killswitch/target events == adjacent-pair changes

### Wave 5C — Properties 9-12
- [ ] **T5.9** — Property 9: Order Lifecycle FSM suite (Task 55.1) — valid FSM paths, partial-fill aggregation equivalence
- [ ] **T5.10** — Property 10: Subscriber Delivery suite (Task 56.1) — every event delivered to every subscriber exactly once across NATS + Redis + UI
- [ ] **T5.11** — Property 11: Self-Healing suite (Task 57.1) — backoff timing, failover, Redis degraded, API latency spike, Ollama fallback
- [ ] **T5.12** — Property 12: Replay Determinism suite (Task 58.1) — double replay produces identical outputs, SimulatedBroker routing

### Wave 5D — Replay Regression
- [ ] **T5.13** — End-to-end replay regression harness (Task 59.1) — CI job replays canonical session twice, diffs outputs, nightly 5K iteration soak
- [ ] **T5.14** — Smoke verification of regression output (Task 59.2) — regression diff empty, nightly completes within wall-clock budget

---

## Phase 6: CI + Checkpoints + Final Validation
**Goal:** Run all tests, reach all checkpoints, validate the complete system.
**Depends on:** Phase 5 (all suites implemented)
**Estimated tasks:** 5

- [ ] **T6.1** — Run full Rust workspace test suite: `cargo test --workspace --release` — all tests pass
- [ ] **T6.2** — Run full Python test suite: `pytest python/` — all hypothesis + unit tests pass
- [ ] **T6.3** — Run UI build + lint: `cd ui && npm run build && npm run lint` — clean build
- [ ] **T6.4** — Run CI purity checks locally: `scripts/check-forbidden-deps.sh`, `check-forbidden-source.sh`, `check-no-polling.sh` — all pass
- [ ] **T6.5** — Reach final checkpoint (Task 60): all Property 1-12 PBT suites pass at 100 iterations in PR CI and 5,000 iterations in nightly soak

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | 14 | Rust component-level proptests |
| 2 | 15 | Python hypothesis test suites |
| 3 | 4 | UI component + integration tests |
| 4 | 8 | Complete in-progress tasks |
| 5 | 14 | End-to-end PBT suites (12 Properties + regression) |
| 6 | 5 | CI validation + checkpoints |
| **Total** | **60** | **Full gap closure** |

### Recommended Execution Order
1. **Phase 1** first — Rust proptests are fastest to write and validate the core logic
2. **Phase 2** in parallel with Phase 3 — Python PBTs and UI tests are independent
3. **Phase 4** after Phase 1-3 — completing in-progress tasks needs the test feedback
4. **Phase 5** last — end-to-end suites require all components to be individually tested
5. **Phase 6** final — validate everything together

### Quick Wins (highest ROI, lowest effort)
- T1.11 (hedge-risk proptests) — 4 proptests already exist, just need expansion
- T1.12 (hedge-exec proptests) — 1 proptest exists, needs FSM coverage
- T2.7 (psychology hypothesis) — formula is simple `clamp(0.35*D + 0.25*E + 0.20*R + 0.20*P, 0, 1)`
- T2.8 (ranking hypothesis) — formula is simple `clamp(0.30*O + 0.25*T + 0.20*N + 0.15*M + 0.10*D, 0, 1)`
- T3.1-T3.3 (UI component tests) — Vitest + React Testing Library, straightforward
