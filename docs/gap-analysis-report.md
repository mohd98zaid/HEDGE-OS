# PROJECT HEDGE — End-to-End Gap Analysis Report

**Generated:** 2026-06-12
**Reference:** `.kiro/specs/project-hedge/tasks.md`

---

## Executive Summary

The core implementation (Foundation + Hot_Path + Warm_AI + Memory_RAG) is **substantially built** with ~55K+ lines of Rust and ~25K+ lines of Python. However, the project has **significant gaps in testing, validation infrastructure, and a few incomplete cross-cutting components**. The most critical gap is the complete absence of end-to-end PBT suites (Group G, Tasks 47-58) and zero hypothesis-based Python tests.

---

## 1. IMPLEMENTATION STATUS BY GROUP

### A. Foundation (Tasks 1-9) — COMPLETE
All 9 tasks marked `[x]`. Workspace scaffold, hedge-core, hedge-bus, hedge-schemas (8 FlatBuffers), hedge-obs, hedge-config, NATS ACLs, and CI purity checks are implemented.

### B. Hot_Path (Tasks 10-18) — IMPLEMENTATION DONE, TESTS MISSING
All implementation sub-tasks (x.1) marked `[x]`. Every crate has substantial code:

| Crate | Total LOC |
|-------|-----------|
| hedge-risk | 3,367 |
| hedge-exec | 3,072 |
| hedge-market-data | ~3,300 |
| hedge-orderflow | ~2,400 |
| hedge-features | ~2,700 |
| hedge-signals | ~2,850 |
| hedge-position | ~1,600 |
| hedge-broker-zerodha | 941 |
| hedge-broker-dhan | 752 |
| hedge-broker-shoonya | 946 |
| hedge-broker-angelone | 968 |
| hedge-broker-simulated | 1,230 |
| hedge-broker-upstox | 908 |

**Missing test sub-tasks** (all marked `[ ]`):

| Task | Crate | Required Test | Status |
|------|-------|---------------|--------|
| 2.2 | hedge-core | Px arithmetic, RingWindow, LatencyTimer proptests | No `tests/` dir |
| 3.2 | hedge-bus | Subscriber delivery, zero-copy tests | Only unit tests in lib modules |
| 4.2 | hedge-schemas | FlatBuffers/JSON round-trip proptests | No proptests |
| 5.2 | hedge-obs | Budget-breach event emission tests | No tests dir |
| 6.2 | hedge-config | Default values, schema-violation tests | No tests dir |
| 7.2 | NATS ACLs | Integration test for ACL enforcement | No integration tests |
| 8.2 | CI purity | Smoke tests for forbidden deps | CI scripts exist, no Rust smoke test |
| 10.2 | hedge-market-data | Tick ingest p99, per-symbol distribution | 1 proptest in breadth.rs (partial) |
| 11.2 | hedge-orderflow | Liquidity pressure bounds, zero-alloc | 1 proptest in engine.rs (partial) |
| 12.2 | hedge-features | Incremental == reference, p99 latency | No proptests (only unit tests) |
| 13.2 | hedge-signals | Strategy gating proptests | 1 proptest in tests/properties.rs (partial) |
| 14.2 | hedge-risk | Risk limit invariant proptests | 4 proptests in tests/properties.rs (partial) |
| 15.2 | hedge-exec | Approval token FSM proptests | 1 proptest in tests/authority.rs (partial) |
| 16.2 | hedge-position | Position aggregation proptests | Proptests in tests/ (partial) |
| 17.2 | Broker adapters | FSM substitutability, credential tests | No tests dir |

### C. Warm_AI_Pipeline (Tasks 19-30) — IMPLEMENTATION DONE, TESTS MISSING
All implementation tasks `[x]`. Python modules are well-structured with ~19K LOC:

| Module | Files | LOC |
|--------|-------|-----|
| psychology | 8 | ~2K+ |
| journal | 10 | ~2K+ |
| governance | 12 | ~2K+ |
| shadow | 10 | ~2K+ |
| ranking | 8 | ~1.5K+ |
| news | 4 | ~1K+ |
| regime | 8 | ~1.5K+ |
| priority | 7 | ~1K+ |
| prev_day | 4 | ~800+ |
| onnx_runtime | 6 | ~1K+ |
| ollama_client | 4 | ~500+ |
| schemas | 20+ | ~2K+ |

**Missing test sub-tasks** (all marked `[ ]`):

| Task | Module | Required Test | Status |
|------|--------|---------------|--------|
| 19.2 | ollama_client | Hypothesis test for fallback routing | Only basic pytest |
| 20.2 | onnx_runtime | Hypothesis latency test | No hypothesis tests |
| 21.2 | news | Hypothesis fast-path, bounds, non-blocking | No hypothesis tests |
| 22.2 | regime | Hypothesis edge-triggered emission | No hypothesis tests |
| 23.2 | priority | Hypothesis totality, edge-triggered | pytest exists but no hypothesis |
| 24.2 | prev_day | Hypothesis round-trip | No hypothesis tests |
| 25.2 | psychology | Hypothesis score formula, threshold ladder | No hypothesis tests |
| 26.2 | ranking | Hypothesis score formula, latency | No hypothesis tests |
| 27.2 | journal | Hypothesis round-trip | No hypothesis tests |
| 28.2 | governance | Hypothesis governance level changes | No hypothesis tests |
| 29.2 | shadow | Hypothesis shadow filtering | No hypothesis tests |

**ZERO files use `from hypothesis` or `import hypothesis` anywhere in the Python codebase.**

### D. Memory_RAG_Layer (Tasks 31-35) — IMPLEMENTATION DONE, TESTS MISSING
All implementation tasks `[x]`. ~5,400 LOC across qdrant, timescale, redis_cache, retrieval.

| Task | Module | Required Test | Status |
|------|--------|---------------|--------|
| 31.2 | qdrant | Hypothesis round-trip | No tests dir |
| 32.2 | timescale | Hypothesis round-trip | No tests dir |
| 33.2 | redis_cache | Hypothesis staleness, reconnection | No tests dir |
| 34.2 | retrieval | Hypothesis pipeline correctness, Hot_Path purity | No tests dir |

### E. UI Gateway + Human_Control_UI (Tasks 36-39) — PARTIAL

| Task | Status | Gap |
|------|--------|-----|
| 36.1 | IN PROGRESS | UI gateway has 3,163 LOC. Likely missing: shadowed AI filtering verification, high-volatility presentation mode |
| 36.2 | NOT STARTED | No proptests for ui-gateway delivery |
| 37.1 | PARTIAL | 17 panel components exist. All panels from design are present |
| 37.2 | NOT STARTED | **ZERO component tests** (no .test.tsx/.spec.tsx files) |
| 38.1 | PARTIAL | KillSwitch, StrategyToggles, SymbolPriorityControls exist |
| 38.2 | NOT STARTED | No integration tests for trader controls |
| 39 | PARTIAL | Checkpoint incomplete |

### F. Cross-Cutting (Tasks 40-46) — PARTIAL

| Task | Status | Gap |
|------|--------|-----|
| 40.1 | PARTIAL | Replay has 2,687 LOC + canonical fixture. Missing: full integration |
| 40.2 | NOT STARTED | No proptests for replay determinism |
| 41.1 | PARTIAL | Supervisor has 2,932 LOC |
| 41.2 | NOT STARTED | No proptests for self-healing |
| 42.1 | PARTIAL | War_Mode partially implemented |
| 42.2 | NOT STARTED | No proptests for war-mode |
| 43.1 | IN PROGRESS | Session manager has 2,111 LOC but task in progress |
| 43.2 | NOT STARTED | No proptests for session manager |
| 44.1 | COMPLETE | WarmCache complete |
| 44.2 | NOT STARTED | No proptests for warmcache non-blocking |
| 45.1 | IN PROGRESS | 5 Grafana dashboards exist but task in progress |
| 45.2 | NOT STARTED | No JSON validation tests |
| 46 | PARTIAL | Checkpoint incomplete |

### G. Integration & PBT Validation (Tasks 47-60) — NOT STARTED
**All 14 tasks unchecked.** This is the largest gap:

- Task 47-58: 12 end-to-end PBT property suites — **NONE IMPLEMENTED**
- Task 59.1: Nightly replay regression — CI workflow exists but is skeletal
- Task 59.2: Smoke verification — NOT IMPLEMENTED
- Task 60: Final integration checkpoint — NOT REACHED

---

## 2. CRITICAL GAPS RANKED BY SEVERITY

### CRITICAL (blocks production readiness)

1. **Zero hypothesis tests in Python** — All 11 Warm_AI_Pipeline test tasks (19.2-29.2) and 4 Memory_RAG_Layer test tasks (31.2-34.2) are completely unimplemented. The design requires hypothesis 6.x for all Python PBT.

2. **End-to-end PBT suites (Group G) entirely missing** — Tasks 47-58 cover the 12 Correctness Properties that are the formal safety guarantees of the system. Without these, there is no proof that Risk Limits, Authority Hierarchy, Latency Budgets, Score Equivalence, Serialization, Feature Equivalence, Strategy Gating, Edge Triggers, FSM Validity, Subscriber Delivery, Self-Healing, or Replay Determinism hold.

3. **Most Rust proptest sub-tasks unchecked** — While some crates have partial proptests (risk has 4, signals has 1, orderflow has 1), many required properties are untested. The proptests that exist may not cover all required properties per their task specifications.

### HIGH (affects reliability)

4. **Zero UI component tests** — 17 React panels with zero .test.tsx files. Task 37.2 requires unit tests for alert ordering, latency histograms, and high-volatility mode.

5. **Missing integration tests for most Rust crates** — Only 6/25 crates have `tests/` directories. Broker adapters (7 crates), hedge-bus, hedge-config, hedge-obs, hedge-replay, hedge-session, hedge-supervisor, hedge-ui-gateway, hedge-warmcache all lack integration tests.

6. **Task 43.1 (Session manager) marked in progress** — May be incomplete.

7. **Task 36.1 (UI gateway) marked in progress** — May be missing features.

### MEDIUM (completeness)

8. **Task 45.1 (Grafana) marked in progress** — Dashboards exist but may not be complete.

9. **Canonical replay fixture minimal** — Only one segment file exists (`seg-0001.rkyv`).

10. **No test for NATS ACL enforcement** (Task 7.2) — ACL config exists but no integration test validates rejection.

---

## 3. WHAT'S ACTUALLY GOOD

- **All 25 Rust crates have substantial implementation** (>500 LOC each, most >1500 LOC)
- **All Python modules are well-structured** with engine/service/publisher/config patterns
- **CI purity enforcement is solid** — 3-layer checks (forbidden deps, forbidden source, no polling)
- **Nightly soak workflow is well-designed** with 4 parallel jobs
- **Docker infrastructure is complete** — Dockerfiles for all services, compose with profiles
- **NATS ACL provisioning** with per-account credentials
- **FlatBuffers schemas** (8 files) covering all Hot_Path events
- **Pydantic schema mirrors** (20+ models) for all JSON events
- **Grafana dashboards** (5 dashboards: hot-path-latency, warm-ai-performance, broker-performance, risk-events, trader-psychology)
