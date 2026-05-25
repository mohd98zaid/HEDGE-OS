# PROJECT HEDGE

Production-grade, ultra-low-latency AI-assisted intraday trading operating
system for NSE/BSE. See `.kiro/specs/project-hedge/` for the requirements,
design, and task plan.

## Layout

```
Cargo.toml                 # workspace root
crates/                    # Rust crates (Hot_Path + ui-gateway)
python/
  hedge_warm_ai/           # Warm_AI_Pipeline microservices
  hedge_memory_rag/        # Memory_RAG_Layer service
docker/                    # one Dockerfile per service + infra configs
docker-compose.yml         # full stack on the hedge-net internal network
ui/                        # React + TypeScript + Tailwind cockpit (Vite)
.kiro/                     # specs and steering
```

## Build status

This commit lands **task 1.1** — workspace and project structure scaffold.
Every crate compiles to a `lib.rs` (and a `main.rs` for service crates) stub.
Concrete implementation arrives in subsequent tasks (2.1, 3.1, ...).

## Quickstart

```bash
# Rust workspace
cargo check --workspace

# Python packages (each is independently installable)
pip install -e python/hedge_warm_ai
pip install -e python/hedge_memory_rag

# UI (configuration only; npm install separately)
cd ui && npm install && npm run dev

# Full stack on Docker
docker compose --profile full up
```
