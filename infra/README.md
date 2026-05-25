# `infra/` — host-level operational artefacts

This directory holds host-level deployment and security artefacts that
sit *outside* the Cargo workspace and the Python package. Everything
under `infra/` is consumed by an operator on a Mumbai VPS or a local GPU
node, not by application code at runtime.

## Layout

```
infra/
├── README.md                     <- this file
└── firewall/
    ├── cloud-llm-domains.txt     <- single source of truth: blocked domains
    ├── egress-deny.rules         <- nftables ruleset (template)
    └── install-egress-firewall.sh<- idempotent installer (sudo, root)
```

## `Ollama_Infrastructure` operational philosophy (R10)

PROJECT HEDGE runs **all** LLM inference locally on Ollama
(R10.1–R10.7). To make that contract enforceable rather than
aspirational, the host the Ollama containers run on is configured to
fail closed when *any* component attempts to reach a public LLM
provider (R10.8). The defence has three layers:

### 1. DNS poisoning (cheap, broad)

`install-egress-firewall.sh` writes
`/etc/dnsmasq.d/hedge-cloud-llm.conf` with one `address=/<domain>/0.0.0.0`
line per entry in `cloud-llm-domains.txt`. The host's resolver returns
`0.0.0.0`/`::` for every blocked domain, so a misconfigured client
trying to reach `api.openai.com` connects to its own loopback and
fails immediately. This catches the 99% case (a library that resolves
its base URL once and caches it).

### 2. nftables L3 drops (defence in depth)

`egress-deny.rules` defines the `inet hedge_egress` table with two
sets — `cloud_llm_v4` and `cloud_llm_v6` — populated at install time
with the resolved IPv4/IPv6 addresses for every domain in the
denylist. Any packet leaving the host with a destination in those
sets is dropped and logged with the `hedge.egress.cloud_llm.*` syslog
prefix. This catches the 0.99% case (a library that hard-codes an IP
or that uses DoH to bypass the local resolver).

### 3. Egress DNS pinning

The same nftables ruleset drops UDP/53 and TCP/53 traffic to anything
outside `127.0.0.0/8`, so applications cannot bypass the poisoned A
records by querying a public resolver directly. This catches the
remaining 0.01% case.

These three layers are independent. A single misconfiguration on any
one of them does not allow egress; an attacker would have to defeat
all three.

### Deploying

On the Ollama host (Ubuntu 22.04+ or Debian 12+):

```bash
sudo apt-get install -y nftables dnsmasq
cd infra/firewall
sudo ./install-egress-firewall.sh           # apply rules
sudo ./install-egress-firewall.sh --dry-run # preview, no changes
```

The script is idempotent: it flushes the `hedge_egress` table and
rewrites the `dnsmasq` snippet on every run. Re-run it after
editing `cloud-llm-domains.txt`.

### Verifying

After install, you should observe (from the host):

```bash
$ dig +short api.openai.com
0.0.0.0
$ curl -sS --max-time 5 https://api.openai.com/
curl: (28) Connection timed out after 5000 milliseconds
$ sudo nft list table inet hedge_egress | head -20
```

A successful connection to `api.openai.com` is a regression — file
it as a P0 immediately.

## Ollama containers (`docker-compose.ollama.yml`)

The compose file at the repository root brings up the four model
microservices required by R10.1–R10.4. Each container:

| Service          | Model                              | Role         | GPU |
|------------------|------------------------------------|--------------|-----|
| `ollama-qwen`    | `qwen2.5:14b-instruct-q4_K_M`      | primary      | 0   |
| `ollama-mistral` | `mistral:7b-instruct-q4_K_M`       | fast         | 1   |
| `ollama-deepseek`| `deepseek-r1:7b-q4_K_M`            | deep         | 2   |
| `ollama-phi`     | `phi3:mini-q4_K_M`                 | lightweight  | 3   |

Notes:

* No host port binding (`expose:`, not `ports:`). The Ollama HTTP API
  is reachable only by other containers on the `hedge-ollama-net`
  Docker network. Use the `OllamaClient` Python module
  (`hedge_warm_ai.ollama_client`) from a Warm_AI service to talk to
  the daemons.
* Each model owns its own named volume so a daemon upgrade for one
  model cannot disturb another's weights.
* GPU pinning is via Docker's standard
  `deploy.resources.reservations.devices.driver=nvidia` block (NVIDIA
  Container Toolkit must be installed on the host —
  `# TODO: production protocol`).
* `OLLAMA_NOHISTORY=1` and `OLLAMA_KEEP_ALIVE=30m` are set on every
  container so the daemon does not call home and keeps weights warm
  in GPU memory between requests.

### Bringing it up

```bash
docker compose -f docker-compose.ollama.yml up -d
docker exec ollama-qwen     ollama pull qwen2.5:14b-instruct-q4_K_M
docker exec ollama-mistral  ollama pull mistral:7b-instruct-q4_K_M
docker exec ollama-deepseek ollama pull deepseek-r1:7b-q4_K_M
docker exec ollama-phi      ollama pull phi3:mini-q4_K_M
```

The first `pull` is the only outbound connection any Ollama container
ever makes; after that, the network is effectively offline relative
to public LLM providers.

## Talking to Ollama from Python (`hedge_warm_ai.ollama_client`)

```python
import asyncio
from hedge_warm_ai import OllamaClient, OllamaModelEndpoint

client = OllamaClient(
    endpoints={
        "qwen":     OllamaModelEndpoint("http://ollama-qwen:11434",     "qwen2.5:14b-instruct-q4_K_M",  timeout_s=30.0),
        "mistral":  OllamaModelEndpoint("http://ollama-mistral:11434",  "mistral:7b-instruct-q4_K_M",   timeout_s=10.0),
        "deepseek": OllamaModelEndpoint("http://ollama-deepseek:11434", "deepseek-r1:7b-q4_K_M",        timeout_s=60.0),
        "phi":      OllamaModelEndpoint("http://ollama-phi:11434",      "phi3:mini-q4_K_M",             timeout_s=5.0),
    },
    fallback_chain={"qwen": "deepseek", "deepseek": "mistral", "mistral": "phi"},
)

async def main() -> None:
    async with client:
        async for tok in client.stream_generate("qwen", prompt="Summarise: ..."):
            print(tok, end="")

asyncio.run(main())
```

On per-model timeout exhaustion, the client publishes one
`ai.ollama.degraded` event on NATS and re-routes the failed request
to the configured fallback model (R10.9). See the `OllamaClient`
docstring for the full contract.
