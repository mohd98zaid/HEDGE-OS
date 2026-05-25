# hedge-warm-ai

Asynchronous AI services for PROJECT HEDGE.

Each entry point in `[project.scripts]` runs as an independent Docker microservice
and communicates with the rest of the system **only** through NATS subjects on the
`ai.*` namespace. Concrete service modules land in tasks 22.x onward.

## ONNX Runtime artefacts (task 20.1)

The `hedge_warm_ai.onnx_runtime` sub-package exposes async wrappers for
the classical-ML and fast-NLP models that drive the News_Intelligence and
AI_Trade_Ranking engines. Every wrapper:

* dispatches inference through a bounded thread pool via `asyncio.to_thread`,
* publishes a per-call `obs.latency.ai_<stage>` record (and a
  `obs.budget.breach.ai_<stage>` event when the configured ceiling is
  exceeded) using the project's `LatencyTracer`,
* loads its ONNX session from disk — **weights are never fetched from
  the internet at runtime.**

### Artefact layout

The runtime resolves model paths via `hedge_warm_ai.onnx_runtime.resolve_layout`.
Resolution order:

1. `--root` argument or `HEDGE_ONNX_MODELS_DIR` environment variable.
2. `$HEDGE_HOME/models/onnx` if `HEDGE_HOME` is set.
3. `/var/lib/hedge/models/onnx` (Linux deploy default).

The conventional tree under that root is:

```
models/onnx/
├── xgboost.onnx
├── lightgbm.onnx
├── isolation_forest.onnx
├── tiny_lstm.onnx
├── finbert/
│   ├── model.onnx
│   └── tokenizer/...
└── distilbert/
    ├── model.onnx
    └── tokenizer/...
```

### Conversion CLI

```bash
# Print the resolved layout
python -m hedge_warm_ai.onnx_runtime.cli list-paths

# Convert a local FinBERT checkpoint to ONNX
python -m hedge_warm_ai.onnx_runtime.cli convert-finbert \
    --checkpoint /path/to/finbert \
    --target /var/lib/hedge/models/onnx/finbert

# Convert a local DistilBERT checkpoint to ONNX
python -m hedge_warm_ai.onnx_runtime.cli convert-distilbert \
    --checkpoint /path/to/distilbert \
    --target /var/lib/hedge/models/onnx/distilbert
```

XGBoost, LightGBM, Isolation Forest, and Tiny LSTM artefacts are emitted
from training scripts via the `convert_*_to_onnx` functions in
`hedge_warm_ai.onnx_runtime.classical`.
