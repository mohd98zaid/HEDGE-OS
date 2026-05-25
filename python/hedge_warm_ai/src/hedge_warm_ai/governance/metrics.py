"""Metric estimators for the AI_Governance_Engine (R23.3, R24.1).

Four pure functions, one per :class:`MetricKind`. Each is implemented
without numpy or scipy so the engine stays import-light in
environments that do not need the heavy ML stack (the rest of the
Warm_AI_Pipeline pulls numpy in for ONNX inference; we deliberately do
not need it here).

Estimator design
----------------

All four estimators consume a bounded :class:`RollingWindow` of recent
samples and produce a single ``[0.0, 1.0]`` scalar where higher values
indicate **more degraded** behaviour. This sign convention is uniform
so the threshold ladder can apply ``value >= threshold`` consistently
across every metric.

* :func:`compute_drift` — a small-sample population-stability-index
  (PSI) on the current window vs. a stable reference window. PSI is
  bounded to ``[0.0, 1.0]`` via ``min(psi, 1.0)`` so the signal sits
  in the same range as the other metrics. Equivalent to the KS-test
  in monotonic behaviour for shape changes; we use PSI instead of
  KS because it is cheaper and robust to small windows.
* :func:`compute_confidence_stability` — variance of consecutive
  confidence outputs over the window, normalised to ``[0.0, 1.0]``
  via the ``confidence ∈ [0, 1]`` upper-bound on variance. Higher
  variance = less stable = more degraded.
* :func:`compute_hallucination_rate` — count of hallucination flags
  over the window divided by the window size, in ``[0.0, 1.0]``.
* :func:`compute_prediction_inaccuracy` — fraction of the recent
  outcome window that did **not** match its component's directional
  intent, in ``[0.0, 1.0]``. Higher = less accurate = more degraded.

The four estimators share a common contract:

* They never raise. An empty or too-short window returns ``0.0``
  (the "best" / least-degraded value) so the engine cannot trip a
  threshold from cold-start absence of data.
* They are pure functions of their inputs. The engine calls them on
  every observation, and the property tests in 28.2 fuzz them
  directly.
"""

from __future__ import annotations

from typing import Sequence

from .state import RollingWindow

__all__ = [
    "compute_confidence_stability",
    "compute_drift",
    "compute_hallucination_rate",
    "compute_prediction_inaccuracy",
]


# ---------------------------------------------------------------------------
# Helpers -------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _clamp_unit(value: float) -> float:
    """Clamp *value* to ``[0.0, 1.0]`` (NaN-safe)."""
    if value != value:  # NaN
        return 0.0
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _histogram(samples: Sequence[float], *, bins: int) -> list[float]:
    """Build a fixed-bin histogram over [0.0, 1.0] for ``samples``.

    ``bins`` divides ``[0.0, 1.0]`` into equal-width buckets. Samples
    outside the range are clamped to the nearest edge bin. The
    histogram is normalised to a probability distribution.
    """
    if bins <= 0:
        raise ValueError(f"bins must be positive; got {bins!r}")
    counts = [0.0] * bins
    if not samples:
        return counts
    width = 1.0 / bins
    for raw in samples:
        if raw != raw:  # NaN
            continue
        x = _clamp_unit(float(raw))
        idx = int(x / width)
        if idx >= bins:
            idx = bins - 1
        counts[idx] += 1.0
    total = sum(counts)
    if total == 0.0:
        return counts
    return [c / total for c in counts]


def _smoothed_log_ratio(p: float, q: float, *, eps: float = 1e-4) -> float:
    """Compute ``log(p / q)`` with eps-smoothing on both sides.

    Eps-smoothing prevents ``log(0)`` and ``division by zero`` when a
    bin is empty in either the reference or the observed window.
    """
    p_smoothed = p + eps
    q_smoothed = q + eps
    # Math without numpy: the natural log lives in the stdlib.
    import math

    return math.log(p_smoothed / q_smoothed)


# ---------------------------------------------------------------------------
# Drift (PSI) ---------------------------------------------------------------
# ---------------------------------------------------------------------------


def compute_drift(
    window: RollingWindow,
    *,
    reference: Sequence[float] = (),
    bins: int = 10,
    min_samples: int = 4,
) -> float:
    """Population-stability-index drift between *window* and *reference*.

    Returns a value in ``[0.0, 1.0]``. The PSI definition::

        PSI = sum_i (p_i - q_i) * log(p_i / q_i)

    where ``p`` is the current window's normalised histogram and
    ``q`` is the reference's. The result is naturally non-negative
    and unbounded above; we apply ``min(psi, 1.0)`` so the value sits
    in the engine's uniform range.

    A ``RollingWindow`` shorter than ``min_samples`` (or an empty
    reference) returns ``0.0``: the engine treats cold-start absence
    of evidence as "no drift" rather than tripping a threshold from
    nothing.

    Args:
        window: Live :class:`RollingWindow` of recent feature values
            in ``[0.0, 1.0]`` (clamped on bin assignment).
        reference: Stable reference distribution (typically the
            engine's first-fill of the window, captured on engine
            startup). When empty, drift is considered ``0.0``.
        bins: Number of equal-width histogram buckets.
        min_samples: Minimum observed-window size before a non-zero
            result is returned. Bounds noise on the cold start.

    Returns:
        Drift score clamped to ``[0.0, 1.0]``.
    """
    samples = window.snapshot()
    if len(samples) < min_samples or not reference:
        return 0.0

    p = _histogram(samples, bins=bins)
    q = _histogram(reference, bins=bins)

    psi = 0.0
    for p_i, q_i in zip(p, q):
        psi += (p_i - q_i) * _smoothed_log_ratio(p_i, q_i)
    return _clamp_unit(psi)


# ---------------------------------------------------------------------------
# Confidence stability ------------------------------------------------------
# ---------------------------------------------------------------------------


def compute_confidence_stability(
    window: RollingWindow,
    *,
    min_samples: int = 4,
) -> float:
    """Variance-based instability of consecutive confidence outputs.

    Returns the **instability** in ``[0.0, 1.0]`` (higher = less
    stable = more degraded). Variance over a sample drawn from
    ``[0, 1]`` is bounded above by ``0.25`` (achieved by an even
    half/half split between 0 and 1); we normalise by ``0.25`` so a
    fully bimodal stream returns ``1.0``.

    Cold-start protection: a window shorter than ``min_samples``
    returns ``0.0``.
    """
    samples = window.snapshot()
    n = len(samples)
    if n < min_samples:
        return 0.0
    mean = sum(samples) / n
    variance = sum((x - mean) ** 2 for x in samples) / n
    # Normalise: max variance over [0,1] is 0.25.
    return _clamp_unit(variance / 0.25)


# ---------------------------------------------------------------------------
# Hallucination rate --------------------------------------------------------
# ---------------------------------------------------------------------------


def compute_hallucination_rate(
    window: RollingWindow,
    *,
    min_samples: int = 1,
) -> float:
    """Fraction of recent observations flagged as hallucinations.

    Each entry in *window* is a 0.0/1.0 indicator (1.0 means the
    service-layer adapter flagged the observation as hallucinated).
    The result is the simple mean — already in ``[0.0, 1.0]``.

    Cold-start protection: an empty window returns ``0.0``.
    """
    samples = window.snapshot()
    n = len(samples)
    if n < min_samples or n == 0:
        return 0.0
    return _clamp_unit(sum(samples) / n)


# ---------------------------------------------------------------------------
# Prediction inaccuracy ----------------------------------------------------
# ---------------------------------------------------------------------------


def compute_prediction_inaccuracy(
    window: RollingWindow,
    *,
    min_samples: int = 1,
) -> float:
    """Fraction of recent component outputs that mis-predicted the outcome.

    Each entry in *window* is a 0.0/1.0 indicator (1.0 means the
    component's output mis-predicted the realised market outcome).
    The result is the simple mean — already in ``[0.0, 1.0]``.

    The engine populates this window from
    :meth:`AiGovernanceEngine.observe_outcome`: each outcome's
    correlation_id is matched against the component's recent output
    history and a 1.0/0.0 indicator is appended.

    Cold-start protection: an empty window returns ``0.0``.
    """
    samples = window.snapshot()
    n = len(samples)
    if n < min_samples or n == 0:
        return 0.0
    return _clamp_unit(sum(samples) / n)
