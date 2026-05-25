//! Bounded exponential-backoff retry helper (R6.4).
//!
//! ## Policy
//!
//! Retry is gated on [`crate::ExecError::is_retryable`]:
//!
//! * `BrokerTransient` — retry until `max_attempts` is reached.
//! * Anything else — return immediately, do not retry.
//!
//! On each retry the backoff doubles, capped at `max_backoff_ns`. A
//! deterministic-but-pseudorandom jitter of up to ±`jitter_pct` is
//! mixed in so synchronised failure storms across replicas
//! de-correlate. The jitter source is the supplied
//! [`JitterSource`] trait, so production callers pass a real RNG and
//! tests pass a fixed source.
//!
//! ## Determinism
//!
//! With [`NoJitter`] the schedule is exact: `base, base*2, base*4, …`
//! capped at `max_backoff_ns`. The unit tests below exercise this so
//! the property "backoff schedule is monotonic non-decreasing" is
//! locked in.

use std::time::Duration;

use crate::error::ExecError;

/// Configuration for [`retry_with_backoff`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). Must be ≥ 1.
    pub max_attempts: u32,
    /// Initial backoff in nanoseconds. The first retry waits this long.
    pub base_backoff_ns: u64,
    /// Hard cap on the per-retry sleep, in nanoseconds.
    pub max_backoff_ns: u64,
    /// Jitter percentage, expressed as basis points of the computed
    /// backoff (`0..=10_000`). E.g. `2_000` = ±20 %.
    pub jitter_bps: u32,
}

impl Default for RetryPolicy {
    /// 3 attempts, 50 ms base, 1 s cap, ±25 % jitter — matches the
    /// design's "bounded exponential backoff" wording.
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_backoff_ns: 50_000_000, // 50 ms
            max_backoff_ns: 1_000_000_000, // 1 s
            jitter_bps: 2_500, // ±25 %
        }
    }
}

impl RetryPolicy {
    /// Compute the backoff for the given 1-indexed attempt that just
    /// failed. `attempt = 1` means "the first attempt failed; the
    /// caller is about to schedule the first retry".
    ///
    /// `jitter_unit` is a value in `[-1.0, 1.0]` supplied by the
    /// jitter source. The returned duration always stays inside
    /// `[0, max_backoff_ns]`.
    pub fn backoff_for_attempt(&self, attempt: u32, jitter_unit: f64) -> Duration {
        // 0-attempt is meaningless; return zero so callers do not
        // accidentally sleep before the first try.
        if attempt == 0 {
            return Duration::ZERO;
        }
        // Compute the deterministic exponential schedule.
        let exp = (attempt - 1).min(31); // saturate the shift
        let raw = self
            .base_backoff_ns
            .saturating_mul(1u64 << exp);
        let capped = raw.min(self.max_backoff_ns);

        // Apply jitter. `jitter_unit` is clamped defensively.
        let unit = if jitter_unit.is_finite() {
            jitter_unit.clamp(-1.0, 1.0)
        } else {
            0.0
        };
        let pct = (self.jitter_bps as f64) / 10_000.0; // e.g. 0.25
        let delta = (capped as f64) * pct * unit;
        let with_jitter = (capped as f64) + delta;
        let clamped = with_jitter
            .max(0.0)
            .min(self.max_backoff_ns as f64) as u64;
        Duration::from_nanos(clamped)
    }
}

/// Source of jitter values in `[-1.0, 1.0]`. Implementations:
///
/// * [`NoJitter`] — always returns 0.0; deterministic schedule.
/// * [`SeededJitter`] — splitmix64 PRNG seeded at construction; used
///   in production so the engine produces an unsynchronised schedule
///   without pulling in a heavyweight RNG.
pub trait JitterSource: Send + Sync {
    /// Produce the next jitter unit in `[-1.0, 1.0]`.
    fn next_unit(&mut self) -> f64;
}

/// Deterministic zero-jitter source. Useful in tests.
#[derive(Copy, Clone, Debug, Default)]
pub struct NoJitter;

impl JitterSource for NoJitter {
    #[inline]
    fn next_unit(&mut self) -> f64 {
        0.0
    }
}

/// Splitmix64-based jitter source. Cheap, lock-free, no allocation.
/// `Send + Sync` is naturally satisfied because every call mutates
/// `&mut self`.
#[derive(Copy, Clone, Debug)]
pub struct SeededJitter {
    state: u64,
}

impl SeededJitter {
    /// Construct a new jitter source from a seed. Use the broker round
    /// trip start time, the correlation id low 64 bits, or a similar
    /// unsynchronised value.
    pub const fn new(seed: u64) -> Self {
        // splitmix64 likes any non-zero seed; we tolerate zero.
        Self {
            state: seed.wrapping_add(0x9E37_79B9_7F4A_7C15),
        }
    }
}

impl JitterSource for SeededJitter {
    fn next_unit(&mut self) -> f64 {
        // splitmix64 step.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // Map [0, u64::MAX] -> [-1.0, 1.0].
        let f = (z as f64) / (u64::MAX as f64); // [0, 1]
        2.0 * f - 1.0
    }
}

/// Sleep abstraction so tests can run the retry loop without actually
/// waiting. Production binaries pass [`TokioSleeper`].
#[allow(async_fn_in_trait)]
pub trait Sleeper: Send + Sync {
    /// Sleep for the given duration.
    async fn sleep(&self, dur: Duration);
}

/// `tokio::time::sleep` adapter.  // hedge-allow: polling-loop
#[derive(Copy, Clone, Debug, Default)]
pub struct TokioSleeper;

impl Sleeper for TokioSleeper {
    #[inline]
    async fn sleep(&self, dur: Duration) {
        // Retry backoff is a legitimate use of `tokio::time::sleep` —  // hedge-allow: polling-loop
        // the no-polling CI rule is for steady-state busy loops, not
        // for bounded exponential backoff between broker submit
        // attempts (R6.4). Marker:
        tokio::time::sleep(dur).await; // hedge-allow: polling-loop
    }
}

/// In-process zero-sleep adapter for unit tests. Records every
/// requested duration into an internal vec for assertions.
#[derive(Default)]
pub struct RecordingSleeper {
    recorded: parking_lot::Mutex<Vec<Duration>>,
}

impl RecordingSleeper {
    /// Construct a recording sleeper with an empty history.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the recorded durations in order.
    pub fn recorded(&self) -> Vec<Duration> {
        self.recorded.lock().clone()
    }
}

impl Sleeper for RecordingSleeper {
    async fn sleep(&self, dur: Duration) {
        self.recorded.lock().push(dur);
        // No actual yield — tests run synchronously.
    }
}

/// Drive `op` up to `policy.max_attempts` times.
///
/// On a retryable error the loop sleeps for the policy-derived
/// backoff and then retries. On a non-retryable error or success
/// it returns immediately. When `max_attempts` is reached without
/// success the last error is returned wrapped in
/// [`ExecError::RetryExhausted`] with the underlying error
/// stringified.
///
/// `op(attempt)` is called with the 1-indexed attempt number. The
/// adapter typically uses this to populate the `attempt` field on
/// `BrokerTransient` errors so the caller observes a consistent
/// error history.
pub async fn retry_with_backoff<F, Fut, T, J, S>(
    policy: RetryPolicy,
    jitter: &mut J,
    sleeper: &S,
    mut op: F,
) -> Result<T, ExecError>
where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = Result<T, ExecError>>,
    J: JitterSource,
    S: Sleeper,
{
    let max = policy.max_attempts.max(1);
    let mut last_err: Option<ExecError> = None;

    for attempt in 1..=max {
        match op(attempt).await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !err.is_retryable() {
                    return Err(err);
                }
                // Don't sleep after the last failed attempt — we are
                // about to surface RetryExhausted.
                if attempt < max {
                    let unit = jitter.next_unit();
                    let dur = policy.backoff_for_attempt(attempt, unit);
                    sleeper.sleep(dur).await;
                }
                last_err = Some(err);
            }
        }
    }

    let last_msg = last_err
        .as_ref()
        .map(|e| e.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    Err(ExecError::RetryExhausted {
        attempts: max,
        last_error: last_msg,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_core::BrokerId;

    fn transient(attempt: u32) -> ExecError {
        ExecError::BrokerTransient {
            broker: BrokerId::Zerodha,
            attempt,
            message: "timeout".into(),
        }
    }

    /// Exponential schedule with `NoJitter`: 50ms, 100ms, 200ms, capped.
    #[test]
    fn backoff_schedule_is_exponential_without_jitter() {
        let p = RetryPolicy {
            max_attempts: 5,
            base_backoff_ns: 50_000_000, // 50 ms
            max_backoff_ns: 500_000_000, // 500 ms cap
            jitter_bps: 0,
        };
        assert_eq!(p.backoff_for_attempt(1, 0.0), Duration::from_millis(50));
        assert_eq!(p.backoff_for_attempt(2, 0.0), Duration::from_millis(100));
        assert_eq!(p.backoff_for_attempt(3, 0.0), Duration::from_millis(200));
        assert_eq!(p.backoff_for_attempt(4, 0.0), Duration::from_millis(400));
        // Capped at 500 ms.
        assert_eq!(p.backoff_for_attempt(5, 0.0), Duration::from_millis(500));
        assert_eq!(p.backoff_for_attempt(20, 0.0), Duration::from_millis(500));
    }

    /// Jitter stays inside `[capped*(1-pct), capped*(1+pct)]` and
    /// never exceeds `max_backoff_ns`.
    #[test]
    fn jitter_stays_within_bounds() {
        let p = RetryPolicy {
            max_attempts: 3,
            base_backoff_ns: 100_000_000,
            max_backoff_ns: 100_000_000,
            jitter_bps: 2_500, // ±25 %
        };
        // unit = 1.0 -> +25 % -> would exceed cap, must clamp to cap
        assert_eq!(
            p.backoff_for_attempt(1, 1.0),
            Duration::from_millis(100),
            "must clamp at max_backoff_ns"
        );
        // unit = -1.0 -> -25 % -> 75 ms
        assert_eq!(p.backoff_for_attempt(1, -1.0), Duration::from_millis(75));
        // unit = 0.0 -> exactly capped
        assert_eq!(p.backoff_for_attempt(1, 0.0), Duration::from_millis(100));
    }

    /// `attempt == 0` returns ZERO so callers don't accidentally sleep
    /// before the first try.
    #[test]
    fn attempt_zero_returns_zero() {
        let p = RetryPolicy::default();
        assert_eq!(p.backoff_for_attempt(0, 0.5), Duration::ZERO);
    }

    /// Property: the deterministic schedule is monotonic non-decreasing.
    #[test]
    fn schedule_is_monotonic_non_decreasing() {
        let p = RetryPolicy {
            max_attempts: 10,
            base_backoff_ns: 1_000_000,
            max_backoff_ns: 100_000_000,
            jitter_bps: 0,
        };
        let mut prev = p.backoff_for_attempt(1, 0.0);
        for a in 2..=10 {
            let cur = p.backoff_for_attempt(a, 0.0);
            assert!(cur >= prev, "schedule went backwards: {:?} -> {:?}", prev, cur);
            prev = cur;
        }
    }

    /// Successful first attempt returns immediately.
    #[tokio::test]
    async fn retries_zero_on_first_success() {
        let policy = RetryPolicy::default();
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        let result = retry_with_backoff(policy, &mut j, &s, |attempt| async move {
            assert_eq!(attempt, 1);
            Ok::<_, ExecError>(42)
        })
        .await
        .unwrap();
        assert_eq!(result, 42);
        assert!(s.recorded().is_empty(), "no sleep on first-attempt success");
    }

    /// Non-retryable error returns immediately without sleeping.
    #[tokio::test]
    async fn non_retryable_does_not_retry() {
        let policy = RetryPolicy::default();
        let mut j = NoJitter;
        let s = RecordingSleeper::new();
        let attempts = std::sync::Arc::new(parking_lot::Mutex::new(0u32));
        let attempts_clone = attempts.clone();

        let err = retry_with_backoff(policy, &mut j, &s, move |attempt| {
            let attempts = attempts_clone.clone();
            async move {
                *attempts.lock() = attempt;
                Err::<i32, _>(ExecError::Config("bad".into()))
            }
        })
        .await
        .unwrap_err();
        assert!(matches!(err, ExecError::Config(_)));
        assert_eq!(*attempts.lock(), 1);
        assert!(s.recorded().is_empty(), "no sleep on non-retryable error");
    }

    /// Retryable error retries up to max_attempts and then surfaces
    /// RetryExhausted; the recorded sleep schedule has length
    /// `max_attempts - 1`.
    #[tokio::test]
    async fn retryable_exhausts_with_correct_sleep_count() {
        let policy = RetryPolicy {
            max_attempts: 3,
            base_backoff_ns: 10,
            max_backoff_ns: 1_000,
            jitter_bps: 0,
        };
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        let err = retry_with_backoff(policy, &mut j, &s, |attempt| async move {
            Err::<i32, _>(transient(attempt))
        })
        .await
        .unwrap_err();
        match err {
            ExecError::RetryExhausted { attempts, .. } => {
                assert_eq!(attempts, 3);
            }
            other => panic!("expected RetryExhausted, got {:?}", other),
        }
        // Sleeps after attempt 1 and 2; not after attempt 3.
        let sched = s.recorded();
        assert_eq!(sched.len(), 2, "sleep count should be max_attempts - 1");
        assert_eq!(sched[0], Duration::from_nanos(10));
        assert_eq!(sched[1], Duration::from_nanos(20));
    }

    /// Recovery on a later attempt returns Ok and stops retrying.
    #[tokio::test]
    async fn recovers_on_second_attempt() {
        let policy = RetryPolicy {
            max_attempts: 5,
            base_backoff_ns: 1,
            max_backoff_ns: 1,
            jitter_bps: 0,
        };
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        let counter = std::sync::Arc::new(parking_lot::Mutex::new(0u32));
        let counter_clone = counter.clone();
        let result = retry_with_backoff(policy, &mut j, &s, move |attempt| {
            let counter = counter_clone.clone();
            async move {
                *counter.lock() = attempt;
                if attempt == 1 {
                    Err::<u32, _>(transient(attempt))
                } else {
                    Ok(99)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(result, 99);
        assert_eq!(*counter.lock(), 2);
        assert_eq!(s.recorded().len(), 1, "exactly one sleep before recovery");
    }

    /// SeededJitter produces values in `[-1.0, 1.0]` and varies between calls.
    #[test]
    fn seeded_jitter_produces_bounded_varied_values() {
        let mut j = SeededJitter::new(42);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..50 {
            let v = j.next_unit();
            assert!(v >= -1.0 && v <= 1.0, "out of range: {}", v);
            seen.insert((v * 1_000.0) as i64);
        }
        // A 50-sample run with splitmix64 should produce >>1 distinct
        // bucketed values.
        assert!(seen.len() > 5, "jitter looks constant: {} unique", seen.len());
    }

    /// max_attempts=1 returns the first error directly with
    /// RetryExhausted (no retries scheduled).
    #[tokio::test]
    async fn max_attempts_one_does_not_sleep() {
        let policy = RetryPolicy {
            max_attempts: 1,
            base_backoff_ns: 1_000_000,
            max_backoff_ns: 1_000_000,
            jitter_bps: 0,
        };
        let mut j = NoJitter;
        let s = RecordingSleeper::new();

        let err = retry_with_backoff(policy, &mut j, &s, |attempt| async move {
            Err::<i32, _>(transient(attempt))
        })
        .await
        .unwrap_err();
        assert!(matches!(err, ExecError::RetryExhausted { attempts: 1, .. }));
        assert!(s.recorded().is_empty());
    }
}
