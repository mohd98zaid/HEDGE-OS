//! Deterministic RNG (mulberry32) with per-stream split from a master seed.
//!
//! Used by every generator so that two runs with the same seed and same
//! wall-clock duration produce byte-identical NATS publish sequences
//! (REQ-3.1, REQ-3.2 of the full-cockpit-data spec).
//!
//! ### Why mulberry32
//!
//! 32-bit state, single multiplication + xorshift per step, no allocations,
//! no platform variance, no security claim — all of which match the
//! "deterministic dev fixture" use case. We do not need cryptographic
//! quality; we need reproducibility and small surface area.

/// Master seed used by every demo-synth run.
pub const MASTER_SEED: u32 = 0x5EEDED_u32;

/// Stable, byte-equal mulberry32 RNG.
///
/// Constructors:
///
/// * [`Mulberry32::with_seed`] — explicit seed.
/// * [`Mulberry32::for_stream`] — [`MASTER_SEED`] mixed with a per-stream
///   tag so different generators don't share a sequence.
#[derive(Copy, Clone, Debug)]
pub struct Mulberry32 {
    state: u32,
}

impl Mulberry32 {
    /// Construct from a raw seed.
    #[inline]
    pub const fn with_seed(seed: u32) -> Self {
        Self { state: seed }
    }

    /// Construct a per-stream RNG by mixing [`MASTER_SEED`] with the stream
    /// tag. Each generator picks a unique tag (see [`stream`]).
    #[inline]
    pub const fn for_stream(tag: u32) -> Self {
        Self {
            state: MASTER_SEED ^ tag,
        }
    }

    /// Advance the RNG and return the next `u32`.
    #[inline]
    pub fn next_u32(&mut self) -> u32 {
        self.state = self.state.wrapping_add(0x6D2B79F5_u32);
        let mut z = self.state;
        z = (z ^ (z >> 15)).wrapping_mul(z | 1);
        z ^= z.wrapping_add((z ^ (z >> 7)).wrapping_mul(z | 61));
        z ^ (z >> 14)
    }

    /// Uniform `f64` in `[0, 1)`.
    #[inline]
    pub fn next_f64(&mut self) -> f64 {
        // Use the upper 53 bits to fit an f64 mantissa exactly.
        let hi = self.next_u32() as u64;
        let lo = self.next_u32() as u64;
        let bits53 = ((hi << 21) | (lo >> 11)) & ((1u64 << 53) - 1);
        bits53 as f64 / (1u64 << 53) as f64
    }

    /// Uniform integer in `[lo, hi)` (exclusive upper bound). Returns `lo`
    /// if `hi <= lo`.
    #[inline]
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        if hi <= lo {
            return lo;
        }
        let span = (hi - lo) as u64;
        lo + ((self.next_u32() as u64) % span) as i64
    }

    /// Uniform `f64` in `[lo, hi)`.
    #[inline]
    pub fn range_f64(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next_f64()
    }
}

/// Stream tags for each generator. Picking distinct tags keeps the per-
/// generator sequences independent so adding a new generator does not
/// shift the byte output of unrelated ones.
pub mod stream {
    pub const TICK: u32 = 0x01;
    pub const BOOK: u32 = 0x02;
    pub const OI: u32 = 0x03;
    pub const BREADTH: u32 = 0x04;
    pub const CONNECTION: u32 = 0x05;
    pub const ORDERFLOW_EVENT: u32 = 0x06;
    pub const ORDERFLOW_HEATMAP: u32 = 0x07;
    pub const FEATURES: u32 = 0x08;
    pub const SIGNAL: u32 = 0x09;
    pub const AI_RANK: u32 = 0x0A;
    pub const RISK: u32 = 0x0B;
    pub const EXEC: u32 = 0x0C;
    pub const POSITION: u32 = 0x0D;
    pub const NEWS: u32 = 0x0E;
    pub const PSYCH: u32 = 0x0F;
    pub const LATENCY: u32 = 0x10;
    pub const REPLAY: u32 = 0x11;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn determinism_same_seed_same_sequence() {
        let mut a = Mulberry32::for_stream(stream::TICK);
        let mut b = Mulberry32::for_stream(stream::TICK);
        for _ in 0..1000 {
            assert_eq!(a.next_u32(), b.next_u32());
        }
    }

    #[test]
    fn distinct_streams_diverge() {
        let mut a = Mulberry32::for_stream(stream::TICK);
        let mut b = Mulberry32::for_stream(stream::BOOK);
        // The two sequences must differ within a few steps. Looking at
        // even one collision in 16 draws is statistically fine.
        let mut differ = false;
        for _ in 0..16 {
            if a.next_u32() != b.next_u32() {
                differ = true;
                break;
            }
        }
        assert!(differ, "two distinct streams produced identical first 16 draws");
    }

    #[test]
    fn next_f64_in_unit_range() {
        let mut r = Mulberry32::for_stream(stream::SIGNAL);
        for _ in 0..1000 {
            let x = r.next_f64();
            assert!((0.0..1.0).contains(&x), "out of range: {}", x);
        }
    }

    #[test]
    fn range_i64_inclusive_lo_exclusive_hi() {
        let mut r = Mulberry32::for_stream(stream::EXEC);
        for _ in 0..200 {
            let v = r.range_i64(10, 20);
            assert!((10..20).contains(&v));
        }
    }

    #[test]
    fn range_i64_handles_degenerate_bounds() {
        let mut r = Mulberry32::for_stream(stream::RISK);
        assert_eq!(r.range_i64(5, 5), 5);
        assert_eq!(r.range_i64(7, 3), 7);
    }
}
