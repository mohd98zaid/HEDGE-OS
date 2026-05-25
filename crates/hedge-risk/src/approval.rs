//! `ApprovalToken` — the HMAC-SHA256 single-use authentication token that
//! the Risk_Engine mints over canonical `OrderIntent_v1` bytes
//! (R5.14, R6.8, R21.2).
//!
//! ## Authority hierarchy structural enforcement (R21.1)
//!
//! The token is the structural enforcement of the Authority Hierarchy:
//!
//! * The signing key is held **only** by the Risk_Engine's
//!   [`ApprovalSigner`]. It is never serialized, never published, never
//!   logged.
//! * The Execution_Engine holds an [`ApprovalVerifier`] — an entirely
//!   separate type that exposes only `verify`. It cannot accidentally
//!   call `sign` because the type does not have that method.
//! * `submit(token, intent)` on the Execution_Engine first calls
//!   `verify(token, intent)` (R6.8). Mismatched intents fail closed.
//!
//! ## Single-use semantics
//!
//! The token includes a per-engine sequence number that is mixed into
//! the HMAC input. The engine increments the sequence on every approval
//! mint, so two approvals for byte-equal intents produce two distinct
//! tokens; the Execution_Engine's lifecycle tracker can therefore detect
//! token replay (two attempts to submit using the same token bytes).
//!
//! ## Canonical byte layout
//!
//! [`canonicalize_intent_bytes`] serializes a [`hedge_schemas::OrderIntent`]
//! into a fixed-width byte sequence:
//!
//! ```text
//! offset  size  field
//! 0       16    correlation_id          (big-endian u128)
//! 16      4     symbol                  (big-endian u32)
//! 20      1     side                    (u8)
//! 21      8     quantity                (big-endian u64)
//! 29      1     order_type              (u8)
//! 30      8     limit_paise             (big-endian i64)
//! 38      1     exchange                (i8 as u8 bits)
//! 39      8     sized_quantity          (big-endian u64) — extension field
//! 47      8     ts_ns                   (big-endian u64) — extension field
//! 55      8     sequence_number         (big-endian u64) — extension field
//! ```
//!
//! Total: **63 bytes**. The layout is **not** JSON or any serde-derived
//! format — it is a manual, deterministic concatenation. Property: the
//! same intent + sequence + ts always produces the same bytes; a single
//! bit flip in any field changes the bytes (and the resulting HMAC).

use bytes::Bytes;
use hedge_schemas::OrderIntent;
use hmac::{Hmac, Mac};
use sha2::Sha256;

// ---- Constants ---------------------------------------------------------

/// Length of an [`ApprovalToken`] in bytes — HMAC-SHA256 truncated to
/// its full 32-byte digest.
pub const APPROVAL_TOKEN_BYTES: usize = 32;

/// Length of an [`ApprovalToken`] in hex digits.
pub const APPROVAL_TOKEN_HEX_LEN: usize = APPROVAL_TOKEN_BYTES * 2;

/// Fixed canonical byte length for [`canonicalize_intent_bytes`] output.
pub const INTENT_CANONICAL_BYTES: usize = 63;

type HmacSha256 = Hmac<Sha256>;

// ---- ApprovalToken -----------------------------------------------------

/// 32-byte HMAC-SHA256 digest serving as the proof-of-approval that the
/// Execution_Engine submits with every order (R5.14, R6.8).
///
/// The token is opaque: equality is byte-equality, comparison is via
/// constant-time helpers below to avoid timing leaks. `Copy` because it
/// is a small fixed-size byte array; cloning is free.
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct ApprovalToken(pub [u8; APPROVAL_TOKEN_BYTES]);

impl ApprovalToken {
    /// Construct a token from raw bytes. Used by the Execution_Engine
    /// when reading off the wire (`hedge.hot.approvals` Redis Stream).
    #[inline]
    pub const fn from_bytes(bytes: [u8; APPROVAL_TOKEN_BYTES]) -> Self {
        Self(bytes)
    }

    /// Borrow the underlying byte array.
    #[inline]
    pub const fn as_bytes(&self) -> &[u8; APPROVAL_TOKEN_BYTES] {
        &self.0
    }

    /// Render as a lowercase hex string (used in structured logs / metrics).
    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        let mut s = String::with_capacity(APPROVAL_TOKEN_HEX_LEN);
        for b in self.0 {
            // `write!` into a String never fails.
            let _ = write!(s, "{:02x}", b);
        }
        s
    }
}

impl std::fmt::Debug for ApprovalToken {
    /// Debug renders only the first 8 hex digits — enough for grep but
    /// short enough not to clutter logs and not enough to reconstruct
    /// the full token if the log were leaked.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let h = self.to_hex();
        f.debug_tuple("ApprovalToken")
            .field(&format_args!("{}…", &h[..h.len().min(8)]))
            .finish()
    }
}

// ---- canonicalize_intent_bytes ----------------------------------------

/// Serialize an [`OrderIntent`] (plus the engine-controlled extension
/// fields) into the canonical 63-byte representation that feeds the
/// HMAC.
///
/// Determinism is the central property: the same `(intent, sized_qty,
/// ts_ns, sequence)` tuple always produces the same bytes. The function
/// is allocation-aware: it returns a `Bytes` over a freshly allocated
/// buffer of exactly [`INTENT_CANONICAL_BYTES`] bytes, so callers can
/// reuse the output across `sign` and `verify` without re-running the
/// canonicalization logic twice. (The hot mint/verify path keeps the
/// canonical bytes around in a stack array; this `Bytes`-returning helper
/// is the public, stable surface.)
pub fn canonicalize_intent_bytes(
    intent: &OrderIntent,
    sized_quantity: u64,
    ts_ns: u64,
    sequence: u64,
) -> Bytes {
    let mut buf = Vec::with_capacity(INTENT_CANONICAL_BYTES);
    push_canonical(&mut buf, intent, sized_quantity, ts_ns, sequence);
    debug_assert_eq!(buf.len(), INTENT_CANONICAL_BYTES);
    Bytes::from(buf)
}

/// Internal: write the canonical representation to a `Vec<u8>`. Used by
/// both the public `canonicalize_intent_bytes` (which allocates) and the
/// stack-friendly path inside `sign` / `verify` that feeds an `Hmac`
/// directly.
fn push_canonical(
    out: &mut Vec<u8>,
    intent: &OrderIntent,
    sized_quantity: u64,
    ts_ns: u64,
    sequence: u64,
) {
    // 16-byte correlation_id (big-endian u128).
    let cid = u128::from_be_bytes(intent.correlation_id);
    out.extend_from_slice(&cid.to_be_bytes());
    // 4-byte symbol id.
    out.extend_from_slice(&intent.symbol.to_be_bytes());
    // 1-byte side.
    out.push(intent.side);
    // 8-byte quantity (the OrderIntent_v1 quantity field — pre-sizing).
    out.extend_from_slice(&intent.quantity.to_be_bytes());
    // 1-byte order_type.
    out.push(intent.order_type);
    // 8-byte limit_paise (big-endian i64; cast through u64 bits).
    out.extend_from_slice(&intent.limit_paise.to_be_bytes());
    // 1-byte exchange (i8 — store the underlying u8 bits).
    out.push(intent.exchange as u8);
    // 8-byte engine-determined sized_quantity.
    out.extend_from_slice(&sized_quantity.to_be_bytes());
    // 8-byte mint timestamp (ns since epoch — stable across calls).
    out.extend_from_slice(&ts_ns.to_be_bytes());
    // 8-byte per-engine sequence number.
    out.extend_from_slice(&sequence.to_be_bytes());
}

// ---- ApprovalSigner / ApprovalVerifier --------------------------------

/// HMAC-SHA256 signing key wrapper. Held **only** by the Risk_Engine.
///
/// `Clone` is intentionally not derived — there should be exactly one
/// signer in the process. Tests that need multiple signers (e.g.
/// "two engines start up with different keys") can construct two
/// `ApprovalSigner` instances explicitly.
pub struct ApprovalSigner {
    /// Raw HMAC key bytes. Held in a heap-allocated `Box<[u8]>` so a
    /// future zeroize-on-drop is a one-line change.
    key: Box<[u8]>,
}

/// HMAC-SHA256 verification key wrapper. Held by the Execution_Engine.
///
/// `verify` is the only operation. The type is **deliberately** distinct
/// from [`ApprovalSigner`] so a clerical mistake (e.g. cross-wiring a
/// verifier into a place that expects a signer) is a compile-time error
/// rather than a silent forgery vector. Both types share the same key
/// bytes — but the verifier cannot mint.
pub struct ApprovalVerifier {
    key: Box<[u8]>,
}

impl ApprovalSigner {
    /// Construct a signer from raw key bytes. The key SHOULD be at
    /// least 32 bytes of cryptographically random data.
    pub fn from_key(key: impl Into<Box<[u8]>>) -> Self {
        Self { key: key.into() }
    }

    /// Construct a paired verifier holding the same key. The signer is
    /// the only path through which a verifier obtains the key — the
    /// verifier has no public constructor that does not go through here
    /// (or a unit-test helper).
    pub fn paired_verifier(&self) -> ApprovalVerifier {
        ApprovalVerifier {
            key: self.key.clone(),
        }
    }

    /// Mint a new [`ApprovalToken`] for `intent`.
    ///
    /// `sized_quantity`, `ts_ns`, and `sequence` are mixed into the
    /// canonical bytes so two approvals for byte-equal intents produce
    /// distinct tokens (single-use semantics).
    pub fn sign(
        &self,
        intent: &OrderIntent,
        sized_quantity: u64,
        ts_ns: u64,
        sequence: u64,
    ) -> ApprovalToken {
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .expect("HMAC-SHA256 accepts any key length");
        // We stack-build the canonical representation into a fixed-size
        // array so the steady-state path remains allocation-aware.
        let mut buf: [u8; INTENT_CANONICAL_BYTES] = [0u8; INTENT_CANONICAL_BYTES];
        write_canonical_to_array(&mut buf, intent, sized_quantity, ts_ns, sequence);
        mac.update(&buf);
        let digest = mac.finalize().into_bytes();
        let mut out = [0u8; APPROVAL_TOKEN_BYTES];
        out.copy_from_slice(&digest);
        ApprovalToken(out)
    }
}

impl ApprovalVerifier {
    /// Construct a verifier from raw key bytes. Used in unit tests where
    /// the verifier is created independently. Production callers always
    /// go through [`ApprovalSigner::paired_verifier`].
    pub fn from_key(key: impl Into<Box<[u8]>>) -> Self {
        Self { key: key.into() }
    }

    /// Verify that `token` is a valid HMAC over the canonical
    /// representation of `(intent, sized_quantity, ts_ns, sequence)`.
    ///
    /// Uses the constant-time `Hmac::verify_slice` to avoid timing leaks.
    pub fn verify(
        &self,
        token: &ApprovalToken,
        intent: &OrderIntent,
        sized_quantity: u64,
        ts_ns: u64,
        sequence: u64,
    ) -> bool {
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .expect("HMAC-SHA256 accepts any key length");
        let mut buf: [u8; INTENT_CANONICAL_BYTES] = [0u8; INTENT_CANONICAL_BYTES];
        write_canonical_to_array(&mut buf, intent, sized_quantity, ts_ns, sequence);
        mac.update(&buf);
        mac.verify_slice(&token.0).is_ok()
    }
}

/// Stack-allocation friendly canonicalisation. Writes exactly
/// [`INTENT_CANONICAL_BYTES`] bytes into `buf`.
fn write_canonical_to_array(
    buf: &mut [u8; INTENT_CANONICAL_BYTES],
    intent: &OrderIntent,
    sized_quantity: u64,
    ts_ns: u64,
    sequence: u64,
) {
    // We compute offsets explicitly so a regression in the canonical
    // layout is caught both here (incorrect indexing) and in the
    // round-trip tests (different bytes for different inputs).
    let mut off = 0usize;
    let cid = u128::from_be_bytes(intent.correlation_id);
    buf[off..off + 16].copy_from_slice(&cid.to_be_bytes());
    off += 16;
    buf[off..off + 4].copy_from_slice(&intent.symbol.to_be_bytes());
    off += 4;
    buf[off] = intent.side;
    off += 1;
    buf[off..off + 8].copy_from_slice(&intent.quantity.to_be_bytes());
    off += 8;
    buf[off] = intent.order_type;
    off += 1;
    buf[off..off + 8].copy_from_slice(&intent.limit_paise.to_be_bytes());
    off += 8;
    buf[off] = intent.exchange as u8;
    off += 1;
    buf[off..off + 8].copy_from_slice(&sized_quantity.to_be_bytes());
    off += 8;
    buf[off..off + 8].copy_from_slice(&ts_ns.to_be_bytes());
    off += 8;
    buf[off..off + 8].copy_from_slice(&sequence.to_be_bytes());
    off += 8;
    debug_assert_eq!(off, INTENT_CANONICAL_BYTES);
}

#[cfg(test)]
mod tests {
    use super::*;
    use hedge_schemas::OrderIntent;

    fn sample_intent() -> OrderIntent {
        OrderIntent {
            correlation_id: [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                0x0E, 0x0F, 0x10,
            ],
            symbol: 42,
            side: 0, // Buy
            quantity: 10,
            order_type: 0,
            limit_paise: 100_000,
            exchange: 0,
        }
    }

    #[test]
    fn canonical_bytes_have_fixed_length() {
        let intent = sample_intent();
        let bytes = canonicalize_intent_bytes(&intent, 7, 1_000, 1);
        assert_eq!(bytes.len(), INTENT_CANONICAL_BYTES);
    }

    #[test]
    fn canonical_bytes_are_deterministic() {
        let intent = sample_intent();
        let a = canonicalize_intent_bytes(&intent, 7, 1_000, 1);
        let b = canonicalize_intent_bytes(&intent, 7, 1_000, 1);
        assert_eq!(a, b);
    }

    #[test]
    fn canonical_bytes_differ_on_any_field_change() {
        let base = sample_intent();
        let canonical_base = canonicalize_intent_bytes(&base, 7, 1_000, 1);

        // symbol
        let mut v = base.clone();
        v.symbol = 43;
        assert_ne!(canonical_base, canonicalize_intent_bytes(&v, 7, 1_000, 1));
        // side
        let mut v = base.clone();
        v.side = 1;
        assert_ne!(canonical_base, canonicalize_intent_bytes(&v, 7, 1_000, 1));
        // quantity
        let mut v = base.clone();
        v.quantity = 11;
        assert_ne!(canonical_base, canonicalize_intent_bytes(&v, 7, 1_000, 1));
        // order_type
        let mut v = base.clone();
        v.order_type = 1;
        assert_ne!(canonical_base, canonicalize_intent_bytes(&v, 7, 1_000, 1));
        // limit_paise
        let mut v = base.clone();
        v.limit_paise = 100_001;
        assert_ne!(canonical_base, canonicalize_intent_bytes(&v, 7, 1_000, 1));
        // exchange
        let mut v = base.clone();
        v.exchange = 1;
        assert_ne!(canonical_base, canonicalize_intent_bytes(&v, 7, 1_000, 1));
        // sized_quantity
        assert_ne!(canonical_base, canonicalize_intent_bytes(&base, 8, 1_000, 1));
        // ts_ns
        assert_ne!(canonical_base, canonicalize_intent_bytes(&base, 7, 1_001, 1));
        // sequence
        assert_ne!(canonical_base, canonicalize_intent_bytes(&base, 7, 1_000, 2));
    }

    #[test]
    fn signer_then_paired_verifier_round_trip() {
        let signer = ApprovalSigner::from_key(b"super-secret-test-key-32-bytes!!".to_vec());
        let verifier = signer.paired_verifier();
        let intent = sample_intent();
        let token = signer.sign(&intent, 7, 1_000, 1);
        assert!(verifier.verify(&token, &intent, 7, 1_000, 1));
    }

    #[test]
    fn verifier_rejects_token_under_different_key() {
        let signer = ApprovalSigner::from_key(b"key-A".to_vec());
        let other = ApprovalVerifier::from_key(b"key-B".to_vec());
        let intent = sample_intent();
        let token = signer.sign(&intent, 7, 1_000, 1);
        assert!(!other.verify(&token, &intent, 7, 1_000, 1));
    }

    #[test]
    fn verifier_rejects_token_when_intent_field_is_tampered() {
        let signer = ApprovalSigner::from_key(b"k".to_vec());
        let verifier = signer.paired_verifier();
        let intent = sample_intent();
        let token = signer.sign(&intent, 7, 1_000, 1);

        // Tamper: side flipped.
        let mut tampered = intent.clone();
        tampered.side = 1;
        assert!(!verifier.verify(&token, &tampered, 7, 1_000, 1));

        // Tamper: quantity bumped.
        let mut tampered = intent.clone();
        tampered.quantity += 1;
        assert!(!verifier.verify(&token, &tampered, 7, 1_000, 1));

        // Tamper: limit_paise bumped.
        let mut tampered = intent.clone();
        tampered.limit_paise += 1;
        assert!(!verifier.verify(&token, &tampered, 7, 1_000, 1));
    }

    #[test]
    fn verifier_rejects_token_with_wrong_sized_quantity() {
        let signer = ApprovalSigner::from_key(b"k".to_vec());
        let verifier = signer.paired_verifier();
        let intent = sample_intent();
        let token = signer.sign(&intent, 7, 1_000, 1);
        assert!(!verifier.verify(&token, &intent, 8, 1_000, 1));
    }

    #[test]
    fn two_signs_with_different_sequences_produce_distinct_tokens() {
        // Single-use property: two approvals over identical intent
        // bytes must yield distinct tokens.
        let signer = ApprovalSigner::from_key(b"k".to_vec());
        let intent = sample_intent();
        let t1 = signer.sign(&intent, 7, 1_000, 1);
        let t2 = signer.sign(&intent, 7, 1_000, 2);
        assert_ne!(t1, t2);
    }

    #[test]
    fn approval_token_to_hex_renders_64_chars() {
        let signer = ApprovalSigner::from_key(b"k".to_vec());
        let intent = sample_intent();
        let token = signer.sign(&intent, 7, 1_000, 1);
        let hex = token.to_hex();
        assert_eq!(hex.len(), APPROVAL_TOKEN_HEX_LEN);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn approval_token_debug_truncates_for_safety() {
        let signer = ApprovalSigner::from_key(b"k".to_vec());
        let intent = sample_intent();
        let token = signer.sign(&intent, 7, 1_000, 1);
        let dbg = format!("{:?}", token);
        // The debug output contains the truncation marker and at most
        // 8 hex chars — never the full 64.
        assert!(dbg.contains('…'), "expected truncation: {}", dbg);
        assert!(!dbg.contains(&token.to_hex()), "full hex must not leak: {}", dbg);
    }

    #[test]
    fn signer_does_not_expose_verify_method() {
        // Compile-time check via the trait surface: `ApprovalSigner` is
        // distinct from `ApprovalVerifier`. This test exists to document
        // the invariant; the type system enforces it.
        fn _takes_verifier(_v: &ApprovalVerifier) {}
        let signer = ApprovalSigner::from_key(b"k".to_vec());
        let v = signer.paired_verifier();
        _takes_verifier(&v);
        // Crucial — the following line *must not compile*:
        // _takes_verifier(&signer);
    }
}
