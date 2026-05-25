//! Typed NATS publisher/subscriber wrappers around `async_nats` 0.x.
//!
//! ### Design
//!
//! * [`NatsClient`] owns a single `async_nats::Client`. The client is
//!   internally refcounted, so cloning it is cheap; the `NatsClient` newtype
//!   exists only to give us a stable public surface that we can extend later
//!   (TLS configuration, account JWT handling, etc.) without breaking
//!   downstream crates.
//! * [`NatsPublisher<T>`] is a thin typed wrapper. It composes a
//!   [`Subject<T>`](crate::Subject) with a [`Codec<T>`](crate::Codec) and
//!   exposes a single `publish(value: &T)` method.
//! * [`NatsSubscriber<T>`] exposes both a typed `recv()` (codec-decoded) and
//!   the **zero-copy** `recv_bytes()` (returns the raw [`Bytes`] from NATS).
//!   FlatBuffers verifiers and accessors take `&[u8]`, so callers can read
//!   the payload in place with no intermediate `Vec` allocation (R1.5).
//!
//! ### Tracing
//!
//! Every `publish`/`recv` call is wrapped in a `tracing::instrument` span
//! tagged with the subject and payload byte length. Spans flow into Loki
//! via the observability scaffolding (task 5.1).

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use tracing::instrument;

use crate::codec::Codec;
use crate::error::BusError;
use crate::subject::Subject;

/// Connection wrapper around an `async_nats::Client`.
///
/// `async_nats::Client` is itself cheaply cloneable (`Arc` underneath), so
/// storing it bare would be enough — we wrap it in our own newtype so the
/// crate's public surface does not leak the underlying type.
#[derive(Clone)]
pub struct NatsClient {
    inner: async_nats::Client,
}

impl NatsClient {
    /// Connect to a NATS cluster at `url` (e.g. `"nats://localhost:4222"`).
    ///
    /// Returns [`BusError::NatsConnect`] on any failure. The underlying
    /// `async_nats::ConnectError` is rendered to a `String` because it is
    /// not `Clone` and contains a non-public inner error.
    ///
    /// This is the **no-auth** constructor. Production deployments must use
    /// [`NatsClient::connect_with_creds`] instead — the broker enforces the
    /// Authority Hierarchy ACLs (R21.3, R30.6) on every published subject,
    /// and a connection without credentials is restricted to whatever the
    /// `default_permissions` clause in `nats-server.conf` allows (typically
    /// nothing in production). The credential-less path is retained for
    /// unit tests against an unauthenticated local broker.
    #[instrument(level = "info", skip_all, fields(nats.url = %url.as_ref()))]
    pub async fn connect(url: impl AsRef<str>) -> Result<Self, BusError> {
        let url_str = url.as_ref().to_owned();
        let inner = async_nats::connect(&url_str)
            .await
            .map_err(|e| BusError::NatsConnect(format!("{}: {}", url_str, e)))?;
        Ok(Self { inner })
    }

    /// Connect to a NATS cluster at `url` using the JWT/NKEY credentials
    /// stored in the `*.creds` file at `creds_path`.
    ///
    /// The `.creds` format is the standard `nsc`-generated bundle —
    /// `-----BEGIN NATS USER JWT-----` followed by `-----BEGIN USER NKEY
    /// SEED-----`. `async_nats::ConnectOptions::with_credentials_file`
    /// reads the file at connect time and validates both blocks; an
    /// invalid or missing file surfaces here as [`BusError::NatsConnect`]
    /// with the path embedded in the error message.
    ///
    /// Every Hot_Path service mounts its account credentials at
    /// `/etc/hedge/nats/<account>.creds` and passes that path here. The
    /// account boundary at the broker is the structural enforcement of
    /// the Authority Hierarchy (R21.3, R30.6); see
    /// `docker/nats/README.md` for the full ACL table.
    #[instrument(
        level = "info",
        skip_all,
        fields(nats.url = %url.as_ref(), nats.creds = %creds_path.as_ref().display())
    )]
    pub async fn connect_with_creds(
        url: impl AsRef<str>,
        creds_path: impl AsRef<Path>,
    ) -> Result<Self, BusError> {
        let url_str = url.as_ref().to_owned();
        let creds = creds_path.as_ref();
        let inner = async_nats::ConnectOptions::with_credentials_file(creds)
            .await
            .map_err(|e| {
                BusError::NatsConnect(format!(
                    "{}: credentials file {} not usable: {}",
                    url_str,
                    creds.display(),
                    e
                ))
            })?
            .connect(&url_str)
            .await
            .map_err(|e| {
                BusError::NatsConnect(format!(
                    "{}: connect with creds {} failed: {}",
                    url_str,
                    creds.display(),
                    e
                ))
            })?;
        Ok(Self { inner })
    }

    /// Borrow the underlying `async_nats::Client` for advanced operations
    /// not exposed by the typed wrapper (jetstream, raw flush, etc.).
    #[inline]
    pub fn raw(&self) -> &async_nats::Client {
        &self.inner
    }

    /// Construct a typed publisher for subject `subject` using `codec`.
    pub fn publisher<T, C>(&self, subject: Subject<T>, codec: C) -> NatsPublisher<T, C>
    where
        C: Codec<T>,
    {
        NatsPublisher {
            client: self.inner.clone(),
            subject,
            codec: Arc::new(codec),
        }
    }

    /// Construct a typed subscriber for subject `subject` using `codec`.
    ///
    /// Returns [`BusError::NatsSubscribe`] on broker rejection. NATS server
    /// ACL violations surface here (R21.3, R30.6).
    #[instrument(level = "debug", skip(self, codec), fields(nats.subject = %subject))]
    pub async fn subscriber<T, C>(
        &self,
        subject: Subject<T>,
        codec: C,
    ) -> Result<NatsSubscriber<T, C>, BusError>
    where
        C: Codec<T>,
    {
        let sub = self
            .inner
            .subscribe(subject.as_str().to_owned())
            .await
            .map_err(|e| BusError::subscribe(subject.as_str(), e))?;
        Ok(NatsSubscriber {
            inner: sub,
            subject,
            codec: Arc::new(codec),
        })
    }
}

/// Typed NATS publisher.
///
/// Construct via [`NatsClient::publisher`]. Holds:
///
/// * a clone of the underlying `async_nats::Client` (cheap — refcounted),
/// * the typed [`Subject<T>`] payloads will be published to,
/// * the [`Codec<T>`] used to serialize each value.
pub struct NatsPublisher<T, C>
where
    C: Codec<T>,
{
    client: async_nats::Client,
    subject: Subject<T>,
    codec: Arc<C>,
}

// Manual `Clone` so the bound is `C: Codec<T>` (which we already require)
// rather than `T: Clone, C: Clone`. `Arc<C>` and `Subject<T>` are both
// always-cheaply-cloneable.
impl<T, C> Clone for NatsPublisher<T, C>
where
    C: Codec<T>,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            subject: self.subject.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<T, C> NatsPublisher<T, C>
where
    C: Codec<T>,
{
    /// The subject this publisher targets.
    #[inline]
    pub fn subject(&self) -> &Subject<T> {
        &self.subject
    }

    /// Publish a typed value.
    ///
    /// Encoding happens before the network call so any encode error fails
    /// fast without sending malformed bytes.
    #[instrument(
        level = "trace",
        skip(self, value),
        fields(nats.subject = %self.subject, payload.bytes)
    )]
    pub async fn publish(&self, value: &T) -> Result<(), BusError> {
        let payload = self.codec.encode(value)?;
        tracing::Span::current().record("payload.bytes", payload.len() as u64);
        self.publish_bytes(payload).await
    }

    /// Publish a pre-encoded payload. Useful when the caller has already
    /// produced a [`Bytes`] payload (e.g. from a FlatBuffers builder reused
    /// across publishes).
    #[instrument(
        level = "trace",
        skip(self, payload),
        fields(nats.subject = %self.subject, payload.bytes = payload.len() as u64)
    )]
    pub async fn publish_bytes(&self, payload: Bytes) -> Result<(), BusError> {
        self.client
            .publish(self.subject.as_str().to_owned(), payload)
            .await
            .map_err(|e| BusError::publish(self.subject.as_str(), e))
    }
}

/// Typed NATS subscriber.
///
/// Wraps `async_nats::Subscriber` (which implements `futures::Stream`).
/// Two receive methods:
///
/// * [`recv_bytes`](Self::recv_bytes) — **zero-copy.** Returns the
///   `Bytes` payload owned by the NATS wire buffer. FlatBuffers
///   accessors can read in place (R1.5).
/// * [`recv`](Self::recv) — codec-decoded. Drives the same wire bytes
///   through the codec to produce a typed `T`.
pub struct NatsSubscriber<T, C>
where
    C: Codec<T>,
{
    inner: async_nats::Subscriber,
    subject: Subject<T>,
    codec: Arc<C>,
}

impl<T, C> NatsSubscriber<T, C>
where
    C: Codec<T>,
{
    /// The subject this subscriber listens on.
    #[inline]
    pub fn subject(&self) -> &Subject<T> {
        &self.subject
    }

    /// Receive the next message as raw [`Bytes`].
    ///
    /// **Zero-copy receive path.** The returned `Bytes` is a refcounted
    /// handle to the NATS wire buffer; FlatBuffers verifiers
    /// (`flatbuffers::root::<T>(&bytes)`) read directly from this slice
    /// without any intermediate `Vec` allocation. This is the canonical
    /// fast path for `md.tick.*`, `md.book.*`, `feat.update.*`, `sig.*`,
    /// `risk.*`, `exec.*`, and `pos.*` payloads.
    ///
    /// Returns [`BusError::SubscriptionClosed`] when the underlying stream
    /// terminates (e.g. the client closed); callers should treat this as
    /// the end of the subscription.
    #[instrument(level = "trace", skip(self), fields(nats.subject = %self.subject))]
    pub async fn recv_bytes(&mut self) -> Result<Bytes, BusError> {
        match self.inner.next().await {
            Some(msg) => Ok(msg.payload),
            None => Err(BusError::SubscriptionClosed {
                subject: self.subject.as_str().to_owned(),
            }),
        }
    }

    /// Receive the next message and decode it through the codec.
    ///
    /// For FlatBuffers payloads, prefer [`recv_bytes`](Self::recv_bytes) and
    /// access the payload in place to keep the receive path zero-copy.
    #[instrument(level = "trace", skip(self), fields(nats.subject = %self.subject))]
    pub async fn recv(&mut self) -> Result<T, BusError> {
        let bytes = self.recv_bytes().await?;
        self.codec.decode(&bytes)
    }

    /// Borrow the underlying `async_nats::Subscriber` for raw stream access.
    #[inline]
    pub fn raw(&mut self) -> &mut async_nats::Subscriber {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: connecting to an unreachable host must surface as a
    /// [`BusError::NatsConnect`] rather than panicking. We point at a
    /// deliberately invalid URL so the call fails fast (no live NATS
    /// dependency in unit tests).
    #[tokio::test]
    async fn connect_to_nonexistent_host_returns_connect_error() {
        // `127.0.0.1:1` is the standard "guaranteed-closed" port on most
        // platforms; on Windows it is also reliably refused. We pair it
        // with a short DNS-resolvable hostname so the failure path is the
        // network-refused branch, not a parse error.
        let res = NatsClient::connect("nats://127.0.0.1:1").await;
        match res {
            Err(BusError::NatsConnect(msg)) => {
                assert!(
                    msg.contains("127.0.0.1:1"),
                    "connect error should embed url, got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected connect failure against 127.0.0.1:1"),
            Err(other) => panic!("expected NatsConnect, got {:?}", other),
        }
    }

    /// `connect_with_creds` must surface a missing credentials file as a
    /// [`BusError::NatsConnect`]. `async_nats::ConnectOptions::
    /// with_credentials_file` opens and parses the file eagerly, so the
    /// file-open error branch is exercised before any network attempt is
    /// made. This is the test path that proves the ACL plumbing fails
    /// closed (R21.3, R30.6) — a service that cannot read its credentials
    /// must NOT silently fall back to an unauthenticated connection.
    #[tokio::test]
    async fn connect_with_missing_creds_returns_connect_error() {
        let nonexistent = std::env::temp_dir().join("hedge-nats-creds-does-not-exist.creds");
        // Defensively ensure the file really is absent.
        let _ = std::fs::remove_file(&nonexistent);

        let res = NatsClient::connect_with_creds("nats://127.0.0.1:1", &nonexistent).await;
        match res {
            Err(BusError::NatsConnect(msg)) => {
                assert!(
                    msg.contains("hedge-nats-creds-does-not-exist.creds"),
                    "connect error should embed creds path, got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected connect failure with missing creds file"),
            Err(other) => panic!("expected NatsConnect, got {:?}", other),
        }
    }
}
