//! End-to-end infrastructure connectivity tests.
//!
//! These tests verify that the core transport substrates (NATS, Redis)
//! are reachable and functional when the Docker infrastructure is running.
//!
//! Run with: `cargo test -p hedge-bus --test e2e_infra`
//! Requires: `docker compose --profile infra up -d` (NATS on :4222, Redis on :6379)

use std::time::Duration;

use hedge_bus::{NatsClient, RedisStreamConsumer, RedisStreamProducer};
use hedge_bus::codec::JsonCodec;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

const NATS_URL: &str = "nats://127.0.0.1:4222";
const REDIS_URL: &str = "redis://127.0.0.1:6379";

// ---- NATS tests -----------------------------------------------------------

#[tokio::test]
async fn nats_connect_succeeds() {
    let client = NatsClient::connect(NATS_URL).await;
    assert!(client.is_ok(), "NATS connect failed: {:?}", client.err());
}

#[tokio::test]
async fn nats_publish_subscribe_round_trip() {
    let client = NatsClient::connect(NATS_URL)
        .await
        .expect("NATS connect failed");

    let subject = hedge_bus::Subject::new("test.e2e.ping");
    let mut subscriber = client
        .subscriber(subject.clone(), JsonCodec::<serde_json::Value>::new())
        .await
        .expect("subscribe failed");

    let payload = serde_json::json!({"msg": "hello", "ts": 12345});
    let publisher = client
        .publisher(subject, JsonCodec::<serde_json::Value>::new());
    publisher
        .publish(&payload)
        .await
        .expect("publish failed");

    let received = tokio::time::timeout(Duration::from_secs(2), subscriber.recv())
        .await
        .expect("recv timed out")
        .expect("recv failed");

    assert_eq!(received["msg"], "hello");
    assert_eq!(received["ts"], 12345);
}

#[tokio::test]
async fn nats_zero_copy_receive() {
    let client = NatsClient::connect(NATS_URL)
        .await
        .expect("NATS connect failed");

    let subject = hedge_bus::Subject::new("test.e2e.zerocopy");
    let mut subscriber = client
        .subscriber(subject.clone(), JsonCodec::<serde_json::Value>::new())
        .await
        .expect("subscribe failed");

    let payload = serde_json::json!({"data": [1, 2, 3]});
    let publisher = client
        .publisher(subject, JsonCodec::<serde_json::Value>::new());
    publisher.publish(&payload).await.expect("publish failed");

    let bytes = tokio::time::timeout(Duration::from_secs(2), subscriber.recv_bytes())
        .await
        .expect("recv timed out")
        .expect("recv failed");

    assert!(!bytes.is_empty());
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("json parse failed");
    assert_eq!(parsed["data"], serde_json::json!([1, 2, 3]));
}

// ---- Redis tests ----------------------------------------------------------

#[tokio::test]
async fn redis_connect_succeeds() {
    let client = redis::Client::open(REDIS_URL).expect("redis client open failed");
    let mut conn: ConnectionManager = client
        .get_connection_manager()
        .await
        .expect("redis connection manager failed");
    let _: () = conn
        .set("e2e:ping", "pong")
        .await
        .expect("redis SET failed");
    let val: String = conn.get("e2e:ping").await.expect("redis GET failed");
    assert_eq!(val, "pong");
    let _: () = conn.del("e2e:ping").await.expect("redis DEL failed");
}

#[tokio::test]
async fn redis_stream_produce_consume() {
    let client = redis::Client::open(REDIS_URL).expect("redis client open failed");
    let mut conn: ConnectionManager = client
        .get_connection_manager()
        .await
        .expect("redis connection manager failed");

    let stream_key: &'static str = Box::leak(format!("hedge.test.e2e.{}", std::process::id()).into_boxed_str());
    let group = "test_group";
    let consumer = "test_consumer";

    let mut producer: RedisStreamProducer<serde_json::Value, JsonCodec<serde_json::Value>> =
        RedisStreamProducer::new(conn.clone(), stream_key, JsonCodec::new());

    let mut consumer_inst: RedisStreamConsumer<serde_json::Value, JsonCodec<serde_json::Value>> =
        RedisStreamConsumer::new(conn.clone(), stream_key, group, consumer, 1000, JsonCodec::new());
    consumer_inst.ensure_group().await.expect("ensure_group failed");

    for i in 0..3u32 {
        let payload = serde_json::json!({"i": i, "ts": 1000 + i});
        producer.xadd(&payload).await.expect("xadd failed");
    }

    let entries = consumer_inst
        .next_batch(10)
        .await
        .expect("next_batch failed");

    assert_eq!(entries.len(), 3, "expected 3 entries, got {}", entries.len());

    for (idx, entry) in entries.iter().enumerate() {
        assert_eq!(entry.payload["i"], idx as u32);
    }

    for entry in &entries {
        consumer_inst.ack(&entry.id).await.expect("ack failed");
    }

    let _: () = conn.del(stream_key).await.expect("del stream failed");
}

// ---- Cross-transport: NATS -> Redis signal flow ----------------------------

#[tokio::test]
async fn signal_flow_nats_to_redis() {
    let nats = NatsClient::connect(NATS_URL)
        .await
        .expect("NATS connect failed");

    let sig_subject = hedge_bus::Subject::new("test.e2e.sig_emitted");
    let mut sig_sub = nats
        .subscriber(sig_subject.clone(), JsonCodec::<serde_json::Value>::new())
        .await
        .expect("subscribe sig");
    let sig_pub = nats
        .publisher(sig_subject, JsonCodec::<serde_json::Value>::new());

    let redis_client = redis::Client::open(REDIS_URL).expect("redis client open");
    let mut redis_conn: ConnectionManager = redis_client
        .get_connection_manager()
        .await
        .expect("redis conn mgr");

    let stream: &'static str = Box::leak(format!("hedge.test.approvals.{}", std::process::id()).into_boxed_str());
    let mut approval_producer: RedisStreamProducer<serde_json::Value, JsonCodec<serde_json::Value>> =
        RedisStreamProducer::new(redis_conn.clone(), stream, JsonCodec::new());
    let mut approval_consumer: RedisStreamConsumer<serde_json::Value, JsonCodec<serde_json::Value>> =
        RedisStreamConsumer::new(redis_conn.clone(), stream, "test_exec_group", "test_exec_consumer", 2000, JsonCodec::new());
    approval_consumer.ensure_group().await.expect("ensure_group failed");

    let signal = serde_json::json!({
        "correlation_id": "abc123",
        "symbol": "RELIANCE",
        "side": "buy",
        "confidence": 0.85
    });
    sig_pub.publish(&signal).await.expect("publish signal");

    let received_sig = tokio::time::timeout(Duration::from_secs(2), sig_sub.recv())
        .await
        .expect("timeout")
        .expect("recv failed");

    assert_eq!(received_sig["correlation_id"], "abc123");
    assert_eq!(received_sig["symbol"], "RELIANCE");

    let approval = serde_json::json!({
        "correlation_id": "abc123",
        "approved": true,
        "sized_quantity": 50,
        "side": "buy"
    });
    approval_producer
        .xadd(&approval)
        .await
        .expect("xadd approval");

    let entries = approval_consumer
        .next_batch(1)
        .await
        .expect("next_batch approval");
    assert_eq!(entries.len(), 1);

    assert_eq!(entries[0].payload["correlation_id"], "abc123");
    assert_eq!(entries[0].payload["approved"], true);
    assert_eq!(entries[0].payload["sized_quantity"], 50);

    let _: () = redis_conn.del(stream).await.expect("cleanup");
}
