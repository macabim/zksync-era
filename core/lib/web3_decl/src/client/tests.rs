//! Tests for `L2Client` focused on rate limiting.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use assert_matches::assert_matches;
use futures::future;
use jsonrpsee::{http_client::transport, rpc_params, types::error::ErrorCode};
use rand::{rngs::StdRng, Rng, SeedableRng};
use test_casing::test_casing;
use zksync_types::{L2ChainId, U64};

use super::{
    metrics::{HttpErrorLabels, RpcErrorLabels},
    *,
};

#[derive(Debug, Clone, Default)]
struct MockService(Arc<Mutex<Vec<Instant>>>);

async fn poll_service(limiter: &SharedRateLimit, service: &MockService) {
    let _ = limiter.acquire(1).await;
    service.0.lock().unwrap().push(Instant::now());
}

#[test_casing(3, [1, 2, 3])]
#[tokio::test]
async fn rate_limiting_with_single_instance(rate_limit: usize) {
    tokio::time::pause();

    let service = MockService::default();
    let limiter = SharedRateLimit::new(rate_limit, Duration::from_secs(1));
    for _ in 0..10 {
        poll_service(&limiter, &service).await;
    }

    let timestamps = service.0.lock().unwrap().clone();
    assert_eq!(timestamps.len(), 10);
    assert_timestamps_spacing_with_mock_clock(&timestamps, rate_limit);
}

#[tokio::test]
async fn rate_limiting_resetting_state() {
    tokio::time::pause();

    let service = MockService::default();
    let limiter = SharedRateLimit::new(2, Duration::from_secs(1));
    poll_service(&limiter, &service).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    poll_service(&limiter, &service).await;
    poll_service(&limiter, &service).await; // should wait for the rate limit window to reset
    poll_service(&limiter, &service).await;

    let timestamps = service.0.lock().unwrap().clone();
    assert_eq!(timestamps.len(), 4);
    let diffs = timestamp_diffs(&timestamps);
    assert_eq!(
        diffs,
        [
            Duration::from_millis(301),
            Duration::from_millis(700),
            Duration::ZERO
        ]
    );
}

#[tokio::test]
async fn no_op_rate_limiting() {
    tokio::time::pause();

    let service = MockService::default();
    let limiter = SharedRateLimit::new(1, Duration::ZERO);
    for _ in 0..10 {
        poll_service(&limiter, &service).await;
    }

    let timestamps = service.0.lock().unwrap().clone();
    assert_eq!(timestamps.len(), 10);
    let diffs = timestamp_diffs(&timestamps);
    assert_eq!(diffs, [Duration::ZERO; 9]);
}

fn timestamp_diffs(timestamps: &[Instant]) -> Vec<Duration> {
    let diffs = timestamps.windows(2).map(|window| match window {
        [prev, next] => *next - *prev,
        _ => unreachable!(),
    });
    diffs.collect()
}

fn assert_timestamps_spacing_with_mock_clock(timestamps: &[Instant], rate_limit: usize) {
    let diffs = timestamp_diffs(timestamps);

    // Since we use a mock clock, `diffs` should be deterministic.
    for (i, &diff) in diffs.iter().enumerate() {
        if i % rate_limit == rate_limit - 1 {
            assert!(diff > Duration::from_secs(1), "{diffs:?}");
        } else {
            assert_eq!(diff, Duration::ZERO, "{diffs:?}");
        }
    }
}

#[test_casing(3, [2, 3, 5])]
#[tokio::test]
async fn rate_limiting_with_multiple_instances(rate_limit: usize) {
    tokio::time::pause();

    let service = MockService::default();
    let limiter = SharedRateLimit::new(rate_limit, Duration::from_secs(1));
    let calls = (0..50).map(|_| {
        let service = service.clone();
        let limiter = limiter.clone();
        async move {
            poll_service(&limiter, &service).await;
        }
    });
    future::join_all(calls).await;

    let timestamps = service.0.lock().unwrap().clone();
    assert_eq!(timestamps.len(), 50);
    assert_timestamps_spacing_with_mock_clock(&timestamps, rate_limit);
}

async fn test_rate_limiting_with_rng(rate_limit: usize, rng_seed: u64) {
    const RATE_LIMIT_WINDOW_MS: u64 = 50;

    let mut rng = StdRng::seed_from_u64(rng_seed);
    let service = MockService::default();
    let rate_limit_window = Duration::from_millis(RATE_LIMIT_WINDOW_MS);
    let limiter = SharedRateLimit::new(rate_limit, rate_limit_window);
    let max_sleep_duration_ms = RATE_LIMIT_WINDOW_MS * 2 / rate_limit as u64;

    let mut call_tasks = vec![];
    for _ in 0..50 {
        let sleep_duration_ms = rng.gen_range(0..=max_sleep_duration_ms);
        tokio::time::sleep(Duration::from_millis(sleep_duration_ms)).await;

        let service = service.clone();
        let limiter = limiter.clone();
        call_tasks.push(tokio::spawn(async move {
            poll_service(&limiter, &service).await;
        }));
    }
    future::try_join_all(call_tasks).await.unwrap();

    let timestamps = service.0.lock().unwrap().clone();
    assert_eq!(timestamps.len(), 50);
    let mut window_start = (0, timestamps[0]);
    // Add an artificial terminal timestamp to check the last rate limiting window.
    let it = timestamps
        .iter()
        .copied()
        .chain([Instant::now() + rate_limit_window])
        .enumerate()
        .skip(1);
    for (i, timestamp) in it {
        if timestamp - window_start.1 >= rate_limit_window {
            assert!(
                i - window_start.0 <= rate_limit,
                "diffs={:?}, idx={i}, window_start={window_start:?}",
                timestamp_diffs(&timestamps)
            );
            window_start = (i, timestamp);
        }
    }
}

#[test_casing(4, [2, 3, 5, 8])]
#[tokio::test]
async fn rate_limiting_with_rng(rate_limit: usize) {
    tokio::time::pause();

    for rng_seed in 0..1_000 {
        println!("Testing RNG seed: {rng_seed}");
        test_rate_limiting_with_rng(rate_limit, rng_seed).await;
    }
}

#[test_casing(4, [2, 3, 5, 8])]
#[tokio::test(flavor = "multi_thread")]
async fn rate_limiting_with_rng_and_threads(rate_limit: usize) {
    const RNG_SEED: u64 = 123;

    test_rate_limiting_with_rng(rate_limit, RNG_SEED).await;
}

#[tokio::test]
async fn wrapping_mock_client() {
    tokio::time::pause();

    let client = MockClient::builder(L2::default())
        .method("ok", || Ok("ok"))
        .method("slow", || async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok("slow")
        })
        .method("rate_limit", || {
            let http_err = transport::Error::Rejected { status_code: 429 };
            Err::<(), _>(Error::Transport(http_err.into()))
        })
        .method("eth_getBlockNumber", || Ok(U64::from(1)))
        .build();

    let mut client = ClientBuilder::<L2, _>::new(client, "http://localhost".parse().unwrap())
        .for_network(L2ChainId::default().into())
        .with_allowed_requests_per_second(NonZeroUsize::new(100).unwrap())
        .build();
    client.set_component("test");

    let metrics = &*Box::leak(Box::default());
    client.metrics = metrics;
    assert_eq!(
        client.rate_limit.rate_limit_window,
        Duration::from_millis(50)
    );
    assert_eq!(client.rate_limit.rate_limit, 5);

    // Check that expected results are passed from the wrapped client.
    for _ in 0..10 {
        let output: String = client.request("ok", rpc_params![]).await.unwrap();
        assert_eq!(output, "ok");
    }

    let mut batch_request = BatchRequestBuilder::new();
    for _ in 0..5 {
        batch_request.insert("ok", rpc_params![]).unwrap();
    }
    batch_request.insert("slow", rpc_params![]).unwrap();
    client.batch_request::<String>(batch_request).await.unwrap();

    // Check that the batch hit the rate limit.
    assert!(
        metrics.rate_limit_latency.contains(&"ok".to_owned()),
        "{metrics:?}"
    );
    assert!(
        metrics.rate_limit_latency.contains(&"slow".to_owned()),
        "{metrics:?}"
    );

    // Check error reporting.
    let err = client
        .request::<String, _>("unknown", rpc_params![])
        .await
        .unwrap_err();
    assert_matches!(err, Error::Call(_));
    let labels = RpcErrorLabels {
        method: "unknown".to_string(),
        code: ErrorCode::MethodNotFound.code(),
    };
    assert!(metrics.rpc_errors.contains(&labels), "{metrics:?}");

    let err = client
        .request::<String, _>("rate_limit", rpc_params![])
        .await
        .unwrap_err();
    assert_matches!(err, Error::Transport(_));
    let labels = HttpErrorLabels {
        method: "rate_limit".to_string(),
        status: Some(429),
    };
    assert!(metrics.http_errors.contains(&labels), "{metrics:?}");
}

// A real HTTP server checks connection independence and the exact request body.
// Drop aborts the listener and all connection tasks, including stalled responses.
struct SyncHttpServer {
    url: SensitiveUrl,
    requests: Arc<Mutex<Vec<(usize, serde_json::Value, Instant)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for SyncHttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SyncHttpServer {
    async fn new(
        reply: impl Fn(usize) -> (Duration, u16, serde_json::Value) + Send + Sync + 'static,
    ) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let reply = Arc::new(reply);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            let mut connection = 0;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                connection += 1;
                let connection = connection;
                let recorded = recorded.clone();
                let reply = reply.clone();
                connections.spawn(async move {
                    let mut data = Vec::new();
                    loop {
                        let header_end = loop {
                            if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                                break end + 4;
                            }
                            let mut buffer = [0; 4096];
                            let Ok(n) = socket.read(&mut buffer).await else { return };
                            if n == 0 { return; }
                            data.extend_from_slice(&buffer[..n]);
                        };
                        let headers = String::from_utf8_lossy(&data[..header_end]).to_lowercase();
                        let length: usize = headers.lines().find_map(|line| line.strip_prefix("content-length: ")).unwrap().parse().unwrap();
                        while data.len() < header_end + length {
                            let mut buffer = [0; 4096];
                            let Ok(n) = socket.read(&mut buffer).await else { return };
                            if n == 0 { return; }
                            data.extend_from_slice(&buffer[..n]);
                        }
                        let body: serde_json::Value = serde_json::from_slice(&data[header_end..header_end + length]).unwrap();
                        data.drain(..header_end + length);
                        let index = {
                            let mut recorded = recorded.lock().unwrap();
                            let index = recorded.len();
                            recorded.push((connection, body.clone(), Instant::now()));
                            index
                        };
                        let (delay, status, value) = reply(index);
                        tokio::time::sleep(delay).await;
                        let body = serde_json::json!({"jsonrpc":"2.0", "id":body["id"], "result":value}).to_string();
                        let response = format!("HTTP/1.1 {status} Response\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}", body.len());
                        if socket.write_all(response.as_bytes()).await.is_err() { return; }
                    }
                });
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }

    fn client(&self) -> Client<L2> {
        Client::http(self.url.clone())
            .unwrap()
            .report_config(false)
            .with_sync_request_hedging()
            .build()
    }
}

#[tokio::test]
async fn sync_hedge_uses_independent_connection_and_exact_parameters() {
    let server = SyncHttpServer::new(|index| {
        (
            if index == 0 {
                Duration::from_secs(8)
            } else {
                Duration::ZERO
            },
            200,
            serde_json::json!({"number":42, "hash":"exact-block"}),
        )
    })
    .await;
    let client = server.client();
    let start = Instant::now();
    let value: serde_json::Value = tokio::time::timeout(
        Duration::from_secs(2),
        client.request("en_syncL2Block", rpc_params![42, true]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(value["number"], 42);
    assert_eq!(value["hash"], "exact-block");
    assert!(start.elapsed() < Duration::from_secs(1));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_ne!(requests[0].0, requests[1].0);
    assert_eq!(requests[0].1["params"], serde_json::json!([42, true]));
    assert_eq!(requests[0].1["params"], requests[1].1["params"]);
    assert!(requests[1].2.duration_since(requests[0].2) >= Duration::from_millis(190));
}

#[tokio::test]
async fn sync_hedge_preserves_null_and_http_errors_without_duplicates() {
    for status in [200, 429, 503] {
        let server =
            SyncHttpServer::new(move |_| (Duration::ZERO, status, serde_json::Value::Null)).await;
        let result = server
            .client()
            .request::<Option<serde_json::Value>, _>("en_syncL2Block", rpc_params![42, true])
            .await;
        if status == 200 {
            assert_eq!(result.unwrap(), None);
        } else {
            assert_matches!(result, Err(Error::Transport(err)) if matches!(err.downcast_ref::<transport::Error>(), Some(transport::Error::Rejected { status_code }) if *status_code == status));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn sync_hedge_never_duplicates_transaction_submission() {
    let server = SyncHttpServer::new(|_| {
        (
            Duration::from_millis(250),
            200,
            serde_json::json!("tx-hash"),
        )
    })
    .await;
    let client = server.client();
    let value: String = client
        .request("eth_sendRawTransaction", rpc_params!["0xabcd"])
        .await
        .unwrap();
    assert_eq!(value, "tx-hash");
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn sync_hedge_shares_rate_limit_with_primary_and_clones() {
    let server =
        SyncHttpServer::new(|_| (Duration::from_secs(8), 200, serde_json::json!("0x2a"))).await;
    let mut client = server.client();
    client.rate_limit = SharedRateLimit::new(1, Duration::from_secs(1));
    let clone = client.clone();
    let calls = async {
        tokio::join!(
            client.request::<String, _>("eth_blockNumber", rpc_params![]),
            clone.request::<String, _>("eth_blockNumber", rpc_params![])
        )
    };
    assert!(tokio::time::timeout(Duration::from_millis(3300), calls)
        .await
        .is_err());
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    for pair in requests.windows(2) {
        assert!(pair[1].2.duration_since(pair[0].2) >= Duration::from_millis(950));
    }
    assert_eq!(
        requests.iter().map(|r| r.0).collect::<HashSet<_>>().len(),
        4
    );
}

#[tokio::test]
async fn sync_hedge_cancels_primary_and_creates_at_most_one_duplicate() {
    struct Cancel(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for Cancel {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let canceled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = canceled.clone();
    let primary = MockClient::builder(L2::default())
        .method("eth_blockNumber", move || {
            let guard = Cancel(counter.clone());
            async move {
                let _guard = guard;
                future::pending::<Result<serde_json::Value, Error>>().await
            }
        })
        .build();
    let server = SyncHttpServer::new(|_| (Duration::ZERO, 200, serde_json::json!("0x2a"))).await;
    let mut client = ClientBuilder::<L2, _>::new(primary, server.url.clone())
        .report_config(false)
        .build();
    client.sync_hedge_delay = Some(Duration::from_millis(200));
    assert_eq!(
        client
            .request::<String, _>("eth_blockNumber", rpc_params![])
            .await
            .unwrap(),
        "0x2a"
    );
    assert_eq!(canceled.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(server.requests.lock().unwrap().len(), 1);

    let slow =
        SyncHttpServer::new(|_| (Duration::from_secs(8), 200, serde_json::json!("0x2a"))).await;
    assert!(tokio::time::timeout(
        Duration::from_secs(1),
        slow.client()
            .request::<String, _>("eth_blockNumber", rpc_params![])
    )
    .await
    .is_err());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(slow.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn sync_hedge_remains_disabled_by_default() {
    let server =
        SyncHttpServer::new(|_| (Duration::from_millis(250), 200, serde_json::json!("0x2a"))).await;
    let client = Client::<L2>::http(server.url.clone())
        .unwrap()
        .report_config(false)
        .build();
    assert_eq!(
        client
            .request::<String, _>("eth_blockNumber", rpc_params![])
            .await
            .unwrap(),
        "0x2a"
    );
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn sync_hedge_preserves_fatal_rpc_errors() {
    let server = SyncHttpServer::new(|_| (Duration::ZERO, 200, serde_json::json!("0x2a"))).await;
    let primary = MockClient::builder(L2::default())
        .method("en_syncL2Block", |_: u32, _: bool| {
            Err::<serde_json::Value, _>(Error::Call(jsonrpsee::types::ErrorObjectOwned::owned(
                -32602,
                "invalid block",
                None::<()>,
            )))
        })
        .build();
    let mut client = ClientBuilder::<L2, _>::new(primary, server.url.clone())
        .report_config(false)
        .build();
    client.sync_hedge_delay = Some(Duration::from_millis(200));
    assert_matches!(client.request::<serde_json::Value, _>("en_syncL2Block", rpc_params![42, true]).await,
        Err(Error::Call(error)) if error.code() == -32602);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(server.requests.lock().unwrap().is_empty());
}
