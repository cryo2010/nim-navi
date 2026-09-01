//! benchRequests reference client: Rust reqwest + tokio (name: rust).
//!
//! Dispatches on NAVI_WORKLOAD (default "requests"):
//!   - "requests":       buffered GET/POST/PUT workload.
//!   - "streamDownload": streamed GET /download, sha1-verified.
//!   - "streamUpload":   streamed POST /upload, sha1-verified.
//!   - "sse":            infinite text/event-stream, inter-arrival latency.
//!   - "ws":             wss text echo round-trips.
//!
//! Each workload is time-boxed with an unmeasured warmup prelude, records
//! per-unit latency into a shared log-bucketed histogram, and prints ONE
//! tab-separated RESULT line. Same contract as the other-language reference
//! clients. HTTP/3 is skipped. All logs go to stderr; only RESULT to stdout.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures_util::{SinkExt, StreamExt};
use sha1::{Digest, Sha1};

// --- Config helpers -------------------------------------------------------
fn env_str(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}
fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Shared, immutable run configuration.
struct Config {
    proto: String,
    bases: Vec<String>,
    stream_bytes: u64,
    seconds: f64,
    warmup_seconds: f64,
    cold: bool,
    n_workers: usize,
    max_buckets: usize,
}

// --- Latency histogram (MUST match the cross-language scheme exactly) ------
struct Hist {
    counts: Vec<u64>,
    total: u64,
    bytes: u64,
}
impl Hist {
    fn new(max_buckets: usize) -> Self {
        Hist { counts: vec![0; max_buckets], total: 0, bytes: 0 }
    }
    fn record(&mut self, us: u64) {
        let v = us.max(1) as f64;
        let mut idx = (v.log2() * 64.0).floor() as i64;
        let hi = self.counts.len() as i64 - 1;
        if idx < 0 {
            idx = 0;
        } else if idx > hi {
            idx = hi;
        }
        self.counts[idx as usize] += 1;
        self.total += 1;
    }
    fn merge(&mut self, other: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(other.counts.iter()) {
            *a += *b;
        }
        self.total += other.total;
        self.bytes += other.bytes;
    }
    /// Return the p-th percentile latency in MILLISECONDS.
    fn percentile(&self, p: f64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let target = ((p / 100.0 * self.total as f64).ceil() as u64).max(1);
        let mut cum: u64 = 0;
        for (idx, &c) in self.counts.iter().enumerate() {
            cum += c;
            if cum >= target {
                return 2f64.powf((idx as f64 + 0.5) / 64.0) / 1000.0;
            }
        }
        let idx = self.counts.len() - 1;
        2f64.powf((idx as f64 + 0.5) / 64.0) / 1000.0
    }
}

const VERBS: [&str; 3] = ["GET", "POST", "PUT"];
const UPLOAD_BLOCK: usize = 1 << 20; // 1 MiB reused block for streamUpload

fn main() {
    let proto = env_str("NAVI_PROTO", "h2");
    if proto == "h3" {
        println!("SKIP\trust\tno reference-client HTTP/3");
        std::process::exit(0);
    }
    let workload = env_str("NAVI_WORKLOAD", "requests");
    if !matches!(
        workload.as_str(),
        "requests" | "streamDownload" | "streamUpload" | "sse" | "ws"
    ) {
        println!("SKIP\trust\t{} not implemented", workload);
        std::process::exit(0);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(run(proto, workload));
}

async fn run(proto: String, workload: String) {
    let host = env_str("NAVI_HOST", "127.0.0.1");
    let base_port: u16 = env_parse("NAVI_BASE_PORT", 9443);
    let servers: usize = env_parse("NAVI_SERVERS", 5);
    let clients: usize = env_parse("NAVI_CLIENTS", 3);
    let concurrency: usize = env_parse("NAVI_CONCURRENCY", 8);
    let cold = env_str("NAVI_MODE", "pooled") == "cold";

    let cfg = Arc::new(Config {
        bases: (0..servers)
            .map(|i| format!("https://{}:{}", host, base_port as usize + i))
            .collect(),
        stream_bytes: env_parse("NAVI_STREAM_BYTES", 1_073_741_824u64),
        seconds: env_parse("NAVI_SECONDS", 20.0),
        warmup_seconds: env_parse("NAVI_WARMUP_SECONDS", 2.0),
        cold,
        n_workers: clients * concurrency,
        max_buckets: (300_000_000f64.log2() * 64.0).floor() as usize + 1,
        proto,
    });

    // One shared client with an internal connection pool (pooled mode).
    let client = Arc::new(build_client(&cfg));
    let rr = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let measure_start = cfg.warmup_seconds;
    let deadline = cfg.warmup_seconds + cfg.seconds;

    let mut handles = Vec::with_capacity(cfg.n_workers);
    for i in 0..cfg.n_workers {
        let client = client.clone();
        let cfg = cfg.clone();
        let rr = rr.clone();
        let workload = workload.clone();
        handles.push(tokio::spawn(async move {
            worker(i, workload, client, cfg, rr, start, measure_start, deadline).await
        }));
    }

    let mut hist = Hist::new(cfg.max_buckets);
    for h in handles {
        let local = h.await.expect("worker task join");
        hist.merge(&local);
    }
    let secs = start.elapsed().as_secs_f64() - measure_start;

    let ops = hist.total;
    let rps = if secs > 0.0 { (ops as f64 / secs).round() as u64 } else { 0 };
    let mbps = if secs > 0.0 { hist.bytes as f64 / secs / 1e6 } else { 0.0 };
    println!(
        "RESULT\trust\t{}\t{:.3}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.1}",
        ops,
        secs,
        rps,
        hist.percentile(50.0),
        hist.percentile(99.0),
        hist.percentile(99.9),
        mbps,
    );
}

fn build_client(cfg: &Config) -> reqwest::Client {
    let mut b = reqwest::Client::builder().danger_accept_invalid_certs(true);
    if cfg.proto == "h1" {
        // Force HTTP/1.1 only.
        b = b.http1_only();
    }
    // For h2 we rely on ALPN negotiation over TLS (do NOT use
    // http2_prior_knowledge, which skips ALPN). The negotiated version is
    // asserted on the first response.
    if cfg.cold {
        b = b.pool_max_idle_per_host(0);
    }
    b.build().expect("build reqwest client")
}

/// Gate the first successful response against the negotiated protocol.
fn check_version(cfg: &Config, resp: &reqwest::Response, checked: &mut bool) {
    if *checked {
        return;
    }
    *checked = true;
    let want = if cfg.proto == "h1" {
        reqwest::Version::HTTP_11
    } else {
        reqwest::Version::HTTP_2
    };
    if resp.version() != want {
        eprintln!("[rust] protocol mismatch: got {:?} want {:?}", resp.version(), want);
        std::process::exit(1);
    }
}

fn pick(cfg: &Config, rr: &AtomicUsize) -> String {
    let slot = rr.fetch_add(1, Ordering::Relaxed) % cfg.bases.len();
    cfg.bases[slot].clone()
}

fn fail(e: impl std::fmt::Display) -> ! {
    eprintln!("[rust] FAIL: {}", e);
    std::process::exit(1);
}

async fn worker(
    i: usize,
    workload: String,
    client: Arc<reqwest::Client>,
    cfg: Arc<Config>,
    rr: Arc<AtomicUsize>,
    start: Instant,
    measure_start: f64,
    deadline: f64,
) -> Hist {
    let mut hist = Hist::new(cfg.max_buckets);
    let mut checked = false;

    // sse and ws hold a single long-lived connection, record their own
    // per-unit latency, and break on the deadline, so they manage their own
    // histogram rather than the per-transfer timing wrapper below.
    if workload == "sse" {
        let base = pick(&cfg, &rr);
        sse(&client, &cfg, &base, &mut checked, start, measure_start, deadline, &mut hist).await;
        return hist;
    }
    if workload == "ws" {
        let base = pick(&cfg, &rr);
        ws(&cfg, &base, start, measure_start, deadline, &mut hist).await;
        return hist;
    }

    let mut n = i;
    while start.elapsed().as_secs_f64() < deadline {
        let base = pick(&cfg, &rr);
        let t0 = Instant::now();
        let bytes = match workload.as_str() {
            "streamDownload" => stream_download(&client, &cfg, &base, &mut checked).await,
            "streamUpload" => stream_upload(&client, &cfg, &base, &mut checked).await,
            _ => {
                let verb = VERBS[n % VERBS.len()];
                n += 1;
                requests_one(&client, &cfg, &base, verb, &mut checked).await
            }
        };
        if start.elapsed().as_secs_f64() >= measure_start {
            hist.record(t0.elapsed().as_micros() as u64);
            hist.bytes += bytes;
        }
    }
    hist
}

/// One buffered request; returns bytes read.
async fn requests_one(
    client: &reqwest::Client,
    cfg: &Config,
    base: &str,
    verb: &str,
    checked: &mut bool,
) -> u64 {
    let url = format!("{}/echo", base);
    let mut req = match verb {
        "POST" => client.post(&url).body("payload-x").header("content-type", "text/plain"),
        "PUT" => client.put(&url).body("payload-x").header("content-type", "text/plain"),
        _ => client.get(&url),
    };
    if cfg.cold {
        req = req.header("connection", "close");
    }
    let resp = req.send().await.unwrap_or_else(|e| fail(e));
    check_version(cfg, &resp, checked);
    // Read the full body (gzip auto-decompressed by reqwest).
    let body = resp.bytes().await.unwrap_or_else(|e| fail(e));
    body.len() as u64
}

/// Streamed download of STREAM_BYTES; sha1-verified against x-sha1 header.
/// Never buffers the whole body. Returns bytes received.
async fn stream_download(
    client: &reqwest::Client,
    cfg: &Config,
    base: &str,
    checked: &mut bool,
) -> u64 {
    let url = format!("{}/download?size={}", base, cfg.stream_bytes);
    let resp = client.get(&url).send().await.unwrap_or_else(|e| fail(e));
    check_version(cfg, &resp, checked);
    let want_sha1 = resp
        .headers()
        .get("x-sha1")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let mut hasher = Sha1::new();
    let mut got: u64 = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let c = chunk.unwrap_or_else(|e| fail(e));
        hasher.update(&c);
        got += c.len() as u64;
    }
    let hex = format!("{:x}", hasher.finalize());
    match want_sha1 {
        Some(w) if w == hex => {}
        other => {
            eprintln!("[rust] streamDownload sha1 mismatch: got {} want {:?}", hex, other);
            std::process::exit(1);
        }
    }
    got
}

/// Streamed upload of STREAM_BYTES from a reused 1 MiB block; constant memory.
/// The server echoes {"sha1","size"} which is verified. Returns bytes sent.
async fn stream_upload(
    client: &reqwest::Client,
    cfg: &Config,
    base: &str,
    checked: &mut bool,
) -> u64 {
    let total = cfg.stream_bytes;
    let block: Arc<Vec<u8>> = Arc::new(vec![b'x'; UPLOAD_BLOCK]);

    // Compute the client-side sha1 over exactly the bytes we will send.
    let mut hasher = Sha1::new();
    let mut left = total;
    while left > 0 {
        let n = left.min(UPLOAD_BLOCK as u64) as usize;
        hasher.update(&block[..n]);
        left -= n as u64;
    }
    let want_hex = format!("{:x}", hasher.finalize());

    // Build a constant-memory body stream yielding copies of the reused block.
    // Each yielded chunk is a Vec<u8> (impl Into<Bytes>); only one 1 MiB copy
    // is live at a time, so memory stays bounded regardless of STREAM_BYTES.
    let mut remaining = total;
    let body_block = block.clone();
    let body_stream = futures_util::stream::poll_fn(move |_| {
        use std::task::Poll;
        if remaining == 0 {
            return Poll::Ready(None);
        }
        let n = remaining.min(UPLOAD_BLOCK as u64) as usize;
        remaining -= n as u64;
        let chunk: Vec<u8> = body_block[..n].to_vec();
        Poll::Ready(Some(Ok::<Vec<u8>, std::io::Error>(chunk)))
    });

    let url = format!("{}/upload", base);
    let resp = client
        .post(&url)
        .header("content-type", "application/octet-stream")
        .header("content-length", total)
        .body(reqwest::Body::wrap_stream(body_stream))
        .send()
        .await
        .unwrap_or_else(|e| fail(e));
    check_version(cfg, &resp, checked);
    let text = resp.text().await.unwrap_or_else(|e| fail(e));

    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|e| fail(e));
    let got_sha1 = v.get("sha1").and_then(|x| x.as_str()).unwrap_or("");
    let got_size = v.get("size").and_then(|x| x.as_u64()).unwrap_or(0);
    if got_sha1 != want_hex || got_size != total {
        eprintln!(
            "[rust] streamUpload mismatch: sha1 got {} want {}, size got {} want {}",
            got_sha1, want_hex, got_size, total
        );
        std::process::exit(1);
    }
    total
}

/// Subscribe to an infinite SSE stream at {base}/events. Each event is a
/// blank-line-terminated frame; a "data:" line carries a ~64 byte payload.
/// Records inter-arrival latency (µs since the previous event in this stream)
/// during the measured window and accumulates payload bytes. Never buffers the
/// whole stream; breaks on the deadline (dropping the response aborts it).
#[allow(clippy::too_many_arguments)]
async fn sse(
    client: &reqwest::Client,
    cfg: &Config,
    base: &str,
    checked: &mut bool,
    start: Instant,
    measure_start: f64,
    deadline: f64,
    hist: &mut Hist,
) {
    let url = format!("{}/events", base);
    let resp = client.get(&url).send().await.unwrap_or_else(|e| fail(e));
    check_version(cfg, &resp, checked);

    let mut stream = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut prev = Instant::now();

    while start.elapsed().as_secs_f64() < deadline {
        let chunk = match stream.next().await {
            Some(c) => c.unwrap_or_else(|e| fail(e)),
            None => break, // stream ended unexpectedly
        };
        buf.extend_from_slice(&chunk);

        // Split off every complete frame terminated by a blank line ("\n\n").
        while let Some(pos) = find_frame_end(&buf) {
            let frame = buf.drain(..pos + 2).collect::<Vec<u8>>();
            let now = Instant::now();
            let payload = sse_data_len(&frame[..pos]);
            if start.elapsed().as_secs_f64() >= measure_start {
                hist.record(now.duration_since(prev).as_micros() as u64);
                hist.bytes += payload as u64;
            }
            prev = now;
            if start.elapsed().as_secs_f64() >= deadline {
                return;
            }
        }
    }
}

/// Index of the first "\n\n" frame separator, if any.
fn find_frame_end(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

/// Total length of the payload carried by all "data:" lines in one frame.
fn sse_data_len(frame: &[u8]) -> usize {
    let mut n = 0;
    for line in frame.split(|&b| b == b'\n') {
        if let Some(rest) = line.strip_prefix(b"data:") {
            // Trim a single optional leading space, per the SSE format.
            n += rest.strip_prefix(b" ").unwrap_or(rest).len();
        }
    }
    n
}

// --- WebSocket workload ----------------------------------------------------

/// A rustls certificate verifier that accepts any server certificate. This is
/// intentionally insecure and used only to talk to the benchmark server's
/// self-signed cert, mirroring reqwest's danger_accept_invalid_certs above.
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// WebSocket text-echo round-trips over wss (an HTTP/1.1 upgrade; no version
/// gate). Records per-round-trip latency (µs) during the measured window and
/// adds the 4-byte "ping" payload to the byte accumulator. Breaks on deadline.
async fn ws(
    cfg: &Config,
    base: &str,
    start: Instant,
    measure_start: f64,
    deadline: f64,
    hist: &mut Hist,
) {
    use tokio_tungstenite::tungstenite::Message;

    // wss URL from the https base.
    let url = format!("{}/ws", base.replacen("https://", "wss://", 1));

    // rustls config that trusts the self-signed server cert.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap_or_else(|e| fail(e))
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
        .with_no_client_auth();
    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(config));

    let (mut socket, _resp) = tokio_tungstenite::connect_async_tls_with_config(
        &url,
        None,
        false,
        Some(connector),
    )
    .await
    .unwrap_or_else(|e| fail(e));

    while start.elapsed().as_secs_f64() < deadline {
        let t0 = Instant::now();
        if let Err(e) = socket.send(Message::Text("ping".into())).await {
            fail(e);
        }
        let msg = match socket.next().await {
            Some(m) => m.unwrap_or_else(|e| fail(e)),
            None => break, // server closed the connection
        };
        match msg {
            Message::Text(ref t) if t.as_str() == "ping" => {}
            other => {
                eprintln!("[rust] ws unexpected reply: {:?}", other);
                std::process::exit(1);
            }
        }
        if start.elapsed().as_secs_f64() >= measure_start {
            hist.record(t0.elapsed().as_micros() as u64);
            hist.bytes += 4;
        }
    }

    let _ = socket.send(Message::Close(None)).await;
}
