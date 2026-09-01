//! benchRequests reference client: Rust reqwest + tokio (name: rust).
//!
//! Benchmarks the buffered requests workload (GET/POST/PUT) over HTTP/1.1 or
//! HTTP/2, time-boxed, recording latency, and prints ONE tab-separated RESULT
//! line. Same contract as the other-language reference clients. HTTP/3 is
//! skipped. All logs go to stderr; only the RESULT line goes to stdout.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

// --- Config helpers -------------------------------------------------------
fn env_str(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}
fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// --- Latency histogram (MUST match the cross-language scheme exactly) ------
struct Hist {
    counts: Vec<u64>,
    total: u64,
}
impl Hist {
    fn new(max_buckets: usize) -> Self {
        Hist { counts: vec![0; max_buckets], total: 0 }
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

fn main() {
    let proto = env_str("NAVI_PROTO", "h2");
    if proto == "h3" {
        println!("SKIP\trust\tno reference-client HTTP/3");
        std::process::exit(0);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(run(proto));
}

async fn run(proto: String) {
    let host = env_str("NAVI_HOST", "127.0.0.1");
    let base_port: u16 = env_parse("NAVI_BASE_PORT", 9443);
    let servers: usize = env_parse("NAVI_SERVERS", 5);
    let seconds: f64 = env_parse("NAVI_SECONDS", 20.0);
    let warmup_seconds: f64 = env_parse("NAVI_WARMUP_SECONDS", 2.0);
    let mode = env_str("NAVI_MODE", "pooled");
    let clients: usize = env_parse("NAVI_CLIENTS", 3);
    let concurrency: usize = env_parse("NAVI_CONCURRENCY", 8);
    let cold = mode == "cold";

    let bases: Arc<Vec<String>> = Arc::new(
        (0..servers)
            .map(|i| format!("https://{}:{}", host, base_port as usize + i))
            .collect(),
    );

    let max_buckets = (300_000_000f64.log2() * 64.0).floor() as usize + 1;

    // One shared client with an internal connection pool (pooled mode).
    let client = Arc::new(build_client(&proto, cold));
    let rr = Arc::new(AtomicUsize::new(0));

    let n_workers = clients * concurrency;
    let start = Instant::now();
    let measure_start = warmup_seconds;
    let deadline = warmup_seconds + seconds;

    let mut handles = Vec::with_capacity(n_workers);
    for i in 0..n_workers {
        let client = client.clone();
        let bases = bases.clone();
        let rr = rr.clone();
        let proto = proto.clone();
        handles.push(tokio::spawn(async move {
            worker(i, client, bases, rr, proto, cold, start, measure_start,
                   deadline, max_buckets)
                .await
        }));
    }

    let mut hist = Hist::new(max_buckets);
    for h in handles {
        let local = h.await.expect("worker task join");
        hist.merge(&local);
    }
    let elapsed = start.elapsed().as_secs_f64() - measure_start;

    let ops = hist.total;
    let secs = elapsed;
    let rps = if secs > 0.0 { (ops as f64 / secs).round() as u64 } else { 0 };
    println!(
        "RESULT\trust\t{}\t{:.3}\t{}\t{:.3}\t{:.3}\t{:.3}\t0.0",
        ops,
        secs,
        rps,
        hist.percentile(50.0),
        hist.percentile(99.0),
        hist.percentile(99.9),
    );
}

fn build_client(proto: &str, cold: bool) -> reqwest::Client {
    let mut b = reqwest::Client::builder().danger_accept_invalid_certs(true);
    if proto == "h1" {
        // Force HTTP/1.1 only.
        b = b.http1_only();
    }
    // For h2 we rely on ALPN negotiation over TLS (do NOT use
    // http2_prior_knowledge, which skips ALPN). The negotiated version is
    // asserted on the first response.
    if cold {
        b = b.pool_max_idle_per_host(0);
    }
    b.build().expect("build reqwest client")
}

#[allow(clippy::too_many_arguments)]
async fn worker(
    i: usize,
    client: Arc<reqwest::Client>,
    bases: Arc<Vec<String>>,
    rr: Arc<AtomicUsize>,
    proto: String,
    cold: bool,
    start: Instant,
    measure_start: f64,
    deadline: f64,
    max_buckets: usize,
) -> Hist {
    let mut hist = Hist::new(max_buckets);
    let mut n = i;
    let mut version_checked = false;
    while start.elapsed().as_secs_f64() < deadline {
        let verb = VERBS[n % VERBS.len()];
        n += 1;
        let slot = rr.fetch_add(1, Ordering::Relaxed) % bases.len();
        let url = format!("{}/echo", bases[slot]);

        let mut req = match verb {
            "POST" => client.post(&url).body("payload-x").header("content-type", "text/plain"),
            "PUT" => client.put(&url).body("payload-x").header("content-type", "text/plain"),
            _ => client.get(&url),
        };
        if cold {
            req = req.header("connection", "close");
        }

        let t0 = Instant::now();
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[rust] FAIL: {}", e);
                std::process::exit(1);
            }
        };

        if !version_checked {
            version_checked = true;
            let want = if proto == "h1" {
                reqwest::Version::HTTP_11
            } else {
                reqwest::Version::HTTP_2
            };
            if resp.version() != want {
                eprintln!(
                    "[rust] protocol mismatch: got {:?} want {:?}",
                    resp.version(),
                    want
                );
                std::process::exit(1);
            }
        }

        // Read the full body (gzip auto-decompressed by reqwest).
        if let Err(e) = resp.bytes().await {
            eprintln!("[rust] FAIL: {}", e);
            std::process::exit(1);
        }

        if start.elapsed().as_secs_f64() >= measure_start {
            hist.record(t0.elapsed().as_micros() as u64);
        }
    }
    hist
}
