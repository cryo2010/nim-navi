// Benchmark client: Rust reqwest (blocking). gzip(true) makes it request and
// transparently decompress the body; the client pools connections by default.
// NAVI_BENCH_COLD=1 disables the idle pool (pool_max_idle_per_host(0)) so every
// request opens a fresh TCP+TLS connection. NAVI_BENCH_CONC>1 spreads the work
// across that many threads (the blocking Client is cloneable and shared).
use reqwest::blocking::Client;
use reqwest::Method;
use std::time::Instant;

fn do_one(c: &Client, url: &str, body: &str) {
    let mut get = |m: Method, path: &str, with_body: bool| {
        let mut rb = c.request(m, format!("{}{}", url, path));
        if with_body {
            rb = rb.body(body.to_string());
        }
        let _ = rb.send().unwrap().bytes().unwrap();
    };
    get(Method::GET, "/get", false);
    get(Method::POST, "/post", true);
    get(Method::PUT, "/put", true);
    get(Method::PATCH, "/patch", true);
    get(Method::DELETE, "/delete", false);
    get(Method::HEAD, "/get", false);
    get(Method::OPTIONS, "/get", false);
}

fn main() {
    let url = std::env::var("NAVI_BENCH_URL").unwrap_or_else(|_| "https://127.0.0.1:8443".into());
    let iters: usize = std::env::var("NAVI_BENCH_ITERS")
        .unwrap_or_else(|_| "3000".into())
        .parse()
        .unwrap();
    let cold = std::env::var("NAVI_BENCH_COLD").map(|v| v == "1").unwrap_or(false);
    let conc: usize = std::env::var("NAVI_BENCH_CONC")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .unwrap_or(1)
        .max(1);
    let warmup = if cold { 20.min(iters) } else { (iters / 10).max(100) };
    let body = "x".repeat(256);

    let mut builder = Client::builder()
        .danger_accept_invalid_certs(true) // self-signed target; TLS still exercised
        .gzip(true);
    if cold {
        builder = builder.pool_max_idle_per_host(0); // no reuse: reconnect per request
    }
    let client = builder.build().unwrap();

    for _ in 0..warmup {
        do_one(&client, &url, &body);
    }

    let (reqs, secs) = if conc > 1 {
        let per = (iters / conc).max(1);
        let t0 = Instant::now();
        let handles: Vec<_> = (0..conc)
            .map(|_| {
                let (c, u, b) = (client.clone(), url.clone(), body.clone());
                std::thread::spawn(move || {
                    for _ in 0..per {
                        do_one(&c, &u, &b);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        (per * conc * 7, t0.elapsed().as_secs_f64())
    } else {
        let t0 = Instant::now();
        for _ in 0..iters {
            do_one(&client, &url, &body);
        }
        (iters * 7, t0.elapsed().as_secs_f64())
    };
    println!("RESULT\trust-reqwest\t{}\t{:.3}\t{:.0}", reqs, secs, reqs as f64 / secs);
}
