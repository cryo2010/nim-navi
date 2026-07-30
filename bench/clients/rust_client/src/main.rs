// Benchmark client: Rust reqwest (blocking). gzip(true) makes it request and
// transparently decompress the body; the client pools connections by default.
use reqwest::blocking::Client;
use reqwest::Method;
use std::time::Instant;

fn main() {
    let url = std::env::var("NAVI_BENCH_URL").unwrap_or_else(|_| "https://127.0.0.1:8443".into());
    let iters: usize = std::env::var("NAVI_BENCH_ITERS")
        .unwrap_or_else(|_| "3000".into())
        .parse()
        .unwrap();
    let warmup = (iters / 10).max(100);
    let body = "x".repeat(256);

    let client = Client::builder()
        .danger_accept_invalid_certs(true) // self-signed target; TLS still exercised
        .gzip(true)
        .build()
        .unwrap();

    let one = |c: &Client| {
        let get = |m: Method, path: &str| {
            let mut rb = c.request(m, format!("{}{}", url, path));
            rb = match path {
                "/post" | "/put" | "/patch" => rb.body(body.clone()),
                _ => rb,
            };
            let _ = rb.send().unwrap().bytes().unwrap();
        };
        get(Method::GET, "/get");
        get(Method::POST, "/post");
        get(Method::PUT, "/put");
        get(Method::PATCH, "/patch");
        get(Method::DELETE, "/delete");
        get(Method::HEAD, "/get");
        get(Method::OPTIONS, "/get");
    };

    for _ in 0..warmup {
        one(&client);
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        one(&client);
    }
    let secs = t0.elapsed().as_secs_f64();
    let reqs = iters * 7;
    println!("RESULT\trust-reqwest\t{}\t{:.3}\t{:.0}", reqs, secs, reqs as f64 / secs);
}
