// Benchmark client: Go net/http. The Transport auto-adds Accept-Encoding: gzip
// and transparently decompresses, so we leave the header alone. Connection
// pooling is on by default; NAVI_BENCH_COLD=1 sets req.Close so every request
// uses a fresh TCP+TLS connection, exposing the connection-setup path.
package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	url := env("NAVI_BENCH_URL", "https://127.0.0.1:8443")
	iters, _ := strconv.Atoi(env("NAVI_BENCH_ITERS", "3000"))
	cold := env("NAVI_BENCH_COLD", "0") == "1"
	conc, _ := strconv.Atoi(env("NAVI_BENCH_CONC", "1"))
	if conc < 1 {
		conc = 1
	}
	warmup := iters / 10
	if cold {
		warmup = 20
	} else if warmup < 100 {
		warmup = 100
	}
	if warmup > iters {
		warmup = iters
	}
	body := strings.Repeat("x", 256)

	maxConns := 8
	if conc > maxConns {
		maxConns = conc // enough pooled connections for the goroutines
	}
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		ForceAttemptHTTP2:   false, // stay on HTTP/1.1 like the others
		MaxIdleConnsPerHost: maxConns,
	}}

	do := func(method, path, reqBody string) {
		var r io.Reader
		if reqBody != "" {
			r = strings.NewReader(reqBody)
		}
		req, _ := http.NewRequest(method, url+path, r)
		req.Close = cold // cold: close after the response so the next request reconnects
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	oneIter := func() {
		do("GET", "/get", "")
		do("POST", "/post", body)
		do("PUT", "/put", body)
		do("PATCH", "/patch", body)
		do("DELETE", "/delete", "")
		do("HEAD", "/get", "")
		do("OPTIONS", "/get", "")
	}

	for i := 0; i < warmup; i++ {
		oneIter()
	}

	var reqs int
	var secs float64
	if conc > 1 {
		per := iters / conc
		if per < 1 {
			per = 1
		}
		t0 := time.Now()
		var wg sync.WaitGroup
		for w := 0; w < conc; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < per; i++ {
					oneIter()
				}
			}()
		}
		wg.Wait()
		secs = time.Since(t0).Seconds()
		reqs = per * conc * 7
	} else {
		t0 := time.Now()
		for i := 0; i < iters; i++ {
			oneIter()
		}
		secs = time.Since(t0).Seconds()
		reqs = iters * 7
	}
	fmt.Printf("RESULT\tgo-nethttp\t%d\t%.3f\t%.0f\n", reqs, secs, float64(reqs)/secs)
}
