// Go reference client for the navi HTTP bench harness (requests workload).
//
// Time-boxed buffered GET/POST/PUT throughput + latency against the local TLS
// server pool. An unmeasured warmup prelude precedes the measured window; each
// measured request's wall time lands in a log-bucketed latency histogram whose
// scheme (floor(log2(us)*64)) matches the Nim/Rust/Node/Python peers so the
// p50/p99/p999 columns are comparable. Emits one RESULT line; fails hard on any
// surfaced transport error or protocol downgrade.
package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// --- latency histogram (must match the peer reference clients) ---

const bucketsPerDoubling = 64

var maxBuckets = int(math.Floor(math.Log2(300000000.0)*bucketsPerDoubling)) + 1

type histogram struct {
	counts []int64
	total  int64
}

func newHistogram() *histogram { return &histogram{counts: make([]int64, maxBuckets)} }

func (h *histogram) record(us int64) {
	v := us
	if v < 1 {
		v = 1
	}
	idx := int(math.Floor(math.Log2(float64(v)) * bucketsPerDoubling))
	if idx < 0 {
		idx = 0
	} else if idx >= maxBuckets {
		idx = maxBuckets - 1
	}
	h.counts[idx]++
	h.total++
}

func (h *histogram) merge(o *histogram) {
	for i := range h.counts {
		h.counts[i] += o.counts[i]
	}
	h.total += o.total
}

// percentileMs returns the p-th percentile in milliseconds (bucket midpoint).
func (h *histogram) percentileMs(p float64) float64 {
	if h.total == 0 {
		return 0
	}
	target := int64(math.Ceil(p / 100.0 * float64(h.total)))
	if target < 1 {
		target = 1
	}
	var cum int64
	for i := range h.counts {
		cum += h.counts[i]
		if cum >= target {
			return math.Pow(2, (float64(i)+0.5)/bucketsPerDoubling) / 1000.0
		}
	}
	return math.Pow(2, (float64(maxBuckets-1)+0.5)/bucketsPerDoubling) / 1000.0
}

func main() {
	proto := envStr("NAVI_PROTO", "h2")
	if proto == "h3" {
		fmt.Println("SKIP\tgo\tno reference-client HTTP/3")
		os.Exit(0)
	}

	host := envStr("NAVI_HOST", "127.0.0.1")
	basePort := envInt("NAVI_BASE_PORT", 9443)
	servers := envInt("NAVI_SERVERS", 5)
	if servers < 1 {
		servers = 1
	}
	seconds := envFloat("NAVI_SECONDS", 20)
	warmup := envFloat("NAVI_WARMUP_SECONDS", 2)
	cold := envStr("NAVI_MODE", "pooled") == "cold"
	clients := envInt("NAVI_CLIENTS", 3)
	concurrency := envInt("NAVI_CONCURRENCY", 8)

	bases := make([]string, servers)
	for i := 0; i < servers; i++ {
		bases[i] = fmt.Sprintf("https://%s:%d", host, basePort+i)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	expect := "HTTP/2.0"
	if proto == "h1" {
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		tr.ForceAttemptHTTP2 = false
		expect = "HTTP/1.1"
	} else {
		tr.ForceAttemptHTTP2 = true
	}
	client := &http.Client{Transport: tr}

	verbs := []string{http.MethodGet, http.MethodPost, http.MethodPut}
	start := time.Now()
	measureStart := start.Add(time.Duration(warmup * float64(time.Second)))
	deadline := measureStart.Add(time.Duration(seconds * float64(time.Second)))

	var verified int32 // guards the one-time protocol gate
	var baseIdx uint64  // shared round-robin cursor across the server pool

	worker := func(h *histogram, seed int) {
		n := seed
		for time.Now().Before(deadline) {
			v := verbs[n%len(verbs)]
			n++
			base := bases[int(atomic.AddUint64(&baseIdx, 1))%len(bases)]
			url := base + "/echo"

			var body io.Reader
			if v == http.MethodPost || v == http.MethodPut {
				body = strings.NewReader("payload-x")
			}
			req, err := http.NewRequest(v, url, body)
			if err != nil {
				fail(v, url, err)
			}
			if body != nil {
				req.Header.Set("Content-Type", "text/plain")
			}
			if cold {
				req.Close = true // fresh connection per request
			}

			t0 := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				fail(v, url, err)
			}
			if atomic.CompareAndSwapInt32(&verified, 0, 1) && resp.Proto != expect {
				fmt.Fprintf(os.Stderr, "FAIL: wrong protocol -- expected %s, got %q\n", expect, resp.Proto)
				os.Exit(1)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if !time.Now().Before(measureStart) {
				h.record(time.Since(t0).Microseconds())
			}
		}
	}

	var wg sync.WaitGroup
	hists := make([]*histogram, clients*concurrency)
	for i := range hists {
		hists[i] = newHistogram()
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			worker(hists[idx], idx)
		}(i)
	}
	wg.Wait()

	final := newHistogram()
	for _, h := range hists {
		final.merge(h)
	}

	elapsed := time.Since(measureStart).Seconds()
	ops := final.total
	rps := 0
	if elapsed > 0 {
		rps = int(float64(ops)/elapsed + 0.5)
	}
	fmt.Printf("RESULT\tgo\t%d\t%.3f\t%d\t%.3f\t%.3f\t%.3f\t%s\n",
		ops, elapsed, rps,
		final.percentileMs(50), final.percentileMs(99), final.percentileMs(99.9),
		"0.0")
}

func fail(verb, url string, err error) {
	fmt.Fprintf(os.Stderr, "FAIL: %s %s -> %v\n", verb, url, err)
	os.Exit(1)
}
