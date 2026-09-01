// Go reference client for the navi HTTP bench harness.
//
// Three time-boxed workloads share one driver (server pool, TLS, h1/h2 selection
// + first-response version gate, warmup/measure windows, CLIENTS*CONCURRENCY
// goroutines, and a log-bucketed latency histogram whose scheme -- floor(log2(us)
// *64) -- matches the Nim/Rust/Node/Python peers so p50/p99/p999 are comparable):
//   requests       - buffered GET/POST/PUT at /echo; unit = one request.
//   streamDownload - GET /download, hashed through sha1 without buffering.
//   streamUpload   - POST /upload, a sha1-hashed streamed body of constant memory.
// Streaming units are one TRANSFER, and RESULT's last field carries MB/s. Emits one
// RESULT line; fails hard on any transport error, protocol downgrade, or checksum
// mismatch.
package main

import (
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
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

func envInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
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

// --- shared driver ---

// config is the resolved run configuration shared by every workload.
type config struct {
	bases       []string
	client      *http.Client
	expect      string // negotiated HTTP version this cell is pinned to
	cold        bool
	clients     int
	concurrency int
	streamBytes int64

	measureStart time.Time
	deadline     time.Time
	verified     int32  // guards the one-time protocol gate
	baseIdx      uint64 // shared round-robin cursor across the server pool
}

func (c *config) nextBase() string {
	return c.bases[int(atomic.AddUint64(&c.baseIdx, 1))%len(c.bases)]
}

// gate applies the one-time first-response protocol check; hard-fail on downgrade.
func (c *config) gate(resp *http.Response) {
	if atomic.CompareAndSwapInt32(&c.verified, 0, 1) && resp.Proto != c.expect {
		fmt.Fprintf(os.Stderr, "FAIL: wrong protocol -- expected %s, got %q\n", c.expect, resp.Proto)
		os.Exit(1)
	}
}

// transferFn performs one unit of work and returns the bytes it moved. It runs in
// both warmup and measured phases; the driver decides whether to record.
type transferFn func(seed int) (bytes int64)

// run fans out CLIENTS*CONCURRENCY goroutines looping do until the deadline,
// records each measured transfer's latency + bytes, and emits the RESULT line.
func (c *config) run(do transferFn) {
	var wg sync.WaitGroup
	hists := make([]*histogram, c.clients*c.concurrency)
	bytesEach := make([]int64, len(hists))
	for i := range hists {
		hists[i] = newHistogram()
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n := idx
			for time.Now().Before(c.deadline) {
				t0 := time.Now()
				b := do(n)
				n++
				if !time.Now().Before(c.measureStart) {
					hists[idx].record(time.Since(t0).Microseconds())
					bytesEach[idx] += b
				}
			}
		}(i)
	}
	wg.Wait()

	final := newHistogram()
	var totalBytes int64
	for i, h := range hists {
		final.merge(h)
		totalBytes += bytesEach[i]
	}

	elapsed := time.Since(c.measureStart).Seconds()
	ops := final.total
	rps, mbps := 0, 0.0
	if elapsed > 0 {
		rps = int(float64(ops)/elapsed + 0.5)
		mbps = float64(totalBytes) / elapsed / 1e6
	}
	fmt.Printf("RESULT\tgo\t%d\t%.3f\t%d\t%.3f\t%.3f\t%.3f\t%.1f\n",
		ops, elapsed, rps,
		final.percentileMs(50), final.percentileMs(99), final.percentileMs(99.9),
		mbps)
}

func main() {
	proto := envStr("NAVI_PROTO", "h2")
	if proto == "h3" {
		fmt.Println("SKIP\tgo\tno reference-client HTTP/3")
		os.Exit(0)
	}
	workload := envStr("NAVI_WORKLOAD", "requests")
	switch workload {
	case "requests", "streamDownload", "streamUpload":
	default:
		fmt.Printf("SKIP\tgo\t%s not implemented\n", workload)
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

	start := time.Now()
	cfg := &config{
		client:       &http.Client{Transport: tr},
		expect:       expect,
		cold:         envStr("NAVI_MODE", "pooled") == "cold",
		clients:      envInt("NAVI_CLIENTS", 3),
		concurrency:  envInt("NAVI_CONCURRENCY", 8),
		streamBytes:  envInt64("NAVI_STREAM_BYTES", 1073741824),
		measureStart: start.Add(time.Duration(warmup * float64(time.Second))),
	}
	cfg.deadline = cfg.measureStart.Add(time.Duration(seconds * float64(time.Second)))
	for i := 0; i < servers; i++ {
		cfg.bases = append(cfg.bases, fmt.Sprintf("https://%s:%d", host, basePort+i))
	}

	switch workload {
	case "requests":
		cfg.run(cfg.doRequest)
	case "streamDownload":
		cfg.run(cfg.doDownload)
	case "streamUpload":
		cfg.run(cfg.doUpload)
	}
}

// doRequest: one buffered GET/POST/PUT at /echo, body fully drained.
func (c *config) doRequest(seed int) int64 {
	verbs := []string{http.MethodGet, http.MethodPost, http.MethodPut}
	v := verbs[seed%len(verbs)]
	url := c.nextBase() + "/echo"

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
	if c.cold {
		req.Close = true // fresh connection per request
	}

	resp, err := c.client.Do(req)
	if err != nil {
		fail(v, url, err)
	}
	c.gate(resp)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return 0
}

// doDownload: GET /download?size=N, streamed through sha1 (never buffered) and
// checked against the server's x-sha1 header. Returns the copied byte count.
func (c *config) doDownload(_ int) int64 {
	url := fmt.Sprintf("%s/download?size=%d", c.nextBase(), c.streamBytes)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fail("GET", url, err)
	}
	if c.cold {
		req.Close = true
	}
	resp, err := c.client.Do(req)
	if err != nil {
		fail("GET", url, err)
	}
	c.gate(resp)
	h := sha1.New()
	n, err := io.Copy(h, resp.Body)
	resp.Body.Close()
	if err != nil {
		fail("GET", url, err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if want := resp.Header.Get("x-sha1"); got != want {
		fmt.Fprintf(os.Stderr, "FAIL: download sha1 mismatch %s -> got %s want %s\n", url, got, want)
		os.Exit(1)
	}
	return n
}

const uploadBlock = 1 << 20 // 1 MiB block reused for constant-memory upload

// doUpload: POST /upload with a streamed body of exactly streamBytes, hashed as
// it is written; server echoes {"sha1","size"} which must match. Returns bytes sent.
func (c *config) doUpload(_ int) int64 {
	url := c.nextBase() + "/upload"
	total := c.streamBytes

	pr, pw := io.Pipe()
	h := sha1.New()
	go func() {
		block := make([]byte, uploadBlock)
		for i := range block {
			block[i] = byte(i)
		}
		remaining := total
		for remaining > 0 {
			chunk := int64(len(block))
			if chunk > remaining {
				chunk = remaining
			}
			h.Write(block[:chunk])
			if _, err := pw.Write(block[:chunk]); err != nil {
				pw.CloseWithError(err)
				return
			}
			remaining -= chunk
		}
		pw.Close()
	}()

	req, err := http.NewRequest(http.MethodPost, url, pr)
	if err != nil {
		fail("POST", url, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = total
	if c.cold {
		req.Close = true
	}
	resp, err := c.client.Do(req)
	if err != nil {
		fail("POST", url, err)
	}
	c.gate(resp)
	var out struct {
		Sha1 string `json:"sha1"`
		Size int64  `json:"size"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if err != nil {
		fail("POST", url, err)
	}
	want := hex.EncodeToString(h.Sum(nil))
	if out.Sha1 != want || out.Size != total {
		fmt.Fprintf(os.Stderr, "FAIL: upload mismatch %s -> sha1 got %s want %s, size got %d want %d\n",
			url, out.Sha1, want, out.Size, total)
		os.Exit(1)
	}
	return total
}

func fail(verb, url string, err error) {
	fmt.Fprintf(os.Stderr, "FAIL: %s %s -> %v\n", verb, url, err)
	os.Exit(1)
}
