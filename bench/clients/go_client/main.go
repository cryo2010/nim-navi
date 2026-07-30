// Benchmark client: Go net/http. The Transport auto-adds Accept-Encoding: gzip
// and transparently decompresses, so we leave the header alone. Connection
// pooling is on by default.
package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	warmup := iters / 10
	if warmup < 100 {
		warmup = 100
	}
	body := strings.Repeat("x", 256)

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		ForceAttemptHTTP2:   false, // stay on HTTP/1.1 like the others
		MaxIdleConnsPerHost: 8,
	}}

	do := func(method, path, reqBody string) {
		var r io.Reader
		if reqBody != "" {
			r = strings.NewReader(reqBody)
		}
		req, _ := http.NewRequest(method, url+path, r)
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
	t0 := time.Now()
	for i := 0; i < iters; i++ {
		oneIter()
	}
	secs := time.Since(t0).Seconds()
	reqs := iters * 7
	fmt.Printf("RESULT\tgo-nethttp\t%d\t%.3f\t%.0f\n", reqs, secs, float64(reqs)/secs)
}
