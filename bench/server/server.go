// Benchmark target: a TLS server that answers every HTTP method with a
// gzip-compressible body (compressed when the client sends Accept-Encoding:
// gzip). HTTP/2 is disabled so all clients are compared over HTTP/1.1 on equal
// footing (std/httpclient is h1-only). Config via env: NAVI_BENCH_ADDR,
// NAVI_BENCH_CERT, NAVI_BENCH_KEY.
package main

import (
	"compress/gzip"
	"crypto/tls"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

// ~8 KiB of varied text: compresses ~3x (realistic), so decompression is real
// work rather than a near-empty run of repeated bytes.
var body = buildBody()

func buildBody() string {
	var b strings.Builder
	words := strings.Fields(`the quick brown fox jumps over a lazy dog while
		navi benchmarks tls compression and every http method against several
		clients written in nim go and rust to compare their per-request cost`)
	for i := 0; b.Len() < 8192; i++ {
		b.WriteString(words[i%len(words)])
		b.WriteByte(' ')
		if i%11 == 0 {
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func handler(w http.ResponseWriter, r *http.Request) {
	io.Copy(io.Discard, r.Body) // drain POST/PUT/PATCH bodies
	r.Body.Close()
	w.Header().Set("Content-Type", "text/plain")
	if r.Method == http.MethodOptions {
		w.Header().Set("Allow", "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS")
	}
	gzipOk := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
	if gzipOk && r.Method != http.MethodHead {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		gz := gzip.NewWriter(w)
		io.WriteString(gz, body)
		gz.Close()
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		io.WriteString(w, body)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	srv := &http.Server{
		Addr:    env("NAVI_BENCH_ADDR", "127.0.0.1:8443"),
		Handler: http.HandlerFunc(handler),
		// Pin HTTP/1.1: an empty TLSNextProto disables the automatic h2 upgrade.
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
	}
	log.Println("benchmark server on", srv.Addr, "(HTTP/1.1, gzip)")
	log.Fatal(srv.ListenAndServeTLS(env("NAVI_BENCH_CERT", "cert.pem"),
		env("NAVI_BENCH_KEY", "key.pem")))
}
