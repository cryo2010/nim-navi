// Fast benchmark origin: a TLS server serving every navi workload endpoint so the
// server is never the bottleneck. HTTP/2 is negotiated via ALPN (TLSNextProto left
// nil); h3 is fronted by Caddy in run.sh. Config via env: NAVI_BENCH_ADDR,
// NAVI_BENCH_CERT, NAVI_BENCH_KEY.
//
// Phase A implements /echo (buffered request/response). /upload, /download,
// /events, /ws are added in later phases.
package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

// ~8 KiB of varied text: compresses ~3x, so (de)compression is real work.
var body = buildBody()

func buildBody() []byte {
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
	return []byte(b.String())
}

// decodeBody inflates the request body per Content-Encoding (gzip/deflate), so the
// server exercises navi's request-compression path the way the stress FastAPI does.
func decodeBody(r *http.Request) {
	defer r.Body.Close()
	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		if zr, err := gzip.NewReader(r.Body); err == nil {
			io.Copy(io.Discard, zr)
			zr.Close()
			return
		}
	case "deflate":
		fr := flate.NewReader(r.Body)
		io.Copy(io.Discard, fr)
		fr.Close()
		return
	}
	io.Copy(io.Discard, r.Body)
}

// encode compresses the response body per x-want-encoding (gzip/deflate; anything
// else, incl. br/zstd, is sent plain to stay stdlib-only). Returns body + encoding.
func encode(want string) ([]byte, string) {
	switch want {
	case "gzip":
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		zw.Write(body)
		zw.Close()
		return buf.Bytes(), "gzip"
	case "deflate":
		var buf bytes.Buffer
		zw, _ := flate.NewWriter(&buf, flate.DefaultCompression)
		zw.Write(body)
		zw.Close()
		return buf.Bytes(), "deflate"
	}
	return body, ""
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	decodeBody(r)
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("x-echo-method", r.Method)
	if v := r.Header.Get("x-stress"); v != "" {
		w.Header().Set("x-echo-stress", v)
	}
	if r.Method == http.MethodOptions {
		w.Header().Set("Allow", "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS")
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	// navi asks via x-want-encoding; the reference clients rely on their stack's
	// default Accept-Encoding: gzip (and auto-decompress), so honor both for a fair
	// comparison where every client does the same decompression work.
	want := r.Header.Get("x-want-encoding")
	if want == "" && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		want = "gzip"
	}
	payload, enc := encode(want)
	if enc != "" {
		w.Header().Set("Content-Encoding", enc)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(payload)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/echo", echoHandler)
	srv := &http.Server{
		Addr:    env("NAVI_BENCH_ADDR", "127.0.0.1:8443"),
		Handler: mux,
		// TLSNextProto nil => HTTP/2 negotiated via ALPN on TLS (h1 clients still
		// get h1). Protocol is a matrix dimension now, not pinned.
	}
	log.Println("bench server on", srv.Addr, "(h1 + h2, gzip)")
	log.Fatal(srv.ListenAndServeTLS(env("NAVI_BENCH_CERT", "cert.pem"),
		env("NAVI_BENCH_KEY", "key.pem")))
}
