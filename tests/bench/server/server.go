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
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const blockSize = 1 << 20 // 1 MiB streaming block

// One reusable 1 MiB LCG block, streamed repeatedly so /download stays constant
// memory regardless of size (matches tests/bench/common/streamcontent.nim's bytes,
// though each side hashes only its own bytes so they need not match).
var dlBlock = buildBlock()

func buildBlock() []byte {
	b := make([]byte, blockSize)
	var x uint32 = 0x12345678
	for i := range b {
		x = x*1664525 + 1013904223
		b[i] = byte(x >> 24)
	}
	return b
}

// /upload: hash the streamed request body incrementally (constant memory), return
// {"sha1","size"} so the client can verify integrity.
func uploadHandler(w http.ResponseWriter, r *http.Request) {
	h := sha1.New()
	n, _ := io.Copy(h, r.Body)
	r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"sha1":"%s","size":%d}`, hex.EncodeToString(h.Sum(nil)), n)
}

// /download?size=N: stream N bytes as the repeated block, with x-sha1 of the exact
// stream (computed before the first write, since headers must precede the body).
func downloadHandler(w http.ResponseWriter, r *http.Request) {
	size := blockSize
	if q := r.URL.Query().Get("size"); q != "" {
		if v, err := strconv.Atoi(q); err == nil {
			size = v
		}
	}
	full, rem := size/blockSize, size%blockSize
	h := sha1.New()
	for i := 0; i < full; i++ {
		h.Write(dlBlock)
	}
	if rem > 0 {
		h.Write(dlBlock[:rem])
	}
	w.Header().Set("x-sha1", hex.EncodeToString(h.Sum(nil)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	for i := 0; i < full; i++ {
		w.Write(dlBlock)
	}
	if rem > 0 {
		w.Write(dlBlock[:rem])
	}
}

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

// /events: a text/event-stream that pushes numbered events as fast as the client
// consumes them (honors Last-Event-ID for resume), so the benchmark measures SSE
// consumption throughput. Runs until the client disconnects.
func eventsHandler(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "no flusher", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	n := 0
	if lid := r.Header.Get("Last-Event-ID"); lid != "" {
		if v, err := strconv.Atoi(lid); err == nil {
			n = v + 1
		}
	}
	data := strings.Repeat("x", 64)
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", n, data); err != nil {
			return
		}
		fl.Flush()
		n++
	}
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
	mux.HandleFunc("/upload", uploadHandler)
	mux.HandleFunc("/download", downloadHandler)
	mux.HandleFunc("/events", eventsHandler)
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
