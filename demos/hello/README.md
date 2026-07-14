# navi/js hello demo

A [navi/js](../../src/navi/js.nim) client that does `GET /hello` against a
Python FastAPI server. The client compiles to JavaScript and runs on Node, and
uses a `beforeRequest` hook to echo the outgoing URL.

## Run with Docker (client + server)

From the repo root:

```
docker compose -f demos/hello/docker-compose.yml up --build
```

Compose builds two images and runs them:

- **server** — FastAPI / uvicorn serving `/hello` on port 8080.
- **client** — the navi/js demo, compiled with `nim js` and run on Node. It
  waits for the server's healthcheck, makes one request, and exits.

Expected client output:

```
-> http://server:8080/hello
status: 200
body:   {"message":"hello from FastAPI"}
```

The client targets `$HELLO_URL`, which compose points at the `server` service.

## Run the client standalone

Needs Node 18+ (global fetch) and any server answering on `:8080`:

```
nim js -r demos/hello/hello.nim
```

Override the target with `HELLO_URL` if the server is elsewhere.
