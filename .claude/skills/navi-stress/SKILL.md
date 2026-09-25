---
name: navi-stress
description: >-
  Start and monitor a navi Dockerized stress soak from a plain-English prompt.
  prompt (string): stress run details. Understood hints:
  workload = websockets|requests|sse|stream upload|stream download;
  protocol = h1|h2|h3|all; client = sync|asyncdispatch|chronos|js|all;
  duration = e.g. "8 hours", "30m", "90s"; plus optional clients/concurrency/servers/recycle.
  Example: "/navi-stress Stress websockets for 8 hours for all protocols using the chronos client".
disable-model-invocation: true
arguments: prompt
allowed-tools: Bash, Read, Edit, Write, Agent, Monitor
---

# navi-stress

Orchestrate the following task: $ARGUMENTS. Turn the task into a matrix of pinned `nimble stress<Workload>` soaks. Fan out to agents (up to eight in parallel) to run the soak, monitor it and report any failures back to you. Each agent should handle one combination of client/protocol/workload (e.g. chronos/h2/sse). Once an agent reports a failure, let the other agents finish, fix the issues serially, and then restart the fanned out stress run. Drive an autonomous **fail → fix → restart** loop until one complete round passes clean on every cell. Then print a report.

`$prompt` is the whole invocation text (also `$ARGUMENTS`). If it is empty, ask the user what to
stress and stop.

## 1. Parse the prompt into a command

Read `$prompt` and pick exactly one workload task, then only the `NAVI_*` knobs the prompt
actually names. Rely on harness defaults for everything unnamed (do not invent values).

**Workload → nimble task** (first keyword match wins):

| Prompt says… | Task |
| --- | --- |
| websocket, websockets, ws | `stressWs` |
| request(s), http, verbs, GET/POST/… | `stressRequests` |
| sse, server-sent, events | `stressSse` |
| upload, stream up | `stressStreamUpload` |
| download, stream down | `stressStreamDownload` |
| (unspecified) | `stress` |

Follow the described **workload**, not any task name the user happens to type. "Stress
websockets" → `stressWs` even if the user wrote `stressRequests`.

**Env knobs** (set only when named in the prompt):

- `NAVI_PROTO` = `all` | `h1` | `h2` | `h3` — from "all protocols"→`all`, "h2"/"http/2"→`h2`,
  "http/3"/"h3"→`h3`, "http/1"/"h1"→`h1`. Default (unset) is `h2`.
- `NAVI_CLIENT` = `sync` | `asyncdispatch` | `chronos` | `js` | `all` — from "chronos client"→
  `chronos`, "asyncdispatch"→`asyncdispatch`, "sync"→`sync`, "js"/"node"→`js`, "all clients"→
  `all`. Default (unset) is `all`.
- `NAVI_SECONDS` = duration in seconds. Parse natural language: "8 hours"→`28800`,
  "90 minutes"/"90m"→`5400`, "30m"→`1800`, "90s"/"90 seconds"→`90`. Default `60`.
- `NAVI_REPORT_SECONDS` = **derived, always set it**: `clamp(round(NAVI_SECONDS / 32), 60, 900)`.
  (28800/32 = 900; 3600 → ~113 → set 120-ish is fine, just clamp; short runs floor at 60.)
- Optional pass-throughs, only if the prompt names them: `NAVI_CLIENT_COUNT`, `NAVI_CONCURRENCY`,
  `NAVI_SERVER_COUNT`, `NAVI_REQ_COMPRESSION`, `NAVI_RESP_COMPRESSION`, `NAVI_CONTENT_TYPES`,
  `NAVI_STREAM_BYTES`, `NAVI_RECYCLE`, `NAVI_KEEPALIVE_MAX`, `NAVI_KEEPALIVE_TIMEOUT`,
  `NAVI_LOG_ERRORS`.

Reference (don't re-derive): tasks + the `runStress` env passthrough live in `navi.nimble:76-116`;
the full knob table with defaults and the pass/fail banner semantics are in
`tests/stress/README.md`.

Important: You can separate the stress run by protocol, client or workload. A simple distribution
is one agent per client or one agent per workload.

**Echo the exact command before running it**, on its own line for observability, e.g.:

```
NAVI_PROTO=all NAVI_SECONDS=28800 NAVI_REPORT_SECONDS=900 NAVI_CLIENT=chronos nimble stressWs
```

Only include the env vars you actually set. Keep the invariant `<env> nimble stress<Workload>`.

## 2. Launch the soak

- Image: `NAVI_PROTO` of `h3` or `all` → image `navi-stress-h3` (heavier build); otherwise
  `navi-stress`. The nimble task builds the image then does `docker run --rm … <image>`.
- Run the built command with **Bash `run_in_background`**, redirecting to
  `<scratchpad>/<workload>.log` (2>&1). The first `docker build` can take a while (minutes for
  the h3 image) — that's expected.
- Once `docker run` has started, find the container id:
  `docker ps --filter ancestor=<image> --format '{{.ID}}'` (retry until non-empty; the build
  must finish first).

## 3. Monitor via `docker logs`, not the piped file

nimble does not propagate exit codes and **reaps its driver process on long soaks** while the
container keeps running (see the `monitor-stress-soaks-via-docker-logs` memory). So watch the
container, not `<workload>.log`.

Start a **Bash `run_in_background`** until-loop over `docker logs -f <id>` that **exits on a
terminal state**, so the harness wakes this skill exactly at the decision point:

- **PASS**: line matching `== <workload>: all cells passed ==`
- **FAIL**: any of `== <workload>: FAILURES ==`, `mismatch`, `checksum`, `Traceback`,
  `err[1-9]`, `panic`, `assert`, `Killed`

Have the loop print which terminal signature it hit and exit. Also set a `ScheduleWakeup`
(~1200s) as a fallback heartbeat in case the container hangs and emits nothing. On each wake,
if still running, sample the latest report lines (`200x… | RSS … | heap … | t=…s`) so you can
show progress and the memory-flatness trend.

## 4. On PASS → report and finish

Go to section 6.

## 5. On FAIL → stop, fix, restart (the core loop)

1. **Preserve evidence before teardown** (the container is `--rm`, so its logs vanish on stop):
   `docker cp <id>:/navi/stress-srv-logs <scratchpad>/srv-logs-<n>`. Snapshot the failing cell
   (workload × proto × client) and the tail of `docker logs <id>` into the scratchpad.
2. **Stop the run**: `docker kill <id>`, and kill the background nimble job.
3. **Ensure the session fix branch** (create once, lazily, on the first failure; reuse it for
   every later fix): `git checkout -b fix/stress-<workload>-<shortslug>` off `main`. If it
   already exists this session, stay on it.
4. **Dispatch an opus Agent** (`subagent_type: claude`, `model: opus`) per failure with the
   failing cell, the log tail, and the preserved server logs. Tell the agent to:
   - Root-cause and fix the issue in the navi source.
   - **Validate with a short, focused run** before committing: same workload, pinned to the
     failing `NAVI_PROTO` and `NAVI_CLIENT`, `NAVI_SECONDS=120`. Do **not** edit `.nim` while a
     stress build is copying the worktree (`stress-builds-from-worktree` memory) — only build
     when no run is active.
   - Commit on the session branch: **one commit per fix**, semantic message, **no AI
     attribution** of any kind (`no-claude-attribution` memory). Do **not** push.
   - Return the root cause (one line) and the commit sha.
5. **Restart the full run** with the original parameters (full duration and matrix) from
   section 2, and resume monitoring at section 3.
6. **Repeat** until a complete run reaches the PASS banner with no `err`/mismatch.

## 6. Final report

Print a markdown summary:

- The exact command(s) run and total wall-clock.
- Iterations: how many failures were fixed; per failure: the cell, a one-line root cause, and
  the commit sha.
- Branch name + `git log --oneline main..<branch>`.
- Final RSS/heap trend from the last report lines (confirm memory stayed flat).
- A clear **PASS** statement, and a reminder that the fixes sit on `<branch>` (unpushed) for
  review.

If no failures occurred, say so: one clean run, no branch created.
