---
name: navi-implement
description: >-
  Implement a batch of GitHub issues end to end on one branch: opus agents implement
  (one commit per issue), opus agents adversarially review, findings get fixed, a short
  stress smoke validates, then the PR is opened and CI is monitored to green.
  issues (string): issue numbers and/or ranges, e.g. "450 451" or "300-312".
  Example: "/navi-implement 450 451" or "/navi-implement 300-312".
disable-model-invocation: true
arguments: [issues]
argument-hint: "[issue numbers or ranges, e.g. 450 451 or 300-312]"
allowed-tools: Bash, Read, Edit, Write, Agent, SendMessage, Monitor, ScheduleWakeup
---

# navi-implement

Create a branch and dispatch opus agents to implement the following issues, one commit per
issue: $ARGUMENTS. Do not defer any part of these issues; no exceptions. Once complete dispatch
opus agents for an adversarial code review, and fix any issues. Then validate the changes using
a short stress run (e.g. `NAVI_PROTO=all NAVI_CLIENT=all NAVI_SECONDS=10 NAVI_REPORT_SECONDS=2
nimble stress`). Then open the PR and monitor the CI build. Make hard decisions yourself since
the user is afk, and provide a report afterwards.

`$ARGUMENTS` is the whole issue list (`$issues` is only its first token). If it is empty, ask
the user which issues to implement and stop. Everything below is autonomous: never block on
a question. When a call is genuinely ambiguous, pick the option a careful maintainer would,
note it in the final report under **Decisions**, and keep going.

## Ground rules (apply throughout)

- **No deferral.** Every acceptance criterion in every issue ships in this batch: code, tests,
  docs, CI wiring. "Follow-up PR", "out of scope", "TODO" and partial fixes are not allowed.
  If an issue is ill-specified, implement the most complete reasonable reading and record the
  interpretation in the report.
- **One commit per issue**, semantic message in the repo style, e.g.
  `fix(h3): guard the sync ws-over-h3 pump behind --threads (#450)`. Never add
  `Co-Authored-By`, session links, or any AI attribution to commits, the PR, or issue comments.
- **Unit tests run through `checkmate`**, never `bash tests/run.sh`; it matches CI and
  enforces assertions. In a worktree under the session scratchpad plain `checkmate` works; in
  `.claude/worktrees` pass `-n:--skipParentCfg -n:-d:ssl` or it compiles the main repo's src.
- **Feature testing goes over TLS** (https/wss). Plain http only behind a TLS-terminating proxy.
  On bare macOS libcrypto cannot be dlopen'd, so TLS/h3/EVP behaviour is proven in Docker or CI,
  not locally; a local `checkmate` pass plus the stress smoke plus green CI is the bar.
- `tests/test_sse` and `tests/test_altsvc` are tracked binaries the suite rebuilds. Before every
  commit run `git checkout -- tests/test_sse tests/test_altsvc` so they never land in a commit.
- Do not edit `.nim` files while a `nimble stress` build is running: it copies the worktree and
  compiles a torn snapshot. Fix, then build; never overlap.
- Use `$ARGUMENTS`, not memory of a previous batch. Re-read each issue from GitHub.

## 1. Resolve the issue list

1. Expand `$ARGUMENTS` into a sorted, de-duplicated list of integers. Accept space or comma
   separated numbers and inclusive ranges (`300-312`, `#300-#312`, `300..312`). Ignore a
   leading `#`.
2. For each number run `gh issue view N --json number,title,body,labels,state,comments`. Skip
   an issue that is closed or is actually a PR, and say so in the report. If nothing is left,
   stop and tell the user.
3. Save every issue's title and body to `<scratchpad>/issues/N.md`: the agents get the full
   text, not a summary.
4. Decide the branch prefix from the labels/titles: all bugs → `fix/`, all enhancements →
   `feat/`, otherwise `fix/`. Name: `<prefix>issues-<first>-<last>` for a contiguous range or
   two issues, else `<prefix>issues-<first>-to-<last>` (e.g. `fix/issues-450-454`).

## 2. Create the branch

```
git status --porcelain   # must be clean apart from the two tracked test binaries
git fetch origin && git checkout main && git pull --ff-only
git checkout -b <branch>
```

If the tree is dirty with unrelated edits, do not stash or discard them: leave them in place
untouched and proceed only if they do not overlap the files the issues touch; otherwise stop and
report the conflict.

## 3. Implement: one opus agent per issue, in scratchpad worktrees

Dispatch **one Agent per issue** (`subagent_type: claude`, `model: opus`), all in a single
message so they run concurrently, at most 6 at a time (queue the rest). Each agent works in
its own worktree under the scratchpad so the builds and test binaries never collide:

```
git worktree add <scratchpad>/wt-<N> <branch>
```

Give every agent: the issue file `<scratchpad>/issues/N.md`, its worktree path, the branch
name, the ground rules above verbatim, and this brief:

- Read the issue and every file it names. Reproduce the bug with a failing test first when the
  issue is a bug; add a covering test for a feature.
- Implement the whole issue. Nothing is deferred, no exceptions. If part of it seems to belong
  elsewhere, do it anyway.
- Validate with `checkmate` in the worktree (add `-n:-d:naviHttp3` when h3 code is touched).
  Run `nim check` for every backend/thread mode the change can affect (sync, asyncdispatch,
  chronos, js; `--threads:on` and `--threads:off`). Smoke-test any `navi/js` change under node;
  `nim check` alone is not enough there. If a test cannot run on macOS (TLS dlopen), say so
  and rely on the CI matrix; do not skip writing the test.
- Commit exactly **one commit** on top of the branch tip in the worktree with a semantic
  message ending in `(#N)`. Do not push. Do not touch the other issues' files unless the issue
  requires it; if it does, say which.
- Return: the commit sha, a three-line summary (what changed, how it was verified, any
  interpretation made), and the list of files touched.

When an agent fails or stalls, message it once with a recap via SendMessage; if there is no
answer, start a fresh agent for that issue with the same brief.

**Integrate** in ascending issue order on the main checkout:

```
git cherry-pick <sha>
```

Resolve conflicts yourself (the later issue adapts to the earlier one). After all picks:

- `git worktree remove --force <scratchpad>/wt-<N>` for each worktree.
- If any issue renamed an API or flipped a default, grep the whole tree for the old idiom; sibling
  commits written in parallel may still use it.
- Run the full `checkmate` on the integrated head, plus `nim check` for the backend/thread
  combinations touched. Fix breakage as a `fixup!` of the responsible issue commit, then
  `GIT_SEQUENCE_EDITOR=true git rebase --autosquash main` so history stays one commit per issue.

## 4. Adversarial review: opus agents, then fix

Dispatch **one review Agent per issue** (`subagent_type: claude`, `model: opus`) against the
integrated branch, concurrently. Give each the issue text, `git show <sha>` of its commit, and
`git diff main...HEAD` for context. The brief:

- You are an adversarial reviewer. Assume the change is wrong and try to prove it: missed
  acceptance criteria, deferred work, unhandled error paths, backend asymmetry (sync vs
  asyncdispatch vs chronos vs js), thread-mode gaps, TLS/h3 parity, resource leaks under churn,
  Windows behaviour, missing or non-asserting tests, docs/README/changelog not updated.
- For each finding give severity (high/medium/low), file:line, a concrete failure scenario, and
  the fix. Verify each finding by reading the code or running a test before reporting it. Do not
  report style nits.

Also dispatch **one cross-cutting reviewer** over the whole `main...HEAD` diff looking for
interactions between the issues.

Triage every finding yourself. Fix every high and medium, and every low that is cheap. Discard
only findings you can show are false with a test or a code citation, and list them in the
report. Fixes land as `fixup!` commits on the responsible issue commit (dispatch opus fix agents
for anything non-trivial, serially per issue to avoid collisions), followed by
`GIT_SEQUENCE_EDITOR=true git rebase --autosquash main`. Re-run `checkmate` on the result. If a
fix was large, run one more review pass on that commit only.

## 5. Validate with a short stress smoke

Run the five-workload smoke over every protocol and client:

```
NAVI_PROTO=all NAVI_CLIENT=all NAVI_SECONDS=10 NAVI_REPORT_SECONDS=2 nimble stress
```

- It needs Docker and builds the `navi-stress-h3` image; the first build takes minutes.
- Detach it from the Bash 10-minute cap: `nohup` double-fork it with output to
  `<scratchpad>/stress.log`, then watch with a Monitor on the log (or `docker logs` of the
  running container) for the terminal banners. Set a ScheduleWakeup (~1200s) as a fallback.
- Pass = all five `== <workload>: all cells passed ==` banners (requests, ws, sse,
  streamUpload, streamDownload) and no `FAILURES`, `mismatch`, `checksum`, `Traceback`,
  `err[1-9]`, `panic`, `assert`, `Killed` lines.
- On failure: preserve `docker cp <id>:/navi/stress-srv-logs <scratchpad>/srv-logs`, stop the
  run, root-cause it (an opus agent if non-trivial), fix as a `fixup!` of the responsible
  commit, autosquash, rerun the smoke. Loop until clean. Throughput numbers from a 10s smoke are
  contention noise; only pass/fail and error signatures matter here.
- Nimble does not propagate the container exit code: read the banners, never trust `$?`.

## 6. Open the PR

1. `git checkout -- tests/test_sse tests/test_altsvc`; confirm `git status` is clean and
   `git log --oneline main..HEAD` is exactly one commit per issue.
2. `git push -u origin <branch>`.
3. `gh pr create --base main` with:
   - Title: semantic, one line, ending with the issue span, e.g.
     `fix: h3 threads:off build, IP-literal h3 verify, sync connect budget (#450 to #454)`.
   - Body: a one-sentence lead ("Fixes the N issues …, one commit each."), then a section per
     issue with `**#N (label, severity)**` and a paragraph on what changed and how it was
     verified, then a **Validation** section (checkmate, nim check matrix, the exact stress
     command and its banners), then a **Decisions** section for interpretations you made.
   - A closing keyword **per issue, one per line**: `Closes #450`, `Closes #451`, …
     A single `Closes #a, #b` line only auto-closes the first.
   - No attribution footer of any kind.
4. Record the PR URL.

## 7. Monitor CI to green

Watch the checks without polling in the foreground:

```
gh pr checks <num> --watch --fail-fast   # in the background, or a Monitor on `gh pr checks`
```

Set a ScheduleWakeup (~600s, then ~1200s) as the fallback heartbeat. On each wake summarise the
state: pending / passed / failed per workflow (CI, http3, windows, badssl, stress-chaos, …).

On a **failure**:

1. `gh run view <run-id> --log-failed` and read the actual error, not the job name.
2. Known flakes, rerun **once** before investigating: the Windows badssl self-signed case on an
   unchanged handshake path; a Windows-only checkmate timeout when the change did not touch
   sockets (the thread test server, not the library). `gh run rerun <run-id> --failed`.
3. Anything else is real. Fix it (opus agent if non-trivial), `fixup!` the responsible commit,
   autosquash, `checkmate`, `git push --force-with-lease`, and go back to watching.
4. Loop until every required check is green. Do not merge; the user merges.

## 8. Final report

Print a markdown report the user can read cold:

- **Outcome**: PR URL, branch, and whether CI is fully green (list any check still pending or
  red, with the reason).
- **Issues**: a table with issue, commit sha, one-line change, how it was verified.
- **Review**: findings fixed (issue, severity, one line each) and findings discarded with why.
- **Stress**: the exact command, pass/fail, iterations, and any fix it forced.
- **CI**: runs, reruns for flakes, fixes pushed.
- **Decisions**: every judgement call made while the user was afk, one line each.
- **Left undone**: must be empty; if anything is not, explain exactly why it was impossible.
