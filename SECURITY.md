# Security Policy

navi takes security seriously. This document explains how to report a
vulnerability. For what navi defends against and how those defenses are verified,
see [THREAT_MODEL.md](THREAT_MODEL.md); for configuring navi beyond its secure
defaults, see [HARDENING.md](HARDENING.md).

## Reporting a vulnerability

**Please report suspected vulnerabilities privately.** Do not open a public
issue or pull request for a suspected security problem.

Use GitHub's private reporting: on the repository, go to the **Security and quality** tab and
click **New draft security advisory** (this opens a private security advisory visible
only to you and the maintainers). GitHub's guide is
[here](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability).

### What to include

A good report lets us reproduce the issue quickly. Please include:

- the navi version (or commit) and the affected backend(s) (`sync`,
  `asyncdispatch`, `chronos`, `js`) plus any relevant build flags (for example
  `-d:naviHttp3`);
- a minimal reproduction or proof of concept: a short program, a malformed input,
  or a server that elicits the behavior;
- the impact you believe it has (what an attacker gains).

## What to expect

- **Acknowledgement** within three business days.
- **Triage and assessment**, working with you to confirm the issue and its
  severity. We may ask for more detail.
- **A fix developed under embargo**, then a coordinated release.
- **Coordinated disclosure**: once a fixed release is available, we publish a
  GitHub Security Advisory (requesting a CVE where warranted) and credit you
  unless you prefer to remain anonymous.

We ask that you give us a reasonable opportunity to release a fix before any
public disclosure.

## Supported versions

navi is pre-1.0. Only the latest release and the `main` branch are supported;
fixes ship in a new release rather than being backported.

## Scope

**In scope:** vulnerabilities in navi's own code, for example a TLS verification
bypass, a parser flaw reachable from server-controlled bytes (HTTP/1.1, HTTP/2
frames or HPACK, HTTP/3), credential leakage across an origin boundary, or a
denial of service in navi's bounded parsing paths.

**Out of scope** (still welcome as regular issues where they apply, but not
handled as navi vulnerabilities):

- Vulnerabilities in dependencies (ngtcp2/nghttp3, BearSSL/chronos).
  Report those to the respective upstream projects; we will pick up the fixed
  release.
- Application misuse such as SSRF (navi connects wherever it is told; validating
  destinations is the application's responsibility) or mishandled secrets. See
  the application-responsibilities section of [THREAT_MODEL.md](THREAT_MODEL.md).
- Denial of service that results from a deliberately unbounded configuration
  (for example leaving `maxResponseBytes` at `0` or timeouts off when talking to
  untrusted servers). [HARDENING.md](HARDENING.md) documents how to bound these;
  choosing not to is a configuration decision, not a navi bug.
