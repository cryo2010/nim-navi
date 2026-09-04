# `include`d by a client AFTER its `import navi[/backend]`, so HttpVersion's
# H1/H2/H3 are in scope. Maps the NAVI_PROTO label to navi's version set.
# Not a standalone module.

proc httpVersions(proto: string): set[HttpVersion] =
  ## Pin exactly the protocol the cell exists to exercise, so a silent up/downgrade
  ## is rejected by strict mode (and by checkVersion). h3 is still reached via an
  ## Alt-Svc discovery leg -- navi exempts that bootstrap request from the {H3} pin.
  case proto
  of "h1": {H1}
  of "h2": {H2}             # ALPN must negotiate h2; no h1 fallback allowed
  of "h3": {H3}            # h3 only; the discovery leg (h2/h1) is exempted internally
  else: {H2}
