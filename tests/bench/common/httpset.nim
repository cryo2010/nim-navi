# `include`d by a client AFTER its `import navi[/backend]`, so HttpVersion's
# H1/H2/H3 are in scope. Maps the NAVI_PROTO label to navi's version set.
# Not a standalone module.

proc httpVersions(proto: string): set[HttpVersion] =
  case proto
  of "h1": {H1}
  of "h2": {H1, H2}          # ALPN negotiates h2 on TLS
  of "h3": {H1, H2, H3}      # h3 opt-in, discovered via Alt-Svc
  else: {H1, H2}
