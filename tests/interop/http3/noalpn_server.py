"""A non-compliant QUIC listener for the #445 ALPN gate test.

It completes a full QUIC/TLS 1.3 handshake and then selects no ALPN protocol at
all: aioquic negotiates ALPN only when `alpn_protocols` is configured, so with it
left unset the EncryptedExtensions message carries no ALPN extension at all
(aioquic tls.py: `if self._alpn_protocols is not None`). That is precisely the
peer #445 is about, and neither Caddy (which always selects h3) nor OpenSSL's own
QUIC server (the compliant side of this exchange) can play the part. Neither
ngtcp2's crypto_ossl binding nor OpenSSL's third-party QUIC TLS interface rejects
such a server, so without navi's own check the connection looks like a usable h3
peer.

Every connection teardown is logged as `terminated error_code=0x...`, so the
caller can also assert that navi closed with crypto_error(no_application_protocol)
(0x178) rather than a clean NO_ERROR.

Usage: noalpn_server.py <certfile> <keyfile> <port>
Prints "listening" on stdout once the socket is bound, so the caller can wait for
it without sleeping.
"""

import asyncio
import sys

from aioquic.asyncio import QuicConnectionProtocol, serve
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, QuicEvent


class LoggingProtocol(QuicConnectionProtocol):
    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, ConnectionTerminated):
            print(
                "terminated error_code=0x%x reason=%r"
                % (event.error_code, event.reason_phrase),
                flush=True,
            )


async def main(certfile: str, keyfile: str, port: int) -> None:
    config = QuicConfiguration(is_client=False, alpn_protocols=None)
    config.load_cert_chain(certfile, keyfile)
    await serve("127.0.0.1", port, configuration=config,
                create_protocol=LoggingProtocol)
    print("listening", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3])))
