#!/usr/bin/env node
// Transparent TCP proxy that inflates the FIX Conflated sandbox's single
// continuous RFC 1950 ZLIB server->client stream, so that a generic FIX
// engine with no knowledge of this sandbox's transport compression (e.g.
// QuickFIX/n) can connect to what looks like a plain, uncompressed FIX
// session. See README.md in this directory for the full rationale and usage.
//
// Client -> server bytes are NOT touched: the sandbox only compresses
// outbound/server->client bytes (see src/B3.Umdf.FixConflated/FixZlibCompression.cs
// and FixTcpClientSession.cs), so this proxy only needs to inflate one leg.
//
// Usage: node zlib-proxy.mjs <listenPort> <upstreamHost> <upstreamPort>

import net from 'node:net';
import zlib from 'node:zlib';

const [, , listenPortArg, upstreamHost, upstreamPortArg] = process.argv;
if (!listenPortArg || !upstreamHost || !upstreamPortArg) {
  console.error('Usage: node zlib-proxy.mjs <listenPort> <upstreamHost> <upstreamPort>');
  process.exit(1);
}

const listenPort = Number(listenPortArg);
const upstreamPort = Number(upstreamPortArg);

const server = net.createServer((downstream) => {
  console.log(`[proxy] downstream connected from ${downstream.remoteAddress}:${downstream.remotePort}`);

  const upstream = net.connect(upstreamPort, upstreamHost, () => {
    console.log(`[proxy] connected upstream ${upstreamHost}:${upstreamPort}`);
  });

  const inflate = zlib.createInflate();
  inflate.on('error', (err) => {
    console.error('[proxy] inflate error', err.message);
    downstream.destroy();
    upstream.destroy();
  });

  // upstream (server, compressed) -> inflate -> downstream (plain FIX client)
  upstream.pipe(inflate).pipe(downstream);

  // downstream (client, plain) -> upstream (server, plain) — no compression
  // on this leg, matching the sandbox's actual transport behavior.
  downstream.pipe(upstream);

  const cleanup = (who) => (err) => {
    if (err) console.error(`[proxy] ${who} error`, err.message);
    downstream.destroy();
    upstream.destroy();
  };

  downstream.on('error', cleanup('downstream'));
  upstream.on('error', cleanup('upstream'));
  downstream.on('close', () => { console.log('[proxy] downstream closed'); upstream.destroy(); });
  upstream.on('close', () => { console.log('[proxy] upstream closed'); downstream.destroy(); });
});

server.listen(listenPort, () => {
  console.log(`[proxy] listening on ${listenPort}, forwarding to ${upstreamHost}:${upstreamPort} (inflating server->client leg)`);
});
