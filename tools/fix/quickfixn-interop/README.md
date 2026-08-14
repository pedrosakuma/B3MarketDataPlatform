# QuickFIX/n interop check

This is a small, independent third-party-engine validation harness for the
FIX Conflated sandbox channel (`docs/FIX-CONFLATED-SANDBOX.md`). It exists to
answer a specific question that `tools/fix/fix-validate.mjs` alone cannot:
**is the sandbox's FIX 4.4 session/encoding actually parseable by a real,
independent, battle-tested FIX engine — not just by our own bespoke
validator?** `fix-validate.mjs` is written by/for this repo, so a clean run
of it proves internal self-consistency, not genuine interoperability.

[QuickFIX/n](https://github.com/connamara/quickfixn) is a mature, widely used
open-source FIX engine. Pointing it at this sandbox (using the vendored
`schemas/fix-conflated/FIX44_UMDFConflated.xml` data dictionary) gives an
outside, non-self-referential check that Logon/session semantics and
application message shapes (`W`, `X`, `f`, `B`, …) are genuinely well-formed
FIX 4.4, not just "well-formed enough for our own parser to agree with
itself."

**Important scope note:** this only validates session/encoding/parseability
against a generic FIX engine — it does **not** validate content-level
fidelity against the real B3 UMDF Conflated product (tag values, book
semantics, conflation behavior). That level of validation is covered
separately by the message-content comparison against real B3 sample captures
documented in `docs/FIX-CONFLATED-SANDBOX.md`, and ultimately only B3's own
(proprietary, non-public) certification script can fully validate production
fidelity.

## Why a proxy is needed

The sandbox wraps its entire outbound (server→client) byte stream in a
single continuous RFC 1950 ZLIB stream from the moment a client connects —
see `src/B3.Umdf.FixConflated/FixZlibCompression.cs` and
`FixTcpClientSession.cs`. This always-on transport compression matches the
real B3 product, but no generic FIX engine (including QuickFIX/n) understands
it out of the box; QuickFIX/n expects plain SOH-delimited FIX bytes directly
on the socket.

`zlib-proxy.mjs` is a small transparent TCP proxy that inflates only the
server→client leg (the sandbox never compresses the client→server leg) before
handing bytes to QuickFIX/n, and passes the client→server leg straight
through unmodified. QuickFIX/n itself never needs to know the sandbox
transport is compressed.

## Prerequisites

- A running FIX Conflated sandbox server: `UMDF_FIX_CONFLATED_ENABLED=true`
  and `UMDF_FIX_CONFLATED_PORT=<port>` when starting `src/B3.Umdf.ConsoleApp`
  (see `docs/FIX-CONFLATED-SANDBOX.md` → "Enabling").
- Node.js (for `zlib-proxy.mjs`).
- .NET 10 SDK (already required by this repo) to build/run
  `QuickFixInteropCheck`, which restores the maintained
  [`QuickFIXn.Core`](https://www.nuget.org/packages/QuickFIXn.Core) NuGet
  package (not the stale 2016 `quickfixn` package — see "Gotcha" below).

## Usage

1. Start the sandbox server with the FIX listener enabled, for example:

   ```bash
   UMDF_FIX_CONFLATED_ENABLED=true UMDF_FIX_CONFLATED_PORT=19400 \
     dotnet run --project src/B3.Umdf.ConsoleApp -- \
     --pcap-prefix pcap/20250331_MBO_084_EQT --ws-port 18200 --speed 1
   ```

2. Start the zlib-inflating proxy in front of the real FIX port, on a
   separate local port (`19401` here, matching the default
   `quickfixn-session.cfg`):

   ```bash
   node tools/fix/quickfixn-interop/zlib-proxy.mjs 19401 127.0.0.1 19400
   ```

3. Run the QuickFIX/n interop check, pointed at the proxy port via
   `quickfixn-session.cfg`:

   ```bash
   cd tools/fix/quickfixn-interop/QuickFixInteropCheck
   dotnet run -- quickfixn-session.cfg
   ```

4. Watch the console output. A successful run logs `OnLogon`, followed by
   `FromApp` lines for every `SecurityStatus (f)` / `MarketDataSnapshotFullRefresh (W)`
   / `MarketDataIncrementalRefresh (X)` / `News (B)` message the server sends,
   with no `MessageParseError` and no unexpected `OnLogout`. Press Enter (or
   Ctrl+C) to stop.

If you want to also exercise the per-session subscription model added by
issue #116 (`MarketDataRequest` / `V`), that currently requires the
tooling-side request to be sent explicitly — QuickFIX/n's `SocketInitiator`
alone does not do this. `tools/fix/fix-validate.mjs` (with `FIX_SECURITY_ID`
set) remains the right tool for that specific flow; this interop check is
intentionally scoped to logon + legacy full-broadcast session validation
against a real third-party engine.

## Gotcha: use `QuickFIXn.Core`, not `quickfixn`

The original `quickfixn` NuGet package (last published in 2016, targeting
old .NET Framework versions only) fails with a `MessageParseError` /
`FormatException` when run under the modern .NET compat shim used by this
repo — it mis-parses the very first byte of the `BeginString` field. This is
a bug in that stale package under a modern runtime, **not** a bug in the
sandbox's FIX output (confirmed independently via a raw Python socket probe
that showed the sandbox's bytes are well-formed FIX 4.4). Use the maintained
fork's package, `QuickFIXn.Core` (already referenced by
`QuickFixInteropCheck.csproj`), which targets `net8.0`/`net10.0` natively and
has no such issue.

## Isolation from the main solution

`QuickFixInteropCheck.csproj` is a standalone project, not referenced by (or
a dependency of) anything under `src/` or `tests/`, and this repo has no
top-level `.sln` tying projects together implicitly. It will never be built
or restored as part of the normal `dotnet build`/`dotnet test` flow for the
main platform — you must `dotnet build`/`dotnet run` inside this directory
explicitly.
