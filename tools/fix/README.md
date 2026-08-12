# FIX validator tooling

`tools/fix/fix-validate.mjs` is a standalone FIX 4.4 tag=value validator for the opt-in FIX Conflated sandbox channel. It opens a real TCP session, performs `Logon`, consumes the automatic `MarketDataSnapshotFullRefresh` plus later `MarketDataIncrementalRefresh` messages, rebuilds the book locally, and periodically compares that state with `GET /book/{symbol}` from the HTTP side that `tools/ws/ws-validate.mjs` already expects.

## Prerequisites

- Enable the FIX sandbox listener:
  - `UMDF_FIX_CONFLATED_ENABLED=true`
  - `UMDF_FIX_CONFLATED_PORT=<tcp-port>`
- Start the console app with an HTTP/WebSocket port as well (for example `--ws-port 8080`) so the validator can query `HTTP_BASE/book/{symbol}`.
- For replay-driven runs, fetch sample PCAPs with `./tools/pcap/download-pcaps.sh` and start `src/B3.Umdf.ConsoleApp` with one or more `--pcap-prefix` values.

## Usage

```bash
HTTP_BASE=http://localhost:8080 node tools/fix/fix-validate.mjs <host> <fix-port> [symbol] [http-base]
```

Examples:

```bash
HTTP_BASE=http://localhost:8080 node tools/fix/fix-validate.mjs localhost 9200 PETR4
node tools/fix/fix-validate.mjs localhost 9200 PETR4 http://localhost:8080
node tools/fix/fix-validate.mjs localhost 9200
```

If you omit `symbol`, the validator adopts the first snapshot symbol it sees.
That is useful for quick smoke runs when you do not yet know which symbols are
present in the current replay.

Useful environment overrides:

- `FIX_SENDER_COMP_ID` / `FIX_TARGET_COMP_ID` — override the default CompIDs. The script defaults to a unique sender ID per run so repeated manual runs do not trip the sandbox reconnect rule for stale `MsgSeqNum` values.
- `FIX_HEARTBEAT_SEC` — requested heartbeat interval (default `30`).
- `CHECK_INTERVAL_MS` — `/book` comparison interval (default `5000`).
- `RUN_SECONDS` — optional auto-stop timer for short validation runs.

If `GET /book/{symbol}` is unavailable (for example it returns `404` in the
current local host setup), the validator keeps running and logs the server-check
as unavailable while still validating the FIX session, snapshot parsing, and
incremental book reconstruction path.

## Example local replay

```bash
./tools/pcap/download-pcaps.sh
UMDF_FIX_CONFLATED_ENABLED=true UMDF_FIX_CONFLATED_PORT=9200 \
  dotnet run --project src/B3.Umdf.ConsoleApp -- \
  --pcap-prefix pcap/20250331_MBO_084_EQT --ws-port 8080 --speed 1
HTTP_BASE=http://localhost:8080 node tools/fix/fix-validate.mjs localhost 9200 PETR4
```

This is the standalone client piece for issue #103 item 1. A later follow-up can wrap it together with a live replay harness and the existing WebSocket validators.
