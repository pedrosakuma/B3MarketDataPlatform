# Configuration

All knobs exposed by the consumer: CLI flags, environment variables, the
`appsettings.json` / multicast JSON files, and the host-level kernel
tuning required for reliable UDP ingest.

Precedence (highest wins): **CLI > environment variable > JSON config > built-in default**.

## CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--pcap-prefix <path>` | — | PCAP file prefix (repeatable for multi-channel). Auto-discovers 4 files per prefix |
| `--multicast-config <file>` | — | JSON config with multicast group addresses/ports for live UDP or replay-to-multicast publishing |
| `--replay-to-multicast` | `false` | Publish replayed PCAP payloads to multicast instead of consuming them in-process |
| `--ws-port <port>` | *(off)* | Start WebSocket subscription server on the given port |
| `--speed <mult>` | `0` | Replay speed: `0` = max, `1` = real-time, `5` = 5× accelerated |

Positional arguments (4 PCAP file paths) are also supported for single-channel backward compatibility.

`--pcap-prefix` expands to `{prefix}_Incremental_FeedA.pcap`, `_Incremental_FeedB.pcap`, `_InstrumentDefinition.pcap`, `_SnapshotRecovery.pcap`.

## Environment Variables

The full set of `UMDF_*` knobs surfaced by `AppSettings` and `Program.cs`:

| Environment Variable | CLI | Default | Description |
|---------------------|-----|---------|-------------|
| `UMDF_WS_PORT` | `--ws-port` | *(off)* | WebSocket server port |
| `UMDF_SPEED` | `--speed` | `0` | Replay speed multiplier |
| `UMDF_REPLAY_TO_MULTICAST` | `--replay-to-multicast` | `false` | Publish replayed PCAP payloads to multicast instead of consuming them in-process |
| `UMDF_MULTICAST_CONFIG` | `--multicast-config` | — | Multicast JSON config path |
| `UMDF_LOG_LEVEL` | — | `Information` | Minimum log level |
| `UMDF_SHUTDOWN_DRAIN_SECONDS` | — | `5` | Graceful shutdown drain timeout |
| **WebSocket / clients** | | | |
| `UMDF_MAX_CONNECTIONS` | — | `0` (unlimited) | Max concurrent WebSocket connections |
| `UMDF_CLIENT_CHANNEL_CAPACITY` | — | `4096` | Per-client outbound queue size (msgs) |
| `UMDF_CLIENT_MAX_PENDING_BYTES` | — | `4194304` | Per-client outbound hard byte cap; client is disconnected as slow consumer when exceeded |
| `UMDF_CLIENT_COALESCE_WINDOW_MS` | — | `10` | Per-client outbound coalesce window; trades a few ms of latency for fewer syscalls under high client counts |
| `UMDF_SLOW_CLIENT_THRESHOLD` | — | `0.75` | Fraction of queue capacity considered congested |
| `UMDF_SLOW_CLIENT_MAX_TICKS` | — | `100` | Consecutive congested write cycles before disconnect |
| `UMDF_CLIENT_OUTLIER_INTERVAL_MS` | — | `1000` | Outlier-sweep period; `0` disables sweep |
| `UMDF_CLIENT_OUTLIER_PRESSURE_PCT` | — | `0.50` | Aggregate-pressure gate (Σpending / (clients × maxPending)) below which the sweep is a no-op |
| `UMDF_CLIENT_OUTLIER_MULTIPLIER` | — | `4.0` | Disconnect threshold = `max(median × multiplier, minBytes)` |
| `UMDF_CLIENT_OUTLIER_MIN_BYTES` | — | `262144` | Floor on the outlier disconnect threshold |
| `UMDF_MAX_SNAPSHOT_REQUESTS_PER_BATCH` | — | `32` | Cap on Book snapshot requests serviced by the dispatch thread per packet (paces connect storms) |
| **Feed / transport** | | | |
| `UMDF_MULTICAST_MERGE_CAPACITY` | — | `1000000` | Capacity of the shared live-UDP merge queue |
| `UMDF_FEED_CHANNEL_CAPACITY` | — | `250000` | Capacity of each per-group feed queue behind the dispatcher |
| `UMDF_INCREMENTAL_RECOVERY_QUEUE_CAPACITY` | — | `200000` | Per-group cap on incrementals retained during a snapshot cycle (drop-oldest on overflow) |
| `UMDF_GROUP_RING_CAPACITY` | — | `65536` | Per-group MPSC dispatch ring capacity (drop-newest on overflow) |

Docker-specific helpers consumed by `docker-entrypoint.sh`:

| Variable | Default | Description |
|----------|---------|-------------|
| `PCAP_PREFIX` | `20250331_MBO_084_EQT,20250929_MBO_072_DRV` | Comma-separated PCAP prefixes (multi-channel) |
| `WS_PORT` | `8080` | WebSocket server port |
| `REPLAY_SPEED` | `2` | Replay speed multiplier |
| `FRONTEND_PORT` | `3000` | Frontend HTTP port |
| `MULTICAST_CONFIG_FILE` | `multicast-compose.json` | Config file under `/app/config/` shared by publisher and consumer |

## Multicast JSON Config

Used by both the live-UDP consumer (`--multicast-config`) and the
publisher (`--replay-to-multicast --multicast-config`).

```json
{
  "channelGroups": [
    {
      "name": "EQT",
      "channels": [
        { "channelId": 84, "type": "IncrementalA", "multicastGroup": "224.0.20.84", "port": 30084 },
        { "channelId": 84, "type": "IncrementalB", "multicastGroup": "224.0.20.85", "port": 30085 },
        { "channelId": 84, "type": "InstrumentDefinition", "multicastGroup": "224.0.20.86", "port": 30086 },
        { "channelId": 84, "type": "SnapshotRecovery", "multicastGroup": "224.0.20.87", "port": 30087 }
      ]
    },
    {
      "name": "DRV",
      "channels": [
        {
          "channelId": 72,
          "type": "IncrementalA",
          "multicastGroup": "224.0.20.72",
          "port": 30072,
          "sourceAddress": "10.0.0.1",
          "localAddress": "10.0.0.10",
          "receiveBufferBytes": 16777216
        }
      ]
    }
  ]
}
```

Optional channel fields:

- `sourceAddress` — enables source-specific multicast (SSM), receiving only from the given sender IP.
- `localAddress` — selects the local NIC/IP used for the multicast membership join (ASM or SSM).
- `receiveBufferBytes` — per-socket UDP receive buffer size. When omitted, the consumer picks per-channel-type defaults: **16 MiB** for `IncrementalA`/`IncrementalB`, **8 MiB** for `SnapshotRecovery`, **2 MiB** for `InstrumentDefinition`. All values are still capped by `net.core.rmem_max`.

When `--replay-to-multicast` is enabled, the same JSON is reused as the publish map:

- `multicastGroup` + `port` become the destination endpoint.
- `localAddress` becomes the optional local bind/interface for outgoing multicast.
- `sourceAddress` is ignored in publisher mode.
- `channelGroups` count/order must match the replay input order so `groupId → multicast route` stays deterministic.

> **Note on `receiveSocketCount`** — exists in `ChannelEntryConfig` to bind multiple sockets per channel via `SO_REUSEPORT`. On **Linux multicast** every bound socket receives a *copy* of each datagram (REUSEPORT only load-balances unicast). Replicating sockets multiplies CPU cost without enlarging the effective kernel buffer. Leave at `1` for multicast and rely on `rmem_max` instead.

## Required host kernel tuning (Linux / WSL2 / Docker)

`net.core.rmem_max` is **not** network-namespaced on most kernels and **cannot be raised from inside a container**. If `rmem_max` on the host is the Linux default (~208 KiB), the kernel will silently clamp the requested 16 MiB receive buffer down to ~208 KiB and **packet loss becomes inevitable** under burst (e.g. market open). Symptoms: persistent feed gaps, recoveries on every burst, crossed books.

Raise on the **host** (not inside the container):

```bash
sudo sysctl -w net.core.rmem_max=67108864
sudo sysctl -w net.core.rmem_default=16777216
# persist:
echo 'net.core.rmem_max=67108864'      | sudo tee /etc/sysctl.d/99-umdf.conf
echo 'net.core.rmem_default=16777216' | sudo tee -a /etc/sysctl.d/99-umdf.conf
sudo sysctl -p /etc/sysctl.d/99-umdf.conf
```

On **WSL2**, the same `sysctl` works at runtime but does not persist across `wsl --shutdown`. To persist, enable systemd in `/etc/wsl.conf` and add the same `/etc/sysctl.d/99-umdf.conf` inside the distro, or use a `[boot] command = ...` line in `/etc/wsl.conf`.

After raising `rmem_max`, the consumer log line should read `recvBuffer=16777216` (or higher) without the `UDP receive buffer was clamped` warning.

## Replay speed range

`REPLAY_SPEED` (or `--speed`) controls the publisher's pacing; the consumer is rate-agnostic by design.

| Speed | Behavior | Status |
|-------|----------|--------|
| `1`–`5` | Production-like throughput, headroom on CPU and SO_RCVBUF | ✅ supported |
| `0` (max) | Publisher floods at line rate; saturates `SO_RCVBUF` and triggers continuous kernel UDP drops → recovery cycles | ⚠️ artificial — not a real-world load profile |

`REPLAY_SPEED=0` is intentionally not a target: at line rate the publisher overruns `SO_RCVBUF` (32 MiB ≈ 21 K packets) before the consumer can drain, and kernel UDP losses dominate. Real B3 feeds are paced by the matching engine. For high-throughput stress prefer `REPLAY_SPEED=2`–`5` with appropriately sized SO_RCVBUF.
