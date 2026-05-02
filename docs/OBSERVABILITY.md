# Observability — `/metrics` (Prometheus) reference

`B3MarketDataPlatform` exposes a Prometheus scrape endpoint on the same
HTTP port as the health probes (`/health`, `/ready`, `/live`, default
port `8080`):

```
GET /metrics
```

Content type: `text/plain; version=0.0.4` (OpenMetrics exposition).

This is the canonical observability surface for the family stack
(otel-collector → prometheus → grafana). There is no OTLP push exporter
yet — pull is what the family converged on (B3TradingPlatform issue #9 /
PR 7-2c). An OTLP exporter can be added behind a config flag without
breaking the pull endpoint.

## Pipeline

The host registers an OpenTelemetry `MeterProvider` with the Prometheus
ASP.NET Core exporter and exposes:

- The **`B3.Umdf`** meter (defined by
  [`MetricsRegistry`](../src/B3.Umdf.Server/MetricsRegistry.cs)) — WebSocket
  / broadcast-pool counters and gauges that live inside the `B3.Umdf.Server`
  library itself.
- The **`B3.Umdf.Consumer`** meter (defined by
  [`MetricsBinder`](../src/B3.Umdf.ConsoleApp/MetricsBinder.cs)) — feed,
  book, transport, and per-symbol observability registered by the
  ConsoleApp at startup.

Both names are wired in `Program.cs` via
`WebSocketHost.AdditionalMeterNames`. Anything created on those two
`Meter`s is automatically scraped — no per-metric registration on the
host side.

> Names are emitted in Prometheus form: dots become underscores
> (`umdf.ws.messages.sent` → `umdf_ws_messages_sent`).

## Metric catalogue

The mapping below is non-exhaustive; the source files above are the
ground truth. Every metric here is currently emitted on `/metrics`.

### WebSocket / broadcast pool (`B3.Umdf` meter)

| Prometheus name | Type | Labels | Semantics |
|---|---|---|---|
| `umdf_ws_connections_active` | UpDownCounter | — | Live WebSocket subscribers (incremented on connect, decremented on disconnect). |
| `umdf_ws_subscribed_symbols` | Gauge | — | Distinct securities with at least one active subscriber (`SubscriptionManager.ActiveSymbolCount`). |
| `umdf_ws_subscriptions_total` | Counter | — | Cumulative `Subscribe` accepts (negative deltas applied on bulk disconnect; treat as activity not a level). |
| `umdf_ws_messages_sent_total` | Counter | — | Total frames pushed to clients across all sessions. |
| `umdf_ws_messages_conflated_total` | Counter | — | Frames collapsed by per-symbol conflation before send. |
| `umdf_ws_slow_disconnects_total` | Counter | — | Sessions terminated for back-pressure violation (1008 close, slow-consumer policy). |
| `umdf_packets_received_total` | Counter | — | Raw UMDF packets observed by the server-side counters. |
| `umdf_gaps_detected_total` | Counter | — | Sequence gaps observed (server-local view). |
| `umdf_parse_errors_total` | Counter | — | SBE decode failures. |
| `umdf_orders_processed_total` | Counter | — | OrderAdded/Updated applied. |
| `umdf_trades_processed_total` | Counter | — | Trades applied. |
| `umdf_deletes_processed_total` | Counter | — | OrderDeleted applied. |
| `umdf_broadcast_pool_rent_hits_total` | Counter | — | `BroadcastBufferPool` rents that hit the pool. |
| `umdf_broadcast_pool_rent_misses_total` | Counter | — | Rents that fell through to a fresh allocation. |
| `umdf_broadcast_pool_return_drops_total` | Counter | — | Returned buffers dropped (pool full / wrong size). |
| `umdf_broadcast_pool_oversize_rents_total` | Counter | — | Rents above the pool's max bucket size. |

### Feed (`B3.Umdf.Consumer` meter)

| Prometheus name | Type | Labels | Semantics |
|---|---|---|---|
| `b3_umdf_feed_state` | Gauge | `group` | `0`=`WaitInstrumentDefinition`, `1`=`Streaming`. |
| `b3_umdf_feed_packets_total` | Counter | `group` | Packets fed into the channel handler. |
| `b3_umdf_feed_duplicates_total` | Counter | `group` | Duplicate packets skipped. |
| `b3_umdf_feed_gaps_total` | Counter | `group` | Channel-level sequence gaps detected. |
| `b3_umdf_feed_instrument_definitions_total` | Counter | `group` | Instrument definitions received. |
| `b3_umdf_feed_reorder_hits_total` | Counter | `group` | Out-of-order packets later drained from the A/B reorder buffer (avoided spurious recovery). |
| `b3_umdf_feed_reorder_buffer_depth` | Gauge | `group` | Current depth of the A/B reorder buffer. |
| `b3_umdf_feed_channel_gaps_absorbed_total` | Counter | `group` | Channel-level gaps absorbed without exiting `Streaming`. |
| `b3_umdf_feed_last_packet_age` | Gauge (ms) | `group` | Milliseconds since the last packet was dispatched (`-1` if none yet). Operational liveness signal. |
| `b3_umdf_feed_ring_depth` | Gauge | `group` | Pending packets in the per-group MPSC dispatch ring. |
| `b3_umdf_feed_ring_dropped_total` | Counter | `group` | Packets dropped on per-group ring overflow. |
| `b3_umdf_feed_ring_dropped_by_channel_total` | Counter | `group`, `channel` | Per-channel breakdown of the drops above. |

### Transport (multicast)

| Prometheus name | Type | Labels | Semantics |
|---|---|---|---|
| `b3_umdf_transport_recvmmsg_syscalls_total` | Counter | `group`, `channel` | `recvmmsg(2)` syscalls returning ≥ 1 datagram. |
| `b3_umdf_transport_recvmmsg_datagrams_total` | Counter | `group`, `channel` | Datagrams received in batched receive. Average batch = datagrams / syscalls. |
| `b3_umdf_transport_membership_joins_total` | Counter | `group`, `channel` | IGMP joins (initial + recovery rejoins). |
| `b3_umdf_transport_membership_leaves_total` | Counter | `group`, `channel` | IGMP leaves (issued when group enters RealTime). |
| `b3_umdf_transport_membership_joined` | Gauge | `group`, `channel` | `1` if currently joined, else `0`. |

### Book

| Prometheus name | Type | Labels | Semantics |
|---|---|---|---|
| `b3_umdf_book_orders_added_total` | Counter | `group` | Orders added to books. |
| `b3_umdf_book_orders_updated_total` | Counter | `group` | Order updates applied. |
| `b3_umdf_book_orders_deleted_total` | Counter | `group` | Orders deleted from books. |
| `b3_umdf_book_trades_total` | Counter | `group` | Trades applied to books. |
| `b3_umdf_book_parse_errors_total` | Counter | `group` | SBE parse errors during book maintenance. |
| `b3_umdf_book_crossings_total` | Counter | `group` | Cross detections raised. |
| `b3_umdf_book_currently_crossed` | Gauge | `group` | Symbols currently crossed (continuous trading). |
| `b3_umdf_book_currently_crossed_auction` | Gauge | `group` | Symbols currently crossed in auction (expected). |
| `b3_umdf_book_currently_locked` | Gauge | `group` | Symbols currently locked (bid==ask). |
| `b3_umdf_book_delete_not_found_total` | Counter | `group` | Deletes for unknown orders (after gap healing). |
| `b3_umdf_book_instruments_replaced_total` | Counter | `group` | InstrumentDefinition replacements applied. |
| `b3_umdf_book_null_price_skips_total` | Counter | `group` | Null-price events skipped. |
| `b3_umdf_book_market_order_*` | Counter | `group` | Market-order lifecycle counters (adds/updates/deletes/transitions). |

### Per-symbol bootstrap / recovery

| Prometheus name | Type | Labels | Semantics |
|---|---|---|---|
| `b3_umdf_persymbol_stale_symbols` | Gauge | `group` | Symbols currently in `Stale` (waiting for a heal snapshot). |
| `b3_umdf_persymbol_tracked_symbols` | Gauge | `group` | Symbols tracked by the per-symbol registry. |
| `b3_umdf_persymbol_lagging_snapshots_total` | Counter | `group` | Snapshot frames behind the highest-seen incremental. |
| `b3_umdf_persymbol_snapshots_healed_total` | Counter | `group` | Snapshots that healed a `Stale` symbol. |
| `b3_umdf_persymbol_snapshots_authoritative_reset_total` | Counter | `group` | Forced healing despite missing intermediate sequences. |
| `b3_umdf_persymbol_authoritative_reset_unsafe_delta_*` | Gauge / Counter | `group` | Severity gauges / sums of forced-heal `(MinHeal − snap)` deltas. Use **max/sum** for alerting; **last** hides spikes between scrapes. |
| `b3_umdf_persymbol_authoritative_reset_discarded_tail_delta_*` | Gauge / Counter | `group` | Same shape, for the discarded-tail delta `(highWater − snap)`. |
| `b3_umdf_symbol_gap_*` | Counter / Gauge | `group`, `kind` | Per-symbol gap detection (count, total size, currently-affected). |

## Hot-path impact

Every counter incremented inside the receive / dispatch / broadcast loops
is a `System.Diagnostics.Metrics.Counter<T>.Add(...)` — an interlocked
update. Gauges are uniformly `ObservableGauge`, evaluated only on scrape.
There is no string formatting, no allocation, and no `IsEnabled` check
on the hot path; the OpenTelemetry SDK collects from the Meter listener
side, separate from the producer.

## Cardinality

- `group` is bounded by the configured channel-group set (typically
  single digits).
- `channel` is the `ChannelType` enum (Inc A, Inc B, Snap A, Snap B,
  InstrDef) — fixed.
- **No `symbol` label is emitted on counters or gauges.** Per-symbol
  detail belongs in the recovery audit endpoint
  (`GET /api/recovery/recent`) and in logs, not in Prometheus.

## Adding a meter

If a new module wants to publish via `/metrics`, either:

1. Reuse the `B3.Umdf` meter
   (`MetricsRegistry.Meter.CreateCounter<long>(...)`) — picked up
   automatically by the host.
2. Or add the meter name to `wsHost.AdditionalMeterNames` before
   `StartAsync` (see `Program.cs` for the pattern).

Mutating `AdditionalMeterNames` after `StartAsync` has no effect — the
`MeterProvider` is built once at host start.
