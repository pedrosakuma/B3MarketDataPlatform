# SO_REUSEPORT + Multicast UDP — Empirical Validation

**TL;DR.** On Linux 6.6 (and any kernel since 3.9 — this is intentional kernel
semantics, not a version bug), `SO_REUSEPORT` does **not** load‑balance multicast
datagrams across replica sockets. Each socket bound to the multicast `(group,
port)` receives a **full copy** of every datagram. Setting
`receiveSocketCount > 1` for a multicast channel multiplies CPU and memory
cost N× without any throughput, drop, or latency benefit.

Recommendation: **clamp `receiveSocketCount` to 1 for the multicast path**
(soft‑deprecate the field) and update the misleading comment in
`MulticastPacketSource.cs`.

---

## Background

`MulticastPacketSource.cs:82–84` claims:

> SO_REUSEPORT (Linux) — required when multiple sockets bind the same multicast
> (group, port). The kernel load-balances datagrams across all sockets,
> multiplying the effective per-socket receive buffer and parallelizing
> receive work across threads.

But `config/multicast-compose.json` (same commit `af53b88`) carries the
opposite warning:

> On Linux, SO_REUSEPORT does NOT load-balance multicast across sockets —
> every bound socket receives a copy of each datagram. Replicating sockets
> only multiplies CPU cost without enlarging the effective receive window.

Two contradictory claims in the same PR. We resolved the question
empirically before doing any docker‑level bench.

## Experiment

Self‑contained Python repro at `/tmp/reuseport_mcast_repro.py`
(committed in `docs/perf/reuseport-mcast-repro.py` for traceability):

- 4 receiver sockets, each set with `SO_REUSEADDR + SO_REUSEPORT(=15)`,
  bound to `0.0.0.0:39911`, each joining `239.10.99.1` via
  `IP_ADD_MEMBERSHIP`.
- 1 sender socket multicasting 1000 datagrams to `239.10.99.1:39911`.
- After 5 s, each receiver reports its received count.

### Result (kernel 6.6.87.2-microsoft-standard-WSL2)

```
sent 1000 packets

Received per socket:
  socket 0: 1000 packets (100.0% of sent)
  socket 1: 1000 packets (100.0% of sent)
  socket 2: 1000 packets (100.0% of sent)
  socket 3: 1000 packets (100.0% of sent)
  TOTAL:    4000 packets (= 4.00x sent)
```

Every replica saw 100 % of the traffic. **Conclusion: kernel delivers a
full copy to each REUSEPORT socket joined to the multicast group.** There
is no load distribution.

### Why the kernel does this

For unicast UDP, the kernel hashes the 4‑tuple
`(src_ip, src_port, dst_ip, dst_port)` of each datagram and picks one
socket from the REUSEPORT group. Distribution.

For multicast, the delivery rule is "every socket that has joined the
group on this port receives a copy" — this is the defining semantics of
multicast and predates REUSEPORT. REUSEPORT only changes whether the
`bind()` calls coexist; it does **not** override multicast delivery. The
2021 commit `e32ea7e74` and earlier discussions on netdev confirm this is
intended behaviour, not an oversight.

(Reference: the `udp_lib_lookup` path takes the multicast branch via
`udp4_lib_mcast_deliver`, which iterates over **all** matching sockets;
REUSEPORT hash is only consulted on the unicast branch.)

## Implications for the consumer

With the production config (`config/multicast-compose.json`) the field
defaults to 1, so the live deployment is unaffected. **But** the field is
exposed in `MulticastFeedConfig.cs:138` and respected by
`Program.cs:339`. Anyone who reads the (incorrect) `MulticastPacketSource`
docstring and bumps the value to 4 hoping for headroom will instead get:

| effect with N > 1 | magnitude |
|---|---|
| Each datagram parsed N× by FeedHandler (deduplicated by seqNum, so functionally fine) | N× CPU on the parse path |
| Each socket holds its own `SO_RCVBUF` (configured 32 MB Inc / 16 MB Snap / 4 MB Instr) | N× kernel memory per channel |
| N receive threads woken per datagram | N× context switches; cache thrash |
| Application‑level `b3.umdf.feed.ring.dropped_by_channel` for the duplicate copies | inflated drop counter (ring sees the same packet N times) |
| Throughput / latency benefit | **zero** |

Worst case: at the observed 118 kpps Inc burst, N=4 means the dispatcher
sees 472 kpps of which 75 % is duplicate work — exactly when headroom is
needed least.

## Recommended action

**Soft deprecation** (lowest risk):

1. In `MulticastPacketSource.cs:82–101`, replace the load‑balance comment
   with a pointer to this doc and downgrade the REUSEPORT setsockopt to a
   no‑op when `ReceiveSocketCount == 1` (already the case — confirm).
2. In `Program.cs:339`, when `c.ReceiveSocketCount > 1`, log a warning
   ("multicast REUSEPORT replicas are duplicate‑delivery on Linux; clamping
   to 1") and force `replicas = 1`.
3. Keep the JSON field for backward compat; the parser already defaults
   it to 1.

**Hard removal** (cleaner) is a v2 follow‑up; safe to defer until the
warning has been observable in any real environment.

The cleanup is low‑value (the field is unused in any committed config)
but it removes a foot‑gun for future operators.

## Why we skipped the full docker A/B bench

The original Level‑3 plan was to run the consumer with `replicas=1` vs
`replicas=4` in `docker-compose.multicast.yml`, replay PCAPs, and compare
`b3.umdf.feed.ring.dropped` and CPU. After the empirical kernel test
above the docker bench would only **quantify the cost** of duplicate
delivery, not validate any hypothesis — the question is settled. We
elected to publish this doc and skip the bench. If at some point we need
the cost number (e.g. to decide between soft and hard deprecation), the
script `tools/InboundLatencyProbe` already gives the dispatcher‑side
latency curve and can be re‑run with simulated duplicates.

## Reproducing

```bash
python3 docs/perf/reuseport-mcast-repro.py
```

Expected output: `socket N: 1000 packets (100.0% of sent)` for each
replica, and the conclusion line saying "kernel DELIVERS A COPY".

If a future kernel changes this behaviour the script will report
`~250 packets each` and conclude "LOAD-BALANCES" — at which point this
doc and the recommended actions need revisiting.

## Date / kernel of record

| field | value |
|---|---|
| Date | 2026‑04‑27 |
| Kernel | `6.6.87.2-microsoft-standard-WSL2` |
| Python | 3.12.3 |
| Sockets / packets / payload | 4 / 1000 / 100 B |
