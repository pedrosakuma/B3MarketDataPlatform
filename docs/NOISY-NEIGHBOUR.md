# Noisy Neighbour Hardening

How `SbeB3UmdfConsumer` behaves under host-level resource contention, and the
deployment knobs that preserve QoS when other workloads share the box.

For application-level fault tolerance see [RESILIENCE.md](./RESILIENCE.md).
For raw throughput design see [PERFORMANCE.md](./PERFORMANCE.md).

## TL;DR

The application is **structurally decoupled** from slow clients (drop-on-full
broadcaster ring, per-client backpressure with disconnect). The remaining
vulnerability is **shared-resource contention with unrelated workloads on the
same host**: CPU, NIC IRQs, LLC, memory bandwidth. Software can detect and
prepare; only the orchestrator can isolate.

## What the consumer already defends against

| Vector inside the process | Defense |
| ------------------------- | ------- |
| Slow WS client back-pressure to dispatch | SPSC bounded broadcaster ring with **drop-on-full + resync**; slow client never blocks dispatch |
| Slow WS client memory growth | Per-client `clientMaxPendingBytes` cap with disconnect (§ RESILIENCE.md) |
| Burst loss from preemption | A/B reorder window (256 packets) + per-symbol snapshot heal |
| First-burst page faults | Fixed pools (ArrayPool, pinned buffers) allocated at startup |
| GC pause amplification | Server GC; per-group sharded state; no shared locks on hot path |

These are sufficient when the consumer has its CPU budget. They do **not**
prevent UDP loss when the kernel cannot schedule the receive thread frequently
enough to drain `SO_RCVBUF` (~16-32 MB).

## What software cannot fix

The cgroup CPU quota (`--cpus`) is enforced on the **process**, not the
thread. Inside the process, `Priority=AboveNormal` on receive threads helps
the kernel scheduler pick them first when ready, but cannot create CPU when
the cgroup is throttled. The same applies to:

- LLC pressure from a noisy neighbour (CAT/MBA are kernel-level)
- NIC RX softirq stolen by another tenant on the same RSS queue
- Cross-NUMA memory access if cores are scheduled on the wrong node

## Detection: scheduler jitter probe

A dedicated low-priority thread sleeps `5 ms` and measures the actual elapsed
time. The overshoot beyond `5 ms` is attributed to scheduler latency. Exposed
via `System.Diagnostics.Metrics` under `b3.umdf.scheduler.*`:

| Instrument | Type | Purpose |
| ---------- | ---- | ------- |
| `b3.umdf.scheduler.jitter_us` | Histogram | Distribution of wakeup overshoot in µs |
| `b3.umdf.scheduler.jitter_max_us` | Gauge | Max since last scrape (resets on read) |
| `b3.umdf.scheduler.probe_ticks` | Counter | Sanity check that the probe is alive |

Healthy host (dedicated cores, no contention): p99 < 200 µs, max < 2 ms.

Contended host: p99 climbs into ms; max can hit hundreds of ms during cgroup
throttle bursts. **A sustained `jitter_max_us > 50 ms` is a leading indicator
that UDP loss is imminent.**

Disable via `UMDF_SCHEDULER_JITTER_PROBE=0` (overhead is ~200 wakeups/s, but
some operators may prefer pristine flame graphs).

## Mitigation: orchestrator hardening

### Docker / Compose

```yaml
services:
  consumer:
    # Pin to specific cores instead of giving a quota.
    # No CFS throttling; scheduler still runs other things on these cores
    # but our threads are guaranteed to be on a dedicated set.
    cpuset: "0-3"
    # Optional bias under contention.
    cpu_shares: 2048
    # Memory: allow request to exceed soft limit; never swap.
    mem_limit: 4g
    memswap_limit: 4g
    # If the receiver buffers are large and host is memory-pressured.
    cap_add:
      - IPC_LOCK              # for future mlockall option
      - SYS_NICE              # so Priority=AboveNormal actually applies
    sysctls:
      net.core.rmem_max: 33554432
      net.core.netdev_max_backlog: 5000
    ulimits:
      memlock:
        soft: -1
        hard: -1
```

Prefer `cpuset` over `--cpus` whenever possible: a quota of `--cpus=2` on a
busy host can suffer from 100 ms scheduler latency under noisy neighbour
load even though the *throughput* budget is fine; `cpuset` removes that.

### Kubernetes

```yaml
apiVersion: v1
kind: Pod
spec:
  # Required for static CPU manager policy to bind cores exclusively.
  containers:
  - name: consumer
    resources:
      requests:
        cpu: "4"               # integer + Guaranteed QoS = static binding
        memory: 4Gi
      limits:
        cpu: "4"
        memory: 4Gi
```

Cluster prerequisites:

- `kubelet --cpu-manager-policy=static` (cores bound exclusively, no migration)
- `kubelet --topology-manager-policy=single-numa-node` (cores + memory on same NUMA node)
- Node labelled and tainted to keep noisy workloads off (`nodeAffinity` + `tolerations`)

### Host-level

- `isolcpus=2-7` kernel cmdline (cores 2-7 reserved from general scheduler)
- `irqaffinity=0-1` (push IRQs to cores 0-1, away from app)
- `nohz_full=2-7 rcu_nocbs=2-7` (no scheduler tick on app cores)
- Pin NIC RX queues manually:
  ```sh
  for q in /proc/irq/*/eth0-rx-*; do echo 0-1 > "$q/smp_affinity_list"; done
  ```
- Disable transparent huge pages if you see GC anomalies:
  `echo madvise > /sys/kernel/mm/transparent_hugepage/enabled`
- Disable swap entirely on the host (`swapoff -a`); never swap a low-latency
  consumer.

## Sysctls worth raising

| Sysctl | Default | Recommended | Why |
| ------ | ------- | ----------- | --- |
| `net.core.rmem_max` | 8388608 | 33554432 | Lets the consumer request 16-32 MB `SO_RCVBUF` |
| `net.core.netdev_max_backlog` | 1000 | 5000 | Per-CPU softirq backlog before drops |
| `net.ipv4.igmp_max_memberships` | 20 | 256 | Allows joining many multicast groups |
| `vm.swappiness` | 60 | 0 | Avoid swap pressure on consumer heap |

## Recommended test before going live

After deploying, validate jitter under realistic neighbour load:

1. Start the consumer normally on the target node.
2. On the same node, start a **stressor** with the workload profile of the
   neighbour you fear (e.g. `stress-ng --cpu N --vm M` for CPU+memory churn).
3. Watch `dotnet-counters monitor --counters B3.Umdf.Consumer` and confirm:
   - `jitter_max_us` stays below 5-10 ms p99
   - `b3.umdf.feed.gaps` does not increase
   - `b3.umdf.persymbol.stale_symbols` stays at 0

If jitter spikes correlate with stress events, the cores are not exclusive
enough. Tighten with `cpuset` / k8s static policy / `isolcpus`.

## Why we deliberately do **not** use SCHED_FIFO

Real-time scheduling (`SCHED_FIFO`/`SCHED_RR`) would burn through preemption
problems immediately: a single bug that loops without yielding takes the
entire core hostage and breaks the host. The trade is not worth the gain
when `cpuset` + `Priority=AboveNormal` already gets us within microseconds
of optimal on a properly isolated node.
