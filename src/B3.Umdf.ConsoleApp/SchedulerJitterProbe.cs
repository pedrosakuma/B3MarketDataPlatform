using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace B3.Umdf.ConsoleApp;

/// <summary>
/// Self-contained scheduler/preemption jitter probe.
///
/// A dedicated thread sleeps for <see cref="TickIntervalMs"/> and measures the
/// actual elapsed time. The overshoot beyond the requested sleep is attributed
/// to scheduler latency (CPU contention from noisy neighbours, GC pauses,
/// IRQ storms, cgroup CFS throttling, etc.).
///
/// Detection only — does not mitigate. Useful as a leading indicator that the
/// host is becoming hostile to real-time-ish workloads (UDP receive, snapshot
/// recovery) before the consumer starts dropping packets.
///
/// Cost: ~200 wakeups/s, no allocations on the hot path. Disable via
/// <c>UMDF_SCHEDULER_JITTER_PROBE=0</c> if even that is unwelcome.
/// </summary>
sealed class SchedulerJitterProbe : IDisposable
{
    public const int TickIntervalMs = 5;

    static readonly Histogram<double> JitterHistogram = MetricsBinder.Meter.CreateHistogram<double>(
        "b3.umdf.scheduler.jitter_us",
        unit: "us",
        description: "Scheduler wakeup overshoot beyond requested sleep (CPU contention / GC / cgroup throttling).");

    readonly CancellationTokenSource _cts = new();
    readonly Thread _thread;
    long _maxJitterUsLastWindow;
    long _ticks;

    public SchedulerJitterProbe()
    {
        _thread = new Thread(Run)
        {
            IsBackground = true,
            Name = "SchedulerJitterProbe",
            Priority = ThreadPriority.AboveNormal,
        };

        MetricsBinder.Meter.CreateObservableGauge(
            "b3.umdf.scheduler.jitter_max_us",
            () => Interlocked.Exchange(ref _maxJitterUsLastWindow, 0),
            unit: "us",
            description: "Max scheduler jitter observed since last scrape (resets on read).");

        MetricsBinder.Meter.CreateObservableCounter(
            "b3.umdf.scheduler.probe_ticks",
            () => Interlocked.Read(ref _ticks),
            unit: "{ticks}",
            description: "Total jitter probe ticks (sanity check that the probe is alive).");
    }

    public void Start() => _thread.Start();

    void Run()
    {
        const long expectedUs = TickIntervalMs * 1000L;
        var ts = Stopwatch.GetTimestamp();
        var token = _cts.Token;

        while (!token.IsCancellationRequested)
        {
            try { Thread.Sleep(TickIntervalMs); }
            catch (ThreadInterruptedException) { break; }

            var now = Stopwatch.GetTimestamp();
            var elapsedUs = (long)((now - ts) * 1_000_000.0 / Stopwatch.Frequency);
            ts = now;

            var jitterUs = elapsedUs - expectedUs;
            if (jitterUs < 0) jitterUs = 0;

            JitterHistogram.Record(jitterUs);
            Interlocked.Increment(ref _ticks);

            // Track max for the gauge (atomic CAS upward).
            long prev;
            do
            {
                prev = Interlocked.Read(ref _maxJitterUsLastWindow);
                if (jitterUs <= prev) break;
            } while (Interlocked.CompareExchange(ref _maxJitterUsLastWindow, jitterUs, prev) != prev);
        }
    }

    public void Dispose()
    {
        try { _cts.Cancel(); } catch { /* already disposed */ }
        try { _thread.Join(TimeSpan.FromSeconds(1)); } catch { /* best effort */ }
        _cts.Dispose();
    }
}
