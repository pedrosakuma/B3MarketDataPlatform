using System.Buffers;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Server;

/// <summary>
/// Periodic sweeper that disconnects pending-bytes outliers under aggregate memory
/// pressure. Owned by <see cref="SubscriptionManager"/>; runs on its own
/// <see cref="Timer"/> thread (background).
///
/// Algorithm:
///  1. Snapshot pending bytes for every connected client into pooled buffers.
///  2. If aggregate &lt; <c>pressurePct × clients × maxPendingBytes</c>, do nothing —
///     the fleet is healthy, individual outliers are not hurting anyone.
///  3. Otherwise compute the median pending and disconnect each client whose
///     pending exceeds <c>max(median × multiplier, minBytes)</c>.
///
/// This protects against the "fixed-threshold pitfall" where a system-wide
/// slowdown (GC pause, market burst, network jitter) lifts every client past
/// an absolute cap and a naive guard would mass-disconnect the whole fleet.
/// The hard cap in <see cref="ClientSession"/> remains in force for genuinely
/// runaway producers.
/// </summary>
internal sealed class OutlierSweeper : IDisposable
{
    private readonly ConcurrentDictionary<string, ClientSession> _clients;
    private readonly long _clientMaxPendingBytes;
    private readonly double _multiplier;
    private readonly long _minBytes;
    private readonly double _pressurePct;
    private readonly ILogger _logger;
    private readonly Timer? _timer;

    public OutlierSweeper(
        ConcurrentDictionary<string, ClientSession> clients,
        long clientMaxPendingBytes,
        double multiplier,
        long minBytes,
        double pressurePct,
        int intervalMs,
        ILogger logger)
    {
        _clients = clients;
        _clientMaxPendingBytes = clientMaxPendingBytes;
        _multiplier = multiplier;
        _minBytes = minBytes;
        _pressurePct = pressurePct;
        _logger = logger;

        if (intervalMs > 0 && multiplier > 0 && clientMaxPendingBytes > 0)
        {
            _timer = new Timer(
                static state => ((OutlierSweeper)state!).RunSweep(),
                this,
                intervalMs,
                intervalMs);
        }
    }

    public void Dispose() => _timer?.Dispose();

    private void RunSweep()
    {
        try
        {
            int n = _clients.Count;
            if (n == 0) return;

            var sessions = ArrayPool<ClientSession>.Shared.Rent(n);
            var pending = ArrayPool<long>.Shared.Rent(n);
            int count = 0;
            long aggregate = 0;
            try
            {
                foreach (var (_, s) in _clients)
                {
                    if (count >= n) break; // dictionary may have grown; ignore late arrivals this tick
                    long p = s.PendingBytes;
                    sessions[count] = s;
                    pending[count] = p;
                    aggregate += p;
                    count++;
                }
                if (count == 0) return;

                long budget = (long)count * _clientMaxPendingBytes;
                if (budget <= 0) return;
                if (aggregate < (long)(budget * _pressurePct)) return;

                long median = ComputeMedian(pending, count);
                long threshold = Math.Max((long)(median * _multiplier), _minBytes);

                int killed = 0;
                for (int i = 0; i < count; i++)
                {
                    if (pending[i] > threshold)
                    {
                        sessions[i].DisconnectAsSlowConsumer(
                            $"outlier sweep: pending={pending[i]} > threshold={threshold} (median={median}, multiplier={_multiplier:F1}, aggregate={aggregate}/{budget})");
                        killed++;
                    }
                }
                if (killed > 0)
                {
                    _logger.LogWarning(
                        "Outlier sweep disconnected {Killed}/{Count} clients (aggregate={Aggregate} / budget={Budget}, median={Median}, threshold={Threshold})",
                        killed, count, aggregate, budget, median, threshold);
                }
            }
            finally
            {
                Array.Clear(sessions, 0, count);
                ArrayPool<ClientSession>.Shared.Return(sessions);
                ArrayPool<long>.Shared.Return(pending);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Outlier sweep failed");
        }
    }

    private static long ComputeMedian(long[] values, int count)
    {
        // O(n log n) sort is fine: count is bounded by MaxConnections (hundreds-thousands)
        // and the sweep runs at ~1 Hz. A copy keeps the index alignment between
        // `values` and the parallel `sessions` array used for the disconnect pass.
        var copy = ArrayPool<long>.Shared.Rent(count);
        try
        {
            Array.Copy(values, copy, count);
            Array.Sort(copy, 0, count);
            return count % 2 == 1
                ? copy[count / 2]
                : (copy[count / 2 - 1] + copy[count / 2]) / 2;
        }
        finally
        {
            ArrayPool<long>.Shared.Return(copy);
        }
    }
}
