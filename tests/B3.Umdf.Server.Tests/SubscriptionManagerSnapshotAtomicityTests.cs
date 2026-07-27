using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Stress test for the centralised subscription snapshot + routing-index publication in
/// <see cref="SubscriptionManager"/>. Concurrent <see cref="SubscriptionManager.AddSubscriptionForTest"/>
/// callers must never publish a torn all-subscriber or immediate/cadence routing snapshot
/// while another thread iterates it.
/// </summary>
public class SubscriptionManagerSnapshotAtomicityTests
{
    [Fact]
    public async Task ConcurrentAddAndDelisted_PreservesSnapshotAtomicity()
    {
        using var sm = new SubscriptionManager();
        const int symbols = 32;
        const int iters = 2_000;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var tasks = new List<Task>();
        for (int t = 0; t < 4; t++)
        {
            int seed = t;
            tasks.Add(Task.Run(() =>
            {
                var rng = new Random(seed);
                for (int i = 0; i < iters && !cts.IsCancellationRequested; i++)
                {
                    ulong sec = (ulong)(rng.Next(symbols) + 1);
                    bool cadence = (i & 1) == 0;
                    sm.AddSubscriptionForTest(
                        $"c{seed}-{i & 0x3F}",
                        sec,
                        cadence ? DataFlags.ConflatedMbp : DataFlags.Mbp,
                        cadence ? (ushort)250 : (ushort)0);
                    if ((i & 7) == 0) sm.NotifyDelisted(sec);
                }
            }, cts.Token));
        }

        // Reader: NotifyDelisted internally snapshots the inner dict under _subLock
        // and then iterates it lock-free. If the helper publishes mutated dicts
        // (instead of fresh COW copies) this would surface as
        // InvalidOperationException("Collection was modified") here.
        tasks.Add(Task.Run(() =>
        {
            int loops = 0;
            while (!cts.IsCancellationRequested && loops < 100_000)
            {
                _ = sm.ActiveSymbolCount;
                ulong securityId = (ulong)(loops % symbols + 1);
                if (sm.GetImmediateMbpSubscribers(securityId) is { } immediate)
                    foreach (var entry in immediate) _ = entry.Key;
                if (sm.GetConflatedMbpSubscribers(securityId, 250) is { } cadence)
                    foreach (var entry in cadence) _ = entry.Key;
                loops++;
            }
        }, cts.Token));

        await Task.WhenAll(tasks);
        Assert.True(sm.ActiveSymbolCount >= 0);
    }
}
