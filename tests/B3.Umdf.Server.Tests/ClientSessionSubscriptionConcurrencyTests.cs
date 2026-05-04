using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Stress tests for the single-lock ownership invariant on
/// <see cref="ClientSession._subscriptions"/>: concurrent add/remove on the
/// feed thread interleaved with iteration of the public <c>Subscriptions</c>
/// snapshot from a reader thread must not throw or corrupt the set.
/// </summary>
public class ClientSessionSubscriptionConcurrencyTests
{
    [Fact]
    public async Task ConcurrentSubscribeUnsubscribeAndIterate_DoesNotThrow()
    {
        var ws = new FakeWebSocket();
        using var session = new ClientSession(ws, channelCapacity: 8192);

        const int writers = 4;
        const int iters = 5_000;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var tasks = new List<Task>();
        for (int w = 0; w < writers; w++)
        {
            int seed = w;
            tasks.Add(Task.Run(() =>
            {
                var rng = new Random(seed);
                for (int i = 0; i < iters && !cts.IsCancellationRequested; i++)
                {
                    ulong id = (ulong)rng.Next(1, 256);
                    if ((i & 1) == 0) session.AddSubscription(id);
                    else session.RemoveSubscription(id);
                }
            }, cts.Token));
        }

        // Reader: keeps iterating the snapshot. With the legacy unguarded HashSet
        // this would throw InvalidOperationException ("Collection was modified")
        // very quickly under contention.
        tasks.Add(Task.Run(() =>
        {
            int loops = 0;
            while (!cts.IsCancellationRequested && loops < 50_000)
            {
                var snap = session.Subscriptions;
                int n = 0;
                foreach (var _ in snap) n++;
                _ = session.IsSubscribed((ulong)(loops & 0xFF));
                loops++;
            }
        }, cts.Token));

        await Task.WhenAll(tasks);
        // No assertion on state — invariant under test is "no exception thrown".
    }
}
