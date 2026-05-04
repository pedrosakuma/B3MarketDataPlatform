using Microsoft.Extensions.Logging;

namespace B3.Umdf.Server.Hosting;

/// <summary>
/// Owns the graceful-drain phase: send a clean WebSocket close frame
/// (1001 / EndpointUnavailable) to every active client so they observe an
/// orderly shutdown rather than a connection reset. The per-client close
/// handshake is bounded by the supplied budget so a misbehaving peer cannot
/// hold the shutdown open indefinitely.
/// </summary>
internal sealed class ShutdownCoordinator
{
    private readonly SubscriptionManager _subscriptionManager;
    private readonly ILogger _logger;

    public ShutdownCoordinator(SubscriptionManager subscriptionManager, ILogger logger)
    {
        _subscriptionManager = subscriptionManager;
        _logger = logger;
    }

    public async Task DrainClientsAsync(TimeSpan closeHandshakeBudget)
    {
        if (closeHandshakeBudget <= TimeSpan.Zero)
            closeHandshakeBudget = TimeSpan.FromSeconds(5);

        var sessions = _subscriptionManager.EnumerateAllClients()
            .Select(kv => kv.Value)
            .ToList();
        if (sessions.Count == 0)
            return;

        using var cts = new CancellationTokenSource(closeHandshakeBudget);
        var tasks = new Task[sessions.Count];
        for (int i = 0; i < sessions.Count; i++)
            tasks[i] = sessions[i].RequestGracefulCloseAsync("server shutting down", cts.Token);

        try { await Task.WhenAll(tasks).ConfigureAwait(false); }
        catch { /* per-client failures already logged inside RequestGracefulCloseAsync */ }

        _logger.LogInformation(
            "Sent WebSocket close (1001) to {Count} active client(s) before stopping Kestrel",
            sessions.Count);
    }
}
