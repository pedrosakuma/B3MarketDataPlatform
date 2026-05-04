using System.Collections.Concurrent;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Server;

/// <summary>
/// Background broadcaster for the periodic <c>RecoveryProgress</c> message.
/// Aggregates <see cref="SymbolStateRegistry"/> counters across every
/// <see cref="BookManager"/> every <see cref="IntervalMs"/> and emits a
/// payload whenever stale symbols exist.
///
/// Edge-triggered idle: when the stale count drops from &gt;0 to 0 a final
/// "all clear" message is sent, then the publisher idles until stale
/// becomes &gt;0 again — avoiding chatty zero-payload broadcasts.
/// </summary>
internal sealed class RecoveryProgressPublisher : IDisposable
{
    public const long IntervalMs = 250;

    private readonly Func<BookManager[]?> _bookManagers;
    private readonly ConcurrentDictionary<string, ClientSession> _clients;
    private readonly ILogger _logger;
    private Timer? _timer;
    private bool _lastNonZero;

    public RecoveryProgressPublisher(
        Func<BookManager[]?> bookManagers,
        ConcurrentDictionary<string, ClientSession> clients,
        ILogger? logger = null)
    {
        _bookManagers = bookManagers;
        _clients = clients;
        _logger = (ILogger?)logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public void Start()
    {
        _timer = new Timer(static state => ((RecoveryProgressPublisher)state!).Push(),
            this, IntervalMs, IntervalMs);
    }

    public void Dispose() => _timer?.Dispose();

    private void Push()
    {
        if (_clients.IsEmpty) return;
        var managers = _bookManagers();
        if (managers is null || managers.Length == 0) return;

        Span<int> perKind = stackalloc int[14];
        perKind.Clear();
        int totalStale = 0;
        int totalKnown = 0;

        foreach (var bm in managers)
        {
            if (bm?.StateRegistry is not { } reg) continue;
            var snap = reg.GetAggregateSnapshot();
            totalStale += snap.TotalStaleSymbols;
            totalKnown += snap.TotalSymbols;
            int n = Math.Min(perKind.Length, snap.StaleByKind.Length);
            for (int i = 0; i < n; i++) perKind[i] += snap.StaleByKind[i];
        }

        bool isNonZero = totalStale > 0;
        if (!isNonZero && !_lastNonZero) return;
        _lastNonZero = isNonZero;

        Span<byte> buf = stackalloc byte[WireProtocol.RecoveryProgressMaxSize];
        int len = WireProtocol.WriteRecoveryProgress(buf, (uint)totalKnown, (uint)totalStale, perKind);
        var payload = new ReadOnlyMemory<byte>(buf[..len].ToArray());
        foreach (var (_, client) in _clients)
        {
            if (client.TryEnqueue(payload)) continue;

            MetricsRegistry.BroadcastDrops.Add(1,
                new KeyValuePair<string, object?>("publisher", "recovery"));
            long total = client.RecordBroadcastDrop();
            if (IsPowerOfTwo(total))
            {
                _logger.LogWarning(
                    "Dropped recovery-progress broadcast for client {ClientId}; total broadcast drops on this session: {Drops}",
                    client.Id, total);
            }
        }
    }

    private static bool IsPowerOfTwo(long n) => n > 0 && (n & (n - 1)) == 0;
}
