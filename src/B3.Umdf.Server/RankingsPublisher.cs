using System.Collections.Concurrent;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Server;

/// <summary>
/// Background broadcaster for the periodic rankings update (top-N volume,
/// gainers, losers). Runs on its own <see cref="Timer"/> at
/// <see cref="IntervalMs"/>; aggregates instrument data across every per-group
/// <see cref="MarketDataManager"/> and emits a single <c>RankingsUpdate</c>
/// payload to all connected clients.
///
/// Also drives <see cref="SymbolRegistry.TryPromote"/> every
/// <see cref="PromoteEveryNTicks"/> tick so that recently-active symbols
/// are escalated into the hot working set.
/// </summary>
internal sealed class RankingsPublisher : IDisposable
{
    public const int TopN = 10;
    public const long IntervalMs = 2000;
    private const int PromoteEveryNTicks = 15; // ~30s at 2000ms interval

    private readonly Func<MarketDataManager[]?> _marketDataManagers;
    private readonly Func<SymbolRegistry?> _symbolRegistry;
    private readonly ConcurrentDictionary<string, ClientSession> _clients;
    private readonly ILogger _logger;
    private Timer? _timer;
    private int _tick;

    public RankingsPublisher(
        Func<MarketDataManager[]?> marketDataManagers,
        Func<SymbolRegistry?> symbolRegistry,
        ConcurrentDictionary<string, ClientSession> clients,
        ILogger? logger = null)
    {
        _marketDataManagers = marketDataManagers;
        _symbolRegistry = symbolRegistry;
        _clients = clients;
        _logger = (ILogger?)logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }

    public void Start()
    {
        _timer = new Timer(static state => ((RankingsPublisher)state!).OnTick(),
            this, IntervalMs, IntervalMs);
    }

    public void Dispose() => _timer?.Dispose();

    private void OnTick()
    {
        if (++_tick % PromoteEveryNTicks == 0)
            _symbolRegistry()?.TryPromote();

        if (_clients.Count > 0)
            Push();
    }

    private void Push()
    {
        var managers = _marketDataManagers();
        var registry = _symbolRegistry();
        if (managers is null || registry is null) return;

        var volumeList = new List<RankingEntry>();
        var gainerList = new List<RankingEntry>();
        var loserList = new List<RankingEntry>();

        foreach (var mdm in managers)
        {
            foreach (var (secId, info) in mdm.InstrumentData)
            {
                if (!registry.TryGetSymbol(secId, out var sym)) continue;

                if (info.TradeVolume is { } vol and > 0)
                    volumeList.Add(new RankingEntry(secId, vol, sym));

                if (info.NetChangeFromPrevDay is { } chg)
                {
                    if (chg > 0) gainerList.Add(new RankingEntry(secId, chg, sym));
                    else if (chg < 0) loserList.Add(new RankingEntry(secId, chg, sym));
                }
            }
        }

        volumeList.Sort((a, b) => b.Value.CompareTo(a.Value));
        gainerList.Sort((a, b) => b.Value.CompareTo(a.Value));
        loserList.Sort((a, b) => a.Value.CompareTo(b.Value));

        var volume = TakeTopN(volumeList, TopN);
        var gainers = TakeTopN(gainerList, TopN);
        var losers = TakeTopN(loserList, TopN);

        var buf = new byte[WireProtocol.RankingsUpdateMaxSize];
        int len = WireProtocol.WriteRankingsUpdate(buf, volume, gainers, losers);
        var payload = new ReadOnlyMemory<byte>(buf, 0, len);

        foreach (var (_, client) in _clients)
        {
            if (client.TryEnqueue(payload)) continue;

            // Drop: increment global counter (tagged by publisher), per-session
            // counter, and warn at power-of-two thresholds so logs don't get
            // spammed by a chronically slow client.
            MetricsRegistry.BroadcastDrops.Add(1,
                new KeyValuePair<string, object?>("publisher", "rankings"));
            long total = client.RecordBroadcastDrop();
            if (IsPowerOfTwo(total))
            {
                _logger.LogWarning(
                    "Dropped rankings broadcast for client {ClientId}; total broadcast drops on this session: {Drops}",
                    client.Id, total);
            }
        }
    }

    private static bool IsPowerOfTwo(long n) => n > 0 && (n & (n - 1)) == 0;

    private static RankingEntry[] TakeTopN(List<RankingEntry> sorted, int n)
        => sorted.Count > n ? sorted.GetRange(0, n).ToArray() : sorted.ToArray();
}
