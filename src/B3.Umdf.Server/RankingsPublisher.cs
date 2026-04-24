using System.Collections.Concurrent;
using B3.Umdf.Book;

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
    private Timer? _timer;
    private int _tick;

    public RankingsPublisher(
        Func<MarketDataManager[]?> marketDataManagers,
        Func<SymbolRegistry?> symbolRegistry,
        ConcurrentDictionary<string, ClientSession> clients)
    {
        _marketDataManagers = marketDataManagers;
        _symbolRegistry = symbolRegistry;
        _clients = clients;
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
            client.TryEnqueue(payload);
    }

    private static RankingEntry[] TakeTopN(List<RankingEntry> sorted, int n)
        => sorted.Count > n ? sorted.GetRange(0, n).ToArray() : sorted.ToArray();
}
