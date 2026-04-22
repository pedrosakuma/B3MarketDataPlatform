using System.Diagnostics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.Server;

namespace B3.Umdf.ConsoleApp;

/// <summary>
/// Encapsulates periodic + final stats printing. Holds the mutable
/// rate-tracking counters as fields so the timer callback can be a
/// trivial method group invocation.
/// </summary>
internal sealed class StatsPrinter
{
    private readonly Stopwatch _sw;
    private readonly Stats _stats;
    private readonly IReadOnlyList<BookManager> _bookManagers;
    private readonly IReadOnlyList<MarketDataManager> _marketDataManagers;
    private readonly SymbolRegistry _symbolRegistry;
    private readonly IReadOnlyList<int> _groupIds;
    private readonly MultiFeedManager? _multiFeed;
    private readonly FeedHandler? _singleFeed;
    private readonly SubscriptionManager? _subscriptionManager;

    private bool _lastReady;
    private long _prevPackets;
    private long _prevTotalEvents;
    private long _prevStatsTicks;

    public StatsPrinter(
        Stopwatch sw,
        Stats stats,
        IReadOnlyList<BookManager> bookManagers,
        IReadOnlyList<MarketDataManager> marketDataManagers,
        SymbolRegistry symbolRegistry,
        IReadOnlyList<int> groupIds,
        MultiFeedManager? multiFeed,
        FeedHandler? singleFeed,
        SubscriptionManager? subscriptionManager)
    {
        _sw = sw;
        _stats = stats;
        _bookManagers = bookManagers;
        _marketDataManagers = marketDataManagers;
        _symbolRegistry = symbolRegistry;
        _groupIds = groupIds;
        _multiFeed = multiFeed;
        _singleFeed = singleFeed;
        _subscriptionManager = subscriptionManager;
    }

    public void PrintPeriodic()
    {
        bool ready;
        long packets;
        string stateStr;

        if (_multiFeed is not null)
        {
            ready = _multiFeed.IsAllReady;
            packets = _multiFeed.TotalPacketCount;
            stateStr = string.Join(", ", _multiFeed.Handlers.Select(h => $"G{h.Key}:{h.Value.State}"));
        }
        else if (_singleFeed is not null)
        {
            ready = _singleFeed.State == FeedState.RealTime;
            packets = _singleFeed.PacketCount;
            stateStr = _singleFeed.State.ToString();
        }
        else return;

        if (ready && !_lastReady)
        {
            if (_singleFeed is not null)
                _subscriptionManager?.SetReady();
            _lastReady = true;
        }

        long nowTicks = _sw.ElapsedTicks;
        double secs = _prevStatsTicks > 0
            ? (double)(nowTicks - _prevStatsTicks) / Stopwatch.Frequency
            : _sw.Elapsed.TotalSeconds;
        if (secs < 0.5) secs = 0.5;

        long totalEvents = TotalEvents();

        long pktRate = (long)((packets - _prevPackets) / secs);
        long evtRate = (long)((totalEvents - _prevTotalEvents) / secs);
        _prevPackets = packets;
        _prevTotalEvents = totalEvents;
        _prevStatsTicks = nowTicks;

        Console.WriteLine();
        Console.WriteLine($"── [{_sw.Elapsed:hh\\:mm\\:ss}] {stateStr} ──");
        Console.WriteLine($"   Packets: {packets:N0} ({pktRate:N0}/s)  |  Events: {totalEvents:N0} ({evtRate:N0}/s)  |  Books: {_bookManagers.Sum(bm => bm.Books.Count):N0}  |  Instruments: {_marketDataManagers.Sum(m => m.InstrumentData.Count):N0}  |  Symbols: {_symbolRegistry.Count:N0}");

        if (_subscriptionManager is not null)
        {
            foreach (var (id, depth, pendingBytes, sent, _) in _subscriptionManager.GetClientStats())
                Console.WriteLine($"   {id}: queue={depth:N0}  pending={pendingBytes:N0}B  sent={sent:N0}");
            if (_subscriptionManager.UpstreamConflated > 0)
                Console.WriteLine($"   upstream conflated (total): {_subscriptionManager.UpstreamConflated:N0}");
        }

        var recoveryParts = new List<string>();
        if (_singleFeed is not null)
        {
            if (_singleFeed.State == FeedState.Recovery || _singleFeed.IncrementalQueueDroppedPackets > 0)
            {
                recoveryParts.Add(
                    $"G{_groupIds[0]}={_singleFeed.State} gap={_singleFeed.IncrementalHandler.LastGapExpected}->{_singleFeed.IncrementalHandler.LastGapReceived} catchupDropped={_singleFeed.IncrementalQueueDroppedPackets:N0}");
            }
        }
        else if (_multiFeed is not null)
        {
            foreach (var (gid, handler) in _multiFeed.Handlers.OrderBy(h => h.Key))
            {
                if (handler.State == FeedState.Recovery || handler.IncrementalQueueDroppedPackets > 0)
                {
                    recoveryParts.Add(
                        $"G{gid}={handler.State} gap={handler.IncrementalHandler.LastGapExpected}->{handler.IncrementalHandler.LastGapReceived} catchupDropped={handler.IncrementalQueueDroppedPackets:N0}");
                }
            }
        }

        if (recoveryParts.Count > 0)
            Console.WriteLine($"   recovery: {string.Join("  ", recoveryParts)}");

        var crossedParts = new List<string>();
        for (int i = 0; i < _bookManagers.Count; i++)
        {
            long crossed = _bookManagers[i].CurrentlyCrossedBooks;
            long auction = _bookManagers[i].CurrentlyCrossedAuction;
            long locked = _bookManagers[i].CurrentlyLockedBooks;
            long transitions = _bookManagers[i].CrossingTransitions;
            if (crossed > 0 || auction > 0 || transitions > 0)
                crossedParts.Add($"G{_groupIds[i]}=trading:{crossed} auction:{auction} locked:{locked} (transitions={transitions:N0})");
        }
        if (crossedParts.Count > 0)
            Console.WriteLine($"   crossed books: {string.Join("  ", crossedParts)}");

        if (!ready && _singleFeed is not null && _singleFeed.State == FeedState.WaitInstrumentDefinition)
            Console.WriteLine($"   InstrDef: {_singleFeed.InstrDefReceived:N0}/{_singleFeed.InstrDefTotalExpected:N0} parsed  ({_singleFeed.InstrDefPacketCount:N0} packets)");

        if (!ready && _multiFeed is not null)
        {
            foreach (var (gid, h) in _multiFeed.Handlers)
            {
                if (h.State == FeedState.WaitInstrumentDefinition)
                    Console.WriteLine($"   G{gid} InstrDef: {h.InstrDefReceived:N0}/{h.InstrDefTotalExpected:N0} parsed  ({h.InstrDefPacketCount:N0} idef pkts, {h.PacketCount:N0} total pkts)");
            }
        }
    }

    public void PrintFinal()
    {
        double totalSecs = _sw.Elapsed.TotalSeconds;
        long packets = _multiFeed?.TotalPacketCount ?? _singleFeed?.PacketCount ?? 0;
        long totalEvents = TotalEvents();

        Console.WriteLine($"═══ Complete ({_sw.Elapsed:hh\\:mm\\:ss}) ═══");
        Console.WriteLine($"  Channel groups: {_groupIds.Count}");
        Console.WriteLine($"  Packets:      {packets:N0}  ({(totalSecs > 0 ? (long)(packets / totalSecs) : 0):N0}/s avg)");
        Console.WriteLine($"  Events:       {totalEvents:N0}  ({(totalSecs > 0 ? (long)(totalEvents / totalSecs) : 0):N0}/s avg)");
        Console.WriteLine($"    Orders:     {_stats.OrderCount:N0}");
        Console.WriteLine($"    Trades:     {_stats.TradeCount:N0}");
        Console.WriteLine($"    Deletes:    {_stats.DeleteCount:N0}");
        Console.WriteLine($"    MarketData: {_stats.MarketDataCount:N0}");
        Console.WriteLine($"    StatusChg:  {_stats.StatusChangeCount:N0}");
        Console.WriteLine($"    FwdTrades:  {_stats.ForwardTradeCount:N0}");
        Console.WriteLine($"    TradeBusts: {_stats.TradeBustCount:N0}");
        Console.WriteLine($"    ExecSumm:   {_stats.ExecSummaryCount:N0}");
        Console.WriteLine($"  Books:        {_bookManagers.Sum(bm => bm.Books.Count):N0}");
        Console.WriteLine($"  Instruments:  {_marketDataManagers.Sum(m => m.InstrumentData.Count):N0}");
        Console.WriteLine($"  Symbols:      {_symbolRegistry.Count:N0}");
    }

    private long TotalEvents() =>
        _stats.OrderCount + _stats.TradeCount + _stats.DeleteCount +
        _stats.MarketDataCount + _stats.StatusChangeCount +
        _stats.ForwardTradeCount + _stats.TradeBustCount + _stats.ExecSummaryCount;
}
