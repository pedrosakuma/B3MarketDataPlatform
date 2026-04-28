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
    private readonly IReadOnlyList<GroupConflationHandler>? _groupHandlers;

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
        SubscriptionManager? subscriptionManager,
        IReadOnlyList<GroupConflationHandler>? groupHandlers = null)
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
        _groupHandlers = groupHandlers;
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
            ready = _singleFeed.State == FeedState.Streaming;
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
        Console.WriteLine($"   MOA/MOC: snap={_bookManagers.Sum(bm => bm.SnapshotMarketOrderAdds):N0} add={_bookManagers.Sum(bm => bm.MarketOrderAdds):N0} upd={_bookManagers.Sum(bm => bm.MarketOrderUpdates):N0} del={_bookManagers.Sum(bm => bm.MarketOrderDeletes):N0} toPx={_bookManagers.Sum(bm => bm.MarketOrderTransitionsToPriced):N0}");

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
            long absorbed = _singleFeed.PerSymbolGapsAbsorbed;
            if (absorbed > 0)
            {
                recoveryParts.Add(
                    $"G{_groupIds[0]} absorbedChannelGaps={absorbed:N0} lastGap={_singleFeed.IncrementalHandler.LastGapExpected}->{_singleFeed.IncrementalHandler.LastGapReceived}");
            }
        }
        else if (_multiFeed is not null)
        {
            foreach (var (gid, handler) in _multiFeed.Handlers.OrderBy(h => h.Key))
            {
                long absorbed = handler.PerSymbolGapsAbsorbed;
                if (absorbed > 0)
                {
                    recoveryParts.Add(
                        $"G{gid} absorbedChannelGaps={absorbed:N0} lastGap={handler.IncrementalHandler.LastGapExpected}->{handler.IncrementalHandler.LastGapReceived}");
                }
            }
        }

        if (recoveryParts.Count > 0)
            Console.WriteLine($"   recovery: {string.Join("  ", recoveryParts)}");

        // Per-symbol heal summary: one line per group with at least one Stale
        // symbol or non-empty stale buffer.
        var perSymbolParts = new List<string>();
        long totalAbsorbed = 0;
        for (int i = 0; i < _bookManagers.Count; i++)
        {
            var bm = _bookManagers[i];
            var reg = bm.StateRegistry;

            var snap = reg.GetAggregateSnapshot();
            long buffered = bm.BufferedMboMessages - bm.ReplayedMboMessages;
            long stalePending = bm.StaleBuffer.EnqueuedCount - bm.StaleBuffer.DrainedCount;
            long bufBytes = bm.StaleBuffer.TotalBytes;
            long authReset = reg.StaleAuthoritativeResetCount;
            if (snap.TotalStaleSymbols > 0 || stalePending > 0 || bm.SnapshotsHealed > 0 || authReset > 0)
            {
                string gate = (_groupHandlers is not null && i < _groupHandlers.Count && _groupHandlers[i].IsFanoutSuppressed)
                    ? " gate:on" : "";
                long evictUnsafe = bm.StaleBuffer.EvictedPerSymbolCapCount;
                long evictSafe = bm.StaleBuffer.SafeEvictedBelowFloorCount;
                long hotProm = bm.StaleBuffer.HotPromotionCount;
                long promRefused = bm.StaleBuffer.PromotionsRefusedGlobalCapCount;
                var promByLevel = bm.StaleBuffer.GetPromotionsByLevel();
                long upperProm = 0;
                for (int li = 2; li < promByLevel.Length; li++) upperProm += promByLevel[li];
                string promExtra = (upperProm > 0 || promRefused > 0)
                    ? $" upperProm:{upperProm}{(promRefused > 0 ? $" refused:{promRefused}" : "")}"
                    : "";
                string floor = (evictUnsafe > 0 || evictSafe > 0 || hotProm > 0 || upperProm > 0)
                    ? $" floorPin[hotProm:{hotProm}{promExtra} evictSafe:{evictSafe:N0} evictUnsafe:{evictUnsafe:N0}]"
                    : "";
                // Forced-heal escape valve: surface count + worst-case severity peek
                // (max unsafeDelta is the strongest single signal of how much data
                // the escape silenced). Only emit when non-zero so quiet groups stay quiet.
                string forced = "";
                if (authReset > 0)
                {
                    uint maxUnsafe = reg.MaxAuthoritativeResetUnsafeDelta;
                    uint maxDiscarded = reg.MaxAuthoritativeResetDiscardedTailDelta;
                    forced = $" forcedHeal[count:{authReset:N0} maxUnsafe:{maxUnsafe:N0} maxDiscardedTail:{maxDiscarded:N0}]";
                }
                perSymbolParts.Add(
                    $"G{_groupIds[i]}=stale:{snap.TotalStaleSymbols}/{snap.TotalSymbols} buf:{stalePending:N0}msg/{bufBytes:N0}B healed:{bm.SnapshotsHealed:N0} skipHA:{bm.SnapshotsSkippedHealthyAhead:N0} rejTooOld:{bm.SnapshotsRejectedTooOld:N0} miss:{bm.SnapshotsMissingRptSeq:N0}{gate}{floor}{forced}");
            }
        }
        if (_multiFeed is not null)
        {
            foreach (var (_, h) in _multiFeed.Handlers)
                totalAbsorbed += h.PerSymbolGapsAbsorbed;
        }
        else if (_singleFeed is not null)
        {
            totalAbsorbed = _singleFeed.PerSymbolGapsAbsorbed;
        }
        if (perSymbolParts.Count > 0 || totalAbsorbed > 0)
        {
            var line = "   per-symbol: " + string.Join("  ", perSymbolParts);
            if (totalAbsorbed > 0)
                line += (perSymbolParts.Count > 0 ? "  " : "") + $"channelGapsAbsorbed:{totalAbsorbed:N0}";
            Console.WriteLine(line);
        }

        // Reference-data drift signals: surfaced only when non-zero so quiet
        // groups stay quiet. Identity changes / timestamp regressions are
        // expected to be ~0 in normal sessions; any non-zero value warrants
        // attention (delisting reuse or out-of-order InstrDef arrivals).
        long totalReplaced = 0, totalIdentityChanged = 0, totalTsRegressed = 0;
        for (int i = 0; i < _bookManagers.Count; i++)
            totalReplaced += _bookManagers[i].InstrumentsReplaced;
        for (int i = 0; i < _marketDataManagers.Count; i++)
        {
            totalIdentityChanged += _marketDataManagers[i].InstrumentIdentityChanged;
            totalTsRegressed += _marketDataManagers[i].SecurityDefinitionsTimestampRegressed;
        }
        if (totalReplaced > 0 || totalIdentityChanged > 0 || totalTsRegressed > 0)
        {
            Console.WriteLine($"   refData: replaced:{totalReplaced:N0} identityChg:{totalIdentityChanged:N0} tsRegressed:{totalTsRegressed:N0}");
        }

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
        Console.WriteLine($"  MOA/MOC:      snap={_bookManagers.Sum(bm => bm.SnapshotMarketOrderAdds):N0} add={_bookManagers.Sum(bm => bm.MarketOrderAdds):N0} upd={_bookManagers.Sum(bm => bm.MarketOrderUpdates):N0} del={_bookManagers.Sum(bm => bm.MarketOrderDeletes):N0} toPx={_bookManagers.Sum(bm => bm.MarketOrderTransitionsToPriced):N0}");

        // Per-symbol heal summary.
        long totalStaleSymbols = 0, totalSymbolsTracked = 0, totalHealed = 0, totalBuffered = 0, totalReplayed = 0, totalDropDup = 0, totalLiveResync = 0, totalAbsorbed = 0;
        for (int i = 0; i < _bookManagers.Count; i++)
        {
            var reg = _bookManagers[i].StateRegistry;
            var snap = reg.GetAggregateSnapshot();
            totalStaleSymbols += snap.TotalStaleSymbols;
            totalSymbolsTracked += snap.TotalSymbols;
            totalHealed += _bookManagers[i].SnapshotsHealed;
            totalBuffered += _bookManagers[i].BufferedMboMessages;
            totalReplayed += _bookManagers[i].ReplayedMboMessages;
        }
        for (int i = 0; i < _marketDataManagers.Count; i++)
        {
            totalDropDup += _marketDataManagers[i].DroppedDuplicateStats;
            totalLiveResync += _marketDataManagers[i].LiveResyncs;
        }
        if (_multiFeed is not null)
            foreach (var (_, h) in _multiFeed.Handlers) totalAbsorbed += h.PerSymbolGapsAbsorbed;
        else if (_singleFeed is not null) totalAbsorbed = _singleFeed.PerSymbolGapsAbsorbed;

        {
            long totalEvictUnsafe = 0, totalEvictSafe = 0, totalHotProm = 0, totalDropPSCap = 0, totalDropGCap = 0;
            long totalAuthReset = 0;
            for (int i = 0; i < _bookManagers.Count; i++)
            {
                var sb = _bookManagers[i].StaleBuffer;
                totalEvictUnsafe += sb.EvictedPerSymbolCapCount;
                totalEvictSafe += sb.SafeEvictedBelowFloorCount;
                totalHotProm += sb.HotPromotionCount;
                totalDropPSCap += sb.DroppedPerSymbolCapCount;
                totalDropGCap += sb.DroppedGlobalCapCount;
                totalAuthReset += _bookManagers[i].StateRegistry.StaleAuthoritativeResetCount;
            }
            Console.WriteLine($"  PerSymbol:    stale={totalStaleSymbols}/{totalSymbolsTracked}  healed={totalHealed:N0}  buffered={totalBuffered:N0}  replayed={totalReplayed:N0}");
            Console.WriteLine($"                dropDup={totalDropDup:N0}  liveResync={totalLiveResync:N0}  channelGapsAbsorbed={totalAbsorbed:N0}");
            Console.WriteLine($"                floorPin: hotProm={totalHotProm:N0} evictSafe={totalEvictSafe:N0} evictUnsafe={totalEvictUnsafe:N0}  drop[psCap={totalDropPSCap:N0} gCap={totalDropGCap:N0}]  authReset={totalAuthReset:N0}");

            long totReplaced = 0, totIdChg = 0, totTsReg = 0, totSecDefSkipped = 0, totNews = 0;
            for (int i = 0; i < _bookManagers.Count; i++)
                totReplaced += _bookManagers[i].InstrumentsReplaced;
            for (int i = 0; i < _marketDataManagers.Count; i++)
            {
                totIdChg += _marketDataManagers[i].InstrumentIdentityChanged;
                totTsReg += _marketDataManagers[i].SecurityDefinitionsTimestampRegressed;
                totSecDefSkipped += _marketDataManagers[i].SecurityDefinitionsSkipped;
                totNews += _marketDataManagers[i].NewsMessagesObserved;
            }
            Console.WriteLine($"  RefData:      secDefSkipped={totSecDefSkipped:N0}  identityChg={totIdChg:N0}  tsRegressed={totTsReg:N0}  replaced={totReplaced:N0}  news={totNews:N0}");
        }
    }

    private long TotalEvents() =>
        _stats.OrderCount + _stats.TradeCount + _stats.DeleteCount +
        _stats.MarketDataCount + _stats.StatusChangeCount +
        _stats.ForwardTradeCount + _stats.TradeBustCount + _stats.ExecSummaryCount;
}
