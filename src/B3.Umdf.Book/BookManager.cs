using System.Runtime.InteropServices;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book;

public sealed class BookManager : IFeedEventHandler, IMarketDataEventHandler
{
    private readonly BookStore _bookStore = new();
    private readonly IBookEventHandler? _eventHandler;
    private readonly ILogger<BookManager> _logger;
    private long _parseErrors;
    private long _orderAdds;
    private long _orderUpdates;
    private long _orderDeletes;
    private long _deleteNotFound;
    private long _nullPriceChangeDeletes;
    private long _tradesFilteredNonReportable;
    private long _marketOrderAdds;
    private long _marketOrderUpdates;
    private long _marketOrderDeletes;
    private long _marketOrderTransitionsToPriced;
    private long _marketOrderTransitionsToMarket;

    /// <summary>
    /// Packet-level SendingTime (nanoseconds since epoch) for the message currently being processed.
    /// Set at the start of OnPacket, consumed by Handle* methods.
    /// </summary>
    private ulong _currentSendingTimeNs;

    /// <summary>
    /// Thread-safe dictionary of all order books.
    /// Safe to read (Count, iterate) from any thread while the feed thread writes.
    /// </summary>
    public IReadOnlyDictionary<ulong, OrderBook> Books => _bookStore.Books;

    /// <summary>Number of SBE parse errors encountered (malformed packets).</summary>
    public long ParseErrors => Volatile.Read(ref _parseErrors);
    public long OrderAdds => Volatile.Read(ref _orderAdds);
    public long OrderUpdates => Volatile.Read(ref _orderUpdates);
    public long OrderDeletes => Volatile.Read(ref _orderDeletes);
    public long DeleteNotFound => Volatile.Read(ref _deleteNotFound);
    /// <summary>
    /// Legacy counter — previously incremented when a NEW null-price order
    /// arrived and was silently dropped. Now always returns 0 because such
    /// orders are tracked in the per-side market tier per spec §12.1
    /// (see <see cref="MarketOrderAdds"/>). Retained for backward
    /// compatibility with existing dashboards and metrics.
    /// </summary>
    public long NullPriceNewSkips => 0L;
    public long NullPriceChangeDeletes => Volatile.Read(ref _nullPriceChangeDeletes);

    /// <summary>Market orders (MOA/MOC) inserted into the per-side market tier
    /// (B3 spec §12.1). Replaces the legacy silent skip — orders are now
    /// tracked even when they have no price.</summary>
    public long MarketOrderAdds => Volatile.Read(ref _marketOrderAdds);
    public long MarketOrderUpdates => Volatile.Read(ref _marketOrderUpdates);
    public long MarketOrderDeletes => Volatile.Read(ref _marketOrderDeletes);
    /// <summary>An order changed from null-price (market) to a real price
    /// (typically when the auction phase resolves). Migrated from the market
    /// tier into the priced <see cref="BookSide"/>.</summary>
    public long MarketOrderTransitionsToPriced => Volatile.Read(ref _marketOrderTransitionsToPriced);
    /// <summary>An existing priced order was modified to remove its price (rare
    /// — typically a venue-side phase rollback). Migrated out of the priced
    /// <see cref="BookSide"/> into the per-side market tier.</summary>
    public long MarketOrderTransitionsToMarket => Volatile.Read(ref _marketOrderTransitionsToMarket);
    public long SnapshotMarketOrderAdds => _snapshotApplier.SnapshotMarketOrderAdds;

    /// <summary>
    /// Trades suppressed from <see cref="IBookEventHandler.OnTrade"/> /
    /// <see cref="IBookEventHandler.OnForwardTrade"/> because they are
    /// flagged as non-reportable per B3 spec §18 (TradeCondition.OutOfSequence
    /// or TrdSubType=LEG_TRADE). The rptSeq stream is still advanced for
    /// these messages — only downstream fanout (trade tape, candles, last
    /// price) is gated.
    /// </summary>
    public long TradesFilteredNonReportable => Volatile.Read(ref _tradesFilteredNonReportable);

    private long _endOfEventCount;
    private long _recoveryMsgCount;
    /// <summary>Total number of messages observed with the EndOfEvent bit set
    /// in MatchEventIndicator (B3 spec §10). Marks the last message of an
    /// atomic exchange-side matching event.</summary>
    public long EndOfEventCount => Volatile.Read(ref _endOfEventCount);
    /// <summary>Total number of messages observed with the RecoveryMsg bit set
    /// in MatchEventIndicator (B3 spec §10). Indicates the message is part of
    /// an exchange-side replay (distinct from this consumer's snapshot
    /// recovery).</summary>
    public long RecoveryMsgCount => Volatile.Read(ref _recoveryMsgCount);

    /// <summary>
    /// Updates MatchEventIndicator counters and fires
    /// <see cref="IBookEventHandler.OnEndOfEvent"/> when the EndOfEvent bit is
    /// set. Called once per processed wire message that carries a
    /// MatchEventIndicator field.
    /// </summary>
    private void TrackMatchEvent(ulong securityId, MatchEventIndicator mei)
    {
        if (mei.IsRecoveryMsg())
            Interlocked.Increment(ref _recoveryMsgCount);
        if (mei.IsEndOfEvent())
        {
            Interlocked.Increment(ref _endOfEventCount);
            try { _eventHandler?.OnEndOfEvent(securityId); }
            catch (Exception ex) { _logger.LogWarning(ex, "OnEndOfEvent threw for securityId={SecurityId}", securityId); }
        }
    }

    /// <summary>
    /// B3 spec §18: trades flagged as out-of-sequence or as leg trades of a
    /// strategy/UDS combo must NOT contribute to the outright instrument's
    /// last-trade price, candles, or aggregated trade tape — they are
    /// reported separately for transparency. Returns false for those.
    /// </summary>
    internal static bool IsReportableTrade(TradeCondition condition, TrdSubType? subType)
    {
        if ((condition & TradeCondition.OutOfSequence) != 0) return false;
        if (subType == TrdSubType.LEG_TRADE) return false;
        return true;
    }

    private long _crossingTransitions;
    private long _currentlyCrossedBooks;
    private long _currentlyCrossedAuction;
    private long _currentlyLockedBooks;
    public long CrossingTransitions => Volatile.Read(ref _crossingTransitions);
    /// <summary>Books currently crossed/locked while in OPEN trading phase (real anomalies).</summary>
    public long CurrentlyCrossedBooks => Volatile.Read(ref _currentlyCrossedBooks);
    /// <summary>Books currently crossed/locked while in a non-OPEN phase (auction/halt/closed) — expected.</summary>
    public long CurrentlyCrossedAuction => Volatile.Read(ref _currentlyCrossedAuction);
    /// <summary>Subset of crossed books where bestBid == bestAsk (locked, not inverted).</summary>
    public long CurrentlyLockedBooks => Volatile.Read(ref _currentlyLockedBooks);


    public BookManager(IBookEventHandler? eventHandler = null, ILogger<BookManager>? logger = null,
        SymbolStateRegistry? stateRegistry = null,
        StaleMboBuffer? staleBuffer = null)
    {
        _eventHandler = eventHandler;
        _logger = logger ?? NullLogger<BookManager>.Instance;
        GapTracker = new SymbolGapTracker(_logger);
        _stateRegistry = stateRegistry ?? throw new ArgumentNullException(nameof(stateRegistry),
            "SymbolStateRegistry is required.");
        _staleBuffer = staleBuffer ?? throw new ArgumentNullException(nameof(staleBuffer),
            "StaleMboBuffer is required.");
        // Wire the registry's Mbo state-change callback so OnSymbolStaleStatusChanged
        // surfaces regardless of which kind exposed the global gap (MBO or stat).
        // Last-writer-wins on the registry: if multiple BookManagers shared a registry
        // (they don't in the current architecture — one registry per group) only the
        // last would be notified.
        if (_eventHandler is { } eh)
            _stateRegistry.SetMboStaleStatusCallback((securityId, isStale) =>
                eh.OnSymbolStaleStatusChanged(securityId, isStale));

        _snapshotApplier = new SnapshotApplier(
            _bookStore,
            _stateRegistry,
            _staleBuffer,
            _eventHandler,
            _logger,
            ReplayDeferredMbo,
            () => CurrentSequenceVersion);
    }

    private readonly SymbolStateRegistry _stateRegistry;
    private readonly StaleMboBuffer _staleBuffer;
    private readonly SnapshotApplier _snapshotApplier;
    private long _bufferedMboMessages;
    private long _replayedMboMessages;
    private long _mboStaleTransitions;
    private long _mboStaleGapSizeSum;
    private long _mboStaleGapSizeMax;

    /// <summary>Per-symbol heal-state registry (one per group).</summary>
    public SymbolStateRegistry StateRegistry => _stateRegistry;

    /// <summary>Per-symbol Stale-window MBO buffer (one per group).</summary>
    public StaleMboBuffer StaleBuffer => _staleBuffer;

    public long BufferedMboMessages => Volatile.Read(ref _bufferedMboMessages);
    public long ReplayedMboMessages => Volatile.Read(ref _replayedMboMessages);
    /// <summary>Number of Healthy→Stale Mbo transitions observed by RouteMbo.</summary>
    public long MboStaleTransitions => Volatile.Read(ref _mboStaleTransitions);
    /// <summary>Cumulative gap size (rptSeq distance) of Healthy→Stale transitions.</summary>
    public long MboStaleGapSizeSum => Volatile.Read(ref _mboStaleGapSizeSum);
    /// <summary>Largest gap size seen on any Healthy→Stale transition.</summary>
    public long MboStaleGapSizeMax => Volatile.Read(ref _mboStaleGapSizeMax);

    /// <summary>
    /// Per-symbol rptSeq gap shadow tracker (telemetry only). The registry
    /// is the source of truth for routing decisions; the tracker remains for
    /// breakdown metrics on instruments without the registry path (e.g.
    /// boot-time before the registry observes the first message).
    /// </summary>
    public SymbolGapTracker GapTracker { get; }

    /// <summary>
    /// Records the per-symbol rptSeq gap (if any) and updates the book's
    /// <see cref="OrderBook.LastRptSeq"/>. Called from every MBO/Trade
    /// handler instead of writing <c>LastRptSeq</c> directly.
    /// The registry is the source of truth, so we skip the shadow tracker
    /// (which would double-count gaps).
    /// </summary>
    private void TrackMboRptSeq(OrderBook book, uint received)
    {
        book.LastRptSeq = received;
    }

    /// <summary>
    /// Per-symbol routing decision for an incoming MBO/Trade message.
    /// Consults the registry: Apply lets the caller proceed; Buffer copies
    /// the body into <see cref="_staleBuffer"/> for later replay; Drop
    /// returns silently.
    /// </summary>
    /// <returns><c>true</c> if the caller should proceed with apply logic.</returns>
    private bool RouteMbo(ulong securityId, ushort templateId, uint? rptSeqOpt, ReadOnlySpan<byte> body, int blockLength)
    {
        if (rptSeqOpt is not { } rptSeq || rptSeq == 0) return true; // can't gap-track without rptSeq
        var result = _stateRegistry.Observe(securityId, SymbolGapKind.Mbo, rptSeq);
        if (result.TransitionedToStale)
        {
            Interlocked.Increment(ref _mboStaleTransitions);
            long gap = (long)result.GapSize;
            Interlocked.Add(ref _mboStaleGapSizeSum, gap);
            // Lock-free max update.
            long currentMax;
            do
            {
                currentMax = Volatile.Read(ref _mboStaleGapSizeMax);
                if (gap <= currentMax) break;
            } while (Interlocked.CompareExchange(ref _mboStaleGapSizeMax, gap, currentMax) != currentMax);
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(
                    "PerSymbol Healthy→Stale secId={SecurityId} templateId={TemplateId} rptSeq={RptSeq} gap={Gap}",
                    securityId, templateId, rptSeq, gap);
            // Note: OnSymbolStaleStatusChanged is fired from the SymbolStateRegistry
            // callback (wired in BookManager's constructor), so it surfaces regardless
            // of which kind triggered the global gap (MBO or stat).
        }
        switch (result.Action)
        {
            case SymbolStateRegistry.ObserveAction.Apply:
                return true;
            case SymbolStateRegistry.ObserveAction.Buffer:
                if (_staleBuffer.Enqueue(securityId, templateId, rptSeq, _currentSendingTimeNs, body,
                        evictedRptSeq =>
                        {
                            // Buffer evicted its oldest entry to make room for this newer message.
                            // Advance MinHeal to evictedRptSeq so future snapshots must be at-least
                            // that fresh — otherwise we'd silently leave a hole in [snap+1..evicted].
                            _stateRegistry!.BumpMinHeal(securityId, SymbolGapKind.Mbo, evictedRptSeq);
                        },
                        blockLength: blockLength))
                    Interlocked.Increment(ref _bufferedMboMessages);
                return false;
            case SymbolStateRegistry.ObserveAction.Drop:
            default:
                return false;
        }
    }

    /// <summary>
    /// Drain and replay the per-symbol stale buffer for one security after
    /// a snapshot heal. Messages with <c>rptSeq ∈ [drainFrom, drainTo]</c>
    /// are dispatched through the same handlers as live messages — the
    /// registry will see them as Healthy + contiguous and route them
    /// through Apply.
    /// </summary>
    internal int ReplayDeferredMbo(ulong securityId, uint drainFrom, uint drainTo)
    {
        return _staleBuffer.Drain(securityId, drainFrom, drainTo, m =>
        {
            _currentSendingTimeNs = m.SendingTimeNs;
            switch (m.TemplateId)
            {
                case Order_MBO_50Data.MESSAGE_ID:
                    if (Order_MBO_50Data.TryParse(m.Span, m.BlockLength, out var orderReader))
                        HandleOrder(in orderReader);
                    break;
                case DeleteOrder_MBO_51Data.MESSAGE_ID:
                    if (DeleteOrder_MBO_51Data.TryParse(m.Span, m.BlockLength, out var delReader))
                        HandleDeleteOrder(in delReader);
                    break;
                case MassDeleteOrders_MBO_52Data.MESSAGE_ID:
                    if (MassDeleteOrders_MBO_52Data.TryParse(m.Span, m.BlockLength, out var mdReader))
                        HandleMassDelete(in mdReader);
                    break;
                case Trade_53Data.MESSAGE_ID:
                    if (Trade_53Data.TryParse(m.Span, m.BlockLength, out var trReader))
                        HandleTrade(in trReader);
                    break;
                case ForwardTrade_54Data.MESSAGE_ID:
                    if (ForwardTrade_54Data.TryParse(m.Span, m.BlockLength, out var fwdReader))
                        HandleForwardTrade(in fwdReader);
                    break;
                case ExecutionSummary_55Data.MESSAGE_ID:
                    if (ExecutionSummary_55Data.TryParse(m.Span, m.BlockLength, out var exReader))
                        HandleExecutionSummary(in exReader);
                    break;
                case TradeBust_57Data.MESSAGE_ID:
                    if (TradeBust_57Data.TryParse(m.Span, m.BlockLength, out var tbReader))
                        HandleTradeBust(in tbReader);
                    break;
            }
            Interlocked.Increment(ref _replayedMboMessages);
        });
    }

    // ── IMarketDataEventHandler ──
    // Receives trading-status updates so CheckCrossing can suppress warnings during
    // auction phases (Pre-open/Reserved, Pause, FinalClosingCall) where bestBid >= bestAsk
    // is normal — orders accumulate without matching until the phase opens.
    void IMarketDataEventHandler.OnSecurityStatusChanged(ulong securityId, InstrumentInfo info)
    {
        if (info.TradingStatus is { } status)
            ApplyTradingStatus(securityId, status);
    }

    void IMarketDataEventHandler.OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        if (info.TradingStatus is { } status)
            ApplyTradingStatus(securityId, status);
    }

    private void ApplyTradingStatus(ulong securityId, int status)
    {
        var book = GetOrCreateBook(securityId);
        if (book.TradingStatus == status)
            return;

        book.TradingStatus = status;
        // Intentionally do NOT re-bucket existing crosses across phase transitions.
        // A cross is attributed to the phase in which it first appeared (CrossedInAuction).
        // This avoids inflating the "trading anomaly" counter when an auction-era cross
        // is still being unwound after the book transitions to OPEN.
    }

    private void CheckCrossing(OrderBook book, string operation, ulong orderId, long price, BookSideType side)
    {
        var bestBid = book.Bids.BestPrice();
        var bestAsk = book.Asks.BestPrice();
        bool nowCrossed = bestBid is { } b && bestAsk is { } a && b.Price >= a.Price;

        if (nowCrossed == book.IsCrossed)
            return; // no transition — suppress noise on books stuck in a crossed state

        book.IsCrossed = nowCrossed;
        bool isAuctionPhase = book.TradingStatus is { } st && st != (int)TradingSessionSubID.OPEN;

        if (nowCrossed)
        {
            var transitions = Interlocked.Increment(ref _crossingTransitions);
            // bestBid/bestAsk are guaranteed non-null here because nowCrossed required both sides populated.
            var bid = bestBid!.Value;
            var ask = bestAsk!.Value;
            bool locked = bid.Price == ask.Price;
            book.CrossedInAuction = isAuctionPhase;
            book.IsLocked = locked;

            if (isAuctionPhase)
            {
                Interlocked.Increment(ref _currentlyCrossedAuction);
                if (locked) Interlocked.Increment(ref _currentlyLockedBooks);
                _logger.LogDebug(
                    "AUCTION-{Kind}: secId={SecurityId} status={Status} after {Op} orderId={OrderId} " +
                    "bestBid={BestBid} bestAsk={BestAsk} rptSeq={RptSeq}",
                    locked ? "LOCKED" : "CROSSED", book.SecurityId, book.TradingStatus, operation, orderId,
                    bid.Price, ask.Price, book.LastRptSeq);
            }
            else
            {
                Interlocked.Increment(ref _currentlyCrossedBooks);
                if (locked) Interlocked.Increment(ref _currentlyLockedBooks);
                _logger.LogWarning(
                    "{Kind}: secId={SecurityId} after {Op} orderId={OrderId} price={Price} side={Side} " +
                    "bestBid={BestBid} bestAsk={BestAsk} rptSeq={RptSeq} status={Status} transitions={Transitions} currentlyCrossed={Crossed}",
                    locked ? "LOCKED" : "CROSSED",
                    book.SecurityId, operation, orderId, price, side,
                    bid.Price, ask.Price, book.LastRptSeq, book.TradingStatus,
                    transitions, Volatile.Read(ref _currentlyCrossedBooks));
            }
        }
        else
        {
            // Decrement against the bucket the cross originated in (not current phase).
            if (book.CrossedInAuction)
            {
                Interlocked.Decrement(ref _currentlyCrossedAuction);
                _logger.LogDebug(
                    "AUCTION-UNCROSSED: secId={SecurityId} after {Op} rptSeq={RptSeq}",
                    book.SecurityId, operation, book.LastRptSeq);
            }
            else
            {
                Interlocked.Decrement(ref _currentlyCrossedBooks);
                _logger.LogInformation(
                    "UNCROSSED: secId={SecurityId} after {Op} rptSeq={RptSeq} currentlyCrossed={Crossed}",
                    book.SecurityId, operation, book.LastRptSeq, Volatile.Read(ref _currentlyCrossedBooks));
            }

            if (book.IsLocked)
                Interlocked.Decrement(ref _currentlyLockedBooks);
            book.CrossedInAuction = false;
            book.IsLocked = false;
        }
    }

    /// <summary>
    /// Freeze the books dictionary for optimized lookups during the hot path.
    /// Called after all instruments are discovered (InstrDef + Snapshot).
    /// </summary>
    public void FreezeBooks()
    {
        _bookStore.Freeze();
    }

    public OrderBook GetOrCreateBook(ulong securityId) => _bookStore.GetOrCreate(securityId);

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        // Extract packet-level SendingTime for use in HandleTrade/HandleForwardTrade
        if (packet.TryGetHeader(out var pktHeader))
            _currentSendingTimeNs = pktHeader.SendingTime;

        try
        {
            var handler = new BookSbeHandler { Owner = this };
            SbeDispatcher.Dispatch(sbePayload, ref handler);
        }
        catch (Exception ex)
        {
            _parseErrors++;
            _logger.LogWarning(ex, "Error processing templateId={TemplateId}", templateId);
        }
    }

    /// <summary>
    /// Zero-cost (devirtualized via generic struct constraint) <see cref="ISbeMessageHandler"/>
    /// implementation that routes each known templateId to the corresponding
    /// <see cref="BookManager"/> Handle* method. Methods left empty here are intentional
    /// no-ops for templates this manager doesn't process (e.g. statistics, prices, news).
    /// <para>
    /// <b>Do not</b> wire <c>OnSequenceReset_1</c> here — sequence resets arrive via
    /// <see cref="IFeedEventHandler.OnSequenceReset"/> from the channel layer, not as
    /// inline SBE messages on the data feed.
    /// </para>
    /// </summary>
    private struct BookSbeHandler : ISbeMessageHandler
    {
        public BookManager Owner;

        public void OnSecurityDefinition_12(in SecurityDefinition_12DataReader reader, int blockLength, int version) => Owner.HandleSecurityDefinition(in reader);
        public void OnOrder_MBO_50(in Order_MBO_50DataReader reader, int blockLength, int version) => Owner.HandleOrder(in reader);
        public void OnDeleteOrder_MBO_51(in DeleteOrder_MBO_51DataReader reader, int blockLength, int version) => Owner.HandleDeleteOrder(in reader);
        public void OnMassDeleteOrders_MBO_52(in MassDeleteOrders_MBO_52DataReader reader, int blockLength, int version) => Owner.HandleMassDelete(in reader);
        public void OnTrade_53(in Trade_53DataReader reader, int blockLength, int version) => Owner.HandleTrade(in reader);
        public void OnEmptyBook_9(in EmptyBook_9DataReader reader, int blockLength, int version) => Owner.HandleEmptyBook(in reader);
        public void OnChannelReset_11(in ChannelReset_11DataReader reader, int blockLength, int version) => Owner.HandleChannelReset();
        public void OnForwardTrade_54(in ForwardTrade_54DataReader reader, int blockLength, int version) => Owner.HandleForwardTrade(in reader);
        public void OnExecutionSummary_55(in ExecutionSummary_55DataReader reader, int blockLength, int version) => Owner.HandleExecutionSummary(in reader);
        public void OnTradeBust_57(in TradeBust_57DataReader reader, int blockLength, int version) => Owner.HandleTradeBust(in reader);
        public void OnSnapshotFullRefresh_Header_30(in SnapshotFullRefresh_Header_30DataReader reader, int blockLength, int version) => Owner.HandleSnapshotHeader(in reader);
        public void OnSnapshotFullRefresh_Orders_MBO_71(in SnapshotFullRefresh_Orders_MBO_71DataReader reader, int blockLength, int version) => Owner.HandleSnapshotOrders(in reader);

        // Intentional no-ops: not consumed by the order book.
        public void OnSequenceReset_1(in SequenceReset_1DataReader reader, int blockLength, int version) { }
        public void OnSequence_2(in Sequence_2DataReader reader, int blockLength, int version) { }
        public void OnSecurityStatus_3(in SecurityStatus_3DataReader reader, int blockLength, int version) { }
        public void OnNews_5(in News_5DataReader reader, int blockLength, int version) { }
        public void OnSecurityGroupPhase_10(in SecurityGroupPhase_10DataReader reader, int blockLength, int version) { }
        public void OnOpeningPrice_15(in OpeningPrice_15DataReader reader, int blockLength, int version) { }
        public void OnTheoreticalOpeningPrice_16(in TheoreticalOpeningPrice_16DataReader reader, int blockLength, int version) { }
        public void OnClosingPrice_17(in ClosingPrice_17DataReader reader, int blockLength, int version) { }
        public void OnAuctionImbalance_19(in AuctionImbalance_19DataReader reader, int blockLength, int version) { }
        public void OnQuantityBand_21(in QuantityBand_21DataReader reader, int blockLength, int version) { }
        public void OnPriceBand_22(in PriceBand_22DataReader reader, int blockLength, int version) { }
        public void OnHighPrice_24(in HighPrice_24DataReader reader, int blockLength, int version) { }
        public void OnLowPrice_25(in LowPrice_25DataReader reader, int blockLength, int version) { }
        public void OnLastTradePrice_27(in LastTradePrice_27DataReader reader, int blockLength, int version) { }
        public void OnSettlementPrice_28(in SettlementPrice_28DataReader reader, int blockLength, int version) { }
        public void OnOpenInterest_29(in OpenInterest_29DataReader reader, int blockLength, int version) { }
        public void OnExecutionStatistics_56(in ExecutionStatistics_56DataReader reader, int blockLength, int version) { }
        public void OnHeaderMessage_0(in HeaderMessage_0DataReader reader, int blockLength, int version) { }
        public void OnUnknownMessage(int templateId, int blockLength, int version, ReadOnlySpan<byte> payload) { }
    }

    public void OnSequenceReset() => HandleSequenceReset();
    public void OnInstrumentDefinitionsComplete(int instrumentCount) { FreezeBooks(); }
    public void OnPacketProcessed() { _eventHandler?.OnBatchComplete(); }

    /// <summary>
    /// Spec §6.5.5.1 — SequenceVersion increment (weekly rollover / failover):
    /// the upstream feed restarts with SequenceNumber=1 in the new version.
    /// Treat as a full epoch reset (drop books, clear pending snapshots,
    /// reset registry). The new <paramref name="newVersion"/> is forwarded
    /// to <see cref="SnapshotApplier"/> so subsequent snapshots whose
    /// LastSequenceVersion is older are silently skipped (spec §7.2).
    /// </summary>
    public void OnSequenceVersionChanged(ushort newVersion)
    {
        Volatile.Write(ref _currentSequenceVersion, newVersion);
        ClearAllBooks();
        ResetPerSymbolEpoch($"SequenceVersionChanged → {newVersion}");
    }

    private int _currentSequenceVersion;

    /// <summary>
    /// Last SequenceVersion observed on the incremental stream and propagated
    /// here via <see cref="OnSequenceVersionChanged"/>. Read by
    /// <see cref="SnapshotApplier"/> to gate stale-version snapshots.
    /// 0 means "no version observed yet"; in that case no gating is applied.
    /// </summary>
    internal ushort CurrentSequenceVersion => (ushort)Volatile.Read(ref _currentSequenceVersion);

    // Feed thread is the sole writer for all book mutations — no locks needed.
    // Callbacks are inline (same thread) so no race condition exists.

    private void HandleSecurityDefinition(in SecurityDefinition_12DataReader reader)
    {
        ulong securityId = (ulong)reader.Data.SecurityID;
        GetOrCreateBook(securityId);
    }

    private void HandleOrder(in Order_MBO_50DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, Order_MBO_50Data.MESSAGE_ID,
                msg.RptSeq is { } orderRs ? (uint)orderRs : null, reader.Block, reader.BlockLength))
            return;

        var book = GetOrCreateBook(securityId);

        var side = msg.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
        var bookSide = book.GetSide(side);
        ulong orderId = (ulong)msg.SecondaryOrderID;

        long? rawPrice = msg.MDEntryPx is { } px ? px.Mantissa : null;
        long quantity = (long)msg.MDEntrySize;
        uint enteringFirm = msg.EnteringFirm is { } ef ? (uint)ef : 0;

        // Spec §12.1 — null-price orders (MOA / MOC) belong to the per-side
        // market tier, not the priced BookSide. Routed early to keep the
        // priced-side branch focused on real prices.
        if (rawPrice is null)
        {
            HandleMarketOrder(book, bookSide, side, orderId, quantity, enteringFirm, msg.RptSeq);
            TrackMatchEvent(securityId, msg.MatchEventIndicator);
            return;
        }

        long price = rawPrice.Value;

        // The new message has a real price. If a market order with this id
        // exists, this is a phase-resolution transition (MOA → priced). Migrate
        // it out of the market tier before falling through to the priced path.
        if (book.TryRemoveMarketOrderAnySide(orderId, out var removedMarketSide))
        {
            _marketOrderTransitionsToPriced++;
            EmitMarketTierChanged(book, removedMarketSide);
        }

        if (bookSide.TryGetOrder(orderId, out var existing))
        {
            _orderUpdates++;
            long oldPrice = existing.Price;

            ref var slot = ref bookSide.GetOrderRef(orderId);
            slot.Price = price;
            slot.Quantity = quantity;
            slot.EnteringFirm = enteringFirm;

            if (oldPrice != price)
            {
                bookSide.MoveOrder(orderId, oldPrice);
            }
            else
            {
                // Same price — also update the per-level list copy so iteration sees the new
                // quantity/firm. MoveOrder takes care of this when price changes.
                bookSide.SyncPriceLevelCopy(orderId);
            }

            if (msg.RptSeq is { } rptSeq)
                TrackMboRptSeq(book, (uint)rptSeq);

            _eventHandler?.OnOrderUpdated(book, in slot);
            CheckCrossing(book, "UPDATE", orderId, price, side);
            TrackMatchEvent(securityId, msg.MatchEventIndicator);
        }
        else
        {
            var entry = new OrderBookEntry
            {
                OrderId = orderId,
                Price = price,
                Quantity = quantity,
                EnteringFirm = enteringFirm,
                SecurityId = securityId,
                Side = side
            };

            bookSide.Add(in entry);
            _orderAdds++;

            if (msg.RptSeq is { } rptSeq)
                TrackMboRptSeq(book, (uint)rptSeq);

            _eventHandler?.OnOrderAdded(book, in entry);
            CheckCrossing(book, "ADD", orderId, price, side);
            TrackMatchEvent(securityId, msg.MatchEventIndicator);
        }
    }

    /// <summary>
    /// Handles an Order_MBO with no MDEntryPx — a MOA/MOC market order per
    /// spec §12.1. Routes ADD/CHANGE through the per-side market tier; if the
    /// order previously lived in the priced BookSide (rare priced→null
    /// downgrade), migrates it across.
    /// </summary>
    private void HandleMarketOrder(OrderBook book, BookSide pricedSide, BookSideType side,
        ulong orderId, long quantity, uint enteringFirm, uint? rptSeq)
    {
        // Uncommon path: an order that was priced is being downgraded to
        // market. Drop it from the priced side first, fire the delete event so
        // priced-side subscribers see consistent state, then upsert into the
        // market tier.
        if (pricedSide.TryGetOrder(orderId, out _))
        {
            pricedSide.Remove(orderId);
            _nullPriceChangeDeletes++;
            _marketOrderTransitionsToMarket++;
            _eventHandler?.OnOrderDeleted(book, orderId, side);
        }

        bool isNew = book.UpsertMarketOrder(orderId, side, quantity, enteringFirm);
        if (isNew) _marketOrderAdds++;
        else _marketOrderUpdates++;

        if (rptSeq is { } rs) TrackMboRptSeq(book, (uint)rs);

        EmitMarketTierChanged(book, side);
    }

    private void HandleDeleteOrder(in DeleteOrder_MBO_51DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, DeleteOrder_MBO_51Data.MESSAGE_ID,
                msg.RptSeq is { } delRs ? (uint)delRs : null, reader.Block, reader.BlockLength))
            return;

        if (!TryLookupBook(securityId, out var book))
            return;

        var side = msg.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
        ulong orderId = (ulong)msg.SecondaryOrderID;

        var removed = book.GetSide(side).Remove(orderId);
        if (removed)
        {
            _orderDeletes++;
        }
        else if (book.RemoveMarketOrder(orderId, side))
        {
            // Market-order delete (MOA/MOC). The priced-order stream has no
            // null-price representation, so clients receive the aggregate
            // market-tier update instead of a per-order delete.
            _marketOrderDeletes++;
            EmitMarketTierChanged(book, side);
        }
        else
        {
            _deleteNotFound++;
        }

        if (msg.RptSeq is { } rptSeq)
            TrackMboRptSeq(book, (uint)rptSeq);

        if (removed)
            _eventHandler?.OnOrderDeleted(book, orderId, side);
        TrackMatchEvent(securityId, msg.MatchEventIndicator);
    }

    private void HandleMassDelete(in MassDeleteOrders_MBO_52DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, MassDeleteOrders_MBO_52Data.MESSAGE_ID,
                msg.RptSeq is { } mdRs ? (uint)mdRs : null, reader.Block, reader.BlockLength))
            return;

        if (!TryLookupBook(securityId, out var book))
            return;

        ApplyMassDelete(book, msg.MDEntryType, msg.RptSeq is { } rptSeq ? (uint)rptSeq : null);
        TrackMatchEvent(securityId, msg.MatchEventIndicator);
    }

    private void ApplyMassDelete(OrderBook book, MDEntryType entryType, uint? rptSeq)
    {
        BookClearSide clearSide;
        if (entryType == MDEntryType.BID)
        {
            book.Bids.Clear();
            book.ClearMarketOrders(BookSideType.Bid);
            clearSide = BookClearSide.Bid;
        }
        else if (entryType == MDEntryType.OFFER)
        {
            book.Asks.Clear();
            book.ClearMarketOrders(BookSideType.Ask);
            clearSide = BookClearSide.Ask;
        }
        else
        {
            book.Clear();
            clearSide = BookClearSide.Both;
        }

        if (rptSeq is { } sequence)
            TrackMboRptSeq(book, sequence);

        _eventHandler?.OnBookCleared(book.SecurityId, clearSide);
    }

    private void HandleTrade(in Trade_53DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, Trade_53Data.MESSAGE_ID,
                msg.RptSeq is { } trRs ? (uint)trRs : null, reader.Block, reader.BlockLength))
            return;

        long price = msg.MDEntryPx.Mantissa;
        long quantity = (long)msg.MDEntrySize;
        long tradeId = (long)(uint)msg.TradeID;
        long tradeTimeNs = (long)(msg.TransactTime.Time ?? _currentSendingTimeNs);

        if (TryLookupBook(securityId, out var book))
        {
            if (msg.RptSeq is { } rptSeq)
                TrackMboRptSeq(book, (uint)rptSeq);
        }

        // Spec §18: filter non-reportable trades (out-of-sequence / leg trades)
        // out of last-trade / candle / tape fanout. RptSeq already advanced.
        if (!IsReportableTrade(msg.TradeCondition, msg.TrdSubType))
        {
            Interlocked.Increment(ref _tradesFilteredNonReportable);
            TrackMatchEvent(securityId, msg.MatchEventIndicator);
            return;
        }

        _eventHandler?.OnTrade(securityId, price, quantity, tradeId, tradeTimeNs);
        TrackMatchEvent(securityId, msg.MatchEventIndicator);
    }

    private void HandleEmptyBook(in EmptyBook_9DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (TryLookupBook(securityId, out var book))
        {
            book.Clear();
            _eventHandler?.OnBookCleared(securityId, BookClearSide.Both);
        }

        // Per B3 spec ("EmptyBook resets RptSeq to 1"): the wire restarts
        // this instrument's RptSeq counter at 1 immediately after EmptyBook.
        // Drop any stale buffered messages from the prior epoch (their rptSeq
        // values are now meaningless under the new epoch) and reset the
        // registry baseline so the next Order at rptSeq=1 is contiguous.
        // Without this, the next Order would hit Healthy.Drop (1 <= lastSeen)
        // and the symbol would silently lose every subsequent update.
        _staleBuffer?.Clear(securityId);
        _stateRegistry?.ResetSymbolEpoch(securityId, SymbolGapKind.Mbo);
    }

    private void HandleForwardTrade(in ForwardTrade_54DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, ForwardTrade_54Data.MESSAGE_ID,
                msg.RptSeq is { } fwdRs ? (uint)fwdRs : null, reader.Block, reader.BlockLength))
            return;

        long price = msg.MDEntryPx.Mantissa;
        long quantity = (long)msg.MDEntrySize;
        long tradeId = (long)(uint)msg.TradeID;
        long tradeTimeNs = (long)(msg.TransactTime.Time ?? _currentSendingTimeNs);

        if (TryLookupBook(securityId, out var book))
        {
            if (msg.RptSeq is { } rptSeq)
                TrackMboRptSeq(book, (uint)rptSeq);
        }

        // Spec §18 filter applies to forward trades as well.
        if (!IsReportableTrade(msg.TradeCondition, msg.TrdSubType))
        {
            Interlocked.Increment(ref _tradesFilteredNonReportable);
            TrackMatchEvent(securityId, msg.MatchEventIndicator);
            return;
        }

        _eventHandler?.OnForwardTrade(securityId, price, quantity, tradeId, tradeTimeNs);
        TrackMatchEvent(securityId, msg.MatchEventIndicator);
    }

    private void HandleExecutionSummary(in ExecutionSummary_55DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, ExecutionSummary_55Data.MESSAGE_ID,
                msg.RptSeq is { } exRs ? (uint)exRs : null, reader.Block, reader.BlockLength))
            return;

        long lastPx = msg.LastPx.Mantissa;
        long fillQty = (long)msg.FillQty;

        if (TryLookupBook(securityId, out var book))
        {
            if (msg.RptSeq is { } rptSeq)
                TrackMboRptSeq(book, (uint)rptSeq);
        }

        _eventHandler?.OnExecutionSummary(securityId, lastPx, fillQty);
    }

    private void HandleTradeBust(in TradeBust_57DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, TradeBust_57Data.MESSAGE_ID,
                msg.RptSeq is { } tbRs ? (uint)tbRs : null, reader.Block, reader.BlockLength))
            return;

        long price = msg.MDEntryPx.Mantissa;
        long quantity = (long)msg.MDEntrySize;
        long tradeId = (long)(uint)msg.TradeID;

        if (TryLookupBook(securityId, out var book))
        {
            if (msg.RptSeq is { } rptSeq)
                TrackMboRptSeq(book, (uint)rptSeq);
        }

        _eventHandler?.OnTradeBust(securityId, price, quantity, tradeId);
        TrackMatchEvent(securityId, msg.MatchEventIndicator);
    }

    // ── Snapshot lifecycle (delegated to SnapshotApplier) ────────────────────

    /// <summary>Number of snapshot cycles where the registry was successfully healed.</summary>
    public long SnapshotsHealed => _snapshotApplier.SnapshotsHealed;
    /// <summary>Snapshots received without a usable LastRptSeq (cannot heal).</summary>
    public long SnapshotsMissingRptSeq => _snapshotApplier.SnapshotsMissingRptSeq;
    /// <summary>Orders_71 chunks dropped because no Header_30 was seen first for that securityID.</summary>
    public long SnapshotChunksOrphaned => _snapshotApplier.SnapshotChunksOrphaned;
    /// <summary>Snapshots rejected because their LastRptSeq is older than the symbol's MinHealRptSeq.</summary>
    public long SnapshotsRejectedTooOld => _snapshotApplier.SnapshotsRejectedTooOld;
    /// <summary>Snapshots ignored at Header_30 because the symbol is already Healthy with a more recent baseline.</summary>
    public long SnapshotsSkippedHealthyAhead => _snapshotApplier.SnapshotsSkippedHealthyAhead;
    /// <summary>Snapshots silently skipped because their LastSequenceVersion is older than the channel's current SequenceVersion (B3 spec §7.2).</summary>
    public long SnapshotsRejectedStaleVersion => _snapshotApplier.SnapshotsRejectedStaleVersion;

    private void HandleSnapshotHeader(in SnapshotFullRefresh_Header_30DataReader reader) => _snapshotApplier.OnHeader(in reader);
    private void HandleSnapshotOrders(in SnapshotFullRefresh_Orders_MBO_71DataReader reader) => _snapshotApplier.OnOrdersChunk(in reader);

    // Exposed internally for tests that don't want to forge raw SBE bytes.
    internal void BeginSnapshotHeader(ulong secId, uint lastRptSeq, bool hasRptSeq, uint ordersExpected)
        => _snapshotApplier.BeginHeader(secId, lastRptSeq, hasRptSeq, ordersExpected);
    internal void BeginChunkedSnapshotForTest(ulong securityId, uint lastRptSeq, uint ordersExpected)
        => _snapshotApplier.BeginChunkedSnapshotForTest(securityId, lastRptSeq, ordersExpected);
    internal void RecordSnapshotChunkForTest(ulong securityId, uint ordersInChunk)
        => _snapshotApplier.RecordSnapshotChunkForTest(securityId, ordersInChunk);
    // Exposed internally for tests: simulates a wire snapshot Header_30 with an
    // explicit LastSequenceVersion, so the version-gate path can be exercised
    // without forging raw SBE bytes.
    internal void OnSnapshotHeaderForTest(ulong securityId, uint lastRptSeq, uint ordersExpected, ushort? lastSequenceVersion)
        => _snapshotApplier.OnHeaderForTest(securityId, lastRptSeq, ordersExpected, lastSequenceVersion);
    internal void StageSnapshotEntryForTest(ulong securityId, BookSideType side, ulong orderId, long price, long quantity)
        => _snapshotApplier.StageSnapshotEntryForTest(securityId, side, orderId, price, quantity);
    internal void StageSnapshotMarketOrderForTest(ulong securityId, BookSideType side, ulong orderId, long quantity, uint enteringFirm = 0)
        => _snapshotApplier.StageSnapshotMarketOrderForTest(securityId, side, orderId, quantity, enteringFirm);
    internal void RecordSnapshotHeader(ulong securityId, uint? lastRptSeq)
        => _snapshotApplier.RecordSnapshotHeader(securityId, lastRptSeq);
    internal void HealAfterSnapshotForTest(ulong securityId)
        => _snapshotApplier.HealAfterSnapshotForTest(securityId);
    internal void HandleEmptyBookForTest(ReadOnlySpan<byte> body)
    {
        if (EmptyBook_9Data.TryParse(body, out var reader))
            HandleEmptyBook(in reader);
    }
    internal void HandleMassDeleteForTest(ulong securityId, MDEntryType entryType, uint? rptSeq = null)
        => ApplyMassDelete(GetOrCreateBook(securityId), entryType, rptSeq);

    /// <summary>
    /// Fast book lookup — uses FrozenDictionary when available (hot path),
    /// falls back to mutable dictionary during setup.
    /// </summary>
    private bool TryLookupBook(ulong securityId, out OrderBook book)
        => _bookStore.TryGet(securityId, out book);

    private void ClearAllBooks()
    {
        foreach (var (secId, book) in _bookStore.Books)
        {
            book.Clear();
            _eventHandler?.OnBookCleared(secId, BookClearSide.Both);
        }
    }

    private void EmitMarketTierChanged(OrderBook book, BookSideType side)
    {
        _eventHandler?.OnMarketTierChanged(
            book,
            side,
            book.MarketOrderQuantity(side),
            book.MarketOrderCount(side));
    }

    private long _epochResets;
    private long _epochResetMessagesDropped;

    /// <summary>Number of catastrophic resets (ChannelReset_11 or SequenceReset) seen.</summary>
    public long EpochResets => Volatile.Read(ref _epochResets);
    /// <summary>Buffered MBO messages dropped during epoch resets.</summary>
    public long EpochResetMessagesDropped => Volatile.Read(ref _epochResetMessagesDropped);

    private void HandleChannelReset()
    {
        ClearAllBooks();
        ResetPerSymbolEpoch("ChannelReset_11");
    }

    private void HandleSequenceReset()
    {
        ClearAllBooks();
        ResetPerSymbolEpoch("SequenceReset");
    }

    /// <summary>
    /// Catastrophic-reset path: drop all buffered MBO bodies, clear pending
    /// snapshot headers, and reset the registry epoch (every (symbol, kind)
    /// → Unknown).
    /// </summary>
    private void ResetPerSymbolEpoch(string reason)
    {
        int dropped = _staleBuffer.ClearAll();
        _snapshotApplier.Clear();
        _stateRegistry.ResetEpoch(reason);
        Interlocked.Add(ref _epochResetMessagesDropped, dropped);
        Interlocked.Increment(ref _epochResets);
    }
}
