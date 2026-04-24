using System.Runtime.InteropServices;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Order_MBO_50Data = B3.Umdf.Mbo.Sbe.V16.V15.Order_MBO_50Data;
using DeleteOrder_MBO_51Data = B3.Umdf.Mbo.Sbe.V16.V15.DeleteOrder_MBO_51Data;
using MassDeleteOrders_MBO_52Data = B3.Umdf.Mbo.Sbe.V16.V15.MassDeleteOrders_MBO_52Data;
using Trade_53Data = B3.Umdf.Mbo.Sbe.V16.V15.Trade_53Data;
using ForwardTrade_54Data = B3.Umdf.Mbo.Sbe.V16.V15.ForwardTrade_54Data;
using ExecutionSummary_55Data = B3.Umdf.Mbo.Sbe.V16.V15.ExecutionSummary_55Data;
using TradeBust_57Data = B3.Umdf.Mbo.Sbe.V16.V15.TradeBust_57Data;

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
    private long _nullPriceNewSkips;
    private long _nullPriceChangeDeletes;
    private long _tradesFilteredNonReportable;

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
    public long NullPriceNewSkips => Volatile.Read(ref _nullPriceNewSkips);
    public long NullPriceChangeDeletes => Volatile.Read(ref _nullPriceChangeDeletes);

    /// <summary>
    /// Trades suppressed from <see cref="IBookEventHandler.OnTrade"/> /
    /// <see cref="IBookEventHandler.OnForwardTrade"/> because they are
    /// flagged as non-reportable per B3 spec §18 (TradeCondition.OutOfSequence
    /// or TrdSubType=LEG_TRADE). The rptSeq stream is still advanced for
    /// these messages — only downstream fanout (trade tape, candles, last
    /// price) is gated.
    /// </summary>
    public long TradesFilteredNonReportable => Volatile.Read(ref _tradesFilteredNonReportable);

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
    private bool RouteMbo(ulong securityId, ushort templateId, uint? rptSeqOpt, ReadOnlySpan<byte> body)
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
                        }))
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
                case Order_MBO_50Data.MESSAGE_ID: HandleOrder(m.Span); break;
                case DeleteOrder_MBO_51Data.MESSAGE_ID: HandleDeleteOrder(m.Span); break;
                case MassDeleteOrders_MBO_52Data.MESSAGE_ID: HandleMassDelete(m.Span); break;
                case Trade_53Data.MESSAGE_ID: HandleTrade(m.Span, Trade_53Data.MESSAGE_SIZE); break;
                case ForwardTrade_54Data.MESSAGE_ID: HandleForwardTrade(m.Span, ForwardTrade_54Data.MESSAGE_SIZE); break;
                case ExecutionSummary_55Data.MESSAGE_ID: HandleExecutionSummary(m.Span); break;
                case TradeBust_57Data.MESSAGE_ID: HandleTradeBust(m.Span); break;
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
        if (!MessageHeader.TryParse(sbePayload, out var header, out _))
            return;

        // Extract packet-level SendingTime for use in HandleTrade/HandleForwardTrade
        if (packet.TryGetHeader(out var pktHeader))
            _currentSendingTimeNs = pktHeader.SendingTime;

        var body = sbePayload[MessageHeader.MESSAGE_SIZE..];

        try
        {
            switch (templateId)
            {
                case SecurityDefinition_12Data.MESSAGE_ID:
                    HandleSecurityDefinition(body, header.BlockLength);
                    break;
                case Order_MBO_50Data.MESSAGE_ID:
                    HandleOrder(body);
                    break;
                case DeleteOrder_MBO_51Data.MESSAGE_ID:
                    HandleDeleteOrder(body);
                    break;
                case MassDeleteOrders_MBO_52Data.MESSAGE_ID:
                    HandleMassDelete(body);
                    break;
                case Trade_53Data.MESSAGE_ID:
                    HandleTrade(body, header.BlockLength);
                    break;
                case EmptyBook_9Data.MESSAGE_ID:
                    HandleEmptyBook(body);
                    break;
                case ChannelReset_11Data.MESSAGE_ID:
                    HandleChannelReset();
                    break;
                case ForwardTrade_54Data.MESSAGE_ID:
                    HandleForwardTrade(body, header.BlockLength);
                    break;
                case ExecutionSummary_55Data.MESSAGE_ID:
                    HandleExecutionSummary(body);
                    break;
                case TradeBust_57Data.MESSAGE_ID:
                    HandleTradeBust(body);
                    break;
                case SnapshotFullRefresh_Header_30Data.MESSAGE_ID:
                    HandleSnapshotHeader(body);
                    break;
                case SnapshotFullRefresh_Orders_MBO_71Data.MESSAGE_ID:
                    HandleSnapshotOrders(body);
                    break;
                default:
                    break;
            }
        }
        catch (Exception ex)
        {
            _parseErrors++;
            _logger.LogWarning(ex, "Error processing templateId={TemplateId}", templateId);
        }
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

    private void HandleSecurityDefinition(ReadOnlySpan<byte> body, int blockLength)
    {
        if (!SecurityDefinition_12Data.TryParse(body, blockLength, out var reader))
            return;

        ulong securityId = (ulong)reader.Data.SecurityID;
        GetOrCreateBook(securityId);
    }

    private void HandleOrder(ReadOnlySpan<byte> body)
    {
        if (!Order_MBO_50Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, Order_MBO_50Data.MESSAGE_ID,
                msg.RptSeq is { } orderRs ? (uint)orderRs : null, body))
            return;

        var book = GetOrCreateBook(securityId);

        var side = msg.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
        var bookSide = book.GetSide(side);
        ulong orderId = (ulong)msg.SecondaryOrderID;

        long? rawPrice = msg.MDEntryPx is { } px ? px.Mantissa : null;
        long quantity = (long)msg.MDEntrySize;
        uint enteringFirm = msg.EnteringFirm is { } ef ? (uint)ef : 0;

        if (bookSide.TryGetOrder(orderId, out var existing))
        {
            if (rawPrice is null)
            {
                _nullPriceChangeDeletes++;
                bookSide.Remove(orderId);
                if (msg.RptSeq is { } rs) TrackMboRptSeq(book, (uint)rs);
                _eventHandler?.OnOrderDeleted(book, orderId, side);
            }
            else
            {
                _orderUpdates++;
                long price = rawPrice.Value;
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
            }
        }
        else
        {
            if (rawPrice is null)
            {
                _nullPriceNewSkips++;
                return;
            }

            long price = rawPrice.Value;
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
        }
    }

    private void HandleDeleteOrder(ReadOnlySpan<byte> body)
    {
        if (!DeleteOrder_MBO_51Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, DeleteOrder_MBO_51Data.MESSAGE_ID,
                msg.RptSeq is { } delRs ? (uint)delRs : null, body))
            return;

        if (!TryLookupBook(securityId, out var book))
            return;

        var side = msg.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
        ulong orderId = (ulong)msg.SecondaryOrderID;

        var removed = book.GetSide(side).Remove(orderId);
        if (removed)
            _orderDeletes++;
        else
            _deleteNotFound++;

        if (msg.RptSeq is { } rptSeq)
            TrackMboRptSeq(book, (uint)rptSeq);

        _eventHandler?.OnOrderDeleted(book, orderId, side);
    }

    private void HandleMassDelete(ReadOnlySpan<byte> body)
    {
        if (!MassDeleteOrders_MBO_52Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, MassDeleteOrders_MBO_52Data.MESSAGE_ID,
                msg.RptSeq is { } mdRs ? (uint)mdRs : null, body))
            return;

        if (!TryLookupBook(securityId, out var book))
            return;

        BookClearSide clearSide;
        var entryType = msg.MDEntryType;
        if (entryType == MDEntryType.BID)
        {
            book.Bids.Clear();
            clearSide = BookClearSide.Bid;
        }
        else if (entryType == MDEntryType.OFFER)
        {
            book.Asks.Clear();
            clearSide = BookClearSide.Ask;
        }
        else
        {
            book.Clear();
            clearSide = BookClearSide.Both;
        }

        if (msg.RptSeq is { } rptSeq)
            TrackMboRptSeq(book, (uint)rptSeq);

        _eventHandler?.OnBookCleared(securityId, clearSide);
    }

    private void HandleTrade(ReadOnlySpan<byte> body, int blockLength)
    {
        if (!Trade_53Data.TryParse(body, blockLength, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, Trade_53Data.MESSAGE_ID,
                msg.RptSeq is { } trRs ? (uint)trRs : null, body))
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
            return;
        }

        _eventHandler?.OnTrade(securityId, price, quantity, tradeId, tradeTimeNs);
    }

    private void HandleEmptyBook(ReadOnlySpan<byte> body)
    {
        if (!EmptyBook_9Data.TryParse(body, out var reader))
            return;

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

    private void HandleForwardTrade(ReadOnlySpan<byte> body, int blockLength)
    {
        if (!ForwardTrade_54Data.TryParse(body, blockLength, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, ForwardTrade_54Data.MESSAGE_ID,
                msg.RptSeq is { } fwdRs ? (uint)fwdRs : null, body))
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
            return;
        }

        _eventHandler?.OnForwardTrade(securityId, price, quantity, tradeId, tradeTimeNs);
    }

    private void HandleExecutionSummary(ReadOnlySpan<byte> body)
    {
        if (!ExecutionSummary_55Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, ExecutionSummary_55Data.MESSAGE_ID,
                msg.RptSeq is { } exRs ? (uint)exRs : null, body))
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

    private void HandleTradeBust(ReadOnlySpan<byte> body)
    {
        if (!TradeBust_57Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!RouteMbo(securityId, TradeBust_57Data.MESSAGE_ID,
                msg.RptSeq is { } tbRs ? (uint)tbRs : null, body))
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

    private void HandleSnapshotHeader(ReadOnlySpan<byte> body) => _snapshotApplier.OnHeader(body);
    private void HandleSnapshotOrders(ReadOnlySpan<byte> body) => _snapshotApplier.OnOrdersChunk(body);

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
    internal void RecordSnapshotHeader(ulong securityId, uint? lastRptSeq)
        => _snapshotApplier.RecordSnapshotHeader(securityId, lastRptSeq);
    internal void HealAfterSnapshotForTest(ulong securityId)
        => _snapshotApplier.HealAfterSnapshotForTest(securityId);
    internal void HandleEmptyBookForTest(ReadOnlySpan<byte> body) => HandleEmptyBook(body);

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
