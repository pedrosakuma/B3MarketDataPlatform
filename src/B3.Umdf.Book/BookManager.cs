using System.Collections.Concurrent;
using System.Collections.Frozen;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Order_MBO_50Data = B3.Umdf.Mbo.Sbe.V16.V6.Order_MBO_50Data;
using DeleteOrder_MBO_51Data = B3.Umdf.Mbo.Sbe.V16.V6.DeleteOrder_MBO_51Data;
using MassDeleteOrders_MBO_52Data = B3.Umdf.Mbo.Sbe.V16.V6.MassDeleteOrders_MBO_52Data;
using Trade_53Data = B3.Umdf.Mbo.Sbe.V16.V15.Trade_53Data;
using ForwardTrade_54Data = B3.Umdf.Mbo.Sbe.V16.V15.ForwardTrade_54Data;
using ExecutionSummary_55Data = B3.Umdf.Mbo.Sbe.V16.V6.ExecutionSummary_55Data;
using TradeBust_57Data = B3.Umdf.Mbo.Sbe.V16.V6.TradeBust_57Data;

namespace B3.Umdf.Book;

public sealed class BookManager : IFeedEventHandler, IMarketDataEventHandler
{
    private readonly ConcurrentDictionary<ulong, OrderBook> _books = new(Environment.ProcessorCount, 4096);
    private volatile FrozenDictionary<ulong, OrderBook>? _frozenBooks;
    private readonly IBookEventHandler? _eventHandler;
    private readonly ILogger<BookManager> _logger;
    private long _parseErrors;
    private long _orderAdds;
    private long _orderUpdates;
    private long _orderDeletes;
    private long _deleteNotFound;
    private long _nullPriceNewSkips;
    private long _nullPriceChangeDeletes;

    /// <summary>
    /// Packet-level SendingTime (nanoseconds since epoch) for the message currently being processed.
    /// Set at the start of OnPacket, consumed by Handle* methods.
    /// </summary>
    private ulong _currentSendingTimeNs;

    /// <summary>
    /// Thread-safe dictionary of all order books.
    /// Safe to read (Count, iterate) from any thread while the feed thread writes.
    /// </summary>
    public IReadOnlyDictionary<ulong, OrderBook> Books => _books;

    /// <summary>Number of SBE parse errors encountered (malformed packets).</summary>
    public long ParseErrors => Volatile.Read(ref _parseErrors);
    public long OrderAdds => Volatile.Read(ref _orderAdds);
    public long OrderUpdates => Volatile.Read(ref _orderUpdates);
    public long OrderDeletes => Volatile.Read(ref _orderDeletes);
    public long DeleteNotFound => Volatile.Read(ref _deleteNotFound);
    public long NullPriceNewSkips => Volatile.Read(ref _nullPriceNewSkips);
    public long NullPriceChangeDeletes => Volatile.Read(ref _nullPriceChangeDeletes);

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
        StaleMboBuffer? staleBuffer = null,
        RecoveryMode recoveryMode = RecoveryMode.Channel)
    {
        _eventHandler = eventHandler;
        _logger = logger ?? NullLogger<BookManager>.Instance;
        GapTracker = new SymbolGapTracker(_logger);
        _recoveryMode = recoveryMode;
        if (recoveryMode == RecoveryMode.PerSymbol)
        {
            _stateRegistry = stateRegistry ?? throw new ArgumentNullException(nameof(stateRegistry),
                "SymbolStateRegistry is required when RecoveryMode is PerSymbol.");
            _staleBuffer = staleBuffer ?? throw new ArgumentNullException(nameof(staleBuffer),
                "StaleMboBuffer is required when RecoveryMode is PerSymbol.");
        }
        else
        {
            _stateRegistry = stateRegistry;
            _staleBuffer = staleBuffer;
        }
    }

    private readonly RecoveryMode _recoveryMode;
    private readonly SymbolStateRegistry? _stateRegistry;
    private readonly StaleMboBuffer? _staleBuffer;
    private long _bufferedMboMessages;
    private long _replayedMboMessages;

    /// <summary>The recovery mode this BookManager is operating in.</summary>
    public RecoveryMode RecoveryMode => _recoveryMode;

    /// <summary>Symbol state registry (non-null only when <see cref="RecoveryMode"/> is PerSymbol).</summary>
    public SymbolStateRegistry? StateRegistry => _stateRegistry;

    /// <summary>Stale MBO buffer (non-null only when <see cref="RecoveryMode"/> is PerSymbol).</summary>
    public StaleMboBuffer? StaleBuffer => _staleBuffer;

    public long BufferedMboMessages => Volatile.Read(ref _bufferedMboMessages);
    public long ReplayedMboMessages => Volatile.Read(ref _replayedMboMessages);

    /// <summary>
    /// Phase 0 shadow tracker for per-symbol rptSeq gaps on MBO/Trade
    /// messages (which share one rptSeq stream per security in the B3
    /// schema). Read-only — does not influence the channel-level Recovery
    /// state machine.
    /// </summary>
    public SymbolGapTracker GapTracker { get; }

    /// <summary>
    /// Records the per-symbol rptSeq gap (if any) and updates the book's
    /// <see cref="OrderBook.LastRptSeq"/>. Called from every MBO/Trade
    /// handler instead of writing <c>LastRptSeq</c> directly.
    /// In PerSymbol mode the registry is the source of truth, so we skip
    /// the Phase 0 shadow tracker (which would double-count gaps).
    /// </summary>
    private void TrackMboRptSeq(OrderBook book, uint received)
    {
        if (_recoveryMode != RecoveryMode.PerSymbol)
            GapTracker.Observe(book.SecurityId, received, book.LastRptSeq, SymbolGapKind.Mbo);
        book.LastRptSeq = received;
    }

    /// <summary>
    /// Per-symbol routing decision for an incoming MBO/Trade message.
    /// In PerSymbol mode, consults the registry: Apply lets the caller
    /// proceed; Buffer copies the body into <see cref="_staleBuffer"/> for
    /// later replay; Drop returns silently. In Channel mode, always Apply.
    /// </summary>
    /// <returns><c>true</c> if the caller should proceed with apply logic.</returns>
    private bool RouteMbo(ulong securityId, ushort templateId, uint? rptSeqOpt, ReadOnlySpan<byte> body)
    {
        if (_recoveryMode != RecoveryMode.PerSymbol) return true;
        if (rptSeqOpt is not { } rptSeq || rptSeq == 0) return true; // can't gap-track without rptSeq
        var result = _stateRegistry!.Observe(securityId, SymbolGapKind.Mbo, rptSeq);
        switch (result.Action)
        {
            case SymbolStateRegistry.ObserveAction.Apply:
                return true;
            case SymbolStateRegistry.ObserveAction.Buffer:
                if (_staleBuffer!.Enqueue(securityId, templateId, rptSeq, _currentSendingTimeNs, body))
                    Interlocked.Increment(ref _bufferedMboMessages);
                return false;
            case SymbolStateRegistry.ObserveAction.Drop:
            default:
                return false;
        }
    }

    /// <summary>
    /// Drain and replay the per-symbol stale buffer for one security after
    /// a snapshot heal. Called by the snapshot handler in PerSymbol mode.
    /// Messages with <c>rptSeq ∈ [drainFrom, drainTo]</c> are dispatched
    /// through the same handlers as live messages — the registry will see
    /// them as Healthy + contiguous and route them through Apply.
    /// </summary>
    internal int ReplayDeferredMbo(ulong securityId, uint drainFrom, uint drainTo)
    {
        if (_recoveryMode != RecoveryMode.PerSymbol || _staleBuffer is null) return 0;
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
        _frozenBooks = _books.ToFrozenDictionary();
    }

    public OrderBook GetOrCreateBook(ulong securityId)
    {
        // Fast path: frozen dictionary lookup (optimized hash)
        if (_frozenBooks is { } frozen)
        {
            if (frozen.TryGetValue(securityId, out var book))
                return book;
        }

        return _books.GetOrAdd(securityId, static id => new OrderBook(id));
    }

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

    public void OnGapDetected(uint expected, uint received) { }
    public void OnSequenceReset() => HandleSequenceReset();
    public void OnSnapshotStart() { }
    public void OnSnapshotComplete(uint lastRptSeq) { FreezeBooks(); }
    public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
    public void OnPacketProcessed() { _eventHandler?.OnBatchComplete(); }

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

    /// <summary>
     /// Per-symbol pending snapshot lastRptSeq indexed by SecurityID. Populated by
     /// <see cref="HandleSnapshotHeader"/> and consumed by <see cref="HandleSnapshotOrders"/>
     /// to drive registry healing in PerSymbol mode. Header_30 always precedes its
     /// matching 71 body in B3's snapshot stream for the same instrument.
     /// </summary>
    private readonly Dictionary<ulong, uint> _pendingSnapshotLastRptSeq = new();
    private long _snapshotsHealed;
    private long _snapshotsMissingRptSeq;

    /// <summary>Number of snapshot cycles where the registry was successfully healed.</summary>
    public long SnapshotsHealed => Volatile.Read(ref _snapshotsHealed);
    /// <summary>Snapshots received in PerSymbol mode without a usable LastRptSeq (cannot heal).</summary>
    public long SnapshotsMissingRptSeq => Volatile.Read(ref _snapshotsMissingRptSeq);

    private void HandleSnapshotHeader(ReadOnlySpan<byte> body)
    {
        if (_recoveryMode != RecoveryMode.PerSymbol) return;
        if (!SnapshotFullRefresh_Header_30Data.TryParse(body, out var reader)) return;

        ref readonly var msg = ref reader.Data;
        RecordSnapshotHeader((ulong)msg.SecurityID, msg.LastRptSeq);
    }

    /// <summary>
    /// Cache the per-symbol lastRptSeq from a snapshot header so the next
    /// matching snapshot body can heal the registry. Exposed internally for
    /// tests that bypass SBE parsing.
    /// </summary>
    internal void RecordSnapshotHeader(ulong securityId, uint? lastRptSeq)
    {
        if (_recoveryMode != RecoveryMode.PerSymbol) return;
        if (lastRptSeq is { } v && v > 0)
            _pendingSnapshotLastRptSeq[securityId] = v;
        else
            _pendingSnapshotLastRptSeq.Remove(securityId);
    }

    /// <summary>
    /// Run the post-snapshot heal flow for a security: transition the registry
    /// to Healthy at the cached lastRptSeq baseline and replay any buffered
    /// messages in the heal window. Exposed internally for tests.
    /// </summary>
    internal void HealAfterSnapshotForTest(ulong securityId)
    {
        var book = GetOrCreateBook(securityId);
        HealAfterSnapshot(securityId, book);
    }

    private void HandleSnapshotOrders(ReadOnlySpan<byte> body)
    {
        if (!SnapshotFullRefresh_Orders_MBO_71Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var book = GetOrCreateBook(securityId);

        book.Clear();

        reader.ReadGroups((in SnapshotFullRefresh_Orders_MBO_71Data.NoMDEntriesData entry) =>
        {
            long? rawPrice = entry.MDEntryPx.Mantissa;
            if (rawPrice is null)
                return; // Market orders have no price — skip

            var side = entry.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
            long price = rawPrice.Value;
            long quantity = (long)entry.MDEntrySize;
            ulong orderId = (ulong)entry.SecondaryOrderID;
            uint enteringFirm = entry.EnteringFirm.Value ?? 0;

            var bookEntry = new OrderBookEntry
            {
                OrderId = orderId,
                Price = price,
                Quantity = quantity,
                EnteringFirm = enteringFirm,
                SecurityId = securityId,
                Side = side
            };

            book.GetSide(side).Add(in bookEntry);
        });

        if (_recoveryMode == RecoveryMode.PerSymbol)
            HealAfterSnapshot(securityId, book);
    }

    private void HealAfterSnapshot(ulong securityId, OrderBook book)
    {
        if (!_pendingSnapshotLastRptSeq.Remove(securityId, out var snapshotRptSeq))
        {
            // No header (or no LastRptSeq) — cannot transition to Healthy without a baseline.
            // Symbol stays Stale; subsequent incremental will continue buffering until next
            // snapshot cycle delivers a usable header. Counter helps surface schema/feed issues.
            Interlocked.Increment(ref _snapshotsMissingRptSeq);
            return;
        }

        book.LastRptSeq = snapshotRptSeq;
        var heal = _stateRegistry!.HealFromSnapshot(securityId, SymbolGapKind.Mbo, snapshotRptSeq);
        if (heal.TransitionedToHealthy)
            Interlocked.Increment(ref _snapshotsHealed);

        if (heal.DrainTo >= heal.DrainFrom)
        {
            int replayed = ReplayDeferredMbo(securityId, heal.DrainFrom, heal.DrainTo);
            if (replayed > 0 && _logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(
                    "PerSymbol heal SecID={SecId}: snapshotRpt={Snap} drain=[{From},{To}] replayed={Replayed}",
                    securityId, snapshotRptSeq, heal.DrainFrom, heal.DrainTo, replayed);
        }
        else
        {
            // No drain window — every buffered message is at-or-below the snapshot
            // baseline (already covered). Drop them.
            _staleBuffer!.Clear(securityId);
        }
    }

    /// <summary>
    /// Fast book lookup — uses FrozenDictionary when available (hot path),
    /// falls back to mutable dictionary during setup.
    /// </summary>
    private bool TryLookupBook(ulong securityId, out OrderBook book)
    {
        if (_frozenBooks is { } frozen && frozen.TryGetValue(securityId, out book!))
            return true;
        return _books.TryGetValue(securityId, out book!);
    }

    private void ClearAllBooks()
    {
        foreach (var (secId, book) in _books)
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
    /// In PerSymbol mode, drive the catastrophic-reset path: drop all
    /// buffered MBO bodies, clear pending snapshot headers, and reset the
    /// registry epoch (every (symbol, kind) → Unknown). Channel mode is a
    /// no-op since there is no per-symbol state to reset.
    /// </summary>
    private void ResetPerSymbolEpoch(string reason)
    {
        if (_recoveryMode != RecoveryMode.PerSymbol) return;
        int dropped = _staleBuffer!.ClearAll();
        _pendingSnapshotLastRptSeq.Clear();
        _stateRegistry!.ResetEpoch(reason);
        Interlocked.Add(ref _epochResetMessagesDropped, dropped);
        Interlocked.Increment(ref _epochResets);
    }
}
