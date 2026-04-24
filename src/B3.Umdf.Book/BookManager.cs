using System.Collections.Concurrent;
using System.Collections.Frozen;
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
        StaleMboBuffer? staleBuffer = null)
    {
        _eventHandler = eventHandler;
        _logger = logger ?? NullLogger<BookManager>.Instance;
        GapTracker = new SymbolGapTracker(_logger);
        _stateRegistry = stateRegistry ?? throw new ArgumentNullException(nameof(stateRegistry),
            "SymbolStateRegistry is required.");
        _staleBuffer = staleBuffer ?? throw new ArgumentNullException(nameof(staleBuffer),
            "StaleMboBuffer is required.");
    }

    private readonly SymbolStateRegistry _stateRegistry;
    private readonly StaleMboBuffer _staleBuffer;
    private long _bufferedMboMessages;
    private long _replayedMboMessages;
    private long _mboStaleTransitions;
    private long _mboStaleGapSizeSum;
    private long _mboStaleGapSizeMax;

    /// <summary>Symbol state registry (PerSymbol recovery is the only supported mode).</summary>
    public SymbolStateRegistry StateRegistry => _stateRegistry;

    /// <summary>Stale MBO buffer (PerSymbol recovery is the only supported mode).</summary>
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
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "PerSymbol Healthy→Stale secId={SecurityId} templateId={TemplateId} rptSeq={RptSeq} gap={Gap}",
                    securityId, templateId, rptSeq, gap);
            _eventHandler?.OnSymbolStaleStatusChanged(securityId, isStale: true);
        }
        switch (result.Action)
        {
            case SymbolStateRegistry.ObserveAction.Apply:
                return true;
            case SymbolStateRegistry.ObserveAction.Buffer:
                if (_staleBuffer.Enqueue(securityId, templateId, rptSeq, _currentSendingTimeNs, body))
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

    public void OnSequenceReset() => HandleSequenceReset();
    public void OnInstrumentDefinitionsComplete(int instrumentCount) { FreezeBooks(); }
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
    /// Per-symbol pending snapshot state indexed by SecurityID. A B3 MBO snapshot for one
    /// instrument arrives as 1× Header_30 followed by N× Orders_71 chunks ("Partial list of
    /// orders"); the sum of order entries across chunks equals
    /// <c>TotNumBids + TotNumOffers</c> from the header. The book is cleared once on
    /// Header_30, every Orders_71 chunk appends to the same book, and the symbol is healed
    /// only when all expected entries have been received.
    /// </summary>
    private readonly Dictionary<ulong, PendingSnapshot> _pendingSnapshots = new();
    private struct PendingSnapshot
    {
        public uint LastRptSeq;        // 0 if no usable rptSeq baseline
        public uint OrdersExpected;    // TotNumBids + TotNumOffers from Header_30
        public uint OrdersReceived;    // accumulated across Orders_71 chunks
        public bool HasRptSeq;         // whether LastRptSeq is usable
        public bool Skipped;           // Header_30 saw the symbol Healthy + ahead of snap; chunks must be dropped silently
    }
    private long _snapshotsHealed;
    private long _snapshotsMissingRptSeq;
    private long _snapshotChunksOrphaned;
    private long _snapshotsRejectedTooOld;
    private long _snapshotsSkippedHealthyAhead;

    /// <summary>Number of snapshot cycles where the registry was successfully healed.</summary>
    public long SnapshotsHealed => Volatile.Read(ref _snapshotsHealed);
    /// <summary>Snapshots received in PerSymbol mode without a usable LastRptSeq (cannot heal).</summary>
    public long SnapshotsMissingRptSeq => Volatile.Read(ref _snapshotsMissingRptSeq);
    /// <summary>Orders_71 chunks dropped because no Header_30 was seen first for that securityID.</summary>
    public long SnapshotChunksOrphaned => Volatile.Read(ref _snapshotChunksOrphaned);
    /// <summary>Snapshots rejected because their LastRptSeq is older than the symbol's MinHealRptSeq
    /// (would leave a hole between the snapshot baseline and our first observed/last good rptSeq).
    /// Symbol stays Stale awaiting a fresher snapshot.</summary>
    public long SnapshotsRejectedTooOld => Volatile.Read(ref _snapshotsRejectedTooOld);
    /// <summary>Snapshots ignored at Header_30 because the symbol is already Healthy with a more
    /// recent book.LastRptSeq than the snapshot baseline. Without this guard the always-on snapshot
    /// stream would clobber a healthy book with stale data, leaving holes after subsequent
    /// incrementals are applied.</summary>
    public long SnapshotsSkippedHealthyAhead => Volatile.Read(ref _snapshotsSkippedHealthyAhead);

    private void HandleSnapshotHeader(ReadOnlySpan<byte> body)
    {
        if (!SnapshotFullRefresh_Header_30Data.TryParse(body, out var reader)) return;

        ref readonly var msg = ref reader.Data;
        ulong secId = (ulong)msg.SecurityID;
        uint expected = msg.TotNumBids + msg.TotNumOffers;
        bool hasRpt = msg.LastRptSeq is { } v && v > 0;
        uint lastRpt = hasRpt ? msg.LastRptSeq!.Value : 0u;

        BeginSnapshotHeader(secId, lastRpt, hasRpt, expected);
    }

    // Exposed internally to share the always-on snapshot guard between the wire-decode
    // path and tests that don't want to forge raw SBE bytes.
    internal void BeginSnapshotHeader(ulong secId, uint lastRptSeq, bool hasRptSeq, uint ordersExpected)
    {
        var book = GetOrCreateBook(secId);

        // GUARD: never apply a snapshot to an already-Healthy symbol. The B3
        // always-on snapshot stream rotates through every instrument
        // periodically and does not target our consumer's specific state — its
        // payload reflects state-as-of some snapshot moment T, which may be
        // either behind or ahead of where we are.
        //
        // - snap <= priorHighWater: snapshot is stale relative to live; applying
        //   would Clear + repopulate at an older state, then registry baseline
        //   stays at priorHighWater (defensive guard) so subsequent live msgs
        //   skip [snap+1..priorHighWater] silently — book ends with [snap+1..pH]
        //   operations missing.
        // - snap > priorHighWater (Healthy idle): snapshot saw msgs [pH+1..snap]
        //   that we either (a) haven't received yet (in-flight live) or (b)
        //   missed silently (genuine UDP loss). Clear + repopulate would
        //   clobber any live operations already applied AND mask the genuine
        //   loss — we'd flip baseline to snap, then late-arriving live msgs in
        //   [pH+1..snap] hit the Drop branch (received <= lastSeen).
        //
        // Healthy symbols already have correct, up-to-date state by definition.
        // If a real gap exists, the next live message > priorHighWater+1 will
        // detect it (Healthy→Stale via Observe) and the NEXT snapshot rotation
        // heals it cleanly. Skipping unconditionally for Healthy is safe.
        //
        // (The pre-per-symbol channel-recovery model never had this problem
        // because snapshots were only consumed during channel-wide Recovery
        // state; Healthy symbols were never touched.)
        if (hasRptSeq && _stateRegistry is not null)
        {
            var state = _stateRegistry.GetState(secId, SymbolGapKind.Mbo);
            if (state == SymbolState.Healthy)
            {
                // Mark the in-flight snapshot as Skipped so subsequent Orders_71 chunks
                // are dropped silently without incrementing the orphan counter.
                _pendingSnapshots[secId] = new PendingSnapshot
                {
                    LastRptSeq = lastRptSeq,
                    OrdersExpected = ordersExpected,
                    OrdersReceived = 0,
                    HasRptSeq = true,
                    Skipped = true,
                };
                Interlocked.Increment(ref _snapshotsSkippedHealthyAhead);
                return;
            }
        }

        // Begin a fresh snapshot for this instrument: clear the book and reset counters.
        // If a previous snapshot for this same instrument was still in progress (incomplete
        // chunks), it gets superseded — chunks from the prior snapshot are abandoned.
        book.Clear();
        _pendingSnapshots[secId] = new PendingSnapshot
        {
            LastRptSeq = lastRptSeq,
            OrdersExpected = ordersExpected,
            OrdersReceived = 0,
            HasRptSeq = hasRptSeq,
        };

        // Empty book snapshot (no Orders_71 chunks will follow): heal immediately.
        if (ordersExpected == 0)
            CompleteSnapshot(secId, book);
    }

    /// <summary>
    /// Test helper: simulate a Header_30 + N Orders_71 chunked snapshot. The book is
    /// cleared, expected orders is set, and heal fires only once
    /// <paramref name="ordersExpected"/> entries have been recorded via
    /// <see cref="RecordSnapshotChunkForTest"/>.
    /// </summary>
    internal void BeginChunkedSnapshotForTest(ulong securityId, uint lastRptSeq, uint ordersExpected)
    {
        var book = GetOrCreateBook(securityId);
        book.Clear();
        _pendingSnapshots[securityId] = new PendingSnapshot
        {
            LastRptSeq = lastRptSeq,
            OrdersExpected = ordersExpected,
            OrdersReceived = 0,
            HasRptSeq = lastRptSeq > 0,
        };
        if (ordersExpected == 0 && lastRptSeq > 0)
            CompleteSnapshot(securityId, book);
    }

    /// <summary>
    /// Test helper: simulate one Orders_71 chunk carrying <paramref name="ordersInChunk"/>
    /// entries. Heal fires automatically once the running total meets the expected count
    /// from <see cref="BeginChunkedSnapshotForTest"/>.
    /// </summary>
    internal void RecordSnapshotChunkForTest(ulong securityId, uint ordersInChunk)
    {
        if (!_pendingSnapshots.ContainsKey(securityId))
        {
            Interlocked.Increment(ref _snapshotChunksOrphaned);
            return;
        }
        var book = GetOrCreateBook(securityId);
        ref var pending = ref System.Runtime.InteropServices.CollectionsMarshal
            .GetValueRefOrNullRef(_pendingSnapshots, securityId);
        pending.OrdersReceived += ordersInChunk;
        if (pending.OrdersReceived >= pending.OrdersExpected)
            CompleteSnapshot(securityId, book);
    }

    /// <summary>
    /// Test helper: cache an snapshot baseline for a single security as if a header had
    /// arrived. Sets OrdersExpected=0 so a subsequent <see cref="HealAfterSnapshotForTest"/>
    /// completes the snapshot and triggers heal even without dispatched Orders_71 chunks.
    /// </summary>
    internal void RecordSnapshotHeader(ulong securityId, uint? lastRptSeq)
    {
        bool hasRpt = lastRptSeq is { } v && v > 0;
        if (hasRpt)
        {
            _pendingSnapshots[securityId] = new PendingSnapshot
            {
                LastRptSeq = lastRptSeq!.Value,
                OrdersExpected = 0,
                OrdersReceived = 0,
                HasRptSeq = true,
            };
        }
        else
        {
            // null/0 rptSeq: keep an entry so HealAfterSnapshotForTest exercises the
            // illiquid auto-promote path (B3 spec §7.4) — Unknown/Stale-without-gap
            // symbols transition to Healthy at baseline=0; the missing-baseline
            // counter only increments when the heal is rejected (e.g., genuine gap).
            _pendingSnapshots[securityId] = new PendingSnapshot
            {
                LastRptSeq = 0,
                OrdersExpected = 0,
                OrdersReceived = 0,
                HasRptSeq = false,
            };
        }
    }

    /// <summary>
    /// Run the post-snapshot heal flow for a security: transition the registry
    /// to Healthy at the cached lastRptSeq baseline and replay any buffered
    /// messages in the heal window. Exposed internally for tests.
    /// </summary>
    internal void HealAfterSnapshotForTest(ulong securityId)
    {
        var book = GetOrCreateBook(securityId);
        CompleteSnapshot(securityId, book);
    }

    /// <summary>Test helper: invoke the EmptyBook_9 dispatch path without forging a SBE header.</summary>
    internal void HandleEmptyBookForTest(ReadOnlySpan<byte> body) => HandleEmptyBook(body);

    private void HandleSnapshotOrders(ReadOnlySpan<byte> body)
    {
        if (!SnapshotFullRefresh_Orders_MBO_71Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        // An Orders_71 chunk must be preceded by a Header_30 for the same instrument.
        // If not (lost packet, out-of-order), drop the chunk — applying it would corrupt
        // the book (we would not know whether to clear first or how to know completion).
        if (!_pendingSnapshots.TryGetValue(securityId, out var pendingPeek))
        {
            Interlocked.Increment(ref _snapshotChunksOrphaned);
            return;
        }

        // Skipped snapshot (Header_30 saw the symbol Healthy + ahead): silently drop chunks
        // and tick OrdersReceived so CompleteSnapshot fires once all expected entries arrive
        // (no orphan-counter increment, no book mutation).
        if (pendingPeek.Skipped)
        {
            uint skippedAdded = 0;
            reader.ReadGroups((in SnapshotFullRefresh_Orders_MBO_71Data.NoMDEntriesData _) =>
            {
                skippedAdded++;
            });
            ref var skipPending = ref System.Runtime.InteropServices.CollectionsMarshal
                .GetValueRefOrNullRef(_pendingSnapshots, securityId);
            skipPending.OrdersReceived += skippedAdded;
            if (skipPending.OrdersReceived >= skipPending.OrdersExpected)
                _pendingSnapshots.Remove(securityId);
            return;
        }

        var book = GetOrCreateBook(securityId);

        uint added = 0;
        reader.ReadGroups((in SnapshotFullRefresh_Orders_MBO_71Data.NoMDEntriesData entry) =>
        {
            added++;
            long? rawPrice = entry.MDEntryPx.Mantissa;
            if (rawPrice is null)
                return; // Market orders have no price — counted toward expected but not added to book

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

        ref var pending = ref System.Runtime.InteropServices.CollectionsMarshal
            .GetValueRefOrNullRef(_pendingSnapshots, securityId);
        // pending cannot be null-ref here: ContainsKey check above + single-threaded access.
        pending.OrdersReceived += added;

        if (pending.OrdersReceived >= pending.OrdersExpected)
            CompleteSnapshot(securityId, book);
    }

    private void CompleteSnapshot(ulong securityId, OrderBook book)
    {
        if (!_pendingSnapshots.Remove(securityId, out var pending))
        {
            // No header recorded — cannot transition to Healthy without a baseline.
            // Symbol stays Stale; subsequent incremental will continue buffering until next
            // snapshot cycle delivers a usable header.
            Interlocked.Increment(ref _snapshotsMissingRptSeq);
            return;
        }

        if (!pending.HasRptSeq)
        {
            // Illiquid instrument case (B3 spec §7.4): LastRptSeq is omitted from the
            // snapshot header when the instrument has not received any incremental
            // updates yet from the incremental stream. Spec explicitly states:
            // "the client system can process the incremental messages related to
            // that instrument without discarding them."
            //
            // Treat the absent LastRptSeq as "anchor at rptSeq=0": promote the
            // symbol to Healthy with baseline=0, so the first incremental that
            // arrives (rptSeq=1) is contiguous (lastSeen+1 == received) and is
            // applied. Without this, illiquid symbols stayed Stale forever and
            // every subsequent live message went into the per-symbol buffer.
            //
            // HealFromSnapshot's defensive Healthy-ahead guard still protects
            // already-Healthy symbols (prev=Healthy + snap=0 <= priorHighWater
            // would reject). Stale symbols with minHeal>0 (genuine gap) also
            // get rejected (snap=0 < minHeal>0), correctly leaving them Stale.
            var illiquidHeal = _stateRegistry!.HealFromSnapshot(securityId, SymbolGapKind.Mbo, 0);
            if (illiquidHeal.Accepted)
            {
                book.LastRptSeq = 0;
                if (illiquidHeal.TransitionedToHealthy)
                {
                    Interlocked.Increment(ref _snapshotsHealed);
                    if (_eventHandler is not null && !_stateRegistry.IsAnyStale(securityId))
                        _eventHandler.OnSymbolStaleStatusChanged(securityId, isStale: false);
                }
            }
            else
            {
                Interlocked.Increment(ref _snapshotsMissingRptSeq);
            }
            return;
        }

        uint snapshotRptSeq = pending.LastRptSeq;
        var heal = _stateRegistry!.HealFromSnapshot(securityId, SymbolGapKind.Mbo, snapshotRptSeq);

        if (!heal.Accepted)
        {
            // Snapshot too old to bridge our gap. The book bytes already applied
            // (Header_30 cleared + Orders_71 chunks repopulated) reflect a valid
            // state at snapshotRptSeq, but applying it as the symbol's working
            // book would discard already-buffered live messages we cannot
            // reconcile. Roll the book back: clear it so the symbol stays Stale
            // and the next-fresh-enough snapshot will rebuild it cleanly. Keep
            // the buffered live messages so they can drain on that next heal.
            book.Clear();
            book.LastRptSeq = 0;
            Interlocked.Increment(ref _snapshotsRejectedTooOld);
            return;
        }

        book.LastRptSeq = snapshotRptSeq;
        if (heal.TransitionedToHealthy)
        {
            Interlocked.Increment(ref _snapshotsHealed);
            // Emit a SymbolStaleStatus flip when the symbol is fully recovered
            // (no other kinds remain Stale). When other kinds are still Stale
            // the symbol-level status is unchanged so no event is emitted.
            if (_eventHandler is not null && !_stateRegistry.IsAnyStale(securityId))
                _eventHandler.OnSymbolStaleStatusChanged(securityId, isStale: false);
        }

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
        int dropped = _staleBuffer.ClearAll();
        _pendingSnapshots.Clear();
        _stateRegistry.ResetEpoch(reason);
        Interlocked.Add(ref _epochResetMessagesDropped, dropped);
        Interlocked.Increment(ref _epochResets);
    }
}
