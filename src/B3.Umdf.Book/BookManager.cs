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
using Trade_53Data = B3.Umdf.Mbo.Sbe.V16.V6.Trade_53Data;
using ForwardTrade_54Data = B3.Umdf.Mbo.Sbe.V16.V6.ForwardTrade_54Data;
using ExecutionSummary_55Data = B3.Umdf.Mbo.Sbe.V16.V6.ExecutionSummary_55Data;
using TradeBust_57Data = B3.Umdf.Mbo.Sbe.V16.V6.TradeBust_57Data;

namespace B3.Umdf.Book;

public sealed class BookManager : IFeedEventHandler
{
    private readonly ConcurrentDictionary<ulong, OrderBook> _books = new();
    private volatile FrozenDictionary<ulong, OrderBook>? _frozenBooks;
    private readonly IBookEventHandler? _eventHandler;
    private readonly ILogger<BookManager> _logger;
    private long _parseErrors;

    /// <summary>
    /// Thread-safe dictionary of all order books.
    /// Safe to read (Count, iterate) from any thread while the feed thread writes.
    /// </summary>
    public IReadOnlyDictionary<ulong, OrderBook> Books => _books;

    /// <summary>Number of SBE parse errors encountered (malformed packets).</summary>
    public long ParseErrors => Volatile.Read(ref _parseErrors);

    public BookManager(IBookEventHandler? eventHandler = null, ILogger<BookManager>? logger = null)
    {
        _eventHandler = eventHandler;
        _logger = logger ?? NullLogger<BookManager>.Instance;
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
        if (sbePayload.Length < MessageHeader.MESSAGE_SIZE)
            return;

        var body = sbePayload[MessageHeader.MESSAGE_SIZE..];

        try
        {
            switch (templateId)
            {
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
                    HandleTrade(body);
                    break;
                case EmptyBook_9Data.MESSAGE_ID:
                    HandleEmptyBook(body);
                    break;
                case ChannelReset_11Data.MESSAGE_ID:
                    ClearAllBooks();
                    break;
                case ForwardTrade_54Data.MESSAGE_ID:
                    HandleForwardTrade(body);
                    break;
                case ExecutionSummary_55Data.MESSAGE_ID:
                    HandleExecutionSummary(body);
                    break;
                case TradeBust_57Data.MESSAGE_ID:
                    HandleTradeBust(body);
                    break;
                case SnapshotFullRefresh_Orders_MBO_71Data.MESSAGE_ID:
                    HandleSnapshotOrders(body);
                    break;
            }
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _parseErrors);
            _logger.LogWarning(ex, "Error processing templateId={TemplateId}", templateId);
        }
    }

    public void OnGapDetected(uint expected, uint received) { }
    public void OnSequenceReset() { ClearAllBooks(); }
    public void OnSnapshotStart() { }
    public void OnSnapshotComplete(uint lastRptSeq) { FreezeBooks(); }
    public void OnInstrumentDefinitionsComplete(int instrumentCount) { }

    private void HandleOrder(ReadOnlySpan<byte> body)
    {
        if (!Order_MBO_50Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var book = GetOrCreateBook(securityId);

        var side = msg.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
        var bookSide = book.GetSide(side);
        ulong orderId = (ulong)msg.SecondaryOrderID;

        long price = msg.MDEntryPx is { } px ? (px.Mantissa ?? 0) : 0;
        long quantity = (long)msg.MDEntrySize;
        uint enteringFirm = msg.EnteringFirm is { } ef ? (uint)ef : 0;

        if (bookSide.TryGetOrder(orderId, out var existing) && existing is not null)
        {
            long oldPrice = existing.Price;

            existing.Price = price;
            existing.Quantity = quantity;
            existing.EnteringFirm = enteringFirm;

            if (oldPrice != price)
                bookSide.MoveOrder(existing, oldPrice);

            if (msg.RptSeq is { } rptSeq)
                book.LastRptSeq = (uint)rptSeq;

            _eventHandler?.OnOrderUpdated(book, existing);
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

            bookSide.Add(entry);

            if (msg.RptSeq is { } rptSeq)
                book.LastRptSeq = (uint)rptSeq;

            _eventHandler?.OnOrderAdded(book, entry);
        }
    }

    private void HandleDeleteOrder(ReadOnlySpan<byte> body)
    {
        if (!DeleteOrder_MBO_51Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!TryLookupBook(securityId, out var book))
            return;

        var side = msg.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
        ulong orderId = (ulong)msg.SecondaryOrderID;

        book.GetSide(side).Remove(orderId);

        if (msg.RptSeq is { } rptSeq)
            book.LastRptSeq = (uint)rptSeq;

        _eventHandler?.OnOrderDeleted(book, orderId, side);
    }

    private void HandleMassDelete(ReadOnlySpan<byte> body)
    {
        if (!MassDeleteOrders_MBO_52Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!TryLookupBook(securityId, out var book))
            return;

        var entryType = msg.MDEntryType;
        if (entryType == MDEntryType.BID)
            book.Bids.Clear();
        else if (entryType == MDEntryType.OFFER)
            book.Asks.Clear();
        else
            book.Clear();

        if (msg.RptSeq is { } rptSeq)
            book.LastRptSeq = (uint)rptSeq;

        _eventHandler?.OnBookCleared(securityId);
    }

    private void HandleTrade(ReadOnlySpan<byte> body)
    {
        if (!Trade_53Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        long price = msg.MDEntryPx.Mantissa;
        long quantity = (long)msg.MDEntrySize;
        long tradeId = (long)(uint)msg.TradeID;

        if (TryLookupBook(securityId, out var book))
        {
            if (msg.RptSeq is { } rptSeq)
                book.LastRptSeq = (uint)rptSeq;
        }

        _eventHandler?.OnTrade(securityId, price, quantity, tradeId);
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
            _eventHandler?.OnBookCleared(securityId);
        }
    }

    private void HandleForwardTrade(ReadOnlySpan<byte> body)
    {
        if (!ForwardTrade_54Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        long price = msg.MDEntryPx.Mantissa;
        long quantity = (long)msg.MDEntrySize;
        long tradeId = (long)(uint)msg.TradeID;

        if (TryLookupBook(securityId, out var book))
        {
            if (msg.RptSeq is { } rptSeq)
                book.LastRptSeq = (uint)rptSeq;
        }

        _eventHandler?.OnForwardTrade(securityId, price, quantity, tradeId);
    }

    private void HandleExecutionSummary(ReadOnlySpan<byte> body)
    {
        if (!ExecutionSummary_55Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        long lastPx = msg.LastPx.Mantissa;
        long fillQty = (long)msg.FillQty;

        if (TryLookupBook(securityId, out var book))
        {
            if (msg.RptSeq is { } rptSeq)
                book.LastRptSeq = (uint)rptSeq;
        }

        _eventHandler?.OnExecutionSummary(securityId, lastPx, fillQty);
    }

    private void HandleTradeBust(ReadOnlySpan<byte> body)
    {
        if (!TradeBust_57Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        long price = msg.MDEntryPx.Mantissa;
        long quantity = (long)msg.MDEntrySize;
        long tradeId = (long)(uint)msg.TradeID;

        if (TryLookupBook(securityId, out var book))
        {
            if (msg.RptSeq is { } rptSeq)
                book.LastRptSeq = (uint)rptSeq;
        }

        _eventHandler?.OnTradeBust(securityId, price, quantity, tradeId);
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
            var side = entry.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
            long price = entry.MDEntryPx.Mantissa ?? 0;
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

            book.GetSide(side).Add(bookEntry);
        });
    }

    /// <summary>
    /// Fast book lookup — uses FrozenDictionary when available (hot path),
    /// falls back to mutable dictionary during setup.
    /// </summary>
    private bool TryLookupBook(ulong securityId, out OrderBook book)
    {
        if (_frozenBooks is { } frozen)
            return frozen.TryGetValue(securityId, out book!);
        return _books.TryGetValue(securityId, out book!);
    }

    private void ClearAllBooks()
    {
        foreach (var (secId, book) in _books)
        {
            book.Clear();
            _eventHandler?.OnBookCleared(secId);
        }
    }
}
