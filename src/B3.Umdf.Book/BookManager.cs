using System.Collections.Frozen;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

using Order_MBO_50Data = B3.Umdf.Mbo.Sbe.V16.V6.Order_MBO_50Data;
using DeleteOrder_MBO_51Data = B3.Umdf.Mbo.Sbe.V16.V6.DeleteOrder_MBO_51Data;
using MassDeleteOrders_MBO_52Data = B3.Umdf.Mbo.Sbe.V16.V6.MassDeleteOrders_MBO_52Data;
using Trade_53Data = B3.Umdf.Mbo.Sbe.V16.V6.Trade_53Data;

namespace B3.Umdf.Book;

public sealed class BookManager : IFeedEventHandler
{
    private Dictionary<ulong, OrderBook> _mutableBooks = new();
    private FrozenDictionary<ulong, OrderBook>? _frozenBooks;
    private readonly IBookEventHandler? _eventHandler;

    /// <summary>
    /// Returns FrozenDictionary after instrument definitions (optimized lookups),
    /// falls back to mutable dictionary during setup.
    /// </summary>
    public IReadOnlyDictionary<ulong, OrderBook> Books =>
        (IReadOnlyDictionary<ulong, OrderBook>?)_frozenBooks ?? _mutableBooks;

    public BookManager(IBookEventHandler? eventHandler = null)
    {
        _eventHandler = eventHandler;
    }

    /// <summary>
    /// Pre-allocate dictionary capacity after instrument definitions are known.
    /// Avoids rehashing during the hot path.
    /// </summary>
    public void EnsureCapacity(int instrumentCount)
    {
        _mutableBooks.EnsureCapacity(instrumentCount);
    }

    /// <summary>
    /// Freeze the books dictionary for optimized lookups during the hot path.
    /// Called after all instruments are discovered (InstrDef + Snapshot).
    /// </summary>
    public void FreezeBooks()
    {
        _frozenBooks = _mutableBooks.ToFrozenDictionary();
    }

    public OrderBook GetOrCreateBook(ulong securityId)
    {
        // Fast path: frozen dictionary lookup (optimized hash)
        if (_frozenBooks is not null)
        {
            if (_frozenBooks.TryGetValue(securityId, out var book))
                return book;
            // New instrument after freeze — create and re-freeze
            book = new OrderBook(securityId);
            _mutableBooks[securityId] = book;
            _frozenBooks = _mutableBooks.ToFrozenDictionary();
            return book;
        }

        // Setup path: mutable dictionary
        if (!_mutableBooks.TryGetValue(securityId, out var mBook))
        {
            mBook = new OrderBook(securityId);
            _mutableBooks[securityId] = mBook;
        }
        return mBook;
    }

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        if (sbePayload.Length < MessageHeader.MESSAGE_SIZE)
            return;

        var body = sbePayload[MessageHeader.MESSAGE_SIZE..];

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
        }
    }

    public void OnGapDetected(uint expected, uint received) { }
    public void OnSequenceReset() { ClearAllBooks(); }
    public void OnSnapshotStart() { }
    public void OnSnapshotComplete(uint lastRptSeq) { FreezeBooks(); }
    public void OnInstrumentDefinitionsComplete(int instrumentCount)
    {
        EnsureCapacity(instrumentCount);
    }

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

    /// <summary>
    /// Fast book lookup — uses FrozenDictionary when available (hot path),
    /// falls back to mutable dictionary during setup.
    /// </summary>
    private bool TryLookupBook(ulong securityId, out OrderBook book)
    {
        if (_frozenBooks is not null)
            return _frozenBooks.TryGetValue(securityId, out book!);
        return _mutableBooks.TryGetValue(securityId, out book!);
    }

    private void ClearAllBooks()
    {
        var books = _frozenBooks is not null
            ? (IEnumerable<KeyValuePair<ulong, OrderBook>>)_frozenBooks
            : _mutableBooks;
        foreach (var (secId, book) in books)
        {
            book.Clear();
            _eventHandler?.OnBookCleared(secId);
        }
    }
}
