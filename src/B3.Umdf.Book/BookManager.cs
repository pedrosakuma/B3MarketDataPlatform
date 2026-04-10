using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Book;

public sealed class BookManager : IFeedEventHandler
{
    private readonly Dictionary<ulong, OrderBook> _books = new();
    private readonly IBookEventHandler? _eventHandler;

    public IReadOnlyDictionary<ulong, OrderBook> Books => _books;

    public BookManager(IBookEventHandler? eventHandler = null)
    {
        _eventHandler = eventHandler;
    }

    public OrderBook GetOrCreateBook(ulong securityId)
    {
        if (!_books.TryGetValue(securityId, out var book))
        {
            book = new OrderBook(securityId);
            _books[securityId] = book;
        }
        return book;
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
    public void OnSnapshotComplete(uint lastRptSeq) { }

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

        bool isUpdate = bookSide.Orders.ContainsKey(orderId);

        var entry = new OrderBookEntry
        {
            OrderId = orderId,
            Price = price,
            Quantity = quantity,
            EnteringFirm = enteringFirm,
            SecurityId = securityId,
            Side = side
        };

        bookSide.AddOrUpdate(entry);

        if (msg.RptSeq is { } rptSeq)
            book.LastRptSeq = (uint)rptSeq;

        if (isUpdate)
            _eventHandler?.OnOrderUpdated(book, entry);
        else
            _eventHandler?.OnOrderAdded(book, entry);
    }

    private void HandleDeleteOrder(ReadOnlySpan<byte> body)
    {
        if (!DeleteOrder_MBO_51Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        if (!_books.TryGetValue(securityId, out var book))
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

        if (!_books.TryGetValue(securityId, out var book))
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

        if (_books.TryGetValue(securityId, out var book))
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

        if (_books.TryGetValue(securityId, out var book))
        {
            book.Clear();
            _eventHandler?.OnBookCleared(securityId);
        }
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
