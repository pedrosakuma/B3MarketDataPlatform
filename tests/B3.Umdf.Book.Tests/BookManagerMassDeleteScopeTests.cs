using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// P12-8 — pins MassDeleteOrders_MBO_52 (B3 BinaryUMDF v2.2.0 §6.5)
/// scope variants. The existing test in <c>SpecComplianceTests</c>
/// covers <see cref="MDEntryType.BID"/> only; this file extends coverage
/// to <see cref="MDEntryType.OFFER"/>, <see cref="MDEntryType.EMPTY_BOOK"/>
/// (and the unspecified fallback), and asserts the rptSeq watermark +
/// OnBookCleared event side propagate correctly in each variant.
/// </summary>
public class BookManagerMassDeleteScopeTests
{
    private const ulong SecurityId = 4242;

    private static (BookManager bm, OrderBook book, List<BookClearSide> clears) NewSeededBook()
    {
        var clears = new List<BookClearSide>();
        var bm = new BookManager(
            eventHandler: new ClearSideCaptureHandler(clears.Add),
            stateRegistry: new SymbolStateRegistry(NullLogger.Instance),
            staleBuffer: new StaleMboBuffer(NullLogger.Instance));
        var book = bm.GetOrCreateBook(SecurityId);
        book.Bids.Add(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10, SecurityId = SecurityId, Side = BookSideType.Bid });
        book.Asks.Add(new OrderBookEntry { OrderId = 2, Price = 110, Quantity = 20, SecurityId = SecurityId, Side = BookSideType.Ask });
        book.UpsertMarketOrder(orderId: 3, BookSideType.Bid, quantity: 30, enteringFirm: 1);
        book.UpsertMarketOrder(orderId: 4, BookSideType.Ask, quantity: 40, enteringFirm: 1);
        return (bm, book, clears);
    }

    [Fact]
    public void MassDeleteOffer_ClearsPricedAndMarketAskOnly()
    {
        var (bm, book, clears) = NewSeededBook();

        bm.HandleMassDeleteForTest(SecurityId, MDEntryType.OFFER, rptSeq: 21);

        Assert.Equal(1, book.Bids.OrderCount);
        Assert.Equal(0, book.Asks.OrderCount);
        Assert.Equal(1, book.MarketOrderCount(BookSideType.Bid));
        Assert.Equal(0, book.MarketOrderCount(BookSideType.Ask));
        Assert.Equal(30, book.MarketOrderQuantity(BookSideType.Bid));
        Assert.Equal(21u, book.LastRptSeq);
        Assert.Equal(new[] { BookClearSide.Ask }, clears);
    }

    [Fact]
    public void MassDeleteEmptyBook_ClearsBothSides()
    {
        var (bm, book, clears) = NewSeededBook();

        bm.HandleMassDeleteForTest(SecurityId, MDEntryType.EMPTY_BOOK, rptSeq: 33);

        Assert.Equal(0, book.Bids.OrderCount);
        Assert.Equal(0, book.Asks.OrderCount);
        Assert.Equal(0, book.MarketOrderCount(BookSideType.Bid));
        Assert.Equal(0, book.MarketOrderCount(BookSideType.Ask));
        Assert.Equal(33u, book.LastRptSeq);
        Assert.Equal(new[] { BookClearSide.Both }, clears);
    }

    [Fact]
    public void MassDeleteUnknownEntryType_FallsBackToBothSides()
    {
        // Defensive contract: any MDEntryType other than BID/OFFER falls
        // through to a full clear (covers EMPTY_BOOK and any future or
        // garbled value the wire might carry). Pins the `else` branch in
        // BookManager.ApplyMassDelete.
        var (bm, book, clears) = NewSeededBook();

        bm.HandleMassDeleteForTest(SecurityId, MDEntryType.TRADE, rptSeq: 7);

        Assert.Equal(0, book.Bids.OrderCount);
        Assert.Equal(0, book.Asks.OrderCount);
        Assert.Equal(7u, book.LastRptSeq);
        Assert.Equal(new[] { BookClearSide.Both }, clears);
    }

    [Fact]
    public void MassDeleteOnAlreadyEmptyBook_StillEmitsClearEvent()
    {
        // Subscribers may rely on OnBookCleared as a synchronization signal
        // (e.g., to drop pending market-tier diffs). The handler must fire
        // even when there's nothing to remove.
        var clears = new List<BookClearSide>();
        var bm = new BookManager(
            eventHandler: new ClearSideCaptureHandler(clears.Add),
            stateRegistry: new SymbolStateRegistry(NullLogger.Instance),
            staleBuffer: new StaleMboBuffer(NullLogger.Instance));
        bm.GetOrCreateBook(SecurityId); // empty book

        bm.HandleMassDeleteForTest(SecurityId, MDEntryType.BID, rptSeq: 1);
        bm.HandleMassDeleteForTest(SecurityId, MDEntryType.OFFER, rptSeq: 2);
        bm.HandleMassDeleteForTest(SecurityId, MDEntryType.EMPTY_BOOK, rptSeq: 3);

        Assert.Equal(
            new[] { BookClearSide.Bid, BookClearSide.Ask, BookClearSide.Both },
            clears);
    }

    [Fact]
    public void MassDelete_NullRptSeq_DoesNotAdvanceLastRptSeq()
    {
        // Snapshot-feed MassDelete carries a null/zero rptSeq (incremental
        // refresh field is suppressed). LastRptSeq must NOT be touched when
        // rptSeq is absent; otherwise the per-symbol gap tracker would
        // briefly see a regressed watermark.
        var (bm, book, _) = NewSeededBook();
        // Seed a non-zero LastRptSeq via an earlier mass delete.
        bm.HandleMassDeleteForTest(SecurityId, MDEntryType.BID, rptSeq: 50);
        Assert.Equal(50u, book.LastRptSeq);

        bm.HandleMassDeleteForTest(SecurityId, MDEntryType.OFFER, rptSeq: null);

        Assert.Equal(50u, book.LastRptSeq);   // unchanged
        Assert.Equal(0, book.Asks.OrderCount); // but the side was still cleared
    }

    private sealed class ClearSideCaptureHandler : IBookEventHandler
    {
        private readonly Action<BookClearSide> _onCleared;
        public ClearSideCaptureHandler(Action<BookClearSide> onCleared) => _onCleared = onCleared;
        public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) { }
        public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) { }
        public void OnBookCleared(ulong securityId, BookClearSide side) => _onCleared(side);
    }
}
