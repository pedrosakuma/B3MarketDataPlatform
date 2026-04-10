namespace B3.Umdf.Book;

public sealed class OrderBook
{
    public ulong SecurityId { get; }
    public BookSide Bids { get; }
    public BookSide Asks { get; }
    public uint LastRptSeq { get; set; }

    public OrderBook(ulong securityId)
    {
        SecurityId = securityId;
        Bids = new BookSide(BookSideType.Bid);
        Asks = new BookSide(BookSideType.Ask);
    }

    public BookSide GetSide(BookSideType side) => side == BookSideType.Bid ? Bids : Asks;

    public void Clear()
    {
        Bids.Clear();
        Asks.Clear();
        LastRptSeq = 0;
    }
}
