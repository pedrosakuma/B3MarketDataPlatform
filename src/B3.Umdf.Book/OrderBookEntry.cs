namespace B3.Umdf.Book;

public sealed class OrderBookEntry
{
    public ulong OrderId { get; set; }
    public long Price { get; set; }
    public long Quantity { get; set; }
    public uint EnteringFirm { get; set; }
    public ulong SecurityId { get; set; }
    public BookSideType Side { get; set; }
}

public enum BookSideType : byte
{
    Bid,
    Ask
}
