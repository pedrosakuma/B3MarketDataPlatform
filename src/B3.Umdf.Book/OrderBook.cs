namespace B3.Umdf.Book;

public sealed class OrderBook
{
    public ulong SecurityId { get; }
    public BookSide Bids { get; }
    public BookSide Asks { get; }
    public uint LastRptSeq { get; set; }

    /// <summary>Lock for synchronizing book reads (snapshots, HTTP) with feed mutations.</summary>
    public readonly object SyncRoot = new();

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

    /// <summary>
    /// Validates the full book state. Returns a list of errors (empty if valid).
    /// Checks internal consistency of both sides and cross-side invariants
    /// (e.g. no order appearing on both sides).
    /// </summary>
    public List<string> Validate()
    {
        var errors = new List<string>();

        errors.AddRange(Bids.Validate().Select(e => $"[Bids] {e}"));
        errors.AddRange(Asks.Validate().Select(e => $"[Asks] {e}"));

        // Cross-side: no orderId should exist on both sides
        foreach (var orderId in Bids.Orders.Keys)
        {
            if (Asks.Orders.ContainsKey(orderId))
                errors.Add($"Order {orderId} exists on both Bid and Ask sides");
        }

        return errors;
    }
}
