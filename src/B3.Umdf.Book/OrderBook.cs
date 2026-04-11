namespace B3.Umdf.Book;

public sealed class OrderBook
{
    private int _version;
    private uint _lastRptSeq;

    public ulong SecurityId { get; }
    public BookSide Bids { get; }
    public BookSide Asks { get; }

    public uint LastRptSeq
    {
        get => _lastRptSeq;
        set => _lastRptSeq = value;
    }

    /// <summary>Current version. Even = stable, Odd = mutation in progress.</summary>
    public int Version => Volatile.Read(ref _version);

    /// <summary>Call before mutating the book. Increments version to odd.</summary>
    public void BeginWrite() => Interlocked.Increment(ref _version);

    /// <summary>Call after mutating the book. Increments version to even.</summary>
    public void EndWrite() => Interlocked.Increment(ref _version);

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
        _lastRptSeq = 0;
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
