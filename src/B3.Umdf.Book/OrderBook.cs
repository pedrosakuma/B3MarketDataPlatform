namespace B3.Umdf.Book;

public sealed class OrderBook
{
    public ulong SecurityId { get; }
    public BookSide Bids { get; }
    public BookSide Asks { get; }
    public uint LastRptSeq { get; set; }

    /// <summary>
    /// True when the most recent crossing check observed bestBid >= bestAsk.
    /// Used to suppress repeated CROSSED warnings for a book that remains in a crossed
    /// state across many subsequent operations (we only want to log on transitions).
    /// </summary>
    public bool IsCrossed { get; set; }

    /// <summary>
    /// True when the current crossed state originated during a non-OPEN phase
    /// (auction/halt). Used so a phase change (e.g. auction → OPEN) does not retroactively
    /// promote auction-era crosses into the "trading anomaly" bucket. Reset when the
    /// book uncrosses; set on every false→true crossing transition based on the phase
    /// at that moment.
    /// </summary>
    public bool CrossedInAuction { get; set; }

    /// <summary>
    /// True when bestBid == bestAsk in the current crossed state (subset of crossed).
    /// </summary>
    public bool IsLocked { get; set; }

    /// <summary>
    /// Last known trading status (TradingSessionSubID) for this instrument, fed by
    /// SecurityStatus_3 / SecurityGroupPhase_10 messages. Null until the first status
    /// is received. During non-OPEN phases (Pre-open/Reserved=21, Pause=2, FinalClosingCall=101)
    /// books are expected to be locked or crossed — auction matching has not run yet.
    /// Mutated only on the feed/group worker thread.
    /// </summary>
    public int? TradingStatus { get; set; }

    /// <summary>
    /// Lock for external readers (HTTP endpoints) that may access the book concurrently.
    /// Not used on the feed thread hot path — all book mutations and snapshot reads
    /// happen on the feed thread, so no synchronization is needed there.
    /// </summary>
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
        IsCrossed = false;
        CrossedInAuction = false;
        IsLocked = false;
        // Note: TradingStatus is intentionally not cleared — phase persists across snapshot resyncs.
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
