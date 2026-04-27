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

    // ── Market orders (B3 spec §12.1 — MOA/MOC priority tier) ────────────────
    //
    // Orders without price (MOA = Market On Auction, MOC = Market On Close) have
    // priority over all priced orders during reserved/pre-opening/final-closing-call
    // phases. They are tracked in a separate per-side bucket rather than in the
    // priced BookSide so that:
    //   - BBO / CheckCrossing computations on Bids/Asks remain unpolluted.
    //   - PriceLevels iteration and snapshot serialization see only real prices.
    //   - Callers that reason about market orders opt in explicitly.
    // Top-of-book BBO consumers should treat HasMarketOrders(side) == true as an
    // implicit "market" tier ranked above any priced level.
    private readonly Dictionary<ulong, MarketOrder> _marketBids = new();
    private readonly Dictionary<ulong, MarketOrder> _marketAsks = new();

    public IReadOnlyDictionary<ulong, MarketOrder> MarketBids => _marketBids;
    public IReadOnlyDictionary<ulong, MarketOrder> MarketAsks => _marketAsks;
    public int MarketOrderCount(BookSideType side) =>
        (side == BookSideType.Bid ? _marketBids : _marketAsks).Count;
    public long MarketOrderQuantity(BookSideType side)
    {
        long total = 0;
        foreach (var order in (side == BookSideType.Bid ? _marketBids : _marketAsks).Values)
            total += order.Quantity;
        return total;
    }
    public bool HasMarketOrders(BookSideType side) => MarketOrderCount(side) > 0;

    /// <summary>
    /// Adds (or overwrites) a market order. Returns true if a new entry was
    /// inserted, false if an existing order was replaced.
    /// </summary>
    public bool UpsertMarketOrder(ulong orderId, BookSideType side, long quantity, uint enteringFirm)
    {
        var bucket = side == BookSideType.Bid ? _marketBids : _marketAsks;
        bool isNew = !bucket.ContainsKey(orderId);
        bucket[orderId] = new MarketOrder
        {
            OrderId = orderId,
            Side = side,
            Quantity = quantity,
            EnteringFirm = enteringFirm,
            SecurityId = SecurityId,
        };
        return isNew;
    }

    public bool TryGetMarketOrder(ulong orderId, BookSideType side, out MarketOrder order)
        => (side == BookSideType.Bid ? _marketBids : _marketAsks).TryGetValue(orderId, out order);

    public bool RemoveMarketOrder(ulong orderId, BookSideType side)
        => (side == BookSideType.Bid ? _marketBids : _marketAsks).Remove(orderId);

    public void ClearMarketOrders(BookSideType side)
    {
        if (side == BookSideType.Bid)
            _marketBids.Clear();
        else
            _marketAsks.Clear();
    }

    /// <summary>
    /// Tries to remove a market order from either side. Useful when the caller
    /// only knows the orderId (e.g. a transition from null-price → priced).
    /// Returns true and outputs the side if found.
    /// </summary>
    public bool TryRemoveMarketOrderAnySide(ulong orderId, out BookSideType side)
    {
        if (_marketBids.Remove(orderId)) { side = BookSideType.Bid; return true; }
        if (_marketAsks.Remove(orderId)) { side = BookSideType.Ask; return true; }
        side = default;
        return false;
    }

    public void Clear()
    {
        Bids.Clear();
        Asks.Clear();
        _marketBids.Clear();
        _marketAsks.Clear();
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
