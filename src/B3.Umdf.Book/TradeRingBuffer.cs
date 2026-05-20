namespace B3.Umdf.Book;

/// <summary>
/// One slot in <see cref="TradeRingBuffer"/>. <see cref="Busted"/> is set when a
/// later TradeBust_57 cancels this trade — see B3 BinaryUMDF v2.2.0 spec §10.
/// <see cref="Flags"/> mirrors the wire-level <c>TradeFlags</c> bitset (e.g.
/// <c>AuctionPrint</c>) so the snapshot history can faithfully replay the bit
/// to late subscribers.
/// </summary>
internal struct TradeSlot
{
    public long Price;
    public long Qty;
    public long TradeId;
    public byte Busted;
    public byte Flags;
}

/// <summary>Fixed-capacity ring buffer of recent trades for a single security.</summary>
internal sealed class TradeRingBuffer
{
    private readonly TradeSlot[] _buf;
    private volatile int _head; // next write position
    private volatile int _count;
    // Mutation version stamp. Bumped on every Add() AND MarkBust() so concurrent
    // readers (AsSpan) can detect any in-place change to the slot array — not
    // just head/count advances. Without this, MarkBust() can flip a Busted byte
    // mid-snapshot and never be retried by the reader.
    private volatile int _version;

    public TradeRingBuffer(int capacity) => _buf = new TradeSlot[capacity];

    public void Add(long price, long qty, long tradeId, byte flags = 0)
    {
        _buf[_head] = new TradeSlot { Price = price, Qty = qty, TradeId = tradeId, Busted = 0, Flags = flags };
        _head = (_head + 1) % _buf.Length;
        if (_count < _buf.Length) _count++;
        _version++;
    }

    /// <summary>
    /// Mark the most recent occurrence of <paramref name="tradeId"/> as busted.
    /// Returns true if found within the retained window; false if the trade was
    /// already evicted (ring is bounded — <see cref="Busted"/> annotation is
    /// best-effort over the recent-history window only). Search is back-to-front
    /// (busts typically reference very recent trades, often the previous one).
    /// </summary>
    public bool MarkBust(long tradeId)
    {
        int count = _count;
        int head = _head;
        int len = _buf.Length;
        for (int i = 1; i <= count; i++)
        {
            int pos = (head - i + len) % len;
            if (_buf[pos].TradeId == tradeId)
            {
                if (_buf[pos].Busted == 0)
                {
                    _buf[pos].Busted = 1;
                    _version++;
                }
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Most recent non-busted trade's price (newest → oldest scan). Returns
    /// false if every retained slot is busted or the ring is empty. Used by
    /// the bust reversal path on <see cref="BookManager"/> to recompute the
    /// effective last-trade price after a <c>TradeBust_57</c> wipes the
    /// most recent print.
    /// </summary>
    public bool TryGetLastNonBustedPrice(out long price, out long quantity, out long tradeId)
    {
        int count = _count;
        int head = _head;
        int len = _buf.Length;
        for (int i = 1; i <= count; i++)
        {
            int pos = (head - i + len) % len;
            if (_buf[pos].Busted == 0)
            {
                price = _buf[pos].Price;
                quantity = _buf[pos].Qty;
                tradeId = _buf[pos].TradeId;
                return true;
            }
        }
        price = 0; quantity = 0; tradeId = 0;
        return false;
    }

    /// <summary>
    /// Snapshot oldest → newest. Safe for concurrent reads.
    /// Re-copies the slot array if any mutation (Add or MarkBust) is detected
    /// during the copy via the <see cref="_version"/> stamp — without this, a
    /// concurrent writer could produce torn reads or a stale Busted byte.
    /// </summary>
    public TradeSlot[] AsSpan()
    {
        const int MaxRetries = 8;
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            int versionBefore = _version;
            int countBefore = _count;
            int headBefore = _head;
            int start = countBefore < _buf.Length ? 0 : headBefore;

            var snapshot = new TradeSlot[countBefore];
            for (int i = 0; i < countBefore; i++)
                snapshot[i] = _buf[(start + i) % _buf.Length];

            // Validate: if version, head, and count are all unchanged, no
            // writer (Add or MarkBust) ran concurrently and the snapshot is
            // consistent.
            if (_version == versionBefore && _count == countBefore && _head == headBefore)
                return snapshot;
        }

        // Fallback: best-effort copy after repeated contention.
        int c = _count;
        int h = _head;
        int s = c < _buf.Length ? 0 : h;
        var fallback = new TradeSlot[c];
        for (int i = 0; i < c; i++)
            fallback[i] = _buf[(s + i) % _buf.Length];
        return fallback;
    }
}
