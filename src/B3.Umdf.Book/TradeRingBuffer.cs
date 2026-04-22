namespace B3.Umdf.Book;

/// <summary>Fixed-capacity ring buffer of recent trades for a single security.</summary>
internal sealed class TradeRingBuffer
{
    private readonly (long Price, long Qty, long TradeId)[] _buf;
    private volatile int _head; // next write position
    private volatile int _count;

    public TradeRingBuffer(int capacity) => _buf = new (long, long, long)[capacity];

    public void Add(long price, long qty, long tradeId)
    {
        _buf[_head] = (price, qty, tradeId);
        _head = (_head + 1) % _buf.Length;
        if (_count < _buf.Length) _count++;
    }

    /// <summary>
    /// Snapshot oldest → newest. Safe for concurrent reads.
    /// Copies the tuple slots under a retry loop because struct assignments
    /// of 24 bytes are not atomic — without this, a concurrent Add() could
    /// produce torn reads (mixed fields from two different trades).
    /// </summary>
    public (long Price, long Qty, long TradeId)[] AsSpan()
    {
        const int MaxRetries = 8;
        for (int attempt = 0; attempt < MaxRetries; attempt++)
        {
            int countBefore = _count;
            int headBefore = _head;
            int start = countBefore < _buf.Length ? 0 : headBefore;

            var snapshot = new (long, long, long)[countBefore];
            for (int i = 0; i < countBefore; i++)
                snapshot[i] = _buf[(start + i) % _buf.Length];

            // Validate: if neither head nor count moved during the copy,
            // no Add() ran concurrently and the snapshot is consistent.
            if (_count == countBefore && _head == headBefore)
                return snapshot;
        }

        // Fallback: best-effort copy after repeated contention.
        int c = _count;
        int h = _head;
        int s = c < _buf.Length ? 0 : h;
        var fallback = new (long, long, long)[c];
        for (int i = 0; i < c; i++)
            fallback[i] = _buf[(s + i) % _buf.Length];
        return fallback;
    }
}
