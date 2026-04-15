namespace B3.Umdf.Server;

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

    /// <summary>Snapshot oldest → newest. Safe for concurrent reads.</summary>
    public IEnumerable<(long Price, long Qty, long TradeId)> AsSpan()
    {
        int count = _count;
        int head = _head;
        int start = count < _buf.Length ? 0 : head;
        for (int i = 0; i < count; i++)
            yield return _buf[(start + i) % _buf.Length];
    }
}
