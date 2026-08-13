using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Benchmarks;

internal sealed class AdvancingFixClock : IFixClock
{
    private DateTimeOffset _utcNow = new(2026, 8, 13, 3, 30, 0, TimeSpan.Zero);
    private long _monotonicTicks;

    public DateTimeOffset UtcNow => _utcNow;
    public long MonotonicTicks => _monotonicTicks;

    public void AdvanceMilliseconds(int milliseconds)
    {
        _utcNow = _utcNow.AddMilliseconds(milliseconds);
        _monotonicTicks += milliseconds;
    }
}

internal sealed class SequentialHeaderProvider : IFixApplicationHeaderProvider
{
    private readonly string _senderCompId;
    private readonly string _targetCompId;
    private int _nextSequenceNumber = 1;

    public SequentialHeaderProvider(string senderCompId = "SERVER", string targetCompId = "CLIENT")
    {
        _senderCompId = senderCompId;
        _targetCompId = targetCompId;
    }

    public FixApplicationSessionHeader NextHeader(DateTimeOffset sendingTime)
        => new(_senderCompId, _targetCompId, _nextSequenceNumber++, sendingTime);
}

internal sealed class StaticInstrumentResolver : IFixMarketDataInstrumentResolver
{
    private readonly FixMarketDataInstrument _instrument;

    public StaticInstrumentResolver(FixMarketDataInstrument instrument)
    {
        _instrument = instrument;
    }

    public bool TryResolve(ulong securityId, out FixMarketDataInstrument instrument)
    {
        if (_instrument.SecurityId == securityId)
        {
            instrument = _instrument;
            return true;
        }

        instrument = default;
        return false;
    }
}

internal sealed class CountingSink : IFixApplicationMessageSink
{
    public int MessageCount { get; private set; }
    public int TotalBytes { get; private set; }

    public void OnMessage(ReadOnlyMemory<byte> message)
    {
        MessageCount++;
        TotalBytes += message.Length;
    }

    public void Reset()
    {
        MessageCount = 0;
        TotalBytes = 0;
    }
}
