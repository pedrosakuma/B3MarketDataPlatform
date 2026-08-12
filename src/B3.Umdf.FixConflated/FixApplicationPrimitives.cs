using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

[Flags]
public enum FixMarketDataEntryFields : byte
{
    None = 0,
    Price = 1 << 0,
    Size = 1 << 1,
    OrderId = 1 << 2,
    TradeId = 1 << 3,
}

public enum FixMdEntryType : byte
{
    Bid = (byte)'0',
    Offer = (byte)'1',
    Trade = (byte)'2',
}

public enum FixMdUpdateAction : byte
{
    New = (byte)'0',
    Change = (byte)'1',
    Delete = (byte)'2',
    DeleteThru = (byte)'3',
}

public readonly record struct FixApplicationSessionHeader(
    string SenderCompId,
    string TargetCompId,
    int MsgSeqNum,
    DateTimeOffset SendingTime,
    string BeginString = FixMessageCodec.BeginString);

public readonly record struct FixMarketDataInstrument
{
    public FixMarketDataInstrument(string symbol, ulong securityId, int priceScale = 0)
    {
        ArgumentNullException.ThrowIfNull(symbol);
        if (priceScale is < 0 or > 18)
            throw new ArgumentOutOfRangeException(nameof(priceScale));

        Symbol = symbol;
        SecurityId = securityId;
        PriceScale = priceScale;
    }

    public string Symbol { get; }
    public ulong SecurityId { get; }
    public int PriceScale { get; }

    public FixMdEntryType GetEntryType(BookSideType side)
        => side == BookSideType.Bid ? FixMdEntryType.Bid : FixMdEntryType.Offer;
}

public readonly record struct FixMarketDataSnapshotRequest
{
    public FixMarketDataSnapshotRequest(string mdReqId, FixMarketDataInstrument instrument)
    {
        ArgumentNullException.ThrowIfNull(mdReqId);
        MdReqId = mdReqId;
        Instrument = instrument;
    }

    public string MdReqId { get; }
    public FixMarketDataInstrument Instrument { get; }
}

public readonly record struct FixMarketDataIncrementalEntry(
    FixMdUpdateAction UpdateAction,
    FixMdEntryType EntryType,
    DateTimeOffset EntryTime,
    FixMarketDataEntryFields Fields = FixMarketDataEntryFields.None,
    long Price = 0,
    long Size = 0,
    ulong OrderId = 0,
    long TradeId = 0);

public interface IFixApplicationHeaderProvider
{
    FixApplicationSessionHeader NextHeader(DateTimeOffset sendingTime);
}

public interface IFixApplicationMessageSink
{
    void OnMessage(ReadOnlyMemory<byte> message);
}

public interface IFixMarketDataInstrumentResolver
{
    bool TryResolve(ulong securityId, out FixMarketDataInstrument instrument);
}

public sealed class FixConflatedMarketDataOptions
{
    public static readonly TimeSpan DefaultConflationInterval = TimeSpan.FromMilliseconds(380);
    public const int DefaultPendingEventCapacity = 65_536;

    public TimeSpan ConflationInterval { get; init; } = DefaultConflationInterval;
    public int InitialBufferSize { get; init; } = 4 * 1024;
    public int PendingEventCapacity { get; init; } = DefaultPendingEventCapacity;
    public bool StartBackgroundWorker { get; init; } = true;
}
