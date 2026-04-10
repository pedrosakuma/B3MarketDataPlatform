namespace B3.Umdf.PcapReplay;

public readonly struct PcapPacket
{
    public long TimestampMicros { get; init; }
    public ReadOnlyMemory<byte> Data { get; init; }
}
