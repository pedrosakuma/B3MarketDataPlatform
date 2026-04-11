using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Transport;

public readonly struct UmdfPacket
{
    public ReadOnlyMemory<byte> Data { get; init; }
    public ChannelType Channel { get; init; }
    public int ChannelGroup { get; init; }
    public long ReceivedTimestampTicks { get; init; }

    public bool TryGetHeader(out PacketHeader header)
    {
        return UmdfPacketHeader.TryRead(Data.Span, out header);
    }
}
