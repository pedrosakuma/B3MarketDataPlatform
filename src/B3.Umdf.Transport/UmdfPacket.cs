namespace B3.Umdf.Transport;

public readonly struct UmdfPacket
{
    public ReadOnlyMemory<byte> Data { get; init; }
    public ChannelType Channel { get; init; }
    public long ReceivedTimestampTicks { get; init; }

    public ref readonly UmdfPacketHeader Header
    {
        get => ref UmdfPacketHeader.Read(Data.Span);
    }
}
