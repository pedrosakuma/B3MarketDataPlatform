using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Transport;

public readonly struct UmdfPacket
{
    public ReadOnlyMemory<byte> Data { get; init; }
    public ChannelType Channel { get; init; }
    public int ChannelGroup { get; init; }
    public long ReceivedTimestampTicks { get; init; }
    internal UmdfPacketLease? Lease { get; init; }

    public bool TryGetHeader(out PacketHeader header)
    {
        return UmdfPacketHeader.TryRead(Data.Span, out header);
    }

    /// <summary>
    /// Retains the packet buffer when the packet must outlive the current call
    /// (for example, when enqueuing for catch-up or another processing stage).
    /// No-op for borrowed packet data.
    /// </summary>
    public void Retain() => Lease?.Retain();

    /// <summary>
    /// Releases the packet buffer after the current owner is done processing it.
    /// No-op for borrowed packet data.
    /// </summary>
    public void Release() => Lease?.Release();

    internal static UmdfPacket CreateOwned(
        ReadOnlyMemory<byte> data,
        ChannelType channel,
        int channelGroup,
        long receivedTimestampTicks,
        UmdfPacketLease lease)
    {
        ArgumentNullException.ThrowIfNull(lease);

        return new UmdfPacket
        {
            Data = data,
            Channel = channel,
            ChannelGroup = channelGroup,
            ReceivedTimestampTicks = receivedTimestampTicks,
            Lease = lease
        };
    }
}
