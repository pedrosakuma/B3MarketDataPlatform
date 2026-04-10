namespace B3.Umdf.Transport;

public interface IPacketSink : IDisposable
{
    ValueTask SendAsync(ReadOnlyMemory<byte> data, ChannelType channel, CancellationToken ct = default);
}
