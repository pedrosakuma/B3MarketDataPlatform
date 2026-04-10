using System.Threading.Channels;
using B3.Umdf.Transport;

namespace B3.Umdf.PcapReplay;

public sealed class InProcessPacketSource : IPacketSource
{
    private readonly Channel<UmdfPacket> _channel;

    public ChannelWriter<UmdfPacket> Writer => _channel.Writer;

    public InProcessPacketSource(int capacity = 4096)
    {
        _channel = Channel.CreateBounded<UmdfPacket>(new BoundedChannelOptions(capacity)
        {
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public async ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
    {
        return await _channel.Reader.ReadAsync(ct);
    }

    public void Dispose()
    {
        _channel.Writer.TryComplete();
    }
}
