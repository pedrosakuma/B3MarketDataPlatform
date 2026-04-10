using System.Net;
using System.Net.Sockets;

namespace B3.Umdf.Transport;

public sealed class MulticastPacketSink : IPacketSink
{
    private readonly UdpClient _client;
    private readonly IPEndPoint _target;
    private readonly ChannelType _channelType;

    public MulticastPacketSink(ChannelConfig config)
    {
        _channelType = config.Type;
        _target = new IPEndPoint(config.MulticastGroup, config.Port);
        _client = new UdpClient();
        _client.Client.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 1);
    }

    public async ValueTask SendAsync(ReadOnlyMemory<byte> data, ChannelType channel, CancellationToken ct = default)
    {
        await _client.SendAsync(data, _target, ct);
    }

    public void Dispose() => _client.Dispose();
}
