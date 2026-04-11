using System.Net;
using System.Net.Sockets;

namespace B3.Umdf.Transport;

public sealed class MulticastPacketSource : IPacketSource
{
    private readonly UdpClient _client;
    private readonly ChannelType _channelType;
    private readonly int _channelGroup;

    public MulticastPacketSource(ChannelConfig config)
    {
        _channelType = config.Type;
        _channelGroup = config.ChannelGroup;
        _client = new UdpClient();
        _client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _client.Client.Bind(new IPEndPoint(IPAddress.Any, config.Port));
        _client.JoinMulticastGroup(config.MulticastGroup, config.SourceAddress ?? IPAddress.Any);
    }

    public async ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
    {
        var result = await _client.ReceiveAsync(ct);
        return new UmdfPacket
        {
            Data = result.Buffer,
            Channel = _channelType,
            ChannelGroup = _channelGroup,
            ReceivedTimestampTicks = Environment.TickCount64
        };
    }

    public void Dispose() => _client.Dispose();
}
