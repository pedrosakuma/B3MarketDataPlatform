using System.Net;

namespace B3.Umdf.Transport;

public sealed record ChannelConfig(
    int ChannelId,
    ChannelType Type,
    IPAddress MulticastGroup,
    int Port,
    IPAddress? SourceAddress = null,
    IPAddress? LocalAddress = null,
    int ReceiveBufferBytes = 16 * 1024 * 1024,
    int ChannelGroup = 0,
    int ReceiveSocketCount = 1,
    TransportKind Transport = TransportKind.Multicast);
