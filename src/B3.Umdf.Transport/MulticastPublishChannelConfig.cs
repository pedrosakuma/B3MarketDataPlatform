using System.Net;

namespace B3.Umdf.Transport;

public sealed record MulticastPublishChannelConfig(
    int ChannelId,
    ChannelType Type,
    IPAddress MulticastGroup,
    int Port,
    IPAddress? LocalAddress = null,
    int ChannelGroup = 0);
