using System.Net;

namespace B3.Umdf.Transport;

public sealed record ChannelConfig(
    int ChannelId,
    ChannelType Type,
    IPAddress MulticastGroup,
    int Port,
    IPAddress? SourceAddress = null);
