namespace B3.Umdf.Transport;

/// <summary>
/// Wire transport selected per channel for ingest.
/// </summary>
public enum TransportKind
{
    /// <summary>
    /// IPv4 multicast (default). The configured group address must be in 224.0.0.0/4
    /// and the consumer issues an IGMP join (ASM) or source-specific join (SSM).
    /// </summary>
    Multicast = 0,

    /// <summary>
    /// Plain unicast UDP. The consumer binds to <see cref="ChannelConfig.MulticastGroup"/>
    /// (treated as bind address; typically <c>0.0.0.0</c>) and <see cref="ChannelConfig.Port"/>
    /// without issuing any IGMP join. Required for Docker Compose bridge networks where
    /// IPv4 multicast is not forwarded between sibling containers and the publisher
    /// (e.g. B3MatchingPlatform's exchange-simulator) emits UMDF frames as unicast UDP.
    /// </summary>
    Unicast = 1,
}
