using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Transport;

/// <summary>
/// Thin wrapper around the SBE-generated <see cref="PacketHeader"/> (16 bytes)
/// and <see cref="FramingHeader"/> (4 bytes per message).
///
/// Wire layout of a UMDF packet (UDP payload):
/// <code>
/// [PacketHeader 16 bytes]
///   [FramingHeader 4 bytes][SBE message (MessageHeader + body)]
///   [FramingHeader 4 bytes][SBE message ...]
///   ...
/// </code>
/// The FramingHeader.MessageLength includes the FramingHeader itself,
/// so the SBE message length is <c>MessageLength - FramingHeader.MESSAGE_SIZE</c>.
/// </summary>
public static class UmdfPacketHeader
{
    public const int Size = PacketHeader.MESSAGE_SIZE; // 16

    public static bool TryRead(ReadOnlySpan<byte> buffer, out PacketHeader header)
    {
        return PacketHeader.TryParse(buffer, out header, out _);
    }
}
