namespace B3.Umdf.PcapReplay;

public static class UdpExtractor
{
    public static ReadOnlyMemory<byte> ExtractUdpPayload(ReadOnlyMemory<byte> frame, uint linkType = 1)
    {
        int offset = linkType switch
        {
            1 => 14,   // Ethernet
            113 => 16, // Linux cooked capture
            _ => throw new NotSupportedException($"Unsupported link type: {linkType}")
        };

        var span = frame.Span;
        int ihl = (span[offset] & 0x0F) * 4;
        offset += ihl;
        // UDP header is 8 bytes: src_port(2) dst_port(2) length(2) checksum(2)
        offset += 8;
        return frame[offset..];
    }

    public static ushort ExtractUdpDstPort(ReadOnlySpan<byte> frame, uint linkType = 1)
    {
        int offset = linkType switch
        {
            1 => 14,
            113 => 16,
            _ => throw new NotSupportedException($"Unsupported link type: {linkType}")
        };
        int ihl = (frame[offset] & 0x0F) * 4;
        offset += ihl;
        return (ushort)((frame[offset + 2] << 8) | frame[offset + 3]);
    }
}
