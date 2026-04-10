namespace B3.Umdf.PcapReplay;

public static class UdpExtractor
{
    public static ReadOnlyMemory<byte> ExtractUdpPayload(ReadOnlyMemory<byte> frame, uint linkType = 1)
    {
        int offset = GetIpOffset(frame.Span, linkType);
        var span = frame.Span;
        int ihl = (span[offset] & 0x0F) * 4;
        offset += ihl;
        // UDP header is 8 bytes: src_port(2) dst_port(2) length(2) checksum(2)
        offset += 8;
        return frame[offset..];
    }

    public static ushort ExtractUdpDstPort(ReadOnlySpan<byte> frame, uint linkType = 1)
    {
        int offset = GetIpOffset(frame, linkType);
        int ihl = (frame[offset] & 0x0F) * 4;
        offset += ihl;
        return (ushort)((frame[offset + 2] << 8) | frame[offset + 3]);
    }

    private static int GetIpOffset(ReadOnlySpan<byte> frame, uint linkType)
    {
        return linkType switch
        {
            1 => GetEthernetIpOffset(frame), // Ethernet (may have VLAN tags)
            113 => 16, // Linux cooked capture (SLL)
            _ => throw new NotSupportedException($"Unsupported link type: {linkType}")
        };
    }

    private static int GetEthernetIpOffset(ReadOnlySpan<byte> frame)
    {
        // Standard Ethernet header: 14 bytes (dst[6] + src[6] + ethertype[2])
        int offset = 12; // skip MACs, point at EtherType
        ushort etherType = (ushort)((frame[offset] << 8) | frame[offset + 1]);

        // Skip 802.1Q VLAN tags (0x8100) — may be stacked (QinQ)
        while (etherType == 0x8100 || etherType == 0x88A8)
        {
            offset += 4; // VLAN tag: TPID(2) + TCI(2)
            etherType = (ushort)((frame[offset] << 8) | frame[offset + 1]);
        }

        return offset + 2; // skip the final EtherType field → IP header starts here
    }
}
