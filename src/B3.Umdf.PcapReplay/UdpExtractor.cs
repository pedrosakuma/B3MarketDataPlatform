namespace B3.Umdf.PcapReplay;

public static class UdpExtractor
{
    public static ReadOnlyMemory<byte> ExtractUdpPayload(ReadOnlyMemory<byte> frame, uint linkType = 1)
    {
        int offset = ComputeUdpPayloadOffset(frame.Span, linkType);
        return frame[offset..];
    }

    /// <summary>
    /// Computes the byte offset from frame start to the UDP payload.
    /// Constant per PCAP file — cache this for zero-overhead packet slicing.
    /// </summary>
    public static int ComputeUdpPayloadOffset(ReadOnlySpan<byte> frame, uint linkType = 1)
    {
        int offset = GetIpOffset(frame, linkType);
        int ihl = (frame[offset] & 0x0F) * 4;
        return offset + ihl + 8; // IP header + UDP header (8 bytes)
    }

    /// <summary>
    /// Best-effort, non-throwing variant of <see cref="ComputeUdpPayloadOffset"/>.
    /// Returns false (and offset = -1) when the frame is too short, the link type is unsupported,
    /// or the computed offset would land outside the captured frame. Intended for corruption-tolerant
    /// PCAP readers that need to skip malformed records without aborting the merge.
    /// </summary>
    public static bool TryComputeUdpPayloadOffset(ReadOnlySpan<byte> frame, uint linkType, out int offset)
    {
        offset = -1;
        try
        {
            if (frame.Length < 28) return false;
            if (linkType != 1 && linkType != 113) return false;

            int ipOff = GetIpOffset(frame, linkType);
            if (ipOff < 0 || ipOff >= frame.Length) return false;
            int ihl = (frame[ipOff] & 0x0F) * 4;
            if (ihl < 20) return false; // IPv4 minimum header length
            int payloadStart = ipOff + ihl + 8;
            if (payloadStart < 0 || payloadStart > frame.Length) return false;

            offset = payloadStart;
            return true;
        }
        catch
        {
            return false;
        }
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
        int offset = 12;
        ushort etherType = (ushort)((frame[offset] << 8) | frame[offset + 1]);
        while (etherType == 0x8100 || etherType == 0x88A8)
        {
            offset += 4;
            etherType = (ushort)((frame[offset] << 8) | frame[offset + 1]);
        }
        return offset + 2;
    }
}
