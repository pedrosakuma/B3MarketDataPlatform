using B3.Umdf.PcapReplay;

namespace B3.Umdf.PcapReplay.Tests;

public class UdpExtractorTests
{
    [Fact]
    public void ExtractUdpPayload_Ethernet_ReturnsPayload()
    {
        // Build a minimal Ethernet + IP + UDP frame
        // Ethernet header: 14 bytes (dst[6] + src[6] + type[2])
        // IP header: 20 bytes (minimum, IHL=5)
        // UDP header: 8 bytes (srcPort[2] + dstPort[2] + length[2] + checksum[2])
        // Payload: "HELLO"

        byte[] payload = "HELLO"u8.ToArray();
        byte[] frame = new byte[14 + 20 + 8 + payload.Length];

        // Ethernet header (14 bytes) - just zeroes for simplicity
        // IP header at offset 14: version=4, IHL=5 -> byte = 0x45
        frame[14] = 0x45;
        // UDP header at offset 34
        // Payload at offset 42
        Array.Copy(payload, 0, frame, 42, payload.Length);

        var extracted = UdpExtractor.ExtractUdpPayload(frame, linkType: 1);

        Assert.Equal(payload, extracted.ToArray());
    }

    [Fact]
    public void ExtractUdpDstPort_Ethernet_ReturnsPort()
    {
        byte[] frame = new byte[14 + 20 + 8];
        frame[14] = 0x45; // IPv4, IHL=5

        // UDP dst port at IP offset + IHL*4 + 2 = 14 + 20 + 2 = 36
        frame[36] = 0x1F; // high byte
        frame[37] = 0x90; // low byte = port 8080

        ushort port = UdpExtractor.ExtractUdpDstPort(frame, linkType: 1);
        Assert.Equal(8080, port);
    }

    [Fact]
    public void ExtractUdpPayload_UnsupportedLinkType_Throws()
    {
        Assert.Throws<NotSupportedException>(() =>
            UdpExtractor.ExtractUdpPayload(new byte[100], linkType: 99));
    }

    [Fact]
    public void ExtractUdpPayload_VlanTagged_ReturnsPayload()
    {
        // Ethernet + 802.1Q VLAN tag (4 extra bytes) + IP + UDP + payload
        byte[] payload = "VLAN"u8.ToArray();
        byte[] frame = new byte[14 + 4 + 20 + 8 + payload.Length]; // 18 byte eth + IP + UDP

        // EtherType at offset 12 = 0x8100 (VLAN)
        frame[12] = 0x81;
        frame[13] = 0x00;
        // VLAN TCI at 14-15 (don't care)
        // Real EtherType at offset 16 = 0x0800 (IPv4)
        frame[16] = 0x08;
        frame[17] = 0x00;
        // IP header at offset 18: version=4, IHL=5
        frame[18] = 0x45;
        // UDP header at offset 38, payload at offset 46
        Array.Copy(payload, 0, frame, 46, payload.Length);

        var extracted = UdpExtractor.ExtractUdpPayload(frame, linkType: 1);
        Assert.Equal(payload, extracted.ToArray());
    }
}
