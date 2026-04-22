using B3.Umdf.PcapReplay;

namespace B3.Umdf.PcapReplay.Tests;

public class UdpExtractorAdditionalTests
{
    [Fact]
    public void ComputeUdpPayloadOffset_Ethernet_NoVlan()
    {
        byte[] frame = new byte[14 + 20 + 8];
        frame[14] = 0x45; // IPv4 IHL=5

        var offset = UdpExtractor.ComputeUdpPayloadOffset(frame, linkType: 1);

        Assert.Equal(42, offset);
    }

    [Fact]
    public void ComputeUdpPayloadOffset_Ethernet_IpOptions_AccountsForExtendedIhl()
    {
        // IHL=7 -> IP header is 28 bytes (8 bytes of options).
        byte[] frame = new byte[14 + 28 + 8];
        frame[14] = 0x47;

        var offset = UdpExtractor.ComputeUdpPayloadOffset(frame, linkType: 1);

        Assert.Equal(14 + 28 + 8, offset);
    }

    [Fact]
    public void ExtractUdpPayload_Sll_LinkType113_ReturnsPayload()
    {
        // Linux cooked (SLL) IP starts at offset 16.
        byte[] payload = "SLL!"u8.ToArray();
        byte[] frame = new byte[16 + 20 + 8 + payload.Length];
        frame[16] = 0x45;
        Array.Copy(payload, 0, frame, 16 + 20 + 8, payload.Length);

        var extracted = UdpExtractor.ExtractUdpPayload(frame, linkType: 113);

        Assert.Equal(payload, extracted.ToArray());
    }

    [Fact]
    public void ExtractUdpDstPort_Sll_LinkType113_ReturnsPort()
    {
        byte[] frame = new byte[16 + 20 + 8];
        frame[16] = 0x45;
        // UDP dst at IP offset + IHL*4 + 2 = 16 + 20 + 2 = 38
        frame[38] = 0x04;
        frame[39] = 0xD2; // 1234

        ushort port = UdpExtractor.ExtractUdpDstPort(frame, linkType: 113);

        Assert.Equal(1234, port);
    }

    [Fact]
    public void ExtractUdpPayload_DoubleVlan_QinQ_ReturnsPayload()
    {
        // Outer VLAN (0x88A8) + inner VLAN (0x8100) + IPv4 (0x0800).
        byte[] payload = "QINQ"u8.ToArray();
        byte[] frame = new byte[14 + 4 + 4 + 20 + 8 + payload.Length];

        // EtherType outer = 0x88A8
        frame[12] = 0x88; frame[13] = 0xA8;
        // Outer VLAN TCI bytes 14-15 (don't care), inner EtherType at 16
        frame[16] = 0x81; frame[17] = 0x00;
        // Inner VLAN TCI bytes 18-19 (don't care), real EtherType at 20
        frame[20] = 0x08; frame[21] = 0x00;
        // IPv4 starts at 22
        frame[22] = 0x45;
        // UDP at 22+20=42, payload at 42+8=50
        Array.Copy(payload, 0, frame, 50, payload.Length);

        var extracted = UdpExtractor.ExtractUdpPayload(frame, linkType: 1);

        Assert.Equal(payload, extracted.ToArray());
    }

    [Fact]
    public void ExtractUdpDstPort_VlanTagged_ReturnsPort()
    {
        byte[] frame = new byte[14 + 4 + 20 + 8];
        frame[12] = 0x81; frame[13] = 0x00;        // VLAN
        frame[16] = 0x08; frame[17] = 0x00;        // IPv4
        frame[18] = 0x45;
        // UDP dst at 18 + 20 + 2 = 40
        frame[40] = 0x1F; frame[41] = 0x90;        // 8080

        ushort port = UdpExtractor.ExtractUdpDstPort(frame, linkType: 1);

        Assert.Equal(8080, port);
    }

    [Fact]
    public void ExtractUdpDstPort_UnsupportedLinkType_Throws()
    {
        Assert.Throws<NotSupportedException>(() =>
            UdpExtractor.ExtractUdpDstPort(new byte[100], linkType: 7));
    }
}
