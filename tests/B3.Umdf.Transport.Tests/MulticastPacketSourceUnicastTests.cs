using System.Net;
using System.Net.Sockets;
using B3.Umdf.Transport;

namespace B3.Umdf.Transport.Tests;

public class MulticastPacketSourceUnicastTests
{
    [Fact]
    public void ParseTransport_DefaultsToMulticast()
    {
        Assert.Equal(TransportKind.Multicast, MulticastFeedConfig.ParseTransport(null));
        Assert.Equal(TransportKind.Multicast, MulticastFeedConfig.ParseTransport(""));
        Assert.Equal(TransportKind.Multicast, MulticastFeedConfig.ParseTransport("multicast"));
        Assert.Equal(TransportKind.Multicast, MulticastFeedConfig.ParseTransport("MULTICAST"));
    }

    [Fact]
    public void ParseTransport_AcceptsUnicastCaseInsensitive()
    {
        Assert.Equal(TransportKind.Unicast, MulticastFeedConfig.ParseTransport("unicast"));
        Assert.Equal(TransportKind.Unicast, MulticastFeedConfig.ParseTransport("Unicast"));
        Assert.Equal(TransportKind.Unicast, MulticastFeedConfig.ParseTransport("UNICAST"));
    }

    [Fact]
    public void ParseTransport_RejectsUnknownValues()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => MulticastFeedConfig.ParseTransport("tcp"));
        Assert.Contains("multicast", ex.Message);
        Assert.Contains("unicast", ex.Message);
    }

    [Fact]
    public void Load_ParsesUnicastTransportField()
    {
        var json = """
        {
          "channelGroups": [{
            "name": "EQT",
            "channels": [
              { "channelId": 84, "type": "IncrementalA",         "transport": "unicast", "multicastGroup": "0.0.0.0", "port": 0 },
              { "channelId": 84, "type": "IncrementalB",                                  "multicastGroup": "224.0.20.85", "port": 30085 }
            ]
          }]
        }
        """;

        var tmp = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tmp, json);
            var configs = MulticastFeedConfig.Load(tmp).ToChannelConfigs();
            Assert.Equal(2, configs.Count);
            Assert.Equal(TransportKind.Unicast, configs[0].Transport);
            Assert.Equal(TransportKind.Multicast, configs[1].Transport);
        }
        finally { File.Delete(tmp); }
    }

    [Fact]
    public void Ctor_Unicast_BindsAndDoesNotJoinMulticastGroup()
    {
        var config = new ChannelConfig(
            ChannelId: 1,
            Type: ChannelType.IncrementalA,
            MulticastGroup: IPAddress.Any,
            Port: 0,
            ReceiveBufferBytes: 64 * 1024,
            Transport: TransportKind.Unicast);

        using var src = new MulticastPacketSource(config);

        Assert.Equal(TransportKind.Unicast, src.Transport);
        Assert.True(src.IsJoined, "unicast source should report ready");
        Assert.Equal(1, src.MembershipJoins);
    }

    [Fact]
    public async Task Ctor_Unicast_ReceivesLoopbackDatagram()
    {
        // Bind to ephemeral port on loopback, send a unicast datagram to it,
        // verify the source returns it without ever issuing an IGMP join.
        int port = GetEphemeralUdpPort();
        var config = new ChannelConfig(
            ChannelId: 1,
            Type: ChannelType.IncrementalA,
            MulticastGroup: IPAddress.Loopback,
            Port: port,
            ReceiveBufferBytes: 64 * 1024,
            Transport: TransportKind.Unicast);

        using var src = new MulticastPacketSource(config);

        var payload = new byte[] { 1, 2, 3, 4, 5 };
        using (var sender = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp))
        {
            sender.SendTo(payload, new IPEndPoint(IPAddress.Loopback, port));
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var pkt = await src.ReceiveAsync(cts.Token);
        try
        {
            Assert.Equal(payload, pkt.Data.ToArray());
            Assert.Equal(ChannelType.IncrementalA, pkt.Channel);
        }
        finally
        {
            pkt.Release();
        }
    }

    [Fact]
    public void Ctor_Unicast_RejectsSourceAddress()
    {
        var config = new ChannelConfig(
            ChannelId: 1,
            Type: ChannelType.IncrementalA,
            MulticastGroup: IPAddress.Any,
            Port: 0,
            SourceAddress: IPAddress.Parse("10.0.0.1"),
            ReceiveBufferBytes: 64 * 1024,
            Transport: TransportKind.Unicast);

        var ex = Assert.Throws<ArgumentException>(() => new MulticastPacketSource(config));
        Assert.Contains("sourceAddress", ex.Message);
    }

    [Fact]
    public void Ctor_Unicast_RejectsLocalAddress()
    {
        var config = new ChannelConfig(
            ChannelId: 1,
            Type: ChannelType.IncrementalA,
            MulticastGroup: IPAddress.Any,
            Port: 0,
            LocalAddress: IPAddress.Parse("10.0.0.1"),
            ReceiveBufferBytes: 64 * 1024,
            Transport: TransportKind.Unicast);

        var ex = Assert.Throws<ArgumentException>(() => new MulticastPacketSource(config));
        Assert.Contains("localAddress", ex.Message);
    }

    [Fact]
    public void LeaveAndRejoin_AreNoOpForUnicast()
    {
        var config = new ChannelConfig(
            ChannelId: 1,
            Type: ChannelType.IncrementalA,
            MulticastGroup: IPAddress.Any,
            Port: 0,
            ReceiveBufferBytes: 64 * 1024,
            Transport: TransportKind.Unicast);

        using var src = new MulticastPacketSource(config);
        long joinsBefore = src.MembershipJoins;
        long leavesBefore = src.MembershipLeaves;

        src.LeaveMulticastGroup();
        src.RejoinMulticastGroup();

        Assert.True(src.IsJoined, "unicast source IsJoined should remain true");
        Assert.Equal(joinsBefore, src.MembershipJoins);
        Assert.Equal(leavesBefore, src.MembershipLeaves);
    }

    private static int GetEphemeralUdpPort()
    {
        using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)probe.LocalEndPoint!).Port;
    }
}
