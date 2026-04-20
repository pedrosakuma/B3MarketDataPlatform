using System.Net;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Transport.Tests;

public class MulticastPacketPublisherTests
{
    [Fact]
    public void Publish_RoutesPacketToMatchingChannelAndCountsBytes()
    {
        var sent = new Dictionary<(int Group, ChannelType Type), FakePublishSocket>();
        var configs = new[]
        {
            new MulticastPublishChannelConfig(84, ChannelType.IncrementalA, IPAddress.Parse("224.0.20.84"), 30084, ChannelGroup: 0),
            new MulticastPublishChannelConfig(72, ChannelType.SnapshotRecovery, IPAddress.Parse("224.0.20.75"), 30075, ChannelGroup: 1),
        };

        using var publisher = new MulticastPacketPublisher(
            configs,
            NullLogger<MulticastPacketPublisher>.Instance,
            (config, endpoint) =>
            {
                var socket = new FakePublishSocket(endpoint);
                sent[(config.ChannelGroup, config.Type)] = socket;
                return socket;
            });

        var packet = new UmdfPacket
        {
            Data = new byte[] { 1, 2, 3, 4 },
            Channel = ChannelType.SnapshotRecovery,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 1
        };

        publisher.Publish(in packet);

        Assert.Equal(1, publisher.PublishedPackets);
        Assert.Equal(4, publisher.PublishedBytes);
        Assert.True(publisher.LastPublishTimestampTicks > 0);
        Assert.Empty(sent[(0, ChannelType.IncrementalA)].Payloads);
        var sentPayload = Assert.Single(sent[(1, ChannelType.SnapshotRecovery)].Payloads);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, sentPayload);
    }

    [Fact]
    public void Publish_ThrowsWhenRouteIsMissing()
    {
        using var publisher = new MulticastPacketPublisher(
            [new MulticastPublishChannelConfig(84, ChannelType.IncrementalA, IPAddress.Parse("224.0.20.84"), 30084, ChannelGroup: 0)],
            NullLogger<MulticastPacketPublisher>.Instance,
            (config, endpoint) => new FakePublishSocket(endpoint));

        var packet = new UmdfPacket
        {
            Data = new byte[] { 9 },
            Channel = ChannelType.SnapshotRecovery,
            ChannelGroup = 0,
            ReceivedTimestampTicks = 1
        };

        var ex = Assert.Throws<InvalidOperationException>(() => publisher.Publish(in packet));
        Assert.Contains("No multicast publish route configured", ex.Message);
    }

    [Fact]
    public void Constructor_ThrowsOnDuplicateRoute()
    {
        var configs = new[]
        {
            new MulticastPublishChannelConfig(84, ChannelType.IncrementalA, IPAddress.Parse("224.0.20.84"), 30084, ChannelGroup: 0),
            new MulticastPublishChannelConfig(84, ChannelType.IncrementalA, IPAddress.Parse("224.0.20.85"), 30085, ChannelGroup: 0),
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            new MulticastPacketPublisher(
                configs,
                NullLogger<MulticastPacketPublisher>.Instance,
                (config, endpoint) => new FakePublishSocket(endpoint)));

        Assert.Contains("Duplicate multicast publish route", ex.Message);
    }

    private sealed class FakePublishSocket : MulticastPacketPublisher.IPublishSocket
    {
        public IPEndPoint Endpoint { get; }
        public List<byte[]> Payloads { get; } = [];

        public FakePublishSocket(IPEndPoint endpoint)
        {
            Endpoint = endpoint;
        }

        public int Pending => 0;

        public void Send(ReadOnlyMemory<byte> payload, out int messagesFlushed, out int bytesFlushed)
        {
            Payloads.Add(payload.ToArray());
            messagesFlushed = 1;
            bytesFlushed = payload.Length;
        }

        public void Flush(out int messagesFlushed, out int bytesFlushed)
        {
            messagesFlushed = 0;
            bytesFlushed = 0;
        }

        public void Dispose() { }
    }
}
