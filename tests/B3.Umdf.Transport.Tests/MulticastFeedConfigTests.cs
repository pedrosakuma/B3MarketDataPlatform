using System.Net;
using B3.Umdf.Transport;

namespace B3.Umdf.Transport.Tests;

public class MulticastFeedConfigTests
{
    [Fact]
    public void Load_ParsesSampleConfig()
    {
        var json = """
        {
          "channelGroups": [
            {
              "name": "EQT",
              "channels": [
                { "channelId": 84, "type": "IncrementalA", "multicastGroup": "224.0.20.84", "port": 30084 },
                { "channelId": 84, "type": "IncrementalB", "multicastGroup": "224.0.20.85", "port": 30085 },
                { "channelId": 84, "type": "InstrumentDefinition", "multicastGroup": "224.0.20.86", "port": 30086 },
                { "channelId": 84, "type": "SnapshotRecovery", "multicastGroup": "224.0.20.87", "port": 30087, "receiveSocketCount": 4 }
              ]
            },
            {
              "name": "DRV",
              "channels": [
                { "channelId": 72, "type": "IncrementalA", "multicastGroup": "224.0.20.72", "port": 30072, "sourceAddress": "10.0.0.1", "localAddress": "10.0.0.10", "receiveBufferBytes": 1048576 }
              ]
            }
          ]
        }
        """;

        var tmpFile = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tmpFile, json);
            var config = MulticastFeedConfig.Load(tmpFile);

            Assert.Equal(2, config.ChannelGroups.Count);
            Assert.Equal("EQT", config.ChannelGroups[0].Name);
            Assert.Equal(4, config.ChannelGroups[0].Channels.Count);
            Assert.Equal("DRV", config.ChannelGroups[1].Name);

            var channelConfigs = config.ToChannelConfigs();
            Assert.Equal(5, channelConfigs.Count);

            // EQT group 0
            var eqtIncrA = channelConfigs[0];
            Assert.Equal(84, eqtIncrA.ChannelId);
            Assert.Equal(ChannelType.IncrementalA, eqtIncrA.Type);
            Assert.Equal(IPAddress.Parse("224.0.20.84"), eqtIncrA.MulticastGroup);
            Assert.Equal(30084, eqtIncrA.Port);
            Assert.Equal(0, eqtIncrA.ChannelGroup);
            Assert.Null(eqtIncrA.SourceAddress);
            Assert.Null(eqtIncrA.LocalAddress);
            Assert.Equal(16 * 1024 * 1024, eqtIncrA.ReceiveBufferBytes);
            Assert.Equal(1, eqtIncrA.ReceiveSocketCount);

            // EQT SnapshotRecovery channel uses receiveSocketCount=4
            var eqtSnap = channelConfigs[3];
            Assert.Equal(ChannelType.SnapshotRecovery, eqtSnap.Type);
            Assert.Equal(4, eqtSnap.ReceiveSocketCount);

            // DRV group 1 with source filter and explicit local interface
            var drvIncrA = channelConfigs[4];
            Assert.Equal(72, drvIncrA.ChannelId);
            Assert.Equal(ChannelType.IncrementalA, drvIncrA.Type);
            Assert.Equal(1, drvIncrA.ChannelGroup);
            Assert.Equal(IPAddress.Parse("10.0.0.1"), drvIncrA.SourceAddress);
            Assert.Equal(IPAddress.Parse("10.0.0.10"), drvIncrA.LocalAddress);
            Assert.Equal(1_048_576, drvIncrA.ReceiveBufferBytes);

            // Group IDs
            Assert.Equal([0, 1], config.GetGroupIds());
        }
        finally
        {
            File.Delete(tmpFile);
        }
    }

    [Fact]
    public void ToChannelConfigs_AssignsGroupIndexCorrectly()
    {
        var config = new MulticastFeedConfig
        {
            ChannelGroups =
            [
                new ChannelGroupConfig
                {
                    Name = "G0",
                    Channels = [
                        new ChannelEntryConfig { ChannelId = 1, Type = ChannelType.IncrementalA, MulticastGroup = "224.0.0.1", Port = 10001 },
                        new ChannelEntryConfig { ChannelId = 1, Type = ChannelType.IncrementalB, MulticastGroup = "224.0.0.2", Port = 10002 },
                    ]
                },
                new ChannelGroupConfig
                {
                    Name = "G1",
                    Channels = [
                        new ChannelEntryConfig { ChannelId = 2, Type = ChannelType.IncrementalA, MulticastGroup = "224.0.0.3", Port = 10003 },
                    ]
                }
            ]
        };

        var flat = config.ToChannelConfigs();
        Assert.Equal(3, flat.Count);
        Assert.All(flat.Take(2), c => Assert.Equal(0, c.ChannelGroup));
        Assert.Equal(1, flat[2].ChannelGroup);
    }

    [Fact]
    public void ToPublishChannelConfigs_ReusesDestinationsAndIgnoresSourceFilter()
    {
        var config = new MulticastFeedConfig
        {
            ChannelGroups =
            [
                new ChannelGroupConfig
                {
                    Name = "G0",
                    Channels =
                    [
                        new ChannelEntryConfig
                        {
                            ChannelId = 84,
                            Type = ChannelType.IncrementalA,
                            MulticastGroup = "224.0.20.84",
                            Port = 30084,
                            SourceAddress = "10.0.0.1",
                            LocalAddress = "10.0.0.10",
                            ReceiveBufferBytes = 1_048_576
                        }
                    ]
                }
            ]
        };

        var publish = config.ToPublishChannelConfigs();

        var route = Assert.Single(publish);
        Assert.Equal(84, route.ChannelId);
        Assert.Equal(ChannelType.IncrementalA, route.Type);
        Assert.Equal(IPAddress.Parse("224.0.20.84"), route.MulticastGroup);
        Assert.Equal(30084, route.Port);
        Assert.Equal(IPAddress.Parse("10.0.0.10"), route.LocalAddress);
        Assert.Equal(0, route.ChannelGroup);
    }
}
