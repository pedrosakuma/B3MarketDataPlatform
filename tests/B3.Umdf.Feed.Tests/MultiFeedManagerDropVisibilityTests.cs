using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the drop-visibility contract for live-push mode (PushPacket): when the
/// per-group ring overflows, the manager must (a) advance both the per-group
/// drop counter and the aggregate <see cref="MultiFeedManager.DroppedPacketsTotal"/>,
/// and (b) emit a power-of-two cadence warning so operators are not blind to
/// app-level backpressure.
/// </summary>
public class MultiFeedManagerDropVisibilityTests
{
    [Fact]
    public void PushPacket_RingFull_IncrementsDropCountersAndLogsAtPowerOfTwo()
    {
        var logger = new RecordingLogger();
        var manager = new MultiFeedManager(
            new[] { 0 },
            new NoopHandler(),
            feedLogger: null,
            marketDataHandler: null,
            logger: logger,
            feedChannelCapacity: 0,
            groupRingCapacity: 2);
        try
        {
            // Ring capacity is 2; we never start the dispatch thread, so the
            // ring fills permanently after the second enqueue. Push 8 packets:
            // 2 succeed, 6 are dropped → log milestones at 1, 2, 4 (3 lines).
            for (int i = 0; i < 8; i++)
                manager.PushPacket(MakePacket(seqNum: (uint)(i + 1)));

            Assert.Equal(6L, manager.DroppedPacketsTotal);
            var stats = manager.GetChannelStats().Single();
            Assert.Equal(0, stats.GroupId);
            Assert.Equal(6L, stats.DroppedPackets);

            // Power-of-two cadence: drops 1, 2, 4 trigger logs (3 entries),
            // drops 3, 5, 6 do not.
            Assert.Equal(3, logger.Warnings.Count);
            Assert.All(logger.Warnings, msg =>
                Assert.Contains("dropped", msg, StringComparison.Ordinal));
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Fact]
    public void PushPacket_UnknownGroup_ReleasesPacket_DoesNotIncrementCounter()
    {
        var logger = new RecordingLogger();
        var manager = new MultiFeedManager(
            new[] { 0 },
            new NoopHandler(),
            feedLogger: null,
            marketDataHandler: null,
            logger: logger,
            feedChannelCapacity: 0,
            groupRingCapacity: 4);
        try
        {
            // Group 99 is not configured: packet must be released without
            // touching the per-group drop counter (no ring exists for it).
            manager.PushPacket(MakePacket(seqNum: 1, channelGroup: 99));

            Assert.Equal(0L, manager.DroppedPacketsTotal);
            Assert.Empty(logger.Warnings);
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Fact]
    public void PushPacket_RingFull_AttributesDropsToOriginatingChannel()
    {
        var logger = new RecordingLogger();
        var manager = new MultiFeedManager(
            new[] { 0 },
            new NoopHandler(),
            feedLogger: null,
            marketDataHandler: null,
            logger: logger,
            feedChannelCapacity: 0,
            groupRingCapacity: 2);
        try
        {
            // Ring capacity = 2; never drained. Fill it with two IncA packets,
            // then overflow with a mix: 3 IncB, 2 Snap, 1 Instr → 6 drops total
            // attributed by channel.
            manager.PushPacket(MakePacket(seqNum: 1, channel: ChannelType.IncrementalA));
            manager.PushPacket(MakePacket(seqNum: 2, channel: ChannelType.IncrementalA));
            for (int i = 0; i < 3; i++)
                manager.PushPacket(MakePacket(seqNum: (uint)(10 + i), channel: ChannelType.IncrementalB));
            for (int i = 0; i < 2; i++)
                manager.PushPacket(MakePacket(seqNum: (uint)(20 + i), channel: ChannelType.SnapshotRecovery));
            manager.PushPacket(MakePacket(seqNum: 30, channel: ChannelType.InstrumentDefinition));

            Assert.Equal(6L, manager.DroppedPacketsTotal);

            var breakdown = manager.GetChannelDropBreakdown()
                .Where(s => s.GroupId == 0)
                .ToDictionary(s => s.Channel, s => s.DroppedPackets);

            Assert.Equal(0L, breakdown[ChannelType.IncrementalA]); // succeeded — no drops
            Assert.Equal(3L, breakdown[ChannelType.IncrementalB]);
            Assert.Equal(2L, breakdown[ChannelType.SnapshotRecovery]);
            Assert.Equal(1L, breakdown[ChannelType.InstrumentDefinition]);

            // Sum invariant: per-channel breakdown must equal per-group total.
            Assert.Equal(
                manager.GetChannelStats().Single().DroppedPackets,
                breakdown.Values.Sum());
        }
        finally
        {
            manager.Dispose();
        }
    }

    private static UmdfPacket MakePacket(uint seqNum, int channelGroup = 0)
        => MakePacket(seqNum, ChannelType.IncrementalA, channelGroup);

    private static UmdfPacket MakePacket(uint seqNum, ChannelType channel, int channelGroup = 0)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return new UmdfPacket
        {
            Data = buf,
            Channel = channel,
            ChannelGroup = channelGroup,
            ReceivedTimestampTicks = 1L,
        };
    }

    private sealed class NoopHandler : IFeedEventHandler
    {
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() { }
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }

    private sealed class RecordingLogger : ILogger<MultiFeedManager>
    {
        public List<string> Warnings { get; } = new();
        IDisposable? ILogger.BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }
}
