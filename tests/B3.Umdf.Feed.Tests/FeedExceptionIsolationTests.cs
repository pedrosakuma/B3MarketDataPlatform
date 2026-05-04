using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the per-packet exception-isolation contract for the Feed pipeline:
/// downstream <see cref="IFeedEventHandler"/> implementations that throw must
/// not destabilise the consumer loop (FeedHandler), the fan-out
/// (CompositeFeedHandler), or the sequence-version reset path (ChannelHandler).
/// Each isolation site exposes a counter so a misbehaving handler is observable
/// instead of silently masked.
/// </summary>
public class FeedExceptionIsolationTests
{
    [Fact]
    public void FeedHandler_HandlerThrows_LoopSurvivesAndCounterIncrements()
    {
        var handler = new ThrowingHandler();
        var feed = new FeedHandler(handler);
        feed.SetStateForTesting(FeedState.Streaming);

        // Three snapshot packets: each triggers OnPacket on the throwing handler.
        // The consumer-loop wrapper must catch, count, and continue.
        for (int i = 0; i < 3; i++)
            feed.FeedPacket(MakeSnapshotPacket((uint)(i + 1)));

        Assert.Equal(3, handler.OnPacketCalls);
        Assert.Equal(3, feed.HandlerExceptionCount);
        Assert.Equal(3, feed.PacketCount);
    }

    [Fact]
    public void CompositeFeedHandler_FirstThrows_SecondStillReceivesEvent()
    {
        var first = new ThrowingHandler();
        var second = new CountingHandler();
        var composite = new CompositeFeedHandler(first, second);

        var pkt = MakeSnapshotPacket(1);
        composite.OnPacket(in pkt, ReadOnlySpan<byte>.Empty, templateId: 30);
        composite.OnSequenceReset();
        composite.OnInstrumentDefinitionsComplete(7);
        composite.OnPacketProcessed();
        composite.OnSequenceVersionChanged(2);

        Assert.Equal(1, second.OnPacketCalls);
        Assert.Equal(1, second.OnSequenceResetCalls);
        Assert.Equal(1, second.OnInstrumentDefinitionsCompleteCalls);
        Assert.Equal(1, second.OnPacketProcessedCalls);
        Assert.Equal(1, second.OnSequenceVersionChangedCalls);
        Assert.Equal(5, composite.DelegateExceptionCount);
    }

    [Fact]
    public void ChannelHandler_SequenceVersionDownstreamThrows_CounterIncrements()
    {
        var sink = new VersionThrowingHandler();
        var channel = new ChannelHandler(sink);

        // Seed v=5 then push v=6,seq=1 to trigger SequenceVersion change which
        // calls OnSequenceVersionChanged on the downstream sink (which throws).
        channel.HandlePacket(MakeIncrementalPacket(version: 5, seq: 1));
        channel.HandlePacket(MakeIncrementalPacket(version: 6, seq: 1));

        Assert.Equal(1, channel.SequenceVersionResets);
        Assert.Equal(1, channel.SequenceResetHandlerExceptionCount);
    }

    private static UmdfPacket MakeSnapshotPacket(uint seqNum)
    {
        const int sbeHeaderSize = 8;
        const int framingSize = 4;
        var buf = new byte[PacketHeader.MESSAGE_SIZE + framingSize + sbeHeaderSize];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(PacketHeader.MESSAGE_SIZE), (ushort)(framingSize + sbeHeaderSize));
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(PacketHeader.MESSAGE_SIZE + framingSize + 2), 30);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.SnapshotRecovery,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 100L,
        };
    }

    private static UmdfPacket MakeIncrementalPacket(ushort version, uint seq)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), version);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seq);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 100L,
        };
    }

    private sealed class ThrowingHandler : IFeedEventHandler
    {
        public int OnPacketCalls;
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
        {
            OnPacketCalls++;
            throw new InvalidOperationException("handler explosion");
        }
        public void OnSequenceReset() => throw new InvalidOperationException("reset boom");
        public void OnInstrumentDefinitionsComplete(int instrumentCount) => throw new InvalidOperationException("instr boom");
        public void OnPacketProcessed() => throw new InvalidOperationException("processed boom");
        public void OnSequenceVersionChanged(ushort newVersion) => throw new InvalidOperationException("version boom");
    }

    private sealed class CountingHandler : IFeedEventHandler
    {
        public int OnPacketCalls;
        public int OnSequenceResetCalls;
        public int OnInstrumentDefinitionsCompleteCalls;
        public int OnPacketProcessedCalls;
        public int OnSequenceVersionChangedCalls;
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) => OnPacketCalls++;
        public void OnSequenceReset() => OnSequenceResetCalls++;
        public void OnInstrumentDefinitionsComplete(int instrumentCount) => OnInstrumentDefinitionsCompleteCalls++;
        public void OnPacketProcessed() => OnPacketProcessedCalls++;
        public void OnSequenceVersionChanged(ushort newVersion) => OnSequenceVersionChangedCalls++;
    }

    private sealed class VersionThrowingHandler : IFeedEventHandler
    {
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnPacketProcessed() { }
        public void OnSequenceVersionChanged(ushort newVersion) => throw new InvalidOperationException("version boom");
    }
}
