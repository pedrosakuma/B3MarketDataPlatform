using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the batch-flush contract for the snapshot dispatch path: snapshot
/// packets that mutate book state (clears, market-tier changes, stale→healthy
/// transitions) MUST trigger <see cref="IFeedEventHandler.OnPacketProcessed"/>
/// so downstream conflation buffers (e.g. GroupConflationHandler) flush
/// promptly.
///
/// Pre-fix: only Incremental and mid-session InstrumentDefinition paths fired
/// OnPacketProcessed. Snapshot-driven events sat buffered until some unrelated
/// non-snapshot packet arrived — silent latency under recovery-heavy windows.
/// </summary>
public class FeedHandlerSnapshotBatchCompleteTests
{
    [Fact]
    public void Streaming_SnapshotPacket_FiresOnPacketProcessed()
    {
        var tracker = new TrackingHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        // Snapshot packets are best-effort dispatched even if the inner SBE
        // body fails to decode (no SecDef registered for templateId in this
        // smoke test). The contract under test is OnPacketProcessed firing
        // once per snapshot packet, regardless of payload outcome.
        handler.FeedPacket(MakeSnapshotPacket(seqNum: 1, templateId: 30));
        handler.FeedPacket(MakeSnapshotPacket(seqNum: 2, templateId: 30));

        Assert.Equal(2, tracker.PacketProcessedCount);
    }

    [Fact]
    public void Streaming_SnapshotFollowedByIncremental_FlushesAfterEachPacket()
    {
        // Mixed flow: snapshot then incremental. Each packet must flush
        // independently so that any state mutated by the snapshot is visible
        // downstream BEFORE the incremental's events are added.
        var tracker = new TrackingHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        handler.FeedPacket(MakeSnapshotPacket(seqNum: 1, templateId: 30));
        Assert.Equal(1, tracker.PacketProcessedCount);

        handler.FeedPacket(MakeIncrementalPacket(seqNum: 1));
        Assert.Equal(2, tracker.PacketProcessedCount);
    }

    private static UmdfPacket MakeSnapshotPacket(uint seqNum, ushort templateId)
    {
        const int sbeHeaderSize = 8;
        const int framingSize = 4;
        var buf = new byte[PacketHeader.MESSAGE_SIZE + framingSize + sbeHeaderSize];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(PacketHeader.MESSAGE_SIZE), (ushort)(framingSize + sbeHeaderSize));
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(PacketHeader.MESSAGE_SIZE + framingSize + 2), templateId);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.SnapshotRecovery,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 100L,
        };
    }

    private static UmdfPacket MakeIncrementalPacket(uint seqNum)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 100L,
        };
    }

    private sealed class TrackingHandler : IFeedEventHandler
    {
        public int PacketProcessedCount;
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() => PacketProcessedCount++;
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }
}
