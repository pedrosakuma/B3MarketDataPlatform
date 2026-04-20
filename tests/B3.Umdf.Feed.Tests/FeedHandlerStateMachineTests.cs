using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

public class FeedHandlerStateMachineTests
{
    // Build a minimal valid packet: 16-byte PacketHeader with the given seqNum.
    // The packet body is empty so MessageDispatcher finds no messages to dispatch.
    private static UmdfPacket MakePacket(ChannelType channel, uint seqNum, ushort seqVer = 1)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), seqVer);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return new UmdfPacket { Data = buf, Channel = channel, ChannelGroup = 1, ReceivedTimestampTicks = 100L };
    }

    private static UmdfPacket MakeOwnedPacket(ChannelType channel, uint seqNum, CountingPacketLease lease, ushort seqVer = 1)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), seqVer);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return UmdfPacket.CreateOwned(
            buf,
            channel,
            channelGroup: 1,
            receivedTimestampTicks: 100L,
            lease);
    }

    [Fact]
    public void InitialState_IsWaitInstrumentDefinition()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        Assert.Equal(FeedState.WaitInstrumentDefinition, handler.State);
    }

    [Fact]
    public void CompleteSnapshotCycle_InWrongState_IsNoOp()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        handler.SetStateForTesting(FeedState.RealTime);
        handler.CompleteSnapshotCycle();
        Assert.Equal(FeedState.RealTime, handler.State);
    }

    [Fact]
    public void CompleteSnapshotCycle_FromWaitSnapshot_TransitionsToRealTime()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        handler.SetStateForTesting(FeedState.WaitSnapshot);
        handler.CompleteSnapshotCycle();
        Assert.Equal(FeedState.RealTime, handler.State);
    }

    [Fact]
    public void CompleteSnapshotCycle_FromRecovery_TransitionsToRealTime()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        handler.SetStateForTesting(FeedState.Recovery);
        handler.CompleteSnapshotCycle();
        Assert.Equal(FeedState.RealTime, handler.State);
    }

    [Fact]
    public void SequenceGap_InRealTime_TransitionsToRecovery()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        handler.SetStateForTesting(FeedState.RealTime);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        Assert.Equal(FeedState.RealTime, handler.State);

        // Jump beyond MaxReorderDistance to bypass the A/B reorder window
        // and force a real gap.
        uint farFuture = 2u + (uint)ChannelHandler.MaxReorderDistance + 1u;
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: farFuture));
        Assert.Equal(FeedState.Recovery, handler.State);
    }

    [Fact]
    public void InSequencePackets_DoNotTriggerRecovery()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        handler.SetStateForTesting(FeedState.RealTime);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 2));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 3));

        Assert.Equal(FeedState.RealTime, handler.State);
    }

    [Fact]
    public void Recovery_ThenCompleteSnapshotCycle_ReturnsToRealTime()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        handler.SetStateForTesting(FeedState.RealTime);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        uint farFuture = 2u + (uint)ChannelHandler.MaxReorderDistance + 1u;
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: farFuture)); // → Recovery
        Assert.Equal(FeedState.Recovery, handler.State);

        handler.SetSnapshotRangeForTesting(minSeqNum: farFuture - 1u, maxSeqNum: farFuture - 1u);
        handler.CompleteSnapshotCycle();
        Assert.Equal(FeedState.RealTime, handler.State);
    }

    [Fact]
    public void GapPacket_InRealTime_IsDeferredUntilRecoveryCompletes()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.RealTime);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        uint farFuture = 2u + (uint)ChannelHandler.MaxReorderDistance + 1u;
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: farFuture));

        Assert.Equal(FeedState.Recovery, handler.State);
        Assert.Equal(1, tracker.PacketProcessedCount);
        Assert.NotNull(tracker.LastGap);
        Assert.Equal((2u, farFuture), tracker.LastGap!.Value);

        handler.SetSnapshotRangeForTesting(minSeqNum: farFuture - 1u, maxSeqNum: farFuture - 1u);
        handler.CompleteSnapshotCycle();

        Assert.Equal(FeedState.RealTime, handler.State);
        Assert.Equal(2, tracker.PacketProcessedCount);
    }

    [Fact]
    public void SmallGap_InRealTime_DoesNotTriggerRecovery_WhenFilledByOtherFeed()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.RealTime);

        // A delivers 1, then jumps to 3 — within the reorder window, so the
        // handler stashes 3 and waits for B (or a late A) to deliver 2.
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 3));
        Assert.Equal(FeedState.RealTime, handler.State);
        Assert.Null(tracker.LastGap);
        Assert.Equal(1, tracker.PacketProcessedCount);

        // B delivers the missing 2 → handler drains 2 then 3 from the buffer.
        handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: 2));
        Assert.Equal(FeedState.RealTime, handler.State);
        Assert.Equal(3, tracker.PacketProcessedCount);
        Assert.Equal(4u, handler.IncrementalHandler.ExpectedSequenceNumber);
        Assert.Equal(1, handler.IncrementalHandler.ReorderHits);
    }

    [Fact]
    public void DuplicateFromOtherFeed_IsCountedAsDuplicate()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.RealTime);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 2));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: 2));

        Assert.Equal(2, tracker.PacketProcessedCount);
        Assert.Equal(2, handler.IncrementalHandler.DuplicatesSkipped);
    }

    [Fact]
    public void QueuedIncrementals_AreDrainedAndAppliedDuringCatchUp()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        // State is WaitInstrumentDefinition: incrementals are queued, not applied
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 2));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 3));
        Assert.Equal(0, tracker.PacketProcessedCount); // not yet applied

        handler.SetStateForTesting(FeedState.WaitSnapshot);
        handler.CompleteSnapshotCycle(); // catch-up drains queue

        Assert.Equal(FeedState.RealTime, handler.State);
        // seqNum=1,2,3 all > snapshotMinSeqNum(0) → all applied via ChannelHandler
        Assert.Equal(3, tracker.PacketProcessedCount);
    }

    [Fact]
    public void GapDuringCatchUp_ReentersRecoveryWithoutApplyingGapPacket()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);

        // Use a jump beyond the reorder window so the catch-up loop sees a real gap.
        uint farFuture = 2u + (uint)ChannelHandler.MaxReorderDistance + 1u;
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: farFuture));

        handler.SetStateForTesting(FeedState.WaitSnapshot);
        handler.CompleteSnapshotCycle();

        Assert.Equal(FeedState.Recovery, handler.State);
        Assert.Equal(1, tracker.PacketProcessedCount);
        Assert.NotNull(tracker.LastGap);
        Assert.Equal((2u, farFuture), tracker.LastGap!.Value);
    }

    [Fact]
    public void SnapshotTooOldForRetainedIncrementals_StaysInRecovery()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 5));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 6));

        handler.SetStateForTesting(FeedState.Recovery);
        handler.SetSnapshotRangeForTesting(minSeqNum: 2, maxSeqNum: 2);
        handler.CompleteSnapshotCycle();

        Assert.Equal(FeedState.Recovery, handler.State);
        Assert.Equal(0, tracker.PacketProcessedCount);
    }

    [Fact]
    public void QueuedOwnedIncrementals_AreReleasedAfterCatchUp()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        var leases = new[]
        {
            new CountingPacketLease(),
            new CountingPacketLease(),
            new CountingPacketLease()
        };

        handler.FeedPacket(MakeOwnedPacket(ChannelType.IncrementalA, seqNum: 1, lease: leases[0]));
        handler.FeedPacket(MakeOwnedPacket(ChannelType.IncrementalA, seqNum: 2, lease: leases[1]));
        handler.FeedPacket(MakeOwnedPacket(ChannelType.IncrementalA, seqNum: 3, lease: leases[2]));

        handler.SetStateForTesting(FeedState.WaitSnapshot);
        handler.CompleteSnapshotCycle();

        Assert.All(leases, lease => Assert.Equal(1, lease.ReleaseCount));
    }

    [Fact]
    public void FeedPacket_UpdatesLastPacketTicks()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        Assert.Equal(0L, handler.LastPacketTicks);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        Assert.Equal(100L, handler.LastPacketTicks);
    }

    private class NopFeedEventHandler : IFeedEventHandler
    {
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnGapDetected(uint expected, uint received) { }
        public void OnSequenceReset() { }
        public void OnSnapshotStart() { }
        public void OnSnapshotComplete(uint lastRptSeq) { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
    }

    [Fact]
    public void TransitionTo_Recovery_TwiceQuickly_DebouncesSnapshotReset()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());

        // Simulate the state machine reaching Recovery once (e.g. initial gap),
        // locking onto the snapshot boundary, then catch-up failing and going
        // back to Recovery shortly after — the debounce should preserve the
        // boundary so we don't re-skip a partial cycle.
        handler.SetStateForTesting(FeedState.RealTime);
        handler.TransitionToForTesting(FeedState.Recovery);          // first entry resets trackers
        handler.SetSnapshotBoundaryFoundForTesting();                // simulate boundary lock during the cycle
        handler.SetStateForTesting(FeedState.CatchUp);
        handler.TransitionToForTesting(FeedState.Recovery);          // re-enter via catch-up failure

        Assert.Equal(1, handler.SnapshotResetsDebounced);
        Assert.True(handler.SnapshotBoundaryFoundForTesting);
    }

    private class TrackingFeedEventHandler : IFeedEventHandler
    {
        public int PacketProcessedCount;
        public (uint Expected, uint Received)? LastGap;
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() => PacketProcessedCount++;
        public void OnGapDetected(uint expected, uint received) => LastGap = (expected, received);
        public void OnSequenceReset() { }
        public void OnSnapshotStart() { }
        public void OnSnapshotComplete(uint lastRptSeq) { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
    }

    private sealed class CountingPacketLease : UmdfPacketLease
    {
        private int _refCount = 1;
        public int ReleaseCount { get; private set; }

        public override void Retain() => Interlocked.Increment(ref _refCount);

        public override void Release()
        {
            if (Interlocked.Decrement(ref _refCount) == 0)
                ReleaseCount++;
        }
    }
}
