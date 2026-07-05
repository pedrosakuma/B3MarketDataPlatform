using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the wall-clock stall escape valve added for issue #71: a *small*
/// SeqNum gap (well within <see cref="ChannelHandler.MaxReorderDistance"/>)
/// that never fills — e.g. a marketdata pod cold-starting mid-session after
/// only a couple of upstream trades, or a genuine double-loss on both A and B
/// — must not wedge the channel forever waiting for a backlog that will never
/// grow past the reorder window. Before this fix, low packet-volume channels
/// (a few packets total) could sit in the reorder buffer indefinitely because
/// the distance-based gap check alone never tripped.
/// </summary>
public class ChannelHandlerColdStartTests
{
    [Fact]
    public void ColdStart_SmallGap_BuffersInitially_NoImmediateGap()
    {
        // A brief reorder window is still honored: the very first packet
        // (small gap vs the seq=1 constructor default) is buffered, not
        // immediately declared a gap, in case the "missing" earlier packets
        // are just reordered and arrive moments later.
        var tracker = new CountingHandler();
        var ch = new ChannelHandler(tracker);

        var result = ch.HandlePacket(MakePacket(seqNum: 5, ticks: 0));

        Assert.Equal(GapResult.InSequence, result);
        Assert.Equal(0, tracker.PacketsProcessed);
        Assert.Equal(1, ch.ReorderBufferDepth);
    }

    [Fact]
    public void ColdStart_SmallGap_NeverFills_ForcesGapAfterStallTimeout()
    {
        var tracker = new CountingHandler();
        var ch = new ChannelHandler(tracker);

        // Live channel already at seq=5 when this instance joins (small gap,
        // well under MaxReorderDistance) — buffered, waiting for seq 1..4
        // which will never arrive (already gone before we joined).
        ch.HandlePacket(MakePacket(seqNum: 5, ticks: 0));
        Assert.Equal(0, tracker.PacketsProcessed);

        // More packets trickle in slowly (low-volume env), each still a
        // "future" packet relative to the stuck baseline — none large enough
        // to trip the distance-based gap on their own.
        ch.HandlePacket(MakePacket(seqNum: 6, ticks: 500));
        ch.HandlePacket(MakePacket(seqNum: 7, ticks: 1_000));
        Assert.Equal(0, tracker.PacketsProcessed);

        // Once real elapsed time exceeds ReorderStallTimeoutMs without
        // _expectedSeqNum advancing, the next packet forces a Gap regardless
        // of the (still small) distance.
        var result = ch.HandlePacket(MakePacket(seqNum: 8, ticks: ChannelHandler.ReorderStallTimeoutMs + 1));
        Assert.Equal(GapResult.Gap, result);
        Assert.Equal(1, ch.GapsDetected);
        Assert.Equal(0, tracker.PacketsProcessed); // Gap itself doesn't dispatch; caller does via AcceptGapAndAdvance

        // Caller (FeedHandler) accepts the gap and drains the backlog —
        // mirrors production behavior on GapResult.Gap. Per the existing
        // AcceptGapAndAdvance contract (see
        // AcceptGapAndAdvance_ReleasesBufferedPacketsBelowGap_WithoutDispatch),
        // only the gap-triggering packet (8) is dispatched; anything buffered
        // below the new baseline is released without dispatch and healed via
        // the next per-symbol snapshot instead. The key regression this test
        // guards against is that the channel un-wedges at all — before this
        // fix it never got here, and packet 8 (and everything after it)
        // would have been buffered forever.
        var gapPacket = MakePacket(seqNum: 8, ticks: ChannelHandler.ReorderStallTimeoutMs + 1);
        ch.AcceptGapAndAdvance(in gapPacket);

        Assert.Equal(1, tracker.PacketsProcessed);
        Assert.Equal(0, ch.ReorderBufferDepth);
        Assert.Equal(9u, ch.ExpectedSequenceNumber);
    }

    [Fact]
    public void ColdStart_ExactBaselineMatch_StillWorksNormally()
    {
        // First packet == the constructor default (seq=1): in-sequence, no
        // stall/backlog involved.
        var tracker = new CountingHandler();
        var ch = new ChannelHandler(tracker);

        var result = ch.HandlePacket(MakePacket(seqNum: 1, ticks: 0));

        Assert.Equal(GapResult.InSequence, result);
        Assert.Equal(1, tracker.PacketsProcessed);
        Assert.Equal(2u, ch.ExpectedSequenceNumber);
    }

    [Fact]
    public void ColdStart_LargeGap_StillSeedsBaselineImmediately_PreExistingBehaviorPreserved()
    {
        var tracker = new CountingHandler();
        var ch = new ChannelHandler(tracker);

        uint farFuture = 1u + (uint)ChannelHandler.MaxReorderDistance + 1u;
        var result = ch.HandlePacket(MakePacket(seqNum: farFuture, ticks: 0));

        Assert.Equal(GapResult.InSequence, result);
        Assert.Equal(1, tracker.PacketsProcessed);
        Assert.Equal(0, ch.GapsDetected);
        Assert.Equal(farFuture + 1, ch.ExpectedSequenceNumber);
    }

    [Fact]
    public void SmallReorder_FillsBeforeStallTimeout_DoesNotForceGap()
    {
        // Legitimate A/B reordering (missing packet arrives well within the
        // stall window) must keep working exactly as before — the escape
        // valve must not fire prematurely.
        var tracker = new CountingHandler();
        var ch = new ChannelHandler(tracker);

        ch.HandlePacket(MakePacket(seqNum: 1, ticks: 0));
        ch.HandlePacket(MakePacket(seqNum: 3, ticks: 10)); // buffered, small gap
        Assert.Equal(1, tracker.PacketsProcessed);

        ch.HandlePacket(MakePacket(seqNum: 2, ticks: 20)); // fills the hole, well within timeout

        Assert.Equal(3, tracker.PacketsProcessed);
        Assert.Equal(0, ch.GapsDetected);
        Assert.Equal(0, ch.ReorderBufferDepth);
    }

    private static UmdfPacket MakePacket(uint seqNum, long ticks, ushort seqVer = 1)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), seqVer);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 0,
            ReceivedTimestampTicks = ticks,
        };
    }

    private sealed class CountingHandler : IFeedEventHandler
    {
        public int PacketsProcessed;
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() => PacketsProcessed++;
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }
}
