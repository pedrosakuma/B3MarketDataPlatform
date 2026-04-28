using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins behavior of <see cref="ChannelHandler"/> across a SequenceVersion bump
/// (B3 spec §6.5.5.1 — weekly rollover or failover).
///
/// The rollover happens at the channel level only; downstream consumers
/// (BookManager, MarketDataManager, SymbolStateRegistry) react via the
/// <see cref="IFeedEventHandler.OnSequenceVersionChanged"/> callback. These
/// tests document the channel-level invariants that the rest of the system
/// relies on.
/// </summary>
public class ChannelHandlerSequenceVersionRolloverTests
{
    private static UmdfPacket Pkt(uint seqNum, ushort seqVer)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), seqVer);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 1L,
        };
    }

    [Fact]
    public void VersionBump_DiscardsReorderBufferedPackets_FromOldVersion()
    {
        // Old-version packets stashed in the reorder buffer (waiting for a
        // hole to fill) belong to a SeqNum space that no longer applies.
        // After a SequenceVersion bump they MUST NOT be dispatched — the
        // upstream layers have just been told to reset, so replaying old
        // increments would re-introduce stale state.
        var tracker = new TrackingHandler();
        var ch = new ChannelHandler(tracker);

        // Establish baseline at V1 starting in-sequence (seq=1) so subsequent
        // gaps are inside the reorder window from a real expected pointer.
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 1, seqVer: 1)));
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 2, seqVer: 1)));
        // seq=4 is one ahead of expected (3) → buffered, awaiting seq=3.
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 4, seqVer: 1)));
        Assert.Equal(1, ch.ReorderBufferDepth);
        Assert.Equal(2, tracker.PacketProcessedCount);

        // Version bump arrives. The buffered V1 seq=4 packet is now meaningless.
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 1, seqVer: 2)));

        Assert.Equal(0, ch.ReorderBufferDepth);
        Assert.Equal(2, ch.CurrentSequenceVersion);
        Assert.Equal(1, ch.SequenceVersionResets);
        Assert.Equal(1, tracker.SequenceVersionChangedCount);
        // V1 seq=1, seq=2 + V2 seq=1 = 3 processed; the buffered V1 seq=4
        // must NOT have leaked through.
        Assert.Equal(3, tracker.PacketProcessedCount);
    }

    [Fact]
    public void VersionBump_NotifiesEventHandler_BeforeProcessingFirstNewVersionPacket()
    {
        // The event handler callback is what triggers BookManager.ClearAllBooks
        // / SymbolStateRegistry reset. It MUST fire before the first new-version
        // packet is dispatched, otherwise that packet would be applied against
        // pre-rollover book state.
        var tracker = new OrderTrackingHandler();
        var ch = new ChannelHandler(tracker);

        // Bootstrap V1 with an in-sequence packet so V1 is fully observed.
        ch.HandlePacket(Pkt(seqNum: 1, seqVer: 1));
        ch.HandlePacket(Pkt(seqNum: 1, seqVer: 2));

        Assert.Equal(3, tracker.Events.Count);
        // V1 seq=1 processed; then version-change notification; then V2 seq=1.
        Assert.Equal("Pkt", tracker.Events[0]);
        Assert.Equal("VersionChanged:2", tracker.Events[1]);
        Assert.Equal("Pkt", tracker.Events[2]);
    }

    [Fact]
    public void VersionBump_FromZero_TreatsAsBootstrap_NoResetEmitted()
    {
        // The very first packet establishes the baseline version; this is
        // not a "change", just bootstrap. No OnSequenceVersionChanged event,
        // no reset counter increment.
        var tracker = new TrackingHandler();
        var ch = new ChannelHandler(tracker);

        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 1, seqVer: 5)));

        Assert.Equal(5, ch.CurrentSequenceVersion);
        Assert.Equal(0, ch.SequenceVersionResets);
        Assert.Equal(0, tracker.SequenceVersionChangedCount);
        Assert.Equal(1, tracker.PacketProcessedCount);
    }

    [Fact]
    public void VersionBump_ResetsExpectedSeqNumToFirstPacketOfNewVersion()
    {
        // After the bump, _expectedSeqNum re-baselines to the first observed
        // SeqNum of the new version (which is typically 1 per spec, but the
        // implementation accepts any non-zero start). A subsequent in-order
        // V2 packet must NOT be flagged as a duplicate or gap.
        var tracker = new TrackingHandler();
        var ch = new ChannelHandler(tracker);

        ch.HandlePacket(Pkt(seqNum: 1000, seqVer: 1));
        ch.HandlePacket(Pkt(seqNum: 1, seqVer: 2));

        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 2, seqVer: 2)));
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 3, seqVer: 2)));
        Assert.Equal((uint)4, ch.ExpectedSequenceNumber);
        Assert.Equal(4, tracker.PacketProcessedCount);
    }

    [Fact]
    public void VersionBump_LateOldVersionPacket_IsDroppedAsDuplicate_NotABackwardsReset()
    {
        // SequenceVersion is monotonically increasing per spec §6.5.5.1. A
        // late-arriving V1 packet after V2 has been established MUST NOT
        // trigger a backwards reset — that would corrupt the V2 epoch
        // (downstream BookManager already cleared V1 state). The packet is
        // silently dropped as a duplicate.
        var tracker = new TrackingHandler();
        var ch = new ChannelHandler(tracker);

        ch.HandlePacket(Pkt(seqNum: 1, seqVer: 1));
        ch.HandlePacket(Pkt(seqNum: 1, seqVer: 2));
        ch.HandlePacket(Pkt(seqNum: 2, seqVer: 2));
        Assert.Equal(2, ch.CurrentSequenceVersion);
        Assert.Equal(1, ch.SequenceVersionResets);
        long dupesBefore = ch.DuplicatesSkipped;

        // Late V1 packet — must be classified as duplicate, not a reset.
        Assert.Equal(GapResult.Duplicate, ch.HandlePacket(Pkt(seqNum: 11, seqVer: 1)));
        Assert.Equal(2, ch.CurrentSequenceVersion);            // unchanged
        Assert.Equal(1, ch.SequenceVersionResets);             // no extra reset
        Assert.Equal(1, tracker.SequenceVersionChangedCount);  // no extra event
        Assert.Equal(dupesBefore + 1, ch.DuplicatesSkipped);
    }

    private sealed class TrackingHandler : IFeedEventHandler
    {
        public int PacketProcessedCount;
        public int SequenceVersionChangedCount;
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() => PacketProcessedCount++;
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) => SequenceVersionChangedCount++;
    }

    private sealed class OrderTrackingHandler : IFeedEventHandler
    {
        public readonly List<string> Events = new();
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() => Events.Add("Pkt");
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) => Events.Add($"VersionChanged:{newVersion}");
    }
}
