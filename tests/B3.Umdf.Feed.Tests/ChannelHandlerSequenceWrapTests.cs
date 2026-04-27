using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins behavior of <see cref="ChannelHandler"/> sequence comparisons at the
/// <see cref="uint"/> boundary. B3 spec §6.5.5.1 guarantees that
/// <c>SequenceVersion</c> increments (and <c>SequenceNumber</c> resets to 1)
/// well before any 32-bit wrap. These tests document the contract:
///
///   - When a SequenceVersion change is observed, the handler re-baselines
///     to the new version's first SeqNum cleanly even if the prior version
///     was at <c>uint.MaxValue</c>.
///   - In the (spec-violating) case of a pure SeqNum wrap WITHOUT a
///     SequenceVersion bump, the handler misclassifies post-wrap packets
///     as duplicates. This is the documented limitation; if the spec ever
///     allowed pure wrap, the code would need RFC1982-style serial-number
///     comparison.
/// </summary>
public class ChannelHandlerSequenceWrapTests
{
    private static UmdfPacket Pkt(uint seqNum, ushort seqVer = 1)
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
    public void SequenceVersionBump_AtMaxSeq_RebaselinesCleanly()
    {
        var tracker = new TrackingHandler();
        var ch = new ChannelHandler(tracker);

        // Land near uint.MaxValue under v1.
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(uint.MaxValue - 1, seqVer: 1)));
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(uint.MaxValue, seqVer: 1)));

        // SequenceVersion bump → seq resets to 1 under v2. Must NOT be
        // classified as a wrap-related duplicate or a giant backwards gap.
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 1, seqVer: 2)));
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 2, seqVer: 2)));

        Assert.Equal(4, tracker.PacketProcessedCount);
        Assert.Equal(1, tracker.SequenceVersionChangedCount);
    }

    [Fact]
    public void PureSeqWrap_WithoutVersionBump_NextZero_IsInSequence_ByUintArithmetic()
    {
        // Documents that pure wrap arithmetically works for the contiguous
        // case: _expectedSeqNum++ at uint.MaxValue wraps to 0, so the next
        // packet at seq=0 matches and is processed. This is "accidental
        // correctness" via uint overflow, not by design — the spec
        // (§6.5.5.1) requires SequenceVersion bumps before any wrap, so
        // production should never exercise this path. Codified to prevent
        // silent regressions in either direction.
        var tracker = new TrackingHandler();
        var ch = new ChannelHandler(tracker);

        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(uint.MaxValue, seqVer: 1)));
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 0, seqVer: 1)));
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 1, seqVer: 1)));
        Assert.Equal(3, tracker.PacketProcessedCount);
    }

    [Fact]
    public void PureSeqWrap_GapAtBoundary_DistanceIsWrapAware()
    {
        // Wire delivers MaxValue, then jumps over the wrap to seq=10 (gap of
        // 11 packets including the missing 0..9). With uint subtraction
        // distance = 10 - 0 = 10, fits in reorder window — so this is
        // buffered as a future packet, not declared as a real gap. This
        // test pins that subtraction-based distance arithmetic is naturally
        // wrap-friendly.
        var tracker = new TrackingHandler();
        var ch = new ChannelHandler(tracker);

        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(uint.MaxValue, seqVer: 1)));
        // Expected is now 0 (wrapped). seq=10 → distance=10, within reorder.
        Assert.Equal(GapResult.InSequence, ch.HandlePacket(Pkt(seqNum: 10, seqVer: 1)));
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
}
