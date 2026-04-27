using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins robustness gaps in the InstrumentDefinition bootstrap. The pre-fix
/// implementation counted <i>raw SecurityDefinition_12 messages</i> against
/// <c>TotNoRelatedSym</c> and never logged a warning when the wire reported
/// inconsistent totals. That made two production failure modes silent:
///   (a) duplicate SecDef messages could complete bootstrap with an
///       INCOMPLETE symbol universe (downstream symbols permanently unknown);
///   (b) a corrupt/changing TotNoRelatedSym was indistinguishable from normal
///       flow.
///
/// Contract pinned here:
///   1. <see cref="FeedHandler.InstrDefDuplicateCount"/> increments for each
///      repeated SecurityID and bootstrap completion is gated on UNIQUE
///      SecurityIDs, not raw message count.
///   2. <see cref="FeedHandler.InstrDefMismatchedTotalCount"/> increments
///      whenever a packet reports a different non-zero
///      <c>TotNoRelatedSym</c> than the first observed value.
/// </summary>
public class FeedHandlerInstrDefRobustnessTests
{
    [Fact]
    public void DuplicateSecurityDefinitions_AreCounted_AndDoNotPrematurelyComplete()
    {
        var tracker = new TrackingHandler();
        var handler = new FeedHandler(tracker);

        // Universe = 3 symbols. Send sec=1, sec=2, then sec=2 again (dup).
        // Pre-fix: _instrDefReceived = 3 ≥ TotNoRelatedSym=3 → falsely
        // transitions to Streaming with sec=3 still missing.
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 1, securityId: 1, totalRelated: 3));
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 2, securityId: 2, totalRelated: 3));
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 3, securityId: 2, totalRelated: 3));

        Assert.Equal(FeedState.WaitInstrumentDefinition, handler.State);
        Assert.Equal(2u, handler.InstrDefReceived); // unique count, not raw
        Assert.Equal(1L, handler.InstrDefDuplicateCount);

        // Real third unique symbol completes bootstrap.
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 4, securityId: 3, totalRelated: 3));
        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal(3u, handler.InstrDefReceived);
    }

    [Fact]
    public void MismatchedTotNoRelatedSym_IsCounted_FirstValueWins()
    {
        var tracker = new TrackingHandler();
        var handler = new FeedHandler(tracker);

        handler.FeedPacket(MakeInstrDefPacket(seqNum: 1, securityId: 1, totalRelated: 5));
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 2, securityId: 2, totalRelated: 5));
        // Intentionally inconsistent total — must not silently retarget the
        // bootstrap completion goalpost; must increment the mismatch counter.
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 3, securityId: 3, totalRelated: 999));

        Assert.Equal(5u, handler.InstrDefTotalExpected);
        Assert.Equal(1L, handler.InstrDefMismatchedTotalCount);
    }

    private static UmdfPacket MakeInstrDefPacket(uint seqNum, ulong securityId, uint totalRelated)
    {
        const int sbeHeaderSize = 8;
        const int framingSize = 4;
        int msgSize = SecurityDefinition_12Data.MESSAGE_SIZE;
        int total = PacketHeader.MESSAGE_SIZE + framingSize + sbeHeaderSize + msgSize;
        var buf = new byte[total];

        // Packet header
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);

        // Framing length
        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(PacketHeader.MESSAGE_SIZE),
            (ushort)(framingSize + sbeHeaderSize + msgSize));

        // SBE header: blockLength @ 0, templateId @ 2.
        int sbeHeaderOffset = PacketHeader.MESSAGE_SIZE + framingSize;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(sbeHeaderOffset), (ushort)msgSize);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(sbeHeaderOffset + 2), SecurityDefinition_12Data.MESSAGE_ID);

        // SecurityDefinition_12Data body
        var body = buf.AsSpan(sbeHeaderOffset + sbeHeaderSize, msgSize);
        var msg = new SecurityDefinition_12Data
        {
            SecurityID = (SecurityID)securityId,
            TotNoRelatedSym = totalRelated,
        };
        msg.TryEncode(body, out _);

        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.InstrumentDefinition,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 100L,
        };
    }

    private sealed class TrackingHandler : IFeedEventHandler
    {
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() { }
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }
}
