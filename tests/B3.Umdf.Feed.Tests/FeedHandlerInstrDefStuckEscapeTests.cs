using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the bootstrap escape valve introduced to keep the consumer from
/// stalling forever inside <see cref="FeedState.WaitInstrumentDefinition"/>
/// when the upstream SecDef stream is pathological — most notably when every
/// SecurityDefinition_12 reports <c>TotNoRelatedSym=0</c> (B3 spec violation
/// observed nowhere in production but trivial to construct).
///
/// Contract: once at least one SecDef has been observed and
/// <see cref="FeedHandler.InstrDefStuckTimeoutMs"/> has elapsed in
/// packet time, the handler force-transitions to Streaming, fires
/// <see cref="IFeedEventHandler.OnInstrumentDefinitionsComplete"/> with the
/// unique-symbol count actually received, and increments
/// <see cref="FeedHandler.InstrDefStuckEscapeFiredCount"/>. Per-symbol heal
/// then bootstraps every symbol that actually exists from the snapshot feed.
/// </summary>
public class FeedHandlerInstrDefStuckEscapeTests
{
    [Fact]
    public void TotNoRelatedSymZero_ForeverStuck_TriggersEscape_AfterTimeout()
    {
        var tracker = new TrackingHandler();
        using var handler = new FeedHandler(tracker);

        // Two SecDefs reporting TotNoRelatedSym=0: pre-fix this would pin the
        // handler in WaitInstrumentDefinition forever (line `_instrDefTotalExpected > 0`
        // never satisfied).
        long t0 = 1_000_000;
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 1, securityId: 1, totalRelated: 0, ticks: t0));
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 2, securityId: 2, totalRelated: 0, ticks: t0 + 5_000));

        // Still inside the stuck window — should remain in WaitInstrumentDefinition.
        Assert.Equal(FeedState.WaitInstrumentDefinition, handler.State);
        Assert.Equal(0L, handler.InstrDefStuckEscapeFiredCount);

        // Crossing the timeout boundary on a subsequent SecDef must fire the
        // escape valve and force-complete bootstrap with the unique-symbol
        // count actually received (3 here).
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 3, securityId: 3, totalRelated: 0,
            ticks: t0 + FeedHandler.InstrDefStuckTimeoutMs));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal(1L, handler.InstrDefStuckEscapeFiredCount);
        Assert.Equal(1, tracker.InstrumentDefinitionsCompletedCount);
        Assert.Equal(3, tracker.LastInstrumentCount);
    }

    [Fact]
    public void EscapeValve_DoesNotFire_WhenBootstrapCompletesNormally()
    {
        var tracker = new TrackingHandler();
        using var handler = new FeedHandler(tracker);

        // Universe of 2 symbols, completed well within the stuck window.
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 1, securityId: 1, totalRelated: 2, ticks: 1_000));
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 2, securityId: 2, totalRelated: 2, ticks: 2_000));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal(0L, handler.InstrDefStuckEscapeFiredCount);
        Assert.Equal(1, tracker.InstrumentDefinitionsCompletedCount);
        Assert.Equal(2, tracker.LastInstrumentCount);
    }

    [Fact]
    public void EscapeValve_DoesNotFire_BeforeFirstSecDef()
    {
        var tracker = new TrackingHandler();
        using var handler = new FeedHandler(tracker);

        // Far beyond the timeout, but no SecDef has arrived yet — handler must
        // remain in WaitInstrumentDefinition (the timer is anchored on the
        // first observed SecDef, not on process start).
        // Feed a non-InstrDef-channel packet to drive HandleWaitInstrumentDefinition.
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 0, securityId: 0, totalRelated: 0,
            ticks: FeedHandler.InstrDefStuckTimeoutMs * 10, channel: ChannelType.IncrementalA));

        Assert.Equal(FeedState.WaitInstrumentDefinition, handler.State);
        Assert.Equal(0L, handler.InstrDefStuckEscapeFiredCount);
    }

    private static UmdfPacket MakeInstrDefPacket(
        uint seqNum, ulong securityId, uint totalRelated, long ticks,
        ChannelType channel = ChannelType.InstrumentDefinition)
    {
        const int sbeHeaderSize = 8;
        const int framingSize = 4;
        int msgSize = SecurityDefinition_12Data.MESSAGE_SIZE;
        int total = PacketHeader.MESSAGE_SIZE + framingSize + sbeHeaderSize + msgSize;
        var buf = new byte[total];

        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);

        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(PacketHeader.MESSAGE_SIZE),
            (ushort)(framingSize + sbeHeaderSize + msgSize));

        int sbeHeaderOffset = PacketHeader.MESSAGE_SIZE + framingSize;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(sbeHeaderOffset), (ushort)msgSize);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(sbeHeaderOffset + 2), SecurityDefinition_12Data.MESSAGE_ID);

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
            Channel = channel,
            ChannelGroup = 1,
            ReceivedTimestampTicks = ticks,
        };
    }

    private sealed class TrackingHandler : IFeedEventHandler
    {
        public int InstrumentDefinitionsCompletedCount { get; private set; }
        public int LastInstrumentCount { get; private set; }
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() { }
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount)
        {
            InstrumentDefinitionsCompletedCount++;
            LastInstrumentCount = instrumentCount;
        }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }
}
