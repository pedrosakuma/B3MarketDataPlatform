using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the snapshot / instrument-definition lifecycle callbacks the FeedHandler
/// invokes on its <see cref="IFeedEventHandler"/>:
///   - <see cref="IFeedEventHandler.OnSnapshotStart"/> fires on every
///     <c>SnapshotFullRefresh_Header_30</c> on the snapshot recovery channel,
///     with the packet's channel group and the symbol's SecurityID.
///   - <see cref="IFeedEventHandler.OnSnapshotComplete"/> fires once per
///     snapshot packet that contained at least one Header_30, with the most
///     recently observed SecurityID.
///   - <see cref="IFeedEventHandler.OnInstrumentDefinitionsComplete(int, bool)"/>
///     fires with <c>wasAborted=false</c> on the normal end-of-replay path,
///     and with <c>wasAborted=true</c> on the bootstrap-stuck escape path.
/// </summary>
public class FeedHandlerLifecycleCallbacksTests
{
    [Fact]
    public void Snapshot_Header30Packet_FiresStartThenCompleteWithSecurityId()
    {
        var tracker = new TrackingHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        handler.FeedPacket(MakeSnapshotHeader30Packet(seqNum: 1, securityId: 4242, channelGroup: 7));

        Assert.Equal(new[] { "Start(7,4242)", "Packet(30)", "Complete(7,4242)", "PacketProcessed" }, tracker.Events);
    }

    [Fact]
    public void Snapshot_NonSnapshotTemplateOnly_DoesNotFireSnapshotLifecycle()
    {
        var tracker = new TrackingHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        // Snapshot channel but no Header_30 in the payload (e.g. trailing
        // Orders_71 fragment from a prior packet — pathological but possible).
        handler.FeedPacket(MakeSnapshotPacketWithTemplate(seqNum: 1, templateId: 71, channelGroup: 1));

        Assert.DoesNotContain(tracker.Events, e => e.StartsWith("Start("));
        Assert.DoesNotContain(tracker.Events, e => e.StartsWith("Complete("));
    }

    [Fact]
    public void NormalBootstrap_FiresInstrumentDefinitionsCompleteWithWasAbortedFalse()
    {
        var tracker = new TrackingHandler();
        var handler = new FeedHandler(tracker);

        // 1 expected, 1 received → completes normally.
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 1, securityId: 1, totalRelated: 1, ticks: 100L));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Single(tracker.InstrumentDefinitionsCompletes);
        Assert.False(tracker.InstrumentDefinitionsCompletes[0].WasAborted);
        Assert.Equal(1, tracker.InstrumentDefinitionsCompletes[0].Count);
    }

    [Fact]
    public void StuckBootstrap_FiresInstrumentDefinitionsCompleteWithWasAbortedTrue()
    {
        var tracker = new TrackingHandler();
        var handler = new FeedHandler(tracker);

        // Receive one SecDef with TotNoRelatedSym=0 so completion is never
        // satisfied by the message stream itself; then advance the wall clock
        // past InstrDefStuckTimeoutMs to trigger the escape valve.
        handler.FeedPacket(MakeInstrDefPacket(seqNum: 1, securityId: 1, totalRelated: 0, ticks: 100L));
        Assert.Equal(FeedState.WaitInstrumentDefinition, handler.State);

        // Any packet on the InstrumentDefinition channel after the timeout
        // window triggers the stuck-escape transition.
        handler.FeedPacket(MakeInstrDefPacket(
            seqNum: 2, securityId: 2, totalRelated: 0,
            ticks: 100L + FeedHandler.InstrDefStuckTimeoutMs + 1));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Single(tracker.InstrumentDefinitionsCompletes);
        Assert.True(tracker.InstrumentDefinitionsCompletes[0].WasAborted);
    }

    private static UmdfPacket MakeSnapshotHeader30Packet(uint seqNum, ulong securityId, int channelGroup)
    {
        const int sbeHeaderSize = 8;
        const int framingSize = 4;
        int msgSize = SnapshotFullRefresh_Header_30Data.MESSAGE_SIZE;
        int total = PacketHeader.MESSAGE_SIZE + framingSize + sbeHeaderSize + msgSize;
        var buf = new byte[total];

        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);

        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(PacketHeader.MESSAGE_SIZE),
            (ushort)(framingSize + sbeHeaderSize + msgSize));

        int sbeHeaderOffset = PacketHeader.MESSAGE_SIZE + framingSize;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(sbeHeaderOffset), (ushort)msgSize);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(sbeHeaderOffset + 2), SnapshotFullRefresh_Header_30Data.MESSAGE_ID);

        var body = buf.AsSpan(sbeHeaderOffset + sbeHeaderSize, msgSize);
        var msg = new SnapshotFullRefresh_Header_30Data
        {
            SecurityID = (SecurityID)securityId,
        };
        msg.TryEncode(body, out _);

        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.SnapshotRecovery,
            ChannelGroup = channelGroup,
            ReceivedTimestampTicks = 100L,
        };
    }

    private static UmdfPacket MakeSnapshotPacketWithTemplate(uint seqNum, ushort templateId, int channelGroup)
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
            ChannelGroup = channelGroup,
            ReceivedTimestampTicks = 100L,
        };
    }

    private static UmdfPacket MakeInstrDefPacket(uint seqNum, ulong securityId, uint totalRelated, long ticks)
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
            Channel = ChannelType.InstrumentDefinition,
            ChannelGroup = 1,
            ReceivedTimestampTicks = ticks,
        };
    }

    private sealed class TrackingHandler : IFeedEventHandler
    {
        public List<string> Events { get; } = new();
        public List<(int Count, bool WasAborted)> InstrumentDefinitionsCompletes { get; } = new();

        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
            => Events.Add($"Packet({templateId})");
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount)
            => InstrumentDefinitionsCompletes.Add((instrumentCount, false));
        public void OnInstrumentDefinitionsComplete(int instrumentCount, bool wasAborted)
            => InstrumentDefinitionsCompletes.Add((instrumentCount, wasAborted));
        public void OnPacketProcessed() => Events.Add("PacketProcessed");
        public void OnSequenceVersionChanged(ushort newVersion) { }
        public void OnSnapshotStart(int channelGroupId, ulong securityId)
            => Events.Add($"Start({channelGroupId},{securityId})");
        public void OnSnapshotComplete(int channelGroupId, ulong securityId)
            => Events.Add($"Complete({channelGroupId},{securityId})");
    }
}
