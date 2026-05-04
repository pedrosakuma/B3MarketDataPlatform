using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the wire-driven SequenceReset propagation contract:
///   - <c>SequenceReset_1</c> on the wire → IFeedEventHandler.OnSequenceReset(_, SequenceResetReason.SequenceReset)
///   - <c>ChannelReset_11</c> on the wire → IFeedEventHandler.OnSequenceReset(_, SequenceResetReason.ChannelReset)
///
/// Pre-fix: the interface declared OnSequenceReset but the dispatcher never
/// invoked it on the actual UMDF reset templates; downstream consumers had no
/// signal to drive epoch resets / pending-snapshot drops without re-decoding
/// the SBE bodies themselves.
/// </summary>
public class MessageDispatcherSequenceResetTests
{
    [Fact]
    public void SequenceReset1Template_FiresOnSequenceResetWithSequenceResetReason()
    {
        var tracker = new RecordingHandler();
        var pkt = MakeSinglePacket(channelGroup: 7, templateId: SequenceReset_1Data.MESSAGE_ID);

        MessageDispatcher.Dispatch(in pkt, tracker);

        Assert.Single(tracker.Resets);
        Assert.Equal((7, SequenceResetReason.SequenceReset), tracker.Resets[0]);
    }

    [Fact]
    public void ChannelReset11Template_FiresOnSequenceResetWithChannelResetReason()
    {
        var tracker = new RecordingHandler();
        var pkt = MakeSinglePacket(channelGroup: 3, templateId: ChannelReset_11Data.MESSAGE_ID);

        MessageDispatcher.Dispatch(in pkt, tracker);

        Assert.Single(tracker.Resets);
        Assert.Equal((3, SequenceResetReason.ChannelReset), tracker.Resets[0]);
    }

    [Fact]
    public void NonResetTemplate_DoesNotFireOnSequenceReset()
    {
        var tracker = new RecordingHandler();
        // SnapshotFullRefresh_Header_30 — definitely not a reset.
        var pkt = MakeSinglePacket(channelGroup: 1, templateId: 30);

        MessageDispatcher.Dispatch(in pkt, tracker);

        Assert.Empty(tracker.Resets);
    }

    [Fact]
    public void RaiseSequenceReset_TestHook_ForwardsReasonAndChannelGroupToHandler()
    {
        var tracker = new RecordingHandler();
        var feed = new FeedHandler(tracker);

        feed.RaiseSequenceReset(SequenceResetReason.ChannelReset, channelGroupId: 42);

        Assert.Single(tracker.Resets);
        Assert.Equal((42, SequenceResetReason.ChannelReset), tracker.Resets[0]);
    }

    private static UmdfPacket MakeSinglePacket(int channelGroup, ushort templateId)
    {
        const int sbeHeaderSize = 8;
        const int framingSize = 4;
        var buf = new byte[PacketHeader.MESSAGE_SIZE + framingSize + sbeHeaderSize];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(PacketHeader.MESSAGE_SIZE),
            (ushort)(framingSize + sbeHeaderSize));
        BinaryPrimitives.WriteUInt16LittleEndian(
            buf.AsSpan(PacketHeader.MESSAGE_SIZE + framingSize + 2),
            templateId);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = channelGroup,
            ReceivedTimestampTicks = 100L,
        };
    }

    private sealed class RecordingHandler : IFeedEventHandler
    {
        public List<(int ChannelGroupId, SequenceResetReason Reason)> Resets { get; } = new();
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnSequenceReset() { }
        public void OnSequenceReset(int channelGroupId, SequenceResetReason reason)
            => Resets.Add((channelGroupId, reason));
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnPacketProcessed() { }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }
}
