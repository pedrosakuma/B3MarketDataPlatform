using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the configurability of <see cref="ChannelHandler.MaxReorderDistanceConfigured"/>
/// and the high-watermark <see cref="ChannelHandler.MaxObservedReorderBufferDepth"/>
/// gauge (recovery improvement #14).
/// </summary>
public class ChannelHandlerConfigurableReorderTests
{
    [Fact]
    public void CustomMaxReorderDistance_NarrowerWindow_DeclaresGapEarlier()
    {
        var tracker = new CountingHandler();
        var ch = new ChannelHandler(tracker, maxReorderDistance: 4);
        Assert.Equal(4, ch.MaxReorderDistanceConfigured);

        ch.HandlePacket(MakePacket(1));

        // distances 1..4 from expected=2 → buffered (within window).
        for (uint s = 3; s <= 6; s++)
        {
            var r = ch.HandlePacket(MakePacket(s));
            Assert.Equal(GapResult.InSequence, r);
        }
        Assert.Equal(4, ch.ReorderBufferDepth);

        // distance 5 (seq=7 vs expected=2) → REAL gap.
        var gap = ch.HandlePacket(MakePacket(7));
        Assert.Equal(GapResult.Gap, gap);
        Assert.Equal(1, ch.GapsDetected);
    }

    [Fact]
    public void MaxObservedReorderBufferDepth_TracksHighWatermark_AcrossDrains()
    {
        var tracker = new CountingHandler();
        var ch = new ChannelHandler(tracker, maxReorderDistance: 16);

        ch.HandlePacket(MakePacket(1));
        // Buffer 3 future packets (seq 3,4,5).
        ch.HandlePacket(MakePacket(3));
        ch.HandlePacket(MakePacket(4));
        ch.HandlePacket(MakePacket(5));
        Assert.Equal(3, ch.ReorderBufferDepth);
        Assert.Equal(3, ch.MaxObservedReorderBufferDepth);

        // Fill the gap with seq=2 → drains 3,4,5 → current depth back to 0.
        ch.HandlePacket(MakePacket(2));
        Assert.Equal(0, ch.ReorderBufferDepth);
        // High-watermark must persist past drain.
        Assert.Equal(3, ch.MaxObservedReorderBufferDepth);

        // Smaller spike (only 1 buffered) MUST NOT lower the high-watermark.
        ch.HandlePacket(MakePacket(8));
        Assert.Equal(1, ch.ReorderBufferDepth);
        Assert.Equal(3, ch.MaxObservedReorderBufferDepth);
    }

    [Fact]
    public void DefaultCtor_PreservesLegacyMaxReorderDistance()
    {
        var ch = new ChannelHandler(new CountingHandler());
        Assert.Equal(ChannelHandler.MaxReorderDistance, ch.MaxReorderDistanceConfigured);
    }

    [Fact]
    public void ZeroMaxReorderDistance_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ChannelHandler(new CountingHandler(), maxReorderDistance: 0));
    }

    private static UmdfPacket MakePacket(uint seqNum, ushort seqVer = 1)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), seqVer);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 0,
            ReceivedTimestampTicks = 1L,
        };
    }

    private sealed class CountingHandler : IFeedEventHandler
    {
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() { }
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }
}
