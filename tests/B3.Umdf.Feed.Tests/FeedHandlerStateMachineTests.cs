using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

public class FeedHandlerStateMachineTests
{
    private static UmdfPacket MakePacket(ChannelType channel, uint seqNum, ushort seqVer = 1)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), seqVer);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return new UmdfPacket { Data = buf, Channel = channel, ChannelGroup = 1, ReceivedTimestampTicks = 100L };
    }

    private static UmdfPacket MakeSnapshotPacket(uint seqNum, ushort templateId, ushort seqVer = 1)
    {
        const int sbeHeaderSize = 8;
        const int framingSize = 4;
        var buf = new byte[PacketHeader.MESSAGE_SIZE + framingSize + sbeHeaderSize];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), seqVer);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(PacketHeader.MESSAGE_SIZE), (ushort)(framingSize + sbeHeaderSize));
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(PacketHeader.MESSAGE_SIZE + framingSize + 2), templateId);
        return new UmdfPacket { Data = buf, Channel = ChannelType.SnapshotRecovery, ChannelGroup = 1, ReceivedTimestampTicks = 100L };
    }

    [Fact]
    public void InitialState_IsWaitInstrumentDefinition()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        Assert.Equal(FeedState.WaitInstrumentDefinition, handler.State);
    }

    [Fact]
    public void WaitInstrumentDefinition_DiscardsIncrementals()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 2));

        Assert.Equal(FeedState.WaitInstrumentDefinition, handler.State);
        Assert.Equal(0, tracker.PacketProcessedCount);
    }

    [Fact]
    public void Streaming_InSequencePackets_AreDispatched()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 2));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 3));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal(3, tracker.PacketProcessedCount);
        Assert.Equal(0L, handler.PerSymbolGapsAbsorbed);
    }

    [Fact]
    public void Streaming_GapBeyondReorderWindow_IsAbsorbedAndCounted()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        uint farFuture = 2u + (uint)ChannelHandler.MaxReorderDistance + 1u;
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: farFuture));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal(1L, handler.PerSymbolGapsAbsorbed);
    }

    [Fact]
    public void Streaming_SmallGap_FilledByOtherFeed_NoCount()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 3));
        Assert.Equal(1, tracker.PacketProcessedCount);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: 2));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal(3, tracker.PacketProcessedCount);
        Assert.Equal(4u, handler.IncrementalHandler.ExpectedSequenceNumber);
        Assert.Equal(1, handler.IncrementalHandler.ReorderHits);
        Assert.Equal(0L, handler.PerSymbolGapsAbsorbed);
    }

    [Fact]
    public void Streaming_DuplicateFromOtherFeed_IsCountedAsDuplicate()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        handler.SetStateForTesting(FeedState.Streaming);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 2));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: 2));

        Assert.Equal(2, handler.IncrementalHandler.DuplicatesSkipped);
    }

    [Fact]
    public void Streaming_SnapshotPacket_IsDispatchedImmediately()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        const ushort headerTemplateId = SnapshotFullRefresh_Header_30Data.MESSAGE_ID;
        handler.FeedPacket(MakeSnapshotPacket(seqNum: 1, templateId: headerTemplateId));

        Assert.Equal(1, tracker.SeenPacketCount);
        Assert.Equal(headerTemplateId, tracker.LastTemplateId);
    }

    [Fact]
    public void Streaming_TwoSnapshotsSameSeqVer_BothDispatched()
    {
        // The unified design has no snapshot-cycle gating; every snapshot
        // packet is dispatched as soon as it arrives so per-symbol heal can
        // happen progressively.
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        const ushort headerTemplateId = SnapshotFullRefresh_Header_30Data.MESSAGE_ID;
        handler.FeedPacket(MakeSnapshotPacket(seqNum: 1, templateId: headerTemplateId, seqVer: 7));
        handler.FeedPacket(MakeSnapshotPacket(seqNum: 2, templateId: headerTemplateId, seqVer: 7));

        Assert.Equal(2, tracker.SeenPacketCount);
    }

    [Fact]
    public void WaitInstrumentDefinition_DiscardsSnapshots()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);

        const ushort headerTemplateId = SnapshotFullRefresh_Header_30Data.MESSAGE_ID;
        handler.FeedPacket(MakeSnapshotPacket(seqNum: 1, templateId: headerTemplateId));

        Assert.Equal(FeedState.WaitInstrumentDefinition, handler.State);
        Assert.Equal(0, tracker.SeenPacketCount);
    }

    [Fact]
    public void FeedPacket_UpdatesLastPacketTicks()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        Assert.Equal(0L, handler.LastPacketTicks);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        Assert.Equal(100L, handler.LastPacketTicks);
    }

    // ── A/B reorder stress tests ──

    [Fact]
    public void AbStress_BurstLossOnA_FilledByB_StaysStreaming_NoChannelGap()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        const uint preBurst = 10;
        const uint burst = 20;
        const uint postBurst = 10;

        for (uint s = 1; s <= preBurst; s++)
            handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: s));

        for (uint s = preBurst + burst + 1; s <= preBurst + burst + postBurst; s++)
            handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: s));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal((int)preBurst, tracker.PacketProcessedCount);
        Assert.Equal(0L, handler.PerSymbolGapsAbsorbed);

        var burstSeqs = new List<uint>();
        for (uint s = preBurst + 1; s <= preBurst + burst; s++) burstSeqs.Add(s);
        var rng = new Random(1234);
        for (int i = burstSeqs.Count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (burstSeqs[i], burstSeqs[j]) = (burstSeqs[j], burstSeqs[i]);
        }
        foreach (var s in burstSeqs)
            handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: s));

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal((int)(preBurst + burst + postBurst), tracker.PacketProcessedCount);
        Assert.Equal(preBurst + burst + postBurst + 1, handler.IncrementalHandler.ExpectedSequenceNumber);
        Assert.True(handler.IncrementalHandler.ReorderHits > 0);
        Assert.Equal(0L, handler.PerSymbolGapsAbsorbed);
    }

    [Fact]
    public void AbStress_HeavyInterleaving_DeduplicatesAndPreservesOrder()
    {
        var tracker = new TrackingFeedEventHandler();
        var handler = new FeedHandler(tracker);
        handler.SetStateForTesting(FeedState.Streaming);

        const uint total = 200;
        var rng = new Random(7);
        var aArrivals = new List<uint>();
        var bArrivals = new List<uint>();
        for (uint s = 1; s <= total; s++)
        {
            aArrivals.Add(s);
            bArrivals.Add(s);
        }
        for (int i = bArrivals.Count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (bArrivals[i], bArrivals[j]) = (bArrivals[j], bArrivals[i]);
        }

        int ai = 0, bi = 0;
        while (ai < aArrivals.Count || bi < bArrivals.Count)
        {
            bool pickA = (rng.NextDouble() < 0.7 && ai < aArrivals.Count) || bi >= bArrivals.Count;
            if (pickA)
                handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: aArrivals[ai++]));
            else
                handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: bArrivals[bi++]));
        }

        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal((int)total, tracker.PacketProcessedCount);
        Assert.Equal(total + 1, handler.IncrementalHandler.ExpectedSequenceNumber);
        Assert.Equal((int)total, handler.IncrementalHandler.DuplicatesSkipped);
    }

    [Fact]
    public void AbStress_BothFeedsLoseSamePacket_AbsorbedAsPerSymbolGap()
    {
        var handler = new FeedHandler(new NopFeedEventHandler());
        handler.SetStateForTesting(FeedState.Streaming);

        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: 1));
        handler.FeedPacket(MakePacket(ChannelType.IncrementalB, seqNum: 1));
        Assert.Equal(FeedState.Streaming, handler.State);

        uint farFuture = 2u + (uint)ChannelHandler.MaxReorderDistance + 1u;
        handler.FeedPacket(MakePacket(ChannelType.IncrementalA, seqNum: farFuture));
        Assert.Equal(FeedState.Streaming, handler.State);
        Assert.Equal(1L, handler.PerSymbolGapsAbsorbed);
    }

    private class NopFeedEventHandler : IFeedEventHandler
    {
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() { }
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
    }

    private class TrackingFeedEventHandler : IFeedEventHandler
    {
        public int PacketProcessedCount;
        public int SeenPacketCount;
        public ushort LastTemplateId;
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
        {
            SeenPacketCount++;
            LastTemplateId = templateId;
        }
        public void OnPacketProcessed() => PacketProcessedCount++;
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
    }
}
