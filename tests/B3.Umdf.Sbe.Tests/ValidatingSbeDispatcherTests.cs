using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Sbe;

namespace B3.Umdf.Sbe.Tests;

public class ValidatingSbeDispatcherTests
{
    private const ushort SchemaId = 2;
    private const ushort SupportedVersion = 16;

    /// <summary>Builds an 8-byte SBE message header followed by <paramref name="extraPayload"/> bytes.</summary>
    private static byte[] BuildHeader(ushort templateId, ushort schemaId, ushort version, ushort blockLength, int extraPayload = 0)
    {
        var buf = new byte[MessageHeader.MESSAGE_SIZE + extraPayload];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0, 2), blockLength);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2, 2), templateId);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(4, 2), schemaId);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(6, 2), version);
        return buf;
    }

    [Fact]
    public void Dispatch_ValidHeader_DispatchesSuccessfullyWithoutMismatch()
    {
        // SequenceReset_1 has BLOCK_LENGTH=0 so an 8-byte header alone is a complete message.
        var buf = BuildHeader(templateId: 1, schemaId: SchemaId, version: 0, blockLength: 0);
        SbeHeaderRejection? rejection = null;
        var dispatcher = new ValidatingSbeDispatcher(
            expectedSchemaId: SchemaId,
            maxSupportedVersion: SupportedVersion,
            onHeaderMismatch: r => rejection = r);

        var handler = new CountingHandler();
        bool dispatched = dispatcher.Dispatch(buf, ref handler);

        Assert.True(dispatched);
        Assert.Null(rejection);
        Assert.Equal(1, handler.DispatchCount);
        Assert.Equal(1, handler.LastTemplateId);
    }

    [Fact]
    public void Dispatch_SchemaIdMismatch_FiresCallbackAndSkips()
    {
        var buf = BuildHeader(templateId: 1, schemaId: 999, version: 0, blockLength: 0);
        SbeHeaderRejection? rejection = null;
        var dispatcher = new ValidatingSbeDispatcher(
            expectedSchemaId: SchemaId,
            maxSupportedVersion: SupportedVersion,
            onHeaderMismatch: r => rejection = r);

        var handler = new CountingHandler();
        bool dispatched = dispatcher.Dispatch(buf, ref handler);

        Assert.False(dispatched);
        Assert.NotNull(rejection);
        Assert.Equal(SbeHeaderRejectReason.SchemaIdMismatch, rejection!.Value.Reason);
        Assert.Equal((ushort)999, rejection.Value.SchemaId);
        Assert.Equal(0, handler.DispatchCount);
    }

    [Fact]
    public void Dispatch_VersionUnsupported_FiresCallbackAndSkips()
    {
        var buf = BuildHeader(templateId: 1, schemaId: SchemaId, version: 99, blockLength: 0);
        SbeHeaderRejection? rejection = null;
        var dispatcher = new ValidatingSbeDispatcher(
            expectedSchemaId: SchemaId,
            maxSupportedVersion: SupportedVersion,
            onHeaderMismatch: r => rejection = r);

        var handler = new CountingHandler();
        bool dispatched = dispatcher.Dispatch(buf, ref handler);

        Assert.False(dispatched);
        Assert.NotNull(rejection);
        Assert.Equal(SbeHeaderRejectReason.VersionUnsupported, rejection!.Value.Reason);
        Assert.Equal((ushort)99, rejection.Value.Version);
        Assert.Equal(0, handler.DispatchCount);
    }

    [Fact]
    public void Dispatch_HeaderTruncated_FiresCallbackAndSkips()
    {
        SbeHeaderRejection? rejection = null;
        var dispatcher = new ValidatingSbeDispatcher(onHeaderMismatch: r => rejection = r);

        var handler = new CountingHandler();
        ReadOnlySpan<byte> tiny = new byte[] { 0x01, 0x02 };
        bool dispatched = dispatcher.Dispatch(tiny, ref handler);

        Assert.False(dispatched);
        Assert.NotNull(rejection);
        Assert.Equal(SbeHeaderRejectReason.HeaderTruncated, rejection!.Value.Reason);
        Assert.Equal(0, handler.DispatchCount);
    }

    [Fact]
    public void Dispatch_BlockLengthExceedsBuffer_FiresCallbackAndSkips()
    {
        // Header says blockLength=200 but buffer carries only 0 extra payload bytes.
        var buf = BuildHeader(templateId: 1, schemaId: SchemaId, version: 0, blockLength: 200);
        SbeHeaderRejection? rejection = null;
        var dispatcher = new ValidatingSbeDispatcher(onHeaderMismatch: r => rejection = r);

        var handler = new CountingHandler();
        bool dispatched = dispatcher.Dispatch(buf, ref handler);

        Assert.False(dispatched);
        Assert.NotNull(rejection);
        Assert.Equal(SbeHeaderRejectReason.BlockLengthImplausible, rejection!.Value.Reason);
        Assert.Equal(0, handler.DispatchCount);
    }

    [Fact]
    public void Dispatch_SchemaMismatch_WithSkipOnMismatchFalse_StillDispatches()
    {
        // Wrong schemaId, but skipOnMismatch=false → still delegates to SbeDispatcher.
        // We use templateId=1 with blockLength=0 so the underlying dispatcher succeeds.
        var buf = BuildHeader(templateId: 1, schemaId: 999, version: 0, blockLength: 0);
        SbeHeaderRejection? rejection = null;
        var dispatcher = new ValidatingSbeDispatcher(
            expectedSchemaId: SchemaId,
            maxSupportedVersion: SupportedVersion,
            skipOnMismatch: false,
            onHeaderMismatch: r => rejection = r);

        var handler = new CountingHandler();
        bool dispatched = dispatcher.Dispatch(buf, ref handler);

        Assert.True(dispatched);
        Assert.NotNull(rejection); // callback still fires for visibility
        Assert.Equal(1, handler.DispatchCount);
    }

    /// <summary>Minimal <see cref="ISbeMessageHandler"/> stub that just counts invocations.</summary>
    private struct CountingHandler : ISbeMessageHandler
    {
        public int DispatchCount;
        public int LastTemplateId;

        public void OnSequenceReset_1(in SequenceReset_1DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 1; }
        public void OnSequence_2(in Sequence_2DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 2; }
        public void OnEmptyBook_9(in EmptyBook_9DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 9; }
        public void OnChannelReset_11(in ChannelReset_11DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 11; }
        public void OnSecurityStatus_3(in SecurityStatus_3DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 3; }
        public void OnSecurityGroupPhase_10(in SecurityGroupPhase_10DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 10; }
        public void OnSecurityDefinition_12(in SecurityDefinition_12DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 12; }
        public void OnNews_5(in News_5DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 5; }
        public void OnOpeningPrice_15(in OpeningPrice_15DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 15; }
        public void OnTheoreticalOpeningPrice_16(in TheoreticalOpeningPrice_16DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 16; }
        public void OnClosingPrice_17(in ClosingPrice_17DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 17; }
        public void OnAuctionImbalance_19(in AuctionImbalance_19DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 19; }
        public void OnQuantityBand_21(in QuantityBand_21DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 21; }
        public void OnPriceBand_22(in PriceBand_22DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 22; }
        public void OnHighPrice_24(in HighPrice_24DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 24; }
        public void OnLowPrice_25(in LowPrice_25DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 25; }
        public void OnLastTradePrice_27(in LastTradePrice_27DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 27; }
        public void OnSettlementPrice_28(in SettlementPrice_28DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 28; }
        public void OnOpenInterest_29(in OpenInterest_29DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 29; }
        public void OnSnapshotFullRefresh_Header_30(in SnapshotFullRefresh_Header_30DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 30; }
        public void OnOrder_MBO_50(in Order_MBO_50DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 50; }
        public void OnDeleteOrder_MBO_51(in DeleteOrder_MBO_51DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 51; }
        public void OnMassDeleteOrders_MBO_52(in MassDeleteOrders_MBO_52DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 52; }
        public void OnTrade_53(in Trade_53DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 53; }
        public void OnForwardTrade_54(in ForwardTrade_54DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 54; }
        public void OnExecutionSummary_55(in ExecutionSummary_55DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 55; }
        public void OnExecutionStatistics_56(in ExecutionStatistics_56DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 56; }
        public void OnTradeBust_57(in TradeBust_57DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 57; }
        public void OnSnapshotFullRefresh_Orders_MBO_71(in SnapshotFullRefresh_Orders_MBO_71DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 71; }
        public void OnHeaderMessage_0(in HeaderMessage_0DataReader reader, int blockLength, int version) { DispatchCount++; LastTemplateId = 0; }
        public void OnUnknownMessage(int templateId, int blockLength, int version, ReadOnlySpan<byte> payload) { DispatchCount++; LastTemplateId = templateId; }
    }
}
