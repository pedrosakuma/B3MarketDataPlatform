using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class InstrumentStatusDecoderTests
{
    [Theory]
    [InlineData(InstrumentStatusDecoder.InstrumentHaltedTransitionCode)]
    [InlineData(InstrumentStatusDecoder.InstrumentResumedTransitionCode)]
    public void TryDecode_SecurityStatusExtension_PreservesExactFields(byte transitionCode)
    {
        const ulong sourceTimestamp = 1_750_000_000_123_456_789;
        var body = BuildBody(
            securityId: 12345,
            status: SecurityTradingStatus.OPEN,
            eventCode: transitionCode,
            sourceTimestamp,
            rptSeq: 77);

        Assert.True(SecurityStatus_3Data.TryParse(body, out var reader));
        Assert.True(InstrumentStatusDecoder.TryDecode(in reader, previousStatus: 21, out var update));

        Assert.Equal(21, update.PreviousStatus);
        Assert.Equal((int)SecurityTradingStatus.OPEN, update.NewStatus);
        Assert.Equal(transitionCode, update.TransitionCode);
        Assert.Null(update.HaltReasonCode);
        Assert.Equal(sourceTimestamp, update.SourceTimestampNanos);
        Assert.Equal(77u, update.RptSeq);
    }

    [Fact]
    public void TryDecode_StandardSecurityTradingEvent_IsNotInstrumentTransition()
    {
        var body = BuildBody(
            securityId: 1,
            status: SecurityTradingStatus.OPEN,
            eventCode: (byte)SecurityTradingEvent.SECURITY_STATUS_CHANGE,
            sourceTimestamp: 123,
            rptSeq: 1);

        Assert.True(SecurityStatus_3Data.TryParse(body, out var reader));
        Assert.False(InstrumentStatusDecoder.TryDecode(in reader, previousStatus: null, out _));
    }

    [Fact]
    public void MarketDataManager_SurfacesPreviousAndNewStatusDeterministically()
    {
        var handler = new CapturingHandler();
        var manager = new MarketDataManager(
            handler,
            stateRegistry: new SymbolStateRegistry(NullLogger.Instance));

        manager.OnPacket(
            in EmptyPacket,
            BuildFrame(42, SecurityTradingStatus.RESERVED,
                (byte)SecurityTradingEvent.SECURITY_STATUS_CHANGE, 100, 1),
            SecurityStatus_3Data.MESSAGE_ID);
        manager.OnPacket(
            in EmptyPacket,
            BuildFrame(42, SecurityTradingStatus.OPEN,
                InstrumentStatusDecoder.InstrumentHaltedTransitionCode, 200, 2),
            SecurityStatus_3Data.MESSAGE_ID);
        manager.OnPacket(
            in EmptyPacket,
            BuildFrame(42, SecurityTradingStatus.OPEN,
                (byte)SecurityTradingEvent.SECURITY_STATUS_CHANGE, 300, 3),
            SecurityStatus_3Data.MESSAGE_ID);

        Assert.Equal(1, handler.Count);
        Assert.Equal(42UL, handler.SecurityId);
        Assert.Equal((int)SecurityTradingStatus.RESERVED, handler.Update.PreviousStatus);
        Assert.Equal((int)SecurityTradingStatus.OPEN, handler.Update.NewStatus);
        Assert.Equal(InstrumentStatusDecoder.InstrumentHaltedTransitionCode, handler.Update.TransitionCode);
        Assert.Null(handler.Update.HaltReasonCode);
        Assert.Equal(200UL, handler.Update.SourceTimestampNanos);
        Assert.Equal(2u, handler.Update.RptSeq);
        Assert.Equal(handler.Update, manager.InstrumentData[42].AdministrativeStatus);
    }

    private static byte[] BuildFrame(
        ulong securityId,
        SecurityTradingStatus status,
        byte eventCode,
        ulong sourceTimestamp,
        uint rptSeq)
    {
        var frame = new byte[MessageHeader.MESSAGE_SIZE + SecurityStatus_3Data.MESSAGE_SIZE];
        SecurityStatus_3Data.WriteHeader(frame);
        BuildBody(securityId, status, eventCode, sourceTimestamp, rptSeq)
            .CopyTo(frame, MessageHeader.MESSAGE_SIZE);
        return frame;
    }

    private static byte[] BuildBody(
        ulong securityId,
        SecurityTradingStatus status,
        byte eventCode,
        ulong sourceTimestamp,
        uint rptSeq)
    {
        var message = new SecurityStatus_3Data
        {
            SecurityID = securityId,
            SecurityTradingStatus = status,
        };
        message.SetSecurityTradingEvent((SecurityTradingEvent)eventCode);
        message.SetRptSeq(rptSeq);

        var body = new byte[SecurityStatus_3Data.MESSAGE_SIZE];
        Assert.True(message.TryEncode(body, out var written));
        Assert.Equal(SecurityStatus_3Data.MESSAGE_SIZE, written);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(24), sourceTimestamp);
        return body;
    }

    private static readonly UmdfPacket EmptyPacket = new()
    {
        Data = ReadOnlyMemory<byte>.Empty,
        Channel = ChannelType.IncrementalA,
        ChannelGroup = 1,
        ReceivedTimestampTicks = 0,
    };

    private sealed class CapturingHandler : IMarketDataEventHandler
    {
        public int Count { get; private set; }
        public ulong SecurityId { get; private set; }
        public InstrumentStatusUpdate Update { get; private set; }

        public void OnInstrumentStatusChanged(
            ulong securityId,
            InstrumentInfo info,
            in InstrumentStatusUpdate update)
        {
            Count++;
            SecurityId = securityId;
            Update = update;
        }
    }
}
