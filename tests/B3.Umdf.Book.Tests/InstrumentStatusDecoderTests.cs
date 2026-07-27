using System.Buffers.Binary;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V17;
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
    public void TryDecode_InstrumentStatus58_LiveHaltPreservesAuthoritativeFields()
    {
        const ulong sourceTimestamp = 1_750_000_000_987_654_321;
        var body = BuildInstrumentStatusBody(
            securityId: 12345,
            status: SecurityTradingStatus.OPEN,
            state: AdministrativeHaltState.HALTED,
            transition: AdministrativeTransitionKind.HALT,
            haltReason: HaltReason.NEWS_HOLD,
            sourceTimestamp,
            rptSeq: 77,
            tradingSessionId: TradingSessionID.REGULAR_TRADING_SESSION,
            isRecovery: false);

        Assert.True(InstrumentStatus_58Data.TryParse(body, out var reader));
        Assert.True(InstrumentStatusDecoder.TryDecode(
            in reader, previousStatus: 21, out var update));

        Assert.Equal(21, update.PreviousStatus);
        Assert.Equal((int)SecurityTradingStatus.OPEN, update.NewStatus);
        Assert.Equal((byte)AdministrativeHaltState.HALTED, update.AdministrativeStateCode);
        Assert.Equal((byte)AdministrativeTransitionKind.HALT, update.TransitionCode);
        Assert.Equal((byte)HaltReason.NEWS_HOLD, update.HaltReasonCode);
        Assert.Equal(sourceTimestamp, update.SourceTimestampNanos);
        Assert.Equal(77u, update.RptSeq);
        Assert.Equal((byte)TradingSessionID.REGULAR_TRADING_SESSION, update.TradingSessionId);
        Assert.False(update.IsRecovery);
        Assert.True(update.IsHalted);
    }

    [Fact]
    public void TryDecode_RecoverySnapshot_BootstrapsStateWithoutTransition()
    {
        var body = BuildInstrumentStatusBody(
            securityId: 7,
            status: SecurityTradingStatus.OPEN,
            state: AdministrativeHaltState.HALTED,
            transition: null,
            haltReason: HaltReason.REGULATORY_HALT,
            sourceTimestamp: 123,
            rptSeq: null,
            tradingSessionId: TradingSessionID.REGULAR_TRADING_SESSION,
            isRecovery: true);

        Assert.True(InstrumentStatus_58Data.TryParse(body, out var reader));
        Assert.True(InstrumentStatusDecoder.TryDecode(
            in reader, previousStatus: null, out var update));

        Assert.Equal(InstrumentStatusDecoder.UnavailableCode, update.TransitionCode);
        Assert.Equal((byte)HaltReason.REGULATORY_HALT, update.HaltReasonCode);
        Assert.Null(update.RptSeq);
        Assert.True(update.IsRecovery);
        Assert.True(update.IsHalted);
    }

    [Fact]
    public void TryDecode_UnknownEnumValues_PreservesRawCodes()
    {
        var body = BuildInstrumentStatusBody(
            securityId: 7,
            status: SecurityTradingStatus.OPEN,
            state: (AdministrativeHaltState)9,
            transition: (AdministrativeTransitionKind)8,
            haltReason: (HaltReason)77,
            sourceTimestamp: 123,
            rptSeq: 9,
            tradingSessionId: (TradingSessionID)6,
            isRecovery: false);

        Assert.True(InstrumentStatus_58Data.TryParse(body, out var reader));
        Assert.True(InstrumentStatusDecoder.TryDecode(
            in reader, previousStatus: null, out var update));

        Assert.Equal((byte?)9, update.AdministrativeStateCode);
        Assert.Equal(8, update.TransitionCode);
        Assert.Equal((byte?)77, update.HaltReasonCode);
        Assert.Equal((byte?)6, update.TradingSessionId);
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

    [Fact]
    public void MarketDataManager_DualTemplateRolloutPacket_EmitsOnlyAuthoritativeEvent()
    {
        var handler = new CapturingHandler();
        var manager = new MarketDataManager(
            handler,
            stateRegistry: new SymbolStateRegistry(NullLogger.Instance));
        var packet = BuildPacket(
            BuildFrame(
                42,
                SecurityTradingStatus.OPEN,
                InstrumentStatusDecoder.InstrumentHaltedTransitionCode,
                200,
                2),
            BuildInstrumentStatusFrame(
                42,
                SecurityTradingStatus.OPEN,
                AdministrativeHaltState.HALTED,
                AdministrativeTransitionKind.HALT,
                HaltReason.PENDING_DISCLOSURE,
                200,
                2,
                isRecovery: false));

        MessageDispatcher.Dispatch(in packet, manager);

        Assert.Equal(1, handler.Count);
        Assert.Equal((byte)HaltReason.PENDING_DISCLOSURE, handler.Update.HaltReasonCode);
        Assert.Equal((byte)AdministrativeHaltState.HALTED, handler.Update.AdministrativeStateCode);
        Assert.Equal(handler.Update, manager.InstrumentData[42].AdministrativeStatus);
    }

    [Fact]
    public void MarketDataManager_RecoveryTemplate58_CachesDetailedBootstrapState()
    {
        var handler = new CapturingHandler();
        var manager = new MarketDataManager(
            handler,
            stateRegistry: new SymbolStateRegistry(NullLogger.Instance));

        manager.OnPacket(
            in EmptyPacket,
            BuildInstrumentStatusFrame(
                42,
                SecurityTradingStatus.OPEN,
                AdministrativeHaltState.HALTED,
                transition: null,
                HaltReason.VOLATILITY_CIRCUIT_BREAKER,
                sourceTimestamp: 987,
                rptSeq: null,
                isRecovery: true),
            InstrumentStatus_58Data.MESSAGE_ID);

        Assert.Equal(1, handler.Count);
        Assert.True(handler.Update.IsRecovery);
        Assert.Equal(InstrumentStatusDecoder.UnavailableCode, handler.Update.TransitionCode);
        Assert.Equal((byte)HaltReason.VOLATILITY_CIRCUIT_BREAKER, handler.Update.HaltReasonCode);
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

    private static byte[] BuildInstrumentStatusFrame(
        ulong securityId,
        SecurityTradingStatus status,
        AdministrativeHaltState state,
        AdministrativeTransitionKind? transition,
        HaltReason? haltReason,
        ulong sourceTimestamp,
        uint? rptSeq,
        bool isRecovery)
    {
        var frame = new byte[MessageHeader.MESSAGE_SIZE + InstrumentStatus_58Data.MESSAGE_SIZE];
        InstrumentStatus_58Data.WriteHeader(frame);
        BuildInstrumentStatusBody(
                securityId,
                status,
                state,
                transition,
                haltReason,
                sourceTimestamp,
                rptSeq,
                TradingSessionID.REGULAR_TRADING_SESSION,
                isRecovery)
            .CopyTo(frame, MessageHeader.MESSAGE_SIZE);
        return frame;
    }

    private static byte[] BuildInstrumentStatusBody(
        ulong securityId,
        SecurityTradingStatus status,
        AdministrativeHaltState state,
        AdministrativeTransitionKind? transition,
        HaltReason? haltReason,
        ulong sourceTimestamp,
        uint? rptSeq,
        TradingSessionID tradingSessionId,
        bool isRecovery)
    {
        var message = new InstrumentStatus_58Data
        {
            SecurityID = securityId,
            MatchEventIndicator = isRecovery
                ? MatchEventIndicator.RecoveryMsg
                : 0,
            TradingSessionID = tradingSessionId,
            SecurityTradingStatus = status,
            AdministrativeHaltState = state,
        };
        message.SetAdministrativeTransitionKind(transition);
        message.SetHaltReason(haltReason);
        message.SetRptSeq(rptSeq);

        var body = new byte[InstrumentStatus_58Data.MESSAGE_SIZE];
        Assert.True(message.TryEncode(body, out var written));
        Assert.Equal(InstrumentStatus_58Data.MESSAGE_SIZE, written);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(16), sourceTimestamp);
        return body;
    }

    private static UmdfPacket BuildPacket(params byte[][] sbeFrames)
    {
        int length = UmdfPacketHeader.Size
            + sbeFrames.Sum(frame => FramingHeader.MESSAGE_SIZE + frame.Length);
        var bytes = new byte[length];
        int offset = UmdfPacketHeader.Size;
        foreach (byte[] frame in sbeFrames)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(
                bytes.AsSpan(offset),
                checked((ushort)(FramingHeader.MESSAGE_SIZE + frame.Length)));
            frame.CopyTo(bytes, offset + FramingHeader.MESSAGE_SIZE);
            offset += FramingHeader.MESSAGE_SIZE + frame.Length;
        }

        return new UmdfPacket
        {
            Data = bytes,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 1,
        };
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
