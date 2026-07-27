using System.Buffers.Binary;
using B3.MarketData.Wire;
using B3.Umdf.Book;
using B3.Umdf.Server;

namespace B3.MarketData.WebSocketClient.Tests;

public class InstrumentStatusWireRoundTripTests
{
    [Fact]
    public void Roundtrip_Halt_AllFieldsPreserved()
    {
        var update = new InstrumentStatusUpdate(
            PreviousStatus: 21,
            NewStatus: 17,
            TransitionCode: InstrumentStatusDecoder.InstrumentHaltedTransitionCode,
            HaltReasonCode: (byte)InstrumentHaltReason.RegulatoryHalt,
            SourceTimestampNanos: 1_750_000_000_123_456_789,
            RptSeq: 77,
            AdministrativeStateCode: InstrumentStatusDecoder.AdministrativeHaltedStateCode,
            TradingSessionId: 1);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];

        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 12345, symbol: "PETR4", in update);

        Assert.True(WireFormat.TryReadHeader(buffer, out var wireLength, out var type));
        Assert.Equal((uint)length, wireLength);
        Assert.Equal(MessageType.InstrumentStatus, type);
        Assert.Equal((ushort)0x00B3, (ushort)type);

        Assert.True(WireFormat.TryReadInstrumentStatus(
            buffer.AsSpan(WireFormat.FramingHeaderSize, length - WireFormat.FramingHeaderSize),
            new DateTime(2026, 7, 27, 0, 0, 0, DateTimeKind.Utc),
            out var decoded));

        Assert.Equal(12345UL, decoded.SecurityId);
        Assert.Equal("PETR4", decoded.Symbol);
        Assert.Equal(21, decoded.PreviousStatus);
        Assert.Equal(17, decoded.NewStatus);
        Assert.Equal(InstrumentStatusTransitionKind.Halted, decoded.Transition);
        Assert.Equal(1, decoded.RawTransitionCode);
        Assert.Equal(InstrumentHaltReason.RegulatoryHalt, decoded.HaltReason);
        Assert.Equal((byte?)InstrumentHaltReason.RegulatoryHalt, decoded.RawHaltReasonCode);
        Assert.Equal(InstrumentAdministrativeState.Halted, decoded.AdministrativeState);
        Assert.Equal((byte?)InstrumentStatusDecoder.AdministrativeHaltedStateCode, decoded.RawAdministrativeStateCode);
        Assert.Equal((byte?)1, decoded.TradingSessionId);
        Assert.True(decoded.IsHalted);
        Assert.Equal(InstrumentStatusDeliveryKind.LiveTransition, decoded.DeliveryKind);
        Assert.False(decoded.IsSnapshot);
        Assert.Equal(1_750_000_000_123_456_789UL, decoded.SourceTimestampNanos);
        Assert.Equal(77u, decoded.RptSeq);
    }

    [Fact]
    public void Roundtrip_FirstSightResume_UsesNullSentinels()
    {
        var update = new InstrumentStatusUpdate(
            PreviousStatus: null,
            NewStatus: 17,
            TransitionCode: InstrumentStatusDecoder.InstrumentResumedTransitionCode,
            HaltReasonCode: null,
            SourceTimestampNanos: 123,
            RptSeq: null,
            AdministrativeStateCode: InstrumentStatusDecoder.AdministrativeActiveStateCode,
            TradingSessionId: 1);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];
        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 7, symbol: "VALE3", in update, isSnapshot: true);

        Assert.True(WireFormat.TryReadInstrumentStatus(
            buffer.AsSpan(WireFormat.FramingHeaderSize, length - WireFormat.FramingHeaderSize),
            DateTime.UnixEpoch,
            out var decoded));

        Assert.Null(decoded.PreviousStatus);
        Assert.Equal(InstrumentStatusTransitionKind.Resumed, decoded.Transition);
        Assert.False(decoded.IsHalted);
        Assert.Equal(InstrumentAdministrativeState.Active, decoded.AdministrativeState);
        Assert.Equal(InstrumentStatusDeliveryKind.Snapshot, decoded.DeliveryKind);
        Assert.True(decoded.IsSnapshot);
        Assert.Null(decoded.RptSeq);
    }

    [Fact]
    public void Roundtrip_DetailedReason_IsSeparateFromTransition()
    {
        var update = new InstrumentStatusUpdate(
            PreviousStatus: 17,
            NewStatus: 17,
            TransitionCode: InstrumentStatusDecoder.InstrumentHaltedTransitionCode,
            HaltReasonCode: (byte)InstrumentHaltReason.NewsHold,
            SourceTimestampNanos: 123,
            RptSeq: 1,
            AdministrativeStateCode: InstrumentStatusDecoder.AdministrativeHaltedStateCode,
            TradingSessionId: 1);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];
        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 7, symbol: "VALE3", in update);

        Assert.True(WireFormat.TryReadInstrumentStatus(
            buffer.AsSpan(WireFormat.FramingHeaderSize, length - WireFormat.FramingHeaderSize),
            DateTime.UnixEpoch,
            out var decoded));

        Assert.Equal(InstrumentStatusTransitionKind.Halted, decoded.Transition);
        Assert.Equal(InstrumentHaltReason.NewsHold, decoded.HaltReason);
        Assert.Equal((byte)InstrumentHaltReason.NewsHold, decoded.RawHaltReasonCode);
    }

    [Fact]
    public void TryRead_LegacyFrameWithoutDeliveryKind_DefaultsToLiveTransition()
    {
        var update = new InstrumentStatusUpdate(
            PreviousStatus: 17,
            NewStatus: 17,
            TransitionCode: InstrumentStatusDecoder.InstrumentHaltedTransitionCode,
            HaltReasonCode: null,
            SourceTimestampNanos: 123,
            RptSeq: 1);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];
        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 7, symbol: "VALE3", in update, isSnapshot: true);
        var payload = buffer.AsSpan(
            WireFormat.FramingHeaderSize,
            length - WireFormat.FramingHeaderSize - 3);

        Assert.True(WireFormat.TryReadInstrumentStatus(
            payload, DateTime.UnixEpoch, out var decoded));
        Assert.Equal(InstrumentStatusDeliveryKind.LiveTransition, decoded.DeliveryKind);
        Assert.False(decoded.IsSnapshot);
        Assert.Equal(InstrumentAdministrativeState.Halted, decoded.AdministrativeState);
        Assert.Null(decoded.RawAdministrativeStateCode);
        Assert.Null(decoded.TradingSessionId);
    }

    [Fact]
    public void Roundtrip_RecoveryMarkedUpdate_IsSnapshotWithNullTransition()
    {
        var update = new InstrumentStatusUpdate(
            PreviousStatus: null,
            NewStatus: 17,
            TransitionCode: InstrumentStatusDecoder.UnavailableCode,
            HaltReasonCode: (byte)InstrumentHaltReason.VolatilityCircuitBreaker,
            SourceTimestampNanos: 123,
            RptSeq: null,
            AdministrativeStateCode: InstrumentStatusDecoder.AdministrativeHaltedStateCode,
            TradingSessionId: 1,
            IsRecovery: true);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];
        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 7, symbol: "VALE3", in update);

        Assert.True(WireFormat.TryReadInstrumentStatus(
            buffer.AsSpan(WireFormat.FramingHeaderSize, length - WireFormat.FramingHeaderSize),
            DateTime.UnixEpoch,
            out var decoded));

        Assert.True(decoded.IsSnapshot);
        Assert.True(decoded.IsHalted);
        Assert.Equal(InstrumentStatusTransitionKind.Unknown, decoded.Transition);
        Assert.Equal(InstrumentHaltReason.VolatilityCircuitBreaker, decoded.HaltReason);
    }

    [Fact]
    public void Roundtrip_UnknownV17Codes_PreserveRawValues()
    {
        var update = new InstrumentStatusUpdate(
            PreviousStatus: null,
            NewStatus: 17,
            TransitionCode: 9,
            HaltReasonCode: 77,
            SourceTimestampNanos: 123,
            RptSeq: 1,
            AdministrativeStateCode: 8,
            TradingSessionId: 6);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];
        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 7, symbol: "VALE3", in update);

        Assert.True(WireFormat.TryReadInstrumentStatus(
            buffer.AsSpan(WireFormat.FramingHeaderSize, length - WireFormat.FramingHeaderSize),
            DateTime.UnixEpoch,
            out var decoded));

        Assert.Equal(InstrumentStatusTransitionKind.Unknown, decoded.Transition);
        Assert.Equal(9, decoded.RawTransitionCode);
        Assert.Equal(InstrumentHaltReason.Unknown, decoded.HaltReason);
        Assert.Equal((byte?)77, decoded.RawHaltReasonCode);
        Assert.Equal(InstrumentAdministrativeState.Unknown, decoded.AdministrativeState);
        Assert.Equal((byte?)8, decoded.RawAdministrativeStateCode);
        Assert.Equal((byte?)6, decoded.TradingSessionId);
    }

    [Fact]
    public void ServerCapabilities_InstrumentStatus_UsesAppendOnlyBit()
    {
        Assert.Equal(0x0004u, (uint)B3.MarketData.WebSocketClient.ServerCapabilities.InstrumentStatus);
        Assert.Equal(0x0004u, (uint)B3.MarketData.Wire.ServerCapabilities.InstrumentStatus);
    }

    [Fact]
    public void ServerHello_WithoutInstrumentStatusBit_IdentifiesOlderServer()
    {
        Span<byte> payload = stackalloc byte[9];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, WireFormat.ProtocolVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(payload[4..], 0x0003);
        payload[8] = 0;

        var (_, capabilities, _) = WireFormat.ReadServerHello(payload);

        Assert.False(capabilities.HasFlag(
            B3.MarketData.WebSocketClient.ServerCapabilities.InstrumentStatus));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(8)]
    [InlineData(24)]
    public void TryRead_TruncatedPayload_ReturnsFalse(int length)
    {
        Assert.False(WireFormat.TryReadInstrumentStatus(
            new byte[length], DateTime.UnixEpoch, out _));
    }

    [Fact]
    public void TryRead_SymbolLengthExceedsPayload_ReturnsFalse()
    {
        var payload = new byte[25];
        payload[8] = 10;

        Assert.False(WireFormat.TryReadInstrumentStatus(
            payload, DateTime.UnixEpoch, out _));
    }
}
