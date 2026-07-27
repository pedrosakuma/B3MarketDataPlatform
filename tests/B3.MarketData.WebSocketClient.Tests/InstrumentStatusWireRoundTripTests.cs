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
            HaltReasonCode: null,
            SourceTimestampNanos: 1_750_000_000_123_456_789,
            RptSeq: 77);
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
        Assert.Null(decoded.HaltReason);
        Assert.Null(decoded.RawHaltReasonCode);
        Assert.True(decoded.IsHalted);
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
            RptSeq: null);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];
        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 7, symbol: "VALE3", in update);

        Assert.True(WireFormat.TryReadInstrumentStatus(
            buffer.AsSpan(WireFormat.FramingHeaderSize, length - WireFormat.FramingHeaderSize),
            DateTime.UnixEpoch,
            out var decoded));

        Assert.Null(decoded.PreviousStatus);
        Assert.Equal(InstrumentStatusTransitionKind.Resumed, decoded.Transition);
        Assert.False(decoded.IsHalted);
        Assert.Null(decoded.RptSeq);
    }

    [Fact]
    public void Roundtrip_FutureDetailedReason_IsSeparateFromTransition()
    {
        var update = new InstrumentStatusUpdate(
            PreviousStatus: 17,
            NewStatus: 17,
            TransitionCode: InstrumentStatusDecoder.InstrumentHaltedTransitionCode,
            HaltReasonCode: (byte)InstrumentHaltReason.NewsHold,
            SourceTimestampNanos: 123,
            RptSeq: 1);
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
