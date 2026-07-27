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
            ReasonCode: InstrumentStatusDecoder.InstrumentHaltedReasonCode,
            SourceTimestampNanos: 1_750_000_000_123_456_789,
            RptSeq: 77);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];

        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 12345, symbol: "PETR4", in update);

        Assert.True(WireFormat.TryReadHeader(buffer, out var wireLength, out var type));
        Assert.Equal((uint)length, wireLength);
        Assert.Equal(MessageType.InstrumentStatus, type);
        Assert.Equal((ushort)0x00B3, (ushort)type);

        var decoded = WireFormat.ReadInstrumentStatus(
            buffer.AsSpan(WireFormat.FramingHeaderSize, length - WireFormat.FramingHeaderSize),
            new DateTime(2026, 7, 27, 0, 0, 0, DateTimeKind.Utc));

        Assert.Equal(12345UL, decoded.SecurityId);
        Assert.Equal("PETR4", decoded.Symbol);
        Assert.Equal(21, decoded.PreviousStatus);
        Assert.Equal(17, decoded.NewStatus);
        Assert.Equal(InstrumentStatusReason.InstrumentHalted, decoded.Reason);
        Assert.Equal(1, decoded.RawReasonCode);
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
            ReasonCode: InstrumentStatusDecoder.InstrumentResumedReasonCode,
            SourceTimestampNanos: 123,
            RptSeq: null);
        var buffer = new byte[WireProtocol.InstrumentStatusMaxSize];
        int length = WireProtocol.WriteInstrumentStatus(
            buffer, securityId: 7, symbol: "VALE3", in update);

        var decoded = WireFormat.ReadInstrumentStatus(
            buffer.AsSpan(WireFormat.FramingHeaderSize, length - WireFormat.FramingHeaderSize),
            DateTime.UnixEpoch);

        Assert.Null(decoded.PreviousStatus);
        Assert.Equal(InstrumentStatusReason.InstrumentResumed, decoded.Reason);
        Assert.False(decoded.IsHalted);
        Assert.Null(decoded.RptSeq);
    }
}
