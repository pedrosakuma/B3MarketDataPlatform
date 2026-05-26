using System.Buffers.Binary;
using B3.Umdf.Book;
using B3.Umdf.Server;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// End-to-end wire roundtrip for the SecurityDefinition channel (issue #55):
/// server-side <c>WireProtocol.WriteSecurityDefinition</c> ⇆ SDK
/// <c>WireFormat.ReadSecurityDefinition</c>. Pins the byte-for-byte contract
/// between the two halves of the codebase so a refactor on either side that
/// silently drifts the bit positions or the Fixed8 scale fails here.
/// </summary>
public class SecurityDefinitionWireRoundTripTests
{
    [Fact]
    public void Roundtrip_FullyPopulated_AllFieldsPreserved()
    {
        var info = new InstrumentInfo
        {
            Symbol = "PETR4",
            MinPriceIncrement = 1_000_000L,    // 0.01 in Fixed8 (1e-8) units
            MinTradeVolume = 100L,
            PriceDivisor = 100L,
            ContractMultiplier = 1L,
            StrikePrice = 25_5000L,
            MaturityDate = 20251231,
            PutOrCall = 0,
            ExerciseStyle = 1,
            SecurityType = 1,
            SecuritySubType = 2,
            Product = 4,
            MarketSegmentID = 7,
            TickSizeDenominator = 100,
            IsinNumber = "BRPETRACNOR9",
            Currency = "BRL",
            Asset = "PETR",
            CfiCode = "ESVUFR",
            SecurityGroup = "EQTY",
            SecurityDescription = "Petrobras PN",
        };

        var buf = new byte[B3.Umdf.Server.WireProtocol.SecurityDefinitionMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WriteSecurityDefinition(buf, securityId: 12345UL, info);

        Assert.True(WireFormat.TryReadHeader(buf, out var hLen, out var type));
        Assert.Equal(len, hLen);
        Assert.Equal(WireFormat.MessageType.SecurityDefinition, type);

        var ev = WireFormat.ReadSecurityDefinition(
            new ReadOnlySpan<byte>(buf, WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(12345UL, ev.SecurityId);
        Assert.Equal("PETR4", ev.Symbol);
        Assert.Equal(0.01m, ev.MinPriceIncrement);
        Assert.Equal(100L, ev.MinTradeVolume);
        Assert.Equal(100L, ev.PriceDivisor);
        Assert.Equal(1L, ev.ContractMultiplier);
        Assert.Equal(25_5000L, ev.StrikePrice);
        Assert.Equal(20251231L, ev.MaturityDate);
        Assert.Equal(0L, ev.PutOrCall);
        Assert.Equal(1L, ev.ExerciseStyle);
        Assert.Equal(1L, ev.SecurityType);
        Assert.Equal(2L, ev.SecuritySubType);
        Assert.Equal(4L, ev.Product);
        Assert.Equal(7L, ev.MarketSegmentID);
        Assert.Equal(100L, ev.TickSizeDenominator);
        Assert.Equal("BRPETRACNOR9", ev.IsinNumber);
        Assert.Equal("BRL", ev.Currency);
        Assert.Equal("PETR", ev.Asset);
        Assert.Equal("ESVUFR", ev.CfiCode);
        Assert.Equal("EQTY", ev.SecurityGroup);
        Assert.Equal("Petrobras PN", ev.SecurityDescription);
    }

    [Fact]
    public void Roundtrip_SymbolOnly_AllFieldsNull()
    {
        var info = new InstrumentInfo { Symbol = "VALE3" };
        var buf = new byte[B3.Umdf.Server.WireProtocol.SecurityDefinitionMaxSize];
        int len = B3.Umdf.Server.WireProtocol.WriteSecurityDefinition(buf, securityId: 7UL, info);

        var ev = WireFormat.ReadSecurityDefinition(
            new ReadOnlySpan<byte>(buf, WireFormat.FramingHeaderSize, len - WireFormat.FramingHeaderSize),
            receivedUtc: DateTime.UtcNow);

        Assert.Equal(7UL, ev.SecurityId);
        Assert.Equal("VALE3", ev.Symbol);
        Assert.Null(ev.MinPriceIncrement);
        Assert.Null(ev.MinTradeVolume);
        Assert.Null(ev.IsinNumber);
        Assert.Null(ev.SecurityDescription);
    }

    [Fact]
    public void SubscribeFlags_SecurityDefinition_IsInEverythingButNotInAll()
    {
        Assert.True(SubscribeFlags.Everything.HasFlag(SubscribeFlags.SecurityDefinition));
        Assert.False(SubscribeFlags.All.HasFlag(SubscribeFlags.SecurityDefinition));
        Assert.Equal((byte)0x20, (byte)SubscribeFlags.SecurityDefinition);
        // Wire-level parity with the server's DataFlags enum.
        Assert.Equal((byte)DataFlags.SecurityDefinition, (byte)SubscribeFlags.SecurityDefinition);
    }
}
