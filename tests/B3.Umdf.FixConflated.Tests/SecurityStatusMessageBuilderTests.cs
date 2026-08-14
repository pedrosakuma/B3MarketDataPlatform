using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class SecurityStatusMessageBuilderTests
{
    [Fact]
    public void Build_Produces_Compact_ProductionLike_SecurityStatus()
    {
        var message = SecurityStatusMessageBuilder.Build(new FixSecurityStatusDefinition
        {
            Instrument = new FixInstrumentReference
            {
                Symbol = "PETR4",
                SecurityId = "12345",
                SecurityIdSource = "8",
                SecurityExchange = "BVMF",
                InstrumentId = 99,
                Product = 4,
                CfiCode = "ESVUFR",
                SecurityGroup = "EQUITY",
                SecurityType = "CS",
                SecuritySubType = "PN",
                ContractMultiplier = 1m,
                SecurityDescription = "PETROBRAS PN"
            },
            SecurityTradingStatus = 2,
            SourceTimestampNanoseconds = 1_786_544_116_789_000_000,
            TradingSessionId = "1",
            TradingSessionSubId = "18"
        });

        Assert.Equal(FixApplicationMsgTypes.SecurityStatus, FixApplicationMessageTestHelpers.GetRequired(message, FixTags.MsgType));
        Assert.Equal("PETR4", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.Symbol));
        Assert.Equal("12345", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.SecurityId));
        Assert.Equal("20260812", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.TradeDate));
        Assert.Equal("20260812-14:15:16.789", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.TransactTime));
        Assert.Equal("1", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.TradingSessionId));
        Assert.Equal("18", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.TradingSessionSubId));
        Assert.Equal("EQUITY", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.SecurityGroup));

        FixMessage decoded = FixApplicationMessageTestHelpers.RoundTrip(message);
        Assert.False(decoded.TryGetString(FixApplicationTags.SecurityTradingStatus, out _));
        Assert.False(decoded.TryGetString(FixApplicationTags.Text, out _));
    }
}
