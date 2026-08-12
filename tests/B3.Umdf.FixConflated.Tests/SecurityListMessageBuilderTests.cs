using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class SecurityListMessageBuilderTests
{
    [Fact]
    public void BuildRequest_Produces_Parseable_SecurityListRequest()
    {
        var message = SecurityListMessageBuilder.BuildRequest(new FixSecurityListRequestDefinition
        {
            SecurityReqId = "req-42",
            SubscriptionRequestType = '1',
            SecurityType = "CS",
            Product = 4,
            CfiCode = "ESVUFR",
            SecurityListRequestType = 4,
            SecurityUpdatesSinceUtc = new DateTimeOffset(2026, 8, 12, 14, 0, 0, TimeSpan.Zero)
        });

        Assert.Equal(FixApplicationMsgTypes.SecurityListRequest, FixApplicationMessageTestHelpers.GetRequired(message, FixTags.MsgType));
        Assert.Equal("req-42", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.SecurityReqId));
        Assert.Equal("1", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.SubscriptionRequestType));

        FixMessage decoded = FixApplicationMessageTestHelpers.RoundTrip(message);
        Assert.Equal("20260812-14:00:00.000", FixApplicationMessageTestHelpers.GetRequired(decoded, FixApplicationTags.SecurityUpdatesSince));
    }

    [Fact]
    public void Build_Produces_Parseable_SecurityList_WithNestedFeedGroups()
    {
        var message = SecurityListMessageBuilder.Build(new FixSecurityListDefinition
        {
            SecurityReqId = "req-42",
            SecurityResponseId = "resp-42",
            SecurityRequestResult = '0',
            LastFragment = true,
            SecurityListId = "list-1",
            SecurityListDesc = "Cash equities",
            Securities =
            [
                new FixSecurityListEntry
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
                        SecurityType = "CS"
                    },
                    MarketDataFeeds =
                    [
                        new FixMarketDataFeed("BOOK", 10, 1),
                        new FixMarketDataFeed("TRADES", 1, 2)
                    ]
                },
                new FixSecurityListEntry
                {
                    Instrument = new FixInstrumentReference
                    {
                        Symbol = "VALE3",
                        SecurityId = "67890",
                        SecurityIdSource = "8",
                        SecurityExchange = "BVMF",
                        InstrumentId = 100,
                        Product = 4,
                        CfiCode = "ESVUFR",
                        SecurityGroup = "EQUITY",
                        SecurityType = "CS"
                    },
                    MarketDataFeeds =
                    [
                        new FixMarketDataFeed("BOOK", 10, 1)
                    ]
                }
            ]
        });

        Assert.Equal(FixApplicationMsgTypes.SecurityList, FixApplicationMessageTestHelpers.GetRequired(message, FixTags.MsgType));
        Assert.Equal("2", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.TotNoRelatedSym));
        Assert.Equal("Y", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.LastFragment));
        Assert.Equal("2", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.NoRelatedSym));
        Assert.Equal(["PETR4", "VALE3"], FixApplicationMessageTestHelpers.GetAllValues(message, FixApplicationTags.Symbol));
        Assert.Equal(["2", "1"], FixApplicationMessageTestHelpers.GetAllValues(message, FixApplicationTags.NoMdFeedTypes));
        Assert.Equal(["BOOK", "TRADES", "BOOK"], FixApplicationMessageTestHelpers.GetAllValues(message, FixApplicationTags.MdFeedType));

        FixMessage decoded = FixApplicationMessageTestHelpers.RoundTrip(message);
        Assert.Equal(["10", "1", "10"], FixApplicationMessageTestHelpers.GetAllValues(decoded, FixApplicationTags.MarketDepth));
        Assert.Equal("list-1", FixApplicationMessageTestHelpers.GetRequired(decoded, FixApplicationTags.SecurityListId));
    }
}
