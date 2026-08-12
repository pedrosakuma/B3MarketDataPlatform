using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class MarketTotalsMessageBuilderTests
{
    [Fact]
    public void BuildBroadcast_Produces_Parseable_MarketTotalsBroadcast()
    {
        var message = MarketTotalsMessageBuilder.BuildBroadcast(new FixMarketTotalsBroadcastDefinition
        {
            Entries =
            [
                new FixMarketTotalsBroadcastEntry('B', "PETR4", new DateOnly(2026, 8, 12), new TimeOnly(14, 15, 16, 789), 1_250_000.55m, 125_000m, 97),
                new FixMarketTotalsBroadcastEntry('B', "VALE3", new DateOnly(2026, 8, 12), new TimeOnly(14, 15, 17), 980_000.10m, 87_500m, 61)
            ]
        });

        Assert.Equal(FixApplicationMsgTypes.MarketTotalsBroadcast, FixApplicationMessageTestHelpers.GetRequired(message, FixTags.MsgType));
        Assert.Equal("2", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.NoMdEntries));
        Assert.Equal(["PETR4", "VALE3"], FixApplicationMessageTestHelpers.GetAllValues(message, FixApplicationTags.Symbol));

        FixMessage decoded = FixApplicationMessageTestHelpers.RoundTrip(message);
        Assert.Equal(["1250000.55", "980000.10"], FixApplicationMessageTestHelpers.GetAllValues(decoded, FixApplicationTags.GrossTradeAmt));
        Assert.Equal(["97", "61"], FixApplicationMessageTestHelpers.GetAllValues(decoded, FixApplicationTags.TotalNumOfTrades));
    }

    [Fact]
    public void BuildComposition_Produces_Parseable_MarketTotalsComposition()
    {
        var message = MarketTotalsMessageBuilder.BuildComposition(new FixMarketTotalsCompositionDefinition
        {
            IndexId = "IBOV",
            LastFragment = true,
            Entries =
            [
                new FixMarketTotalsCompositionEntry
                {
                    Symbol = "PETR4",
                    SecurityDescription = "PETROBRAS PN",
                    SecurityGroups = ["ENERGY", "IBOV"]
                },
                new FixMarketTotalsCompositionEntry
                {
                    Symbol = "VALE3",
                    SecurityDescription = "VALE ON"
                }
            ]
        });

        Assert.Equal(FixApplicationMsgTypes.MarketTotalsComposition, FixApplicationMessageTestHelpers.GetRequired(message, FixTags.MsgType));
        Assert.Equal("2", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.TotNoRelatedSym));
        Assert.Equal("Y", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.LastFragment));
        Assert.Equal("IBOV", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.IndexId));

        FixMessage decoded = FixApplicationMessageTestHelpers.RoundTrip(message);
        Assert.Equal(["PETR4", "VALE3"], FixApplicationMessageTestHelpers.GetAllValues(decoded, FixApplicationTags.Symbol));
        Assert.Equal(["2"], FixApplicationMessageTestHelpers.GetAllValues(decoded, FixApplicationTags.NoSecurityGroups));
        Assert.Equal(["ENERGY", "IBOV"], FixApplicationMessageTestHelpers.GetAllValues(decoded, FixApplicationTags.SecurityGroup));
    }

    [Fact]
    public void BuildRequestAndResponse_Produce_Parseable_MarketTotalsControlMessages()
    {
        FixMessage request = MarketTotalsMessageBuilder.BuildRequest(new FixMarketTotalsRequestDefinition
        {
            MdReqId = "totals-1",
            SubscriptionRequestType = '0'
        });

        FixMessage response = MarketTotalsMessageBuilder.BuildResponse(new FixMarketTotalsResponseDefinition
        {
            MdReqId = "totals-1",
            MdReqRejReason = '1',
            Text = "Unsupported filter"
        });

        Assert.Equal(FixApplicationMsgTypes.MarketTotalsRequest, FixApplicationMessageTestHelpers.GetRequired(request, FixTags.MsgType));
        Assert.Equal(FixApplicationMsgTypes.MarketTotalsResponse, FixApplicationMessageTestHelpers.GetRequired(response, FixTags.MsgType));

        FixMessage decodedRequest = FixApplicationMessageTestHelpers.RoundTrip(request);
        FixMessage decodedResponse = FixApplicationMessageTestHelpers.RoundTrip(response);
        Assert.Equal("0", FixApplicationMessageTestHelpers.GetRequired(decodedRequest, FixApplicationTags.SubscriptionRequestType));
        Assert.Equal("1", FixApplicationMessageTestHelpers.GetRequired(decodedResponse, FixApplicationTags.MdReqRejReason));
        Assert.Equal("Unsupported filter", FixApplicationMessageTestHelpers.GetRequired(decodedResponse, FixApplicationTags.Text));
    }
}
