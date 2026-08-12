using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class NewsMessageBuilderTests
{
    [Fact]
    public void Build_Splits_BodyText_Into_NoLinesOfText_Group()
    {
        var message = NewsMessageBuilder.Build(new FixNewsDefinition
        {
            OrigTimeNanoseconds = 1_786_544_116_789_000_000,
            Headline = "PETR4 enters auction",
            BodyText = "Line one\nLine two",
            NewsSource = "B3",
            NewsId = "9001",
            LanguageCode = "pt",
            Language = "pt-BR",
            UrlLink = "https://example.test/news/9001",
            RelatedInstruments =
            [
                new FixInstrumentReference
                {
                    Symbol = "PETR4",
                    SecurityId = "12345",
                    SecurityIdSource = "8",
                    SecurityExchange = "BVMF",
                    InstrumentId = 99
                }
            ]
        });

        Assert.Equal(FixApplicationMsgTypes.News, FixApplicationMessageTestHelpers.GetRequired(message, FixTags.MsgType));
        Assert.Equal("20260812-14:15:16.789", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.OrigTime));
        Assert.Equal("PETR4 enters auction", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.Headline));
        Assert.Equal("B3", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.NewsSource));
        Assert.Equal("1", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.NoRelatedSym));
        Assert.Equal("2", FixApplicationMessageTestHelpers.GetRequired(message, FixApplicationTags.NoLinesOfText));
        Assert.Equal(["Line one", "Line two"], FixApplicationMessageTestHelpers.GetAllValues(message, FixApplicationTags.Text));

        FixMessage decoded = FixApplicationMessageTestHelpers.RoundTrip(message);
        Assert.Equal(["Line one", "Line two"], FixApplicationMessageTestHelpers.GetAllValues(decoded, FixApplicationTags.Text));
        Assert.Equal("https://example.test/news/9001", FixApplicationMessageTestHelpers.GetRequired(decoded, FixApplicationTags.UrlLink));
    }
}
