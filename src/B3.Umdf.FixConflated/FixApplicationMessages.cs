using System.Globalization;

namespace B3.Umdf.FixConflated;

/// <summary>
/// Small, self-contained instrument descriptor used by the low-frequency FIX
/// application message builders in this project. Future transport wiring can
/// populate it from <c>SymbolRegistry</c>, <c>InstrumentInfo</c>, or any other
/// upstream source without introducing extra project references here.
/// </summary>
public sealed record FixInstrumentReference
{
    public string? Symbol { get; init; }
    public string? SecurityId { get; init; }
    public string? SecurityIdSource { get; init; }
    public string? SecurityExchange { get; init; }
    public int? InstrumentId { get; init; }
    public int? PutOrCall { get; init; }
    public int? Product { get; init; }
    public string? CfiCode { get; init; }
    public string? SecurityGroup { get; init; }
    public string? SecurityType { get; init; }
    public string? SecuritySubType { get; init; }
    public DateOnly? MaturityDate { get; init; }
    public decimal? ContractMultiplier { get; init; }
    public string? SecurityDescription { get; init; }
}

public sealed record FixSecurityStatusDefinition
{
    public required FixInstrumentReference Instrument { get; init; }
    public required int SecurityTradingStatus { get; init; }
    public required long SourceTimestampNanoseconds { get; init; }
    public string? SecurityStatusReqId { get; init; }
    public bool? UnsolicitedIndicator { get; init; }
    public long? TradSesOpenTimeNanoseconds { get; init; }
    public int? SecurityTradingEvent { get; init; }
    public char? HaltReason { get; init; }
    public decimal? BuyVolume { get; init; }
    public decimal? SellVolume { get; init; }
    public decimal? HighPrice { get; init; }
    public decimal? LowPrice { get; init; }
    public decimal? LastPrice { get; init; }
    public DateOnly? TradeDate { get; init; }
    public string? Text { get; init; }
    public string? TradingSessionId { get; init; }
    public string? TradingSessionSubId { get; init; }
}

public sealed record FixNewsDefinition
{
    public required long OrigTimeNanoseconds { get; init; }
    public required string Headline { get; init; }
    public string BodyText { get; init; } = string.Empty;
    public char? Urgency { get; init; }
    public string? NewsId { get; init; }
    public string? LanguageCode { get; init; }
    public string? Language { get; init; }
    public IReadOnlyList<FixInstrumentReference> RelatedInstruments { get; init; } = [];
    public string? UrlLink { get; init; }
    public string NewsSourceCode { get; init; } = "17";
}

public readonly record struct FixMarketDataFeed(string MdFeedType, int MarketDepth, int MdBookType);

public sealed record FixSecurityListEntry
{
    public required FixInstrumentReference Instrument { get; init; }
    public IReadOnlyList<FixMarketDataFeed> MarketDataFeeds { get; init; } = [];
    public string? Currency { get; init; }
    public string? SettlType { get; init; }
    public DateOnly? SettlDate { get; init; }
    public string? MaturityMonthYear { get; init; }
    public DateOnly? IssueDate { get; init; }
    public string? SettlCurrency { get; init; }
    public string? Asset { get; init; }
    public decimal? MinPriceIncrement { get; init; }
    public decimal? TickSizeDenominator { get; init; }
}

public sealed record FixSecurityListDefinition
{
    public IReadOnlyList<FixSecurityListEntry> Securities { get; init; } = [];
    public string? SecurityReqId { get; init; }
    public string? SecurityResponseId { get; init; }
    public char? SecurityRequestResult { get; init; }
    public bool LastFragment { get; init; } = true;
    public string? SecurityListId { get; init; }
    public string? SecurityListRefId { get; init; }
    public string? SecurityListDesc { get; init; }
    public int? SecurityListType { get; init; }
    public int? SecurityListTypeSource { get; init; }
    public DateTimeOffset? TransactTimeUtc { get; init; }
}

public sealed record FixSecurityListRequestDefinition
{
    public required string SecurityReqId { get; init; }
    public required char SubscriptionRequestType { get; init; }
    public string? SecurityType { get; init; }
    public int? Product { get; init; }
    public string? CfiCode { get; init; }
    public int? SecurityListRequestType { get; init; }
    public DateTimeOffset? SecurityUpdatesSinceUtc { get; init; }
}

public readonly record struct FixMarketTotalsBroadcastEntry(
    char MdEntryType,
    string Symbol,
    DateOnly EntryDateUtc,
    TimeOnly EntryTimeUtc,
    decimal GrossTradeAmount,
    decimal TotalVolumeTraded,
    int TotalNumberOfTrades);

public sealed record FixMarketTotalsBroadcastDefinition
{
    public IReadOnlyList<FixMarketTotalsBroadcastEntry> Entries { get; init; } = [];
}

public sealed record FixMarketTotalsCompositionEntry
{
    public required string Symbol { get; init; }
    public required string SecurityDescription { get; init; }
    public IReadOnlyList<string> SecurityGroups { get; init; } = [];
}

public sealed record FixMarketTotalsCompositionDefinition
{
    public IReadOnlyList<FixMarketTotalsCompositionEntry> Entries { get; init; } = [];
    public bool LastFragment { get; init; } = true;
    public string? IndexId { get; init; }
}

public sealed record FixMarketTotalsRequestDefinition
{
    public required string MdReqId { get; init; }
    public required char SubscriptionRequestType { get; init; }
}

public sealed record FixMarketTotalsResponseDefinition
{
    public required string MdReqId { get; init; }
    public char? MdReqRejReason { get; init; }
    public string? Text { get; init; }
}

internal static class FixApplicationMsgTypes
{
    public const string News = "B";
    public const string SecurityStatus = "f";
    public const string SecurityListRequest = "x";
    public const string SecurityList = "y";
    public const string MarketTotalsBroadcast = "UTOT";
    public const string MarketTotalsComposition = "UTOTC";
    public const string MarketTotalsRequest = "UTOTQ";
    public const string MarketTotalsResponse = "UTOTP";
}

internal static class FixApplicationTags
{
    public const int SecurityIdSource = 22;
    public const int NoLinesOfText = 33;
    public const int OrigTime = 42;
    public const int SecurityId = 48;
    public const int Symbol = 55;
    public const int Text = 58;
    public const int TransactTime = 60;
    public const int Urgency = 61;
    public const int TradeDate = 75;
    public const int SecurityDescription = 107;
    public const int Currency = 15;
    public const int DeliverToCompId = 128;
    public const int Headline = 148;
    public const int UrlLink = 149;
    public const int NoRelatedSym = 146;
    public const int SecurityType = 167;
    public const int PutOrCall = 201;
    public const int SecurityExchange = 207;
    public const int SettlCurrency = 120;
    public const int ContractMultiplier = 231;
    public const int SettlType = 63;
    public const int SettlDate = 64;
    public const int MaturityMonthYear = 200;
    public const int IssueDate = 225;
    public const int MDReqID = 262;
    public const int SubscriptionRequestType = 263;
    public const int MarketDepth = 264;
    public const int NoMdEntries = 268;
    public const int MdEntryType = 269;
    public const int MdEntryDate = 272;
    public const int MdEntryTime = 273;
    public const int MdReqRejReason = 281;
    public const int SecurityReqId = 320;
    public const int SecurityResponseId = 322;
    public const int SecurityStatusReqId = 324;
    public const int UnsolicitedIndicator = 325;
    public const int SecurityTradingStatus = 326;
    public const int HaltReason = 327;
    public const int BuyVolume = 330;
    public const int SellVolume = 331;
    public const int HighPrice = 332;
    public const int LowPrice = 333;
    public const int TradSesOpenTime = 342;
    public const int TradingSessionId = 336;
    public const int TradingSessionSubId = 625;
    public const int GrossTradeAmt = 381;
    public const int TotalVolumeTraded = 387;
    public const int TotNoRelatedSym = 393;
    public const int LastPx = 31;
    public const int Product = 460;
    public const int CfiCode = 461;
    public const int CountryOfIssue = 470;
    public const int MaturityDate = 541;
    public const int SecurityListRequestType = 559;
    public const int SecurityRequestResult = 560;
    public const int MinPriceIncrement = 969;
    public const int SecurityUpdateAction = 980;
    public const int SecuritySubType = 762;
    public const int LastFragment = 893;
    public const int MdBookType = 1021;
    public const int MdFeedType = 1022;
    public const int NoMdFeedTypes = 1141;
    public const int SecurityGroup = 1151;
    public const int SecurityTradingEvent = 1174;
    public const int SecurityListId = 1465;
    public const int SecurityListRefId = 1466;
    public const int SecurityListDesc = 1467;
    public const int SecurityListType = 1470;
    public const int SecurityListTypeSource = 1471;
    public const int NewsId = 1472;
    public const int LanguageCode = 1474;
    public const int IndexId = 6107;
    public const int TotalNumOfTrades = 6139;
    public const int SecurityUpdatesSince = 6935;
    public const int Language = 6936;
    public const int Asset = 6937;
    public const int TickSizeDenominator = 9749;
    public const int NewsSource = 6940;
    public const int InstrumentId = 9219;
    public const int NoSecurityGroups = 37022;
}

internal static class FixApplicationMessageBuilderSupport
{
    public static DateTimeOffset FromUnixNanoseconds(long value)
        => DateTimeOffset.UnixEpoch.AddTicks(value / 100);

    public static string FormatLocalDate(DateOnly value)
        => value.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

    public static string FormatUtcTime(TimeOnly value)
        => value.Millisecond == 0
            ? value.ToString("HH:mm:ss", CultureInfo.InvariantCulture)
            : value.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);

    public static string FormatDecimal(decimal value)
        => value.ToString(CultureInfo.InvariantCulture);

    public static string[] SplitTextLines(string text)
        => text.Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace('\r', '\n')
            .Split('\n', StringSplitOptions.None);

    public static void AppendInstrumentPrefix(FixMessage message, FixInstrumentReference instrument)
    {
        AddOptionalString(message, FixApplicationTags.Symbol, instrument.Symbol);
        AddOptionalString(message, FixApplicationTags.SecurityId, instrument.SecurityId);
        AddOptionalString(message, FixApplicationTags.SecurityIdSource, instrument.SecurityIdSource);
        AddOptionalString(message, FixApplicationTags.SecurityExchange, instrument.SecurityExchange);
    }

    public static void AppendInstrumentSuffix(FixMessage message, FixInstrumentReference instrument)
    {
        AddOptionalInt(message, FixApplicationTags.InstrumentId, instrument.InstrumentId);
        AddOptionalInt(message, FixApplicationTags.PutOrCall, instrument.PutOrCall);
        AddOptionalInt(message, FixApplicationTags.Product, instrument.Product);
        AddOptionalString(message, FixApplicationTags.CfiCode, instrument.CfiCode);
        AddOptionalString(message, FixApplicationTags.SecurityGroup, instrument.SecurityGroup);
        AddOptionalString(message, FixApplicationTags.SecurityType, instrument.SecurityType);
        AddOptionalString(message, FixApplicationTags.SecuritySubType, instrument.SecuritySubType);
        AddOptionalDate(message, FixApplicationTags.MaturityDate, instrument.MaturityDate);
        AddOptionalDecimal(message, FixApplicationTags.ContractMultiplier, instrument.ContractMultiplier);
        AddOptionalString(message, FixApplicationTags.SecurityDescription, instrument.SecurityDescription);
    }

    public static void AddRequiredString(FixMessage message, int tag, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);
        message.Add(tag, value);
    }

    public static void AddOptionalString(FixMessage message, int tag, string? value)
    {
        if (!string.IsNullOrEmpty(value))
            message.Add(tag, value);
    }

    public static void AddOptionalInt(FixMessage message, int tag, int? value)
    {
        if (value is { } actual)
            message.Add(tag, actual);
    }

    public static void AddOptionalBoolean(FixMessage message, int tag, bool? value)
    {
        if (value is { } actual)
            message.AddBoolean(tag, actual);
    }

    public static void AddOptionalChar(FixMessage message, int tag, char? value)
    {
        if (value is { } actual)
            message.Add(tag, actual.ToString());
    }

    public static void AddOptionalDecimal(FixMessage message, int tag, decimal? value)
    {
        if (value is { } actual)
            message.Add(tag, FormatDecimal(actual));
    }

    public static void AddOptionalDate(FixMessage message, int tag, DateOnly? value)
    {
        if (value is { } actual)
            message.Add(tag, FormatLocalDate(actual));
    }

    public static void AddOptionalUtcTimestamp(FixMessage message, int tag, DateTimeOffset? value)
    {
        if (value is { } actual)
            message.Add(tag, FixValueFormatting.FormatUtcTimestamp(actual));
    }

    public static void AddOptionalUnixNanosTimestamp(FixMessage message, int tag, long? value)
    {
        if (value is { } actual)
            message.Add(tag, FixValueFormatting.FormatUtcTimestamp(FromUnixNanoseconds(actual)));
    }
}
