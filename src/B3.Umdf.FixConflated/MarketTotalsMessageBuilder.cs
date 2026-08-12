namespace B3.Umdf.FixConflated;

/// <summary>
/// Best-effort builders for the proprietary MarketTotals* messages exposed by
/// the vendored UMDF conflated dictionary.
/// </summary>
public static class MarketTotalsMessageBuilder
{
    public static FixMessage BuildBroadcast(FixMarketTotalsBroadcastDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        if (definition.Entries.Count == 0)
            throw new ArgumentException("MarketTotalsBroadcast requires at least one entry.", nameof(definition));

        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixApplicationMsgTypes.MarketTotalsBroadcast);
        message.Add(FixApplicationTags.NoMdEntries, definition.Entries.Count);

        foreach (FixMarketTotalsBroadcastEntry entry in definition.Entries)
        {
            ArgumentException.ThrowIfNullOrEmpty(entry.Symbol);
            message.Add(FixApplicationTags.MdEntryType, entry.MdEntryType.ToString());
            message.Add(FixApplicationTags.Symbol, entry.Symbol);
            message.Add(FixApplicationTags.MdEntryDate, FixApplicationMessageBuilderSupport.FormatLocalDate(entry.EntryDateUtc));
            message.Add(FixApplicationTags.MdEntryTime, FixApplicationMessageBuilderSupport.FormatUtcTime(entry.EntryTimeUtc));
            message.Add(FixApplicationTags.GrossTradeAmt, FixApplicationMessageBuilderSupport.FormatDecimal(entry.GrossTradeAmount));
            message.Add(FixApplicationTags.TotalVolumeTraded, FixApplicationMessageBuilderSupport.FormatDecimal(entry.TotalVolumeTraded));
            message.Add(FixApplicationTags.TotalNumOfTrades, entry.TotalNumberOfTrades);
        }

        return message;
    }

    public static FixMessage BuildComposition(FixMarketTotalsCompositionDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        if (definition.Entries.Count == 0)
            throw new ArgumentException("MarketTotalsComposition requires at least one entry.", nameof(definition));

        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixApplicationMsgTypes.MarketTotalsComposition);
        message.Add(FixApplicationTags.TotNoRelatedSym, definition.Entries.Count);
        message.AddBoolean(FixApplicationTags.LastFragment, definition.LastFragment);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.IndexId, definition.IndexId);
        message.Add(FixApplicationTags.NoRelatedSym, definition.Entries.Count);

        foreach (FixMarketTotalsCompositionEntry entry in definition.Entries)
        {
            ArgumentException.ThrowIfNullOrEmpty(entry.Symbol);
            ArgumentException.ThrowIfNullOrEmpty(entry.SecurityDescription);
            message.Add(FixApplicationTags.Symbol, entry.Symbol);
            message.Add(FixApplicationTags.SecurityDescription, entry.SecurityDescription);

            if (entry.SecurityGroups.Count == 0)
                continue;

            message.Add(FixApplicationTags.NoSecurityGroups, entry.SecurityGroups.Count);
            foreach (string securityGroup in entry.SecurityGroups)
                FixApplicationMessageBuilderSupport.AddRequiredString(message, FixApplicationTags.SecurityGroup, securityGroup);
        }

        return message;
    }

    public static FixMessage BuildRequest(FixMarketTotalsRequestDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrEmpty(definition.MdReqId);

        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixApplicationMsgTypes.MarketTotalsRequest);
        message.Add(FixApplicationTags.MDReqID, definition.MdReqId);
        message.Add(FixApplicationTags.SubscriptionRequestType, definition.SubscriptionRequestType.ToString());
        return message;
    }

    public static FixMessage BuildResponse(FixMarketTotalsResponseDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrEmpty(definition.MdReqId);

        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixApplicationMsgTypes.MarketTotalsResponse);
        message.Add(FixApplicationTags.MDReqID, definition.MdReqId);
        FixApplicationMessageBuilderSupport.AddOptionalChar(message, FixApplicationTags.MdReqRejReason, definition.MdReqRejReason);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.Text, definition.Text);
        return message;
    }
}
