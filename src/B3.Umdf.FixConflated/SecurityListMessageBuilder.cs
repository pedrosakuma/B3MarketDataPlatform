namespace B3.Umdf.FixConflated;

/// <summary>
/// Builds <c>SecurityList</c> and <c>SecurityListRequest</c> messages from
/// small DTOs so transport wiring can stay decoupled from upstream registries.
/// </summary>
public static class SecurityListMessageBuilder
{
    public static FixMessage Build(FixSecurityListDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixApplicationMsgTypes.SecurityList);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.SecurityReqId, definition.SecurityReqId);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.SecurityResponseId, definition.SecurityResponseId);
        FixApplicationMessageBuilderSupport.AddOptionalChar(message, FixApplicationTags.SecurityRequestResult, definition.SecurityRequestResult);
        message.Add(FixApplicationTags.TotNoRelatedSym, definition.Securities.Count);
        message.AddBoolean(FixApplicationTags.LastFragment, definition.LastFragment);

        if (definition.Securities.Count > 0)
        {
            message.Add(FixApplicationTags.NoRelatedSym, definition.Securities.Count);
            foreach (FixSecurityListEntry security in definition.Securities)
            {
                ArgumentNullException.ThrowIfNull(security.Instrument);
                if (security.MarketDataFeeds.Count == 0)
                    throw new ArgumentException("SecurityList entries require at least one MD feed type.", nameof(definition));

                FixApplicationMessageBuilderSupport.AppendInstrumentPrefix(message, security.Instrument);
                message.Add(FixApplicationTags.NoMdFeedTypes, security.MarketDataFeeds.Count);
                foreach (FixMarketDataFeed feed in security.MarketDataFeeds)
                {
                    ArgumentException.ThrowIfNullOrEmpty(feed.MdFeedType);
                    message.Add(FixApplicationTags.MdFeedType, feed.MdFeedType);
                    message.Add(FixApplicationTags.MarketDepth, feed.MarketDepth);
                    message.Add(FixApplicationTags.MdBookType, feed.MdBookType);
                }

                FixApplicationMessageBuilderSupport.AppendInstrumentSuffix(message, security.Instrument);
            }
        }

        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.SecurityListId, definition.SecurityListId);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.SecurityListRefId, definition.SecurityListRefId);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.SecurityListDesc, definition.SecurityListDesc);
        FixApplicationMessageBuilderSupport.AddOptionalInt(message, FixApplicationTags.SecurityListType, definition.SecurityListType);
        FixApplicationMessageBuilderSupport.AddOptionalInt(message, FixApplicationTags.SecurityListTypeSource, definition.SecurityListTypeSource);
        FixApplicationMessageBuilderSupport.AddOptionalUtcTimestamp(message, FixApplicationTags.TransactTime, definition.TransactTimeUtc);
        return message;
    }

    public static FixMessage BuildRequest(FixSecurityListRequestDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrEmpty(definition.SecurityReqId);

        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixApplicationMsgTypes.SecurityListRequest);
        message.Add(FixApplicationTags.SecurityReqId, definition.SecurityReqId);
        message.Add(FixApplicationTags.SubscriptionRequestType, definition.SubscriptionRequestType.ToString());
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.SecurityType, definition.SecurityType);
        FixApplicationMessageBuilderSupport.AddOptionalInt(message, FixApplicationTags.Product, definition.Product);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.CfiCode, definition.CfiCode);
        FixApplicationMessageBuilderSupport.AddOptionalInt(message, FixApplicationTags.SecurityListRequestType, definition.SecurityListRequestType);
        FixApplicationMessageBuilderSupport.AddOptionalUtcTimestamp(message, FixApplicationTags.SecurityUpdatesSince, definition.SecurityUpdatesSinceUtc);
        return message;
    }
}
