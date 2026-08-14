namespace B3.Umdf.FixConflated;

/// <summary>
/// Builds low-frequency <c>SecurityStatus</c> application messages using the
/// existing session-plane <see cref="FixMessage"/> model.
/// </summary>
public static class SecurityStatusMessageBuilder
{
    public static FixMessage Build(FixSecurityStatusDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(definition.Instrument);

        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixApplicationMsgTypes.SecurityStatus);

        FixApplicationMessageBuilderSupport.AppendInstrumentPrefix(message, definition.Instrument);

        DateOnly tradeDate = definition.TradeDate
            ?? DateOnly.FromDateTime(FixApplicationMessageBuilderSupport.FromUnixNanoseconds(definition.SourceTimestampNanoseconds).UtcDateTime);
        message.Add(
            FixApplicationTags.TransactTime,
            FixValueFormatting.FormatUtcTimestamp(
                FixApplicationMessageBuilderSupport.FromUnixNanoseconds(definition.SourceTimestampNanoseconds)));
        message.Add(FixApplicationTags.TradeDate, FixApplicationMessageBuilderSupport.FormatLocalDate(tradeDate));
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.TradingSessionId, definition.TradingSessionId);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.TradingSessionSubId, definition.TradingSessionSubId);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.SecurityGroup, definition.Instrument.SecurityGroup);

        return message;
    }
}
