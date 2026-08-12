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

        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.SecurityStatusReqId, definition.SecurityStatusReqId);
        FixApplicationMessageBuilderSupport.AppendInstrumentPrefix(message, definition.Instrument);
        FixApplicationMessageBuilderSupport.AppendInstrumentSuffix(message, definition.Instrument);
        FixApplicationMessageBuilderSupport.AddOptionalBoolean(message, FixApplicationTags.UnsolicitedIndicator, definition.UnsolicitedIndicator);
        FixApplicationMessageBuilderSupport.AddOptionalUnixNanosTimestamp(message, FixApplicationTags.TradSesOpenTime, definition.TradSesOpenTimeNanoseconds);
        message.Add(FixApplicationTags.SecurityTradingStatus, definition.SecurityTradingStatus);

        DateOnly tradeDate = definition.TradeDate
            ?? DateOnly.FromDateTime(FixApplicationMessageBuilderSupport.FromUnixNanoseconds(definition.SourceTimestampNanoseconds).UtcDateTime);
        message.Add(FixApplicationTags.TradeDate, FixApplicationMessageBuilderSupport.FormatLocalDate(tradeDate));

        FixApplicationMessageBuilderSupport.AddOptionalChar(message, FixApplicationTags.HaltReason, definition.HaltReason);
        FixApplicationMessageBuilderSupport.AddOptionalDecimal(message, FixApplicationTags.BuyVolume, definition.BuyVolume);
        FixApplicationMessageBuilderSupport.AddOptionalDecimal(message, FixApplicationTags.SellVolume, definition.SellVolume);
        FixApplicationMessageBuilderSupport.AddOptionalDecimal(message, FixApplicationTags.HighPrice, definition.HighPrice);
        FixApplicationMessageBuilderSupport.AddOptionalDecimal(message, FixApplicationTags.LowPrice, definition.LowPrice);
        FixApplicationMessageBuilderSupport.AddOptionalDecimal(message, FixApplicationTags.LastPx, definition.LastPrice);
        message.Add(
            FixApplicationTags.TransactTime,
            FixValueFormatting.FormatUtcTimestamp(
                FixApplicationMessageBuilderSupport.FromUnixNanoseconds(definition.SourceTimestampNanoseconds)));
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.Text, definition.Text);
        FixApplicationMessageBuilderSupport.AddOptionalInt(message, FixApplicationTags.SecurityTradingEvent, definition.SecurityTradingEvent);

        return message;
    }
}
