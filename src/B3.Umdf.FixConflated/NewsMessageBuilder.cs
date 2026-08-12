namespace B3.Umdf.FixConflated;

/// <summary>
/// Maps fully reassembled news payloads onto FIX 4.4 <c>News</c> messages.
/// </summary>
public static class NewsMessageBuilder
{
    public static FixMessage Build(FixNewsDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentException.ThrowIfNullOrEmpty(definition.Headline);
        ArgumentException.ThrowIfNullOrEmpty(definition.NewsSource);

        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixApplicationMsgTypes.News);
        message.Add(
            FixApplicationTags.OrigTime,
            FixValueFormatting.FormatUtcTimestamp(
                FixApplicationMessageBuilderSupport.FromUnixNanoseconds(definition.OrigTimeNanoseconds)));
        FixApplicationMessageBuilderSupport.AddOptionalChar(message, FixApplicationTags.Urgency, definition.Urgency);
        message.Add(FixApplicationTags.Headline, definition.Headline);
        message.Add(FixApplicationTags.NewsSource, definition.NewsSource);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.NewsId, definition.NewsId);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.LanguageCode, definition.LanguageCode);
        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.Language, definition.Language);

        if (definition.RelatedInstruments.Count > 0)
        {
            message.Add(FixApplicationTags.NoRelatedSym, definition.RelatedInstruments.Count);
            foreach (FixInstrumentReference instrument in definition.RelatedInstruments)
            {
                FixApplicationMessageBuilderSupport.AppendInstrumentPrefix(message, instrument);
                FixApplicationMessageBuilderSupport.AppendInstrumentSuffix(message, instrument);
            }
        }

        string[] lines = FixApplicationMessageBuilderSupport.SplitTextLines(definition.BodyText);
        message.Add(FixApplicationTags.NoLinesOfText, lines.Length);
        foreach (string line in lines)
            message.Add(FixApplicationTags.Text, line);

        FixApplicationMessageBuilderSupport.AddOptionalString(message, FixApplicationTags.UrlLink, definition.UrlLink);
        return message;
    }
}
