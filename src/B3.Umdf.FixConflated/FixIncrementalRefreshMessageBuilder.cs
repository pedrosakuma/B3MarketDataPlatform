using System.Globalization;

namespace B3.Umdf.FixConflated;

internal static class FixIncrementalRefreshMessageBuilder
{
    public static FixMessage Build(
        FixMarketDataInstrument instrument,
        ReadOnlySpan<FixMarketDataIncrementalEntry> entries,
        string? mdReqId = null)
    {
        if (entries.IsEmpty)
            throw new ArgumentException("At least one incremental entry is required.", nameof(entries));

        var message = new FixMessage(EstimateFieldCapacity(entries.Length, mdReqId is not null));
        message.Add(FixTags.MsgType, FixMsgTypes.MarketDataIncrementalRefresh);
        if (!string.IsNullOrEmpty(mdReqId))
            message.Add(FixTags.MDReqId, mdReqId!);

        message.Add(FixTags.NoMDEntries, entries.Length);

        foreach (ref readonly var entry in entries)
        {
            message.Add(FixTags.MDUpdateAction, ((char)entry.UpdateAction).ToString());
            message.Add(FixTags.MDEntryType, ((char)entry.EntryType).ToString());
            message.Add(FixTags.Symbol, instrument.Symbol);
            message.Add(FixTags.SecurityId, instrument.SecurityId.ToString(CultureInfo.InvariantCulture));

            if ((entry.Fields & FixMarketDataEntryFields.Price) != 0)
                message.Add(FixTags.MDEntryPx, FormatScaledPrice(entry.Price, instrument.PriceScale));
            if ((entry.Fields & FixMarketDataEntryFields.Size) != 0)
                message.Add(FixTags.MDEntrySize, entry.Size.ToString(CultureInfo.InvariantCulture));

            message.Add(FixTags.MDEntryDate, FormatUtcDate(entry.EntryTime));
            message.Add(FixTags.MDEntryTime, FormatUtcTime(entry.EntryTime));

            if ((entry.Fields & FixMarketDataEntryFields.OrderId) != 0)
                message.Add(FixTags.OrderId, entry.OrderId.ToString(CultureInfo.InvariantCulture));
            if ((entry.Fields & FixMarketDataEntryFields.TradeId) != 0)
                message.Add(FixTags.TradeId, entry.TradeId.ToString(CultureInfo.InvariantCulture));
        }

        return message;
    }

    private static int EstimateFieldCapacity(int entryCount, bool hasMdReqId)
    {
        const int maxFieldsPerEntry = 10;
        return 2 + (hasMdReqId ? 1 : 0) + (entryCount * maxFieldsPerEntry);
    }

    private static string FormatUtcDate(DateTimeOffset value)
        => value.UtcDateTime.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

    private static string FormatUtcTime(DateTimeOffset value)
        => value.UtcDateTime.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);

    private static string FormatScaledPrice(long value, int scale)
    {
        if (scale <= 0)
            return value.ToString(CultureInfo.InvariantCulture);

        decimal divisor = 1m;
        for (int i = 0; i < scale; i++)
            divisor *= 10m;

        decimal scaled = value / divisor;
        return scaled.ToString($"F{scale}", CultureInfo.InvariantCulture);
    }
}
