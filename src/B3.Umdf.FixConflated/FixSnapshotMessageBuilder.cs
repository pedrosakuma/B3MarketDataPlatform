using System.Globalization;
using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

internal static class FixSnapshotMessageBuilder
{
    public static FixMessage Build(FixMarketDataSnapshotRequest request, OrderBook book, DateTimeOffset entryTime)
    {
        ArgumentNullException.ThrowIfNull(book);

        int entryCount = book.Bids.OrderCount + book.Asks.OrderCount;
        var message = new FixMessage(5 + (entryCount * 6));
        message.Add(FixTags.MsgType, FixMsgTypes.MarketDataSnapshotFullRefresh);
        message.Add(FixTags.MDReqId, request.MdReqId);
        message.Add(FixTags.Symbol, request.Instrument.Symbol);
        message.Add(FixTags.SecurityId, request.Instrument.SecurityId.ToString(CultureInfo.InvariantCulture));

        message.Add(FixTags.NoMDEntries, entryCount);

        string entryDate = entryTime.UtcDateTime.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        string entryTimeValue = entryTime.UtcDateTime.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
        AppendSide(message, book.Bids, FixMdEntryType.Bid, request.Instrument.PriceScale, entryDate, entryTimeValue);
        AppendSide(message, book.Asks, FixMdEntryType.Offer, request.Instrument.PriceScale, entryDate, entryTimeValue);
        return message;
    }

    private static void AppendSide(
        FixMessage message,
        BookSide side,
        FixMdEntryType entryType,
        int priceScale,
        string entryDate,
        string entryTime)
    {
        foreach (KeyValuePair<long, List<OrderBookEntry>> level in side.PriceLevels)
        {
            foreach (OrderBookEntry order in level.Value)
            {
                message.Add(FixTags.MDEntryType, ((char)entryType).ToString());
                message.Add(FixTags.MDEntryPx, FormatScaledPrice(order.Price, priceScale));
                message.Add(FixTags.MDEntrySize, order.Quantity.ToString(CultureInfo.InvariantCulture));
                message.Add(FixTags.MDEntryDate, entryDate);
                message.Add(FixTags.MDEntryTime, entryTime);
                message.Add(FixTags.OrderId, order.OrderId.ToString(CultureInfo.InvariantCulture));
            }
        }
    }

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
