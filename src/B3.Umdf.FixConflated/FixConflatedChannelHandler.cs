using System.Globalization;
using System.Text;
using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

public sealed class FixConflatedChannelHandler : IBookEventHandler, IMarketDataEventHandler, IDisposable, IFixConflatedQueueMetricsSource
{
    private readonly FixConflatedSessionHub _hub;
    private readonly IFixClock _clock;
    private readonly FixConflatedMarketDataPublisher _publisher;

    public FixConflatedChannelHandler(
        int groupId,
        FixConflatedSessionHub hub,
        IFixMarketDataInstrumentResolver instrumentResolver,
        FixConflatedMarketDataOptions? options = null,
        IFixClock? clock = null)
    {
        GroupId = groupId;
        _hub = hub ?? throw new ArgumentNullException(nameof(hub));
        _clock = clock ?? SystemFixClock.Instance;
        _publisher = new FixConflatedMarketDataPublisher(
            hub,
            new SyntheticHeaderProvider(),
            instrumentResolver ?? throw new ArgumentNullException(nameof(instrumentResolver)),
            options,
            _clock);
        FixConflatedMetrics.RegisterGroup(groupId, this);
    }

    public int GroupId { get; }
    public int PendingQueueDepth => _publisher.PendingEventCount;
    public long DroppedQueueEntries => _publisher.DroppedEvents;

    public void Dispose()
    {
        FixConflatedMetrics.UnregisterGroup(GroupId);
        _publisher.Dispose();
    }

    public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) => _publisher.OnOrderAdded(book, in entry);
    public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) => _publisher.OnOrderUpdated(book, in entry);
    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) => _publisher.OnOrderDeleted(book, orderId, side);
    public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) => _publisher.OnTrade(securityId, price, quantity, tradeId, sendingTimeNs);
    public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs, TradeFlags flags) => _publisher.OnTrade(securityId, price, quantity, tradeId, sendingTimeNs, flags);
    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) => _publisher.OnForwardTrade(securityId, price, quantity, tradeId, sendingTimeNs);
    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs, TradeFlags flags) => _publisher.OnForwardTrade(securityId, price, quantity, tradeId, sendingTimeNs, flags);
    public void OnBookCleared(ulong securityId, BookClearSide side) => _publisher.OnBookCleared(securityId, side);
    public void OnBatchComplete()
    {
    }

    public void FlushIfDue()
    {
    }

    public void FlushNow() => _publisher.FlushNow();
    public void OnEpochReset(SnapshotClearReason reason) => _publisher.OnEpochReset(reason);

    public void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info)
    {
        if (info.TradingStatus is not int tradingStatus || !TryCreateInstrumentReference(securityId, info, out FixInstrumentReference? instrument))
            return;

        _hub.BroadcastApplication(new FixApplicationDispatch(
            SecurityStatusMessageBuilder.Build(new FixSecurityStatusDefinition
        {
            Instrument = instrument!,
            SecurityTradingStatus = tradingStatus,
            SourceTimestampNanoseconds = GetTimestampNanoseconds(info.LastUpdateTimestamp),
            TradingSessionId = "1",
            TradingSessionSubId = tradingStatus.ToString(CultureInfo.InvariantCulture),
            TradeDate = info.LastUpdateTimestamp == 0
                ? DateOnly.FromDateTime(_clock.UtcNow.UtcDateTime)
                : DateOnly.FromDateTime(DateTimeOffset.FromUnixTimeMilliseconds((long)info.LastUpdateTimestamp / 1_000_000).UtcDateTime),
        }), securityId));
    }

    public void OnNews(
        ulong securityIdOrZero,
        ulong newsId,
        byte source,
        ushort language,
        long origTimeNanos,
        ReadOnlySpan<byte> headline,
        ReadOnlySpan<byte> text,
        ReadOnlySpan<byte> url)
    {
        _hub.BroadcastApplication(NewsMessageBuilder.Build(new FixNewsDefinition
        {
            OrigTimeNanoseconds = origTimeNanos > 0 ? origTimeNanos : GetTimestampNanoseconds(0),
            Headline = DecodeUtf8(headline),
            BodyText = DecodeUtf8(text),
            NewsId = newsId == 0 ? null : newsId.ToString(CultureInfo.InvariantCulture),
            LanguageCode = language == 0 ? null : language.ToString(CultureInfo.InvariantCulture),
            UrlLink = url.IsEmpty ? null : DecodeUtf8(url),
            NewsSourceCode = source == 0 ? "17" : source.ToString(CultureInfo.InvariantCulture),
        }));
    }

    public void OnInstrumentStatusChanged(ulong securityId, InstrumentInfo info, in InstrumentStatusUpdate update)
    {
    }

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
    }

    public void OnSecurityDefinitionChanged(ulong securityId, InstrumentInfo info)
    {
    }

    public void OnPriceBandChanged(ulong securityId, InstrumentInfo info)
    {
    }

    public void OnAuctionChanged(ulong securityId, InstrumentInfo info)
    {
    }

    public void OnInstrumentReplaced(ulong securityId, string? oldSymbol, string newSymbol)
    {
    }

    private bool TryCreateInstrumentReference(ulong securityId, InstrumentInfo? info, out FixInstrumentReference? instrument)
    {
        string? symbol = info?.Symbol;
        string? securityGroup = info?.SecurityGroup;
        string? cfiCode = info?.CfiCode;
        int? putOrCall = info?.PutOrCall;
        int? product = info?.Product;
        string? securityDescription = info?.SecurityDescription;
        DateOnly? maturityDate = TryParseDateOnly(info?.MaturityDate);

        if (string.IsNullOrWhiteSpace(symbol) && info is null)
        {
            instrument = null;
            return false;
        }

        instrument = new FixInstrumentReference
        {
            Symbol = symbol,
            SecurityId = securityId.ToString(CultureInfo.InvariantCulture),
            SecurityGroup = securityGroup,
            CfiCode = cfiCode,
            PutOrCall = putOrCall,
            Product = product,
            MaturityDate = maturityDate,
            SecurityDescription = securityDescription,
        };
        return true;
    }

    private static DateOnly? TryParseDateOnly(int? yyyymmdd)
    {
        if (yyyymmdd is not int value || value <= 0)
            return null;
        if (!DateOnly.TryParseExact(value.ToString(CultureInfo.InvariantCulture), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateOnly parsed))
            return null;
        return parsed;
    }

    private long GetTimestampNanoseconds(ulong lastUpdateTimestamp)
    {
        if (lastUpdateTimestamp > 0)
            return checked((long)lastUpdateTimestamp);
        return checked(_clock.UtcNow.ToUnixTimeMilliseconds() * 1_000_000);
    }

    private static string DecodeUtf8(ReadOnlySpan<byte> bytes)
        => bytes.IsEmpty ? string.Empty : Encoding.UTF8.GetString(bytes);

    private sealed class SyntheticHeaderProvider : IFixApplicationHeaderProvider
    {
        private int _nextSequenceNumber;

        public FixApplicationSessionHeader NextHeader(DateTimeOffset sendingTime)
            => new("UMDF-SANDBOX", "FIX-CLIENT", Interlocked.Increment(ref _nextSequenceNumber), sendingTime);
    }
}
