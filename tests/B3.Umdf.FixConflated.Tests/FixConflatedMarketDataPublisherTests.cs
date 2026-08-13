using B3.Umdf.Book;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixConflatedMarketDataPublisherTests
{
    [Fact]
    public void ConflationWindow_Batches_Multiple_Deltas_Into_One_Message()
    {
        var clock = new FakeFixClock();
        var sink = new CapturingSink();
        var publisher = CreatePublisher(clock, sink);
        OrderBook book = new(1234);

        var add = CreateEntry(book.SecurityId, 7001, BookSideType.Bid, 2810, 100);
        var update = CreateEntry(book.SecurityId, 7001, BookSideType.Bid, 2811, 90);

        publisher.OnOrderAdded(book, in add);
        publisher.OnOrderUpdated(book, in update);
        publisher.FlushIfDue();
        Assert.Empty(sink.Messages);

        clock.AdvanceMilliseconds(400);
        publisher.FlushIfDue();

        byte[] raw = Assert.Single(sink.Messages);
        FixMessage decoded = Decode(raw);
        Assert.Equal("1", GetRequired(decoded, FixTags.MsgSeqNum));

        IReadOnlyList<IReadOnlyDictionary<int, string>> entries = ParseIncrementalEntries(decoded);
        Assert.Equal(2, entries.Count);
        Assert.Equal("0", entries[0][FixTags.MDUpdateAction]);
        Assert.Equal("28.10", entries[0][FixTags.MDEntryPx]);
        Assert.Equal("1", entries[1][FixTags.MDUpdateAction]);
        Assert.Equal("28.11", entries[1][FixTags.MDEntryPx]);
    }

    [Fact]
    public void ConflationWindow_Splits_Deltas_Across_Window_Boundaries()
    {
        var clock = new FakeFixClock();
        var sink = new CapturingSink();
        var publisher = CreatePublisher(clock, sink);
        OrderBook book = new(1234);

        var first = CreateEntry(book.SecurityId, 8001, BookSideType.Bid, 2810, 100);
        publisher.OnOrderAdded(book, in first);
        publisher.FlushIfDue();

        clock.AdvanceMilliseconds(400);
        publisher.FlushIfDue();

        publisher.OnOrderDeleted(book, 8001, BookSideType.Bid);
        publisher.FlushIfDue();

        clock.AdvanceMilliseconds(400);
        publisher.FlushIfDue();

        Assert.Equal(2, sink.Messages.Count);

        IReadOnlyList<IReadOnlyDictionary<int, string>> firstEntries = ParseIncrementalEntries(Decode(sink.Messages[0]));
        IReadOnlyList<IReadOnlyDictionary<int, string>> secondEntries = ParseIncrementalEntries(Decode(sink.Messages[1]));

        Assert.Single(firstEntries);
        Assert.Single(secondEntries);
        Assert.Equal("0", firstEntries[0][FixTags.MDUpdateAction]);
        Assert.Equal("2", secondEntries[0][FixTags.MDUpdateAction]);
    }

    [Fact]
    public void Trades_Bypass_Conflation_And_Flush_Immediately()
    {
        var clock = new FakeFixClock();
        var sink = new CapturingSink();
        var publisher = CreatePublisher(clock, sink);
        OrderBook book = new(1234);

        var add = CreateEntry(book.SecurityId, 9001, BookSideType.Bid, 2810, 100);
        publisher.OnOrderAdded(book, in add);
        publisher.FlushIfDue();
        Assert.Empty(sink.Messages);

        var tradeTime = new DateTimeOffset(2026, 8, 12, 19, 20, 30, 555, TimeSpan.Zero);
        publisher.OnTrade(1234, 2812, 250, 771122, tradeTime.ToUnixTimeMilliseconds() * 1_000_000);
        publisher.FlushIfDue();

        Assert.Single(sink.Messages);
        FixMessage tradeMessage = Decode(sink.Messages[0]);
        IReadOnlyList<IReadOnlyDictionary<int, string>> tradeEntries = ParseIncrementalEntries(tradeMessage);
        Assert.Single(tradeEntries);
        Assert.Equal("2", tradeEntries[0][FixTags.MDEntryType]);
        Assert.Equal("28.12", tradeEntries[0][FixTags.MDEntryPx]);
        Assert.Equal("250", tradeEntries[0][FixTags.MDEntrySize]);
        Assert.Equal("771122", tradeEntries[0][FixTags.TradeId]);

        clock.AdvanceMilliseconds(400);
        publisher.FlushIfDue();

        Assert.Equal(2, sink.Messages.Count);
        FixMessage bookMessage = Decode(sink.Messages[1]);
        IReadOnlyList<IReadOnlyDictionary<int, string>> bookEntries = ParseIncrementalEntries(bookMessage);
        Assert.Single(bookEntries);
        Assert.Equal("0", bookEntries[0][FixTags.MDEntryType]);
    }

    [Fact]
    public void BookClear_Maps_To_DeleteThru()
    {
        var clock = new FakeFixClock();
        var sink = new CapturingSink();
        var publisher = CreatePublisher(clock, sink);

        publisher.OnBookCleared(1234, BookClearSide.Bid);
        publisher.FlushIfDue();
        Assert.Empty(sink.Messages);

        clock.AdvanceMilliseconds(400);
        publisher.FlushIfDue();

        byte[] raw = Assert.Single(sink.Messages);
        FixMessage decoded = Decode(raw);
        IReadOnlyList<IReadOnlyDictionary<int, string>> entries = ParseIncrementalEntries(decoded);
        Assert.Single(entries);
        Assert.Equal("3", entries[0][FixTags.MDUpdateAction]);
        Assert.Equal("0", entries[0][FixTags.MDEntryType]);
        Assert.False(entries[0].ContainsKey(FixTags.OrderId));
        Assert.False(entries[0].ContainsKey(FixTags.MDEntryPx));
    }

    [Fact]
    public void DirectBroadcastSink_Bypasses_EncodeDecode_RoundTrip()
    {
        var clock = new FakeFixClock();
        var sink = new DirectCapturingSink();
        var publisher = CreatePublisher(clock, sink);
        OrderBook book = new(1234);

        var add = CreateEntry(book.SecurityId, 7001, BookSideType.Bid, 2810, 100);
        var update = CreateEntry(book.SecurityId, 7001, BookSideType.Bid, 2811, 90);

        publisher.OnOrderAdded(book, in add);
        publisher.OnOrderUpdated(book, in update);
        clock.AdvanceMilliseconds(400);
        publisher.FlushIfDue();

        Assert.Empty(sink.Messages);
        FixMessage message = Assert.Single(sink.ApplicationMessages);
        Assert.Equal(FixMsgTypes.MarketDataIncrementalRefresh, GetRequired(message, FixTags.MsgType));

        IReadOnlyList<IReadOnlyDictionary<int, string>> entries = ParseIncrementalEntries(message);
        Assert.Equal(2, entries.Count);
        Assert.Equal("0", entries[0][FixTags.MDUpdateAction]);
        Assert.Equal("28.10", entries[0][FixTags.MDEntryPx]);
        Assert.Equal("1", entries[1][FixTags.MDUpdateAction]);
        Assert.Equal("28.11", entries[1][FixTags.MDEntryPx]);
    }

    private static FixConflatedMarketDataPublisher CreatePublisher(FakeFixClock clock, CapturingSink sink)
    {
        return new FixConflatedMarketDataPublisher(
            sink,
            new SequentialHeaderProvider(),
            new StaticInstrumentResolver(new FixMarketDataInstrument("PETR4", 1234, 2)),
            new FixConflatedMarketDataOptions
            {
                ConflationInterval = TimeSpan.FromMilliseconds(380),
                StartBackgroundWorker = false,
            },
            clock);
    }

    private static OrderBookEntry CreateEntry(ulong securityId, ulong orderId, BookSideType side, long price, long quantity)
    {
        return new OrderBookEntry
        {
            SecurityId = securityId,
            OrderId = orderId,
            Side = side,
            Price = price,
            Quantity = quantity,
        };
    }

    private static FixMessage Decode(byte[] raw)
    {
        FixDecodeResult decoded = FixMessageCodec.Decode(raw);
        Assert.True(decoded.Success, decoded.Error.ToString());
        return decoded.Message!;
    }

    private static IReadOnlyList<IReadOnlyDictionary<int, string>> ParseIncrementalEntries(FixMessage message)
    {
        List<IReadOnlyDictionary<int, string>> entries = [];
        Dictionary<int, string>? current = null;
        bool insideGroup = false;

        foreach (FixField field in message.Fields)
        {
            if (field.Tag == FixTags.NoMDEntries)
            {
                insideGroup = true;
                continue;
            }

            if (!insideGroup || field.Tag == FixTags.CheckSum)
                continue;

            if (field.Tag == FixTags.MDUpdateAction)
            {
                current = [];
                entries.Add(current);
            }

            Assert.NotNull(current);
            current[field.Tag] = field.Value;
        }

        return entries;
    }

    private static string GetRequired(FixMessage message, int tag)
    {
        Assert.True(message.TryGetString(tag, out string? value));
        return value!;
    }

    private sealed class FakeFixClock : IFixClock
    {
        private DateTimeOffset _utcNow = new(2026, 8, 12, 19, 0, 0, TimeSpan.Zero);
        private long _monotonicTicks;

        public DateTimeOffset UtcNow => _utcNow;
        public long MonotonicTicks => _monotonicTicks;

        public void AdvanceMilliseconds(int milliseconds)
        {
            _utcNow = _utcNow.AddMilliseconds(milliseconds);
            _monotonicTicks += milliseconds;
        }
    }

    private sealed class SequentialHeaderProvider : IFixApplicationHeaderProvider
    {
        private int _nextSequenceNumber = 1;

        public FixApplicationSessionHeader NextHeader(DateTimeOffset sendingTime)
            => new("SERVER", "CLIENT", _nextSequenceNumber++, sendingTime);
    }

    private sealed class StaticInstrumentResolver : IFixMarketDataInstrumentResolver
    {
        private readonly FixMarketDataInstrument _instrument;

        public StaticInstrumentResolver(FixMarketDataInstrument instrument)
        {
            _instrument = instrument;
        }

        public bool TryResolve(ulong securityId, out FixMarketDataInstrument instrument)
        {
            if (_instrument.SecurityId == securityId)
            {
                instrument = _instrument;
                return true;
            }

            instrument = default;
            return false;
        }
    }

    private class CapturingSink : IFixApplicationMessageSink
    {
        public List<byte[]> Messages { get; } = [];

        public void OnMessage(ReadOnlyMemory<byte> message)
            => Messages.Add(message.ToArray());
    }

    private sealed class DirectCapturingSink : CapturingSink, IFixApplicationBroadcastSink
    {
        public List<FixMessage> ApplicationMessages { get; } = [];

        public void OnApplicationMessage(FixMessage message)
            => ApplicationMessages.Add(message.Clone());
    }
}
