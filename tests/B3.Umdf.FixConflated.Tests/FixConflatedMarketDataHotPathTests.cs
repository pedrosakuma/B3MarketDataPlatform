using System.Diagnostics;
using B3.Umdf.Book;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

[Collection(nameof(AllocationSensitiveCollection))]
public sealed class FixConflatedMarketDataHotPathTests
{
    [Fact]
    public void HotPath_Callbacks_Allocate_Below_Threshold()
    {
        var clock = new FakeFixClock();
        var sink = new CapturingSink();
        using var publisher = CreatePublisher(clock, sink, startBackgroundWorker: false);
        OrderBook book = new(1234);
        OrderBookEntry entry = CreateEntry(book.SecurityId, 7001, BookSideType.Bid, 281000, 100);

        publisher.OnOrderAdded(book, in entry);
        publisher.OnTrade(book.SecurityId, 281050, 10, 9001, 0);
        publisher.FlushNow();

        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);

        long beforeBytes = GC.GetAllocatedBytesForCurrentThread();
        publisher.OnOrderAdded(book, in entry);
        publisher.OnTrade(book.SecurityId, 281050, 10, 9002, 0);
        long afterBytes = GC.GetAllocatedBytesForCurrentThread();

        long deltaBytes = afterBytes - beforeBytes;
        Assert.True(deltaBytes < 512,
            $"Fix hot-path callbacks allocated {deltaBytes} bytes for one order delta + one trade; threshold is 512 bytes total.");
    }

    [Fact]
    public void BlockedSink_DoesNot_Stall_HotPath_Enqueue()
    {
        var clock = new FakeFixClock();
        using var sink = new BlockingSink();
        using var publisher = CreatePublisher(clock, sink, startBackgroundWorker: true, pendingEventCapacity: 4096);
        OrderBook book = new(1234);
        OrderBookEntry entry = CreateEntry(book.SecurityId, 7001, BookSideType.Bid, 281000, 100);

        publisher.OnTrade(book.SecurityId, 281050, 10, 9001, 0);
        Assert.True(sink.FirstMessageStarted.Wait(TimeSpan.FromSeconds(1)));

        Stopwatch sw = Stopwatch.StartNew();
        for (int i = 0; i < 512; i++)
            publisher.OnOrderAdded(book, in entry);
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromMilliseconds(100),
            $"Hot-path enqueue took {sw.Elapsed.TotalMilliseconds:F1} ms while the FIX sink was blocked.");
        sink.ReleaseWriter.Set();
        publisher.FlushNow();
    }

    private static FixConflatedMarketDataPublisher CreatePublisher(
        FakeFixClock clock,
        IFixApplicationMessageSink sink,
        bool startBackgroundWorker,
        int pendingEventCapacity = 1024)
    {
        return new FixConflatedMarketDataPublisher(
            sink,
            new SequentialHeaderProvider(),
            new StaticInstrumentResolver(new FixMarketDataInstrument("PETR4", 1234, 4)),
            new FixConflatedMarketDataOptions
            {
                ConflationInterval = TimeSpan.FromMilliseconds(380),
                PendingEventCapacity = pendingEventCapacity,
                StartBackgroundWorker = startBackgroundWorker,
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

    private sealed class FakeFixClock : IFixClock
    {
        private DateTimeOffset _utcNow = new(2026, 8, 12, 19, 0, 0, TimeSpan.Zero);

        public DateTimeOffset UtcNow => _utcNow;
        public long MonotonicTicks => 0;
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

    private sealed class CapturingSink : IFixApplicationMessageSink
    {
        public void OnMessage(ReadOnlyMemory<byte> message)
        {
        }
    }

    private sealed class BlockingSink : IFixApplicationMessageSink, IDisposable
    {
        public ManualResetEventSlim FirstMessageStarted { get; } = new(false);
        public ManualResetEventSlim ReleaseWriter { get; } = new(false);

        public void OnMessage(ReadOnlyMemory<byte> message)
        {
            FirstMessageStarted.Set();
            ReleaseWriter.Wait(TimeSpan.FromSeconds(5));
        }

        public void Dispose()
        {
            FirstMessageStarted.Dispose();
            ReleaseWriter.Dispose();
        }
    }
}
