using B3.Umdf.Book;
using BenchmarkDotNet.Attributes;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class FixConflatedMarketDataPublisherBenchmarks
{
    [Params(8, 64, 256)]
    public int EventCount;

    private readonly OrderBook _book = new(1234);
    private readonly AdvancingFixClock _clock = new();
    private readonly CountingSink _sink = new();
    private FixConflatedMarketDataPublisher _publisher = null!;
    private PublisherOperation[] _operations = null!;

    [GlobalSetup]
    public void Setup()
    {
        _publisher = new FixConflatedMarketDataPublisher(
            _sink,
            new SequentialHeaderProvider(),
            new StaticInstrumentResolver(new FixMarketDataInstrument("PETR4", 1234, 2)),
            new FixConflatedMarketDataOptions
            {
                ConflationInterval = TimeSpan.FromMilliseconds(380),
                PendingEventCapacity = 8_192,
                StartBackgroundWorker = false,
            },
            _clock);
        _operations = BuildOperations();
    }

    [Benchmark]
    public int PublishConflatedWindow()
    {
        _sink.Reset();
        for (int i = 0; i < _operations.Length; i++)
        {
            ref readonly PublisherOperation operation = ref _operations[i];
            switch (operation.Kind)
            {
                case PublisherOperationKind.Add:
                {
                    OrderBookEntry entry = operation.Entry;
                    _publisher.OnOrderAdded(_book, in entry);
                    break;
                }
                case PublisherOperationKind.Update:
                {
                    OrderBookEntry entry = operation.Entry;
                    _publisher.OnOrderUpdated(_book, in entry);
                    break;
                }
                case PublisherOperationKind.Delete:
                    _publisher.OnOrderDeleted(_book, operation.OrderId, operation.Side);
                    break;
            }
        }

        _publisher.FlushNow();
        _clock.AdvanceMilliseconds(1);
        return _sink.TotalBytes;
    }

    [GlobalCleanup]
    public void Cleanup()
        => _publisher.Dispose();

    private PublisherOperation[] BuildOperations()
    {
        var operations = new PublisherOperation[EventCount];
        var rng = new Random(42);
        var liveBidOrders = new List<ulong>(EventCount);
        var liveAskOrders = new List<ulong>(EventCount);
        ulong nextOrderId = 20_000;

        for (int i = 0; i < operations.Length; i++)
        {
            bool isBid = (i & 1) == 0;
            BookSideType side = isBid ? BookSideType.Bid : BookSideType.Ask;
            List<ulong> liveOrders = isBid ? liveBidOrders : liveAskOrders;
            double action = rng.NextDouble();

            if (action < 0.30 && liveOrders.Count > 0)
            {
                int index = rng.Next(liveOrders.Count);
                ulong orderId = liveOrders[index];
                liveOrders[index] = liveOrders[^1];
                liveOrders.RemoveAt(liveOrders.Count - 1);
                operations[i] = new PublisherOperation(PublisherOperationKind.Delete, default, orderId, side);
                continue;
            }

            if (action < 0.60 && liveOrders.Count > 0)
            {
                ulong orderId = liveOrders[rng.Next(liveOrders.Count)];
                operations[i] = new PublisherOperation(
                    PublisherOperationKind.Update,
                    CreateEntry(orderId, side, PickPrice(side, i), 100 + (i % 50)),
                    orderId,
                    side);
                continue;
            }

            ulong next = nextOrderId++;
            liveOrders.Add(next);
            operations[i] = new PublisherOperation(
                PublisherOperationKind.Add,
                CreateEntry(next, side, PickPrice(side, i), 100 + (i % 50)),
                next,
                side);
        }

        return operations;
    }

    private static OrderBookEntry CreateEntry(ulong orderId, BookSideType side, long price, long quantity)
        => new()
        {
            SecurityId = 1234,
            OrderId = orderId,
            Side = side,
            Price = price,
            Quantity = quantity,
        };

    private static long PickPrice(BookSideType side, int i)
    {
        int offset = i % 8;
        return side == BookSideType.Bid
            ? 2810 - offset
            : 2812 + offset;
    }

    private readonly record struct PublisherOperation(
        PublisherOperationKind Kind,
        OrderBookEntry Entry,
        ulong OrderId,
        BookSideType Side);

    private enum PublisherOperationKind : byte
    {
        Add = 0,
        Update = 1,
        Delete = 2,
    }
}
