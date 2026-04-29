using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using B3.Umdf.Book;
using B3.Umdf.Book.Benchmarks;

if (args.Length > 0 && args[0] == "alloc-probe")
{
    OnPacketAllocProbe.Run();
    return;
}

// Pass the benchmark class name (or `*`) on the CLI to pick which suite to run, e.g.
//   dotnet run -c Release -- --filter '*BookManagerOnPacketBenchmarks*'
//   dotnet run -c Release -- --filter '*BookSideBenchmarks*'
//   dotnet run -c Release -- alloc-probe        (one-shot allocation probe, not BDN)
BenchmarkSwitcher.FromAssembly(typeof(BookSideBenchmarks).Assembly).Run(args);

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class BookSideBenchmarks
{
    // Simulates realistic MBO order flow:
    // - Concentrated activity near best price (top of book)
    // - Mix of adds, updates, and deletes
    // - Multiple price levels with varying depth

    private (ulong OrderId, long Price, long Qty, bool IsDelete)[] _operations = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int OperationCount;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);
        _operations = new (ulong, long, long, bool)[OperationCount];

        // Generate realistic order flow:
        // - Prices concentrated around 1000 (±50 ticks, but 80% within ±10)
        // - 40% adds, 30% updates (same orderId), 30% deletes
        ulong nextOrderId = 1;
        var liveOrders = new List<ulong>();

        for (int i = 0; i < OperationCount; i++)
        {
            double r = rng.NextDouble();

            if (r < 0.30 && liveOrders.Count > 0) // 30% delete
            {
                int idx = rng.Next(liveOrders.Count);
                ulong orderId = liveOrders[idx];
                liveOrders[idx] = liveOrders[^1];
                liveOrders.RemoveAt(liveOrders.Count - 1);
                _operations[i] = (orderId, 0, 0, true);
            }
            else if (r < 0.60 && liveOrders.Count > 0) // 30% update
            {
                ulong orderId = liveOrders[rng.Next(liveOrders.Count)];
                long price = GeneratePrice(rng);
                _operations[i] = (orderId, price, rng.Next(1, 1000), false);
            }
            else // 40% add
            {
                ulong orderId = nextOrderId++;
                long price = GeneratePrice(rng);
                liveOrders.Add(orderId);
                _operations[i] = (orderId, price, rng.Next(1, 1000), false);
            }
        }
    }

    private static long GeneratePrice(Random rng)
    {
        // 80% of activity within ±10 ticks of center (top of book)
        // 20% across wider range (±50 ticks)
        double r = rng.NextDouble();
        int offset = r < 0.8
            ? rng.Next(-10, 11)
            : rng.Next(-50, 51);
        return 1000 + offset;
    }

    [Benchmark(Baseline = true)]
    public long Current_SortedDictionary()
    {
        var side = new BookSide(BookSideType.Bid);
        long bboAccumulator = 0;

        foreach (var (orderId, price, qty, isDel) in _operations)
        {
            if (isDel)
                side.Remove(orderId);
            else
                side.AddOrUpdate(new OrderBookEntry { OrderId = orderId, Price = price, Quantity = qty });

            // Mirror BookManager.CheckCrossing: top-of-book is read on every mutation.
            // The accumulator prevents the JIT from eliding BestPrice() as dead code.
            var best = side.BestPrice();
            if (best.HasValue)
                bboAccumulator += best.Value.Price + best.Value.TotalQty;
        }

        return bboAccumulator;
    }

    [Benchmark]
    public long InvertedList_SwapRemove()
    {
        var side = new InvertedBookSide(BookSideType.Bid);
        long bboAccumulator = 0;

        foreach (var (orderId, price, qty, isDel) in _operations)
        {
            if (isDel)
                side.Remove(orderId);
            else
                side.AddOrUpdate(new InvertedOrderEntry { OrderId = orderId, Price = price, Quantity = qty });

            var best = side.BestPrice();
            if (best.HasValue)
                bboAccumulator += best.Value.Price + best.Value.TotalQty;
        }

        return bboAccumulator;
    }
}

// ─── Inverted Sorted List + Swap-Remove implementation ───

public sealed class InvertedOrderEntry
{
    public ulong OrderId;
    public long Price;
    public long Quantity;
    internal int LevelOrderIndex; // for O(1) swap-remove
}

public sealed class InvertedBookSide
{
    private readonly Dictionary<ulong, InvertedOrderEntry> _orders = new();

    // Sorted with BEST at end.
    // Bids: ascending (lowest→highest, best=highest at end)
    // Asks: descending (highest→lowest, best=lowest at end)
    private readonly List<(long Price, List<InvertedOrderEntry> Orders)> _levels = new();
    private readonly bool _ascending;

    public InvertedBookSide(BookSideType side)
    {
        _ascending = side == BookSideType.Bid;
    }

    public void AddOrUpdate(InvertedOrderEntry entry)
    {
        if (_orders.TryGetValue(entry.OrderId, out var existing))
            RemoveFromLevels(existing);

        _orders[entry.OrderId] = entry;
        AddToLevels(entry);
    }

    public bool Remove(ulong orderId)
    {
        if (!_orders.Remove(orderId, out var entry))
            return false;
        RemoveFromLevels(entry);
        return true;
    }

    public (long Price, long TotalQty)? BestPrice()
    {
        if (_levels.Count == 0) return null;
        var best = _levels[^1];
        long totalQty = 0;
        foreach (var o in best.Orders)
            totalQty += o.Quantity;
        return (best.Price, totalQty);
    }

    private void AddToLevels(InvertedOrderEntry entry)
    {
        int idx = BinarySearchPrice(entry.Price);
        if (idx >= 0)
        {
            var orders = _levels[idx].Orders;
            entry.LevelOrderIndex = orders.Count;
            orders.Add(entry);
        }
        else
        {
            int insertIdx = ~idx;
            var orders = new List<InvertedOrderEntry> { entry };
            entry.LevelOrderIndex = 0;
            _levels.Insert(insertIdx, (entry.Price, orders));
        }
    }

    private void RemoveFromLevels(InvertedOrderEntry entry)
    {
        int levelIdx = BinarySearchPrice(entry.Price);
        if (levelIdx < 0) return;

        var orders = _levels[levelIdx].Orders;
        int orderIdx = entry.LevelOrderIndex;

        // Swap-remove: O(1)
        int lastIdx = orders.Count - 1;
        if (orderIdx < lastIdx)
        {
            orders[orderIdx] = orders[lastIdx];
            orders[orderIdx].LevelOrderIndex = orderIdx;
        }
        orders.RemoveAt(lastIdx);

        if (orders.Count == 0)
            _levels.RemoveAt(levelIdx);
    }

    private int BinarySearchPrice(long price)
    {
        int lo = 0, hi = _levels.Count - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cmp = _ascending
                ? _levels[mid].Price.CompareTo(price)
                : price.CompareTo(_levels[mid].Price);
            if (cmp == 0) return mid;
            if (cmp < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return ~lo;
    }
}
