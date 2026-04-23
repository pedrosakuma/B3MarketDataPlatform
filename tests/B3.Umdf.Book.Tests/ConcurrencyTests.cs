namespace B3.Umdf.Book.Tests;

using Microsoft.Extensions.Logging.Abstractions;

public class ConcurrencyTests
{
    [Fact]
    public async Task ConcurrentDictionary_EnumerateDuringWrite_NoException()
    {
        // Verifies that ConcurrentDictionary-based Books can be safely
        // enumerated from one thread while the feed thread adds entries.
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bookManager = new BookManager(stateRegistry: reg, staleBuffer: buf);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        // Writer thread: continuously add books
        var writer = Task.Run(() =>
        {
            ulong id = 1;
            while (!cts.Token.IsCancellationRequested)
            {
                bookManager.GetOrCreateBook(id++);
                if (id > 100_000) id = 1;
            }
        });

        // Reader threads: continuously enumerate
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                int count = 0;
                foreach (var _ in bookManager.Books)
                    count++;
                // Just ensure no InvalidOperationException
            }
        })).ToArray();

        await Task.WhenAll([writer, .. readers]);
    }

    [Fact]
    public async Task OrderBook_ConcurrentReadWrite_NoCorruption()
    {
        // Verifies that reading book state while orders are added doesn't crash.
        var book = new OrderBook(42);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var writer = Task.Run(() =>
        {
            long id = 1;
            while (!cts.Token.IsCancellationRequested)
            {
                var entry = new OrderBookEntry
                {
                    OrderId = (ulong)id,
                    Price = 1000 + (id % 100),
                    Quantity = id * 10,
                    SecurityId = 42,
                    Side = id % 2 == 0 ? BookSideType.Bid : BookSideType.Ask
                };
                book.GetSide(entry.Side).Add(entry);
                id++;
                if (id > 10_000)
                {
                    book.Clear();
                    id = 1;
                }
            }
        });

        var reader = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var bidCount = book.Bids.Orders.Count;
                var askCount = book.Asks.Orders.Count;
                _ = book.LastRptSeq;
            }
        });

        await Task.WhenAll(writer, reader);
    }

    [Fact]
    public async Task MarketDataManager_ConcurrentAccess_NoException()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var mdm = new MarketDataManager(stateRegistry: reg);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        var writer = Task.Run(() =>
        {
            ulong id = 1;
            while (!cts.Token.IsCancellationRequested)
            {
                var info = mdm.GetOrCreateInfo(id);
                info.LastTradePrice = (long)id * 100;
                id++;
                if (id > 50_000) id = 1;
            }
        });

        var reader = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                int count = 0;
                foreach (var kv in mdm.InstrumentData)
                {
                    _ = kv.Value.LastTradePrice;
                    count++;
                }
            }
        });

        await Task.WhenAll(writer, reader);
    }
}
