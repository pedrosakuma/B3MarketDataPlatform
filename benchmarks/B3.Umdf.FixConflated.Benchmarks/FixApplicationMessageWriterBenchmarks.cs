using BenchmarkDotNet.Attributes;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class FixApplicationMessageWriterBenchmarks
{
    [Params(1, 16, 64)]
    public int EntryCount;

    private FixApplicationMessageWriter _writer = null!;
    private FixApplicationSessionHeader _header;
    private FixMarketDataInstrument _instrument;
    private FixMarketDataIncrementalEntry[] _entries = null!;

    [GlobalSetup]
    public void Setup()
    {
        _writer = new FixApplicationMessageWriter(initialBufferSize: 16 * 1024);
        _header = new FixApplicationSessionHeader(
            SenderCompId: "SERVER",
            TargetCompId: "CLIENT",
            MsgSeqNum: 1,
            SendingTime: new DateTimeOffset(2026, 8, 13, 3, 30, 0, TimeSpan.Zero));
        _instrument = new FixMarketDataInstrument("PETR4", 1234, priceScale: 2);
        _entries = BuildEntries();
    }

    [Benchmark]
    public int WriteIncrementalRefresh()
        => _writer.WriteIncrementalRefresh(_header, _instrument, _entries).Length;

    [GlobalCleanup]
    public void Cleanup()
        => _writer.Dispose();

    private FixMarketDataIncrementalEntry[] BuildEntries()
    {
        var entries = new FixMarketDataIncrementalEntry[EntryCount];
        var liveOrders = new List<(ulong OrderId, FixMdEntryType EntryType)>(EntryCount);
        ulong nextOrderId = 10_000;

        for (int i = 0; i < entries.Length; i++)
        {
            DateTimeOffset entryTime = _header.SendingTime.AddMilliseconds(i);

            if (i % 8 == 7)
            {
                entries[i] = new FixMarketDataIncrementalEntry(
                    FixMdUpdateAction.New,
                    FixMdEntryType.Trade,
                    entryTime,
                    FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.TradeId,
                    Price: 2812 + (i % 5),
                    Size: 100 + i,
                    TradeId: 50_000 + i);
                continue;
            }

            if (i % 5 == 4 && liveOrders.Count > 0)
            {
                var live = liveOrders[i % liveOrders.Count];
                entries[i] = new FixMarketDataIncrementalEntry(
                    FixMdUpdateAction.Delete,
                    live.EntryType,
                    entryTime,
                    FixMarketDataEntryFields.OrderId,
                    OrderId: live.OrderId);
                continue;
            }

            if (i % 3 == 2 && liveOrders.Count > 0)
            {
                int liveIndex = i % liveOrders.Count;
                var live = liveOrders[liveIndex];
                entries[i] = new FixMarketDataIncrementalEntry(
                    FixMdUpdateAction.Change,
                    live.EntryType,
                    entryTime,
                    FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.OrderId,
                    Price: PickPrice(live.EntryType, i),
                    Size: 150 + i,
                    OrderId: live.OrderId);
                continue;
            }

            FixMdEntryType entryType = (i & 1) == 0 ? FixMdEntryType.Bid : FixMdEntryType.Offer;
            ulong orderId = nextOrderId++;
            liveOrders.Add((orderId, entryType));
            entries[i] = new FixMarketDataIncrementalEntry(
                FixMdUpdateAction.New,
                entryType,
                entryTime,
                FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.OrderId,
                Price: PickPrice(entryType, i),
                Size: 100 + i,
                OrderId: orderId);
        }

        return entries;
    }

    private static long PickPrice(FixMdEntryType entryType, int i)
    {
        int offset = i % 6;
        return entryType == FixMdEntryType.Bid
            ? 2810 - offset
            : 2812 + offset;
    }
}
