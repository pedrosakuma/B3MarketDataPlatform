using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Benchmarks;

/// <summary>
/// MBP wire-bandwidth comparison vs MBO on the same realistic hot-symbol
/// trace used by <see cref="HotSymbolOnPacketBenchmarks"/>. Drives 100k MBO
/// SBE messages through the full <see cref="BookManager"/> dispatch path with
/// a counting <see cref="IBookEventHandler"/> attached, then reports total
/// bytes that would be emitted on each path.
///
/// MBO byte cost per event:
///   OrderAdded/OrderUpdated → 37 bytes; OrderDeleted → 21 bytes.
/// MBP byte cost per <em>level touched per conflation window</em>:
///   LevelUpdate → 33 bytes; LevelDeleted → 21 bytes.
///
/// The <c>BatchSize</c> parameter controls how many MBO events accumulate
/// between <c>OnPacketProcessed</c> calls — i.e., the conflation window. Larger
/// windows mean more touches per (side, price) collapse into a single MBP
/// frame, so MBP savings grow.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class MbpBandwidthBenchmarks
{
    [Params(100_000)]
    public int MessageCount;

    /// <summary>Conflation window: MBO events between OnPacketProcessed calls.</summary>
    [Params(1, 16, 64, 256)]
    public int BatchSize;

    private BookManager _bookManager = null!;
    private BandwidthCounter _counter = null!;
    private byte[][] _sbeSlices = null!;
    private ushort[] _templateIds = null!;
    private UmdfPacket _emptyPacket;

    [GlobalSetup]
    public void Setup()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        _counter = new BandwidthCounter();
        _bookManager = new BookManager(eventHandler: _counter,
            stateRegistry: registry, staleBuffer: staleBuffer);

        const ulong securityId = 1;
        registry.EnsureRegistered(securityId);
        registry.HealFromSnapshot(securityId, SymbolGapKind.Mbo, snapshotRptSeq: 0);
        _bookManager.FreezeBooks();

        _sbeSlices = new byte[MessageCount][];
        _templateIds = new ushort[MessageCount];

        var rng = new Random(Seed: 42);
        var liveBidOrders = new List<ulong>(capacity: 4096);
        var liveAskOrders = new List<ulong>(capacity: 4096);
        ulong nextOrderId = 1;
        uint rptSeq = 0;

        for (int i = 0; i < MessageCount; i++)
        {
            rptSeq++;
            bool isBid = (i & 1) == 0;
            var live = isBid ? liveBidOrders : liveAskOrders;
            var entryType = isBid ? MDEntryType.BID : MDEntryType.OFFER;

            double r = rng.NextDouble();
            if (r < 0.30 && live.Count > 0)
            {
                int idx = rng.Next(live.Count);
                ulong orderId = live[idx];
                live[idx] = live[^1];
                live.RemoveAt(live.Count - 1);
                _sbeSlices[i] = HotSymbolOnPacketBenchmarks_EncodingHelpers.EncodeDeleteOrder(securityId, orderId, rptSeq, entryType);
                _templateIds[i] = DeleteOrder_MBO_51Data.MESSAGE_ID;
            }
            else if (r < 0.60 && live.Count > 0)
            {
                ulong orderId = live[rng.Next(live.Count)];
                _sbeSlices[i] = HotSymbolOnPacketBenchmarks_EncodingHelpers.EncodeOrder(securityId, orderId,
                    price: PickPrice(isBid, rng), qty: rng.Next(1, 1000),
                    rptSeq, MDUpdateAction.CHANGE, entryType);
                _templateIds[i] = Order_MBO_50Data.MESSAGE_ID;
            }
            else
            {
                ulong orderId = nextOrderId++;
                live.Add(orderId);
                _sbeSlices[i] = HotSymbolOnPacketBenchmarks_EncodingHelpers.EncodeOrder(securityId, orderId,
                    price: PickPrice(isBid, rng), qty: rng.Next(1, 1000),
                    rptSeq, MDUpdateAction.NEW, entryType);
                _templateIds[i] = Order_MBO_50Data.MESSAGE_ID;
            }
        }

        _emptyPacket = new UmdfPacket
        {
            Data = ReadOnlyMemory<byte>.Empty,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 0,
        };
    }

    private static long PickPrice(bool isBid, Random rng)
    {
        // Bids 990–999, asks 1001–1010 (never crossed). 80% land within 3 ticks
        // of the inner level → realistic depth concentration that MBP exploits
        // with a small conflated-key set.
        double r = rng.NextDouble();
        int offset = r < 0.8 ? rng.Next(0, 3) : rng.Next(0, 10);
        return isBid ? 999 - offset : 1001 + offset;
    }

    [Benchmark]
    public long Run()
    {
        _counter.Reset();
        for (int i = 0; i < _sbeSlices.Length; i++)
        {
            _bookManager.OnPacket(in _emptyPacket, _sbeSlices[i], _templateIds[i]);
            if ((i + 1) % BatchSize == 0)
                _bookManager.OnPacketProcessed();
        }
        _bookManager.OnPacketProcessed();
        return _counter.MboBytes + _counter.MbpBytes;
    }

    [GlobalCleanup]
    public void Report()
    {
        // Prints once per param set so the run summary at end carries the
        // bandwidth deltas.
        long mbo = _counter.MboBytes;
        long mbp = _counter.MbpBytes;
        double ratio = mbo == 0 ? 0 : (double)mbp / mbo;
        Console.WriteLine($"[MbpBandwidth] BatchSize={BatchSize} MboBytes={mbo:N0} MbpBytes={mbp:N0} Ratio={ratio:F3} Savings={(1 - ratio) * 100:F1}%");
    }
}

/// <summary>Counts wire bytes the server <i>would</i> emit on the MBO and MBP routes.</summary>
internal sealed class BandwidthCounter : IBookEventHandler
{
    public long MboBytes;
    public long MbpBytes;

    // Buffer of (side, price) keys touched in the current conflation window
    // mapped to the OrderBook reference (so the flush can read the cached
    // aggregate at batch boundary, mirroring GroupConflationHandler.FlushLevelBuffer).
    private readonly Dictionary<(byte, long), OrderBook> _dirty = new();

    public void Reset()
    {
        MboBytes = 0;
        MbpBytes = 0;
        _dirty.Clear();
    }

    public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) => MboBytes += 37;
    public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) => MboBytes += 37;
    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) => MboBytes += 21;

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) { }
    public void OnBookCleared(ulong securityId, BookClearSide side) { }

    public void OnPriceLevelChanged(OrderBook book, BookSideType side, long price)
    {
        _dirty[((byte)side, price)] = book;
    }

    public void OnBatchComplete()
    {
        foreach (var (key, book) in _dirty)
        {
            var (sideByte, price) = key;
            var side = (BookSideType)sideByte;
            book.GetSide(side).TryGetLevelAggregate(price, out long qty, out _);
            // qty == 0 (and TryGetLevelAggregate returned false) → drained → LevelDeleted.
            MbpBytes += qty > 0 ? 33 : 21;
        }
        _dirty.Clear();
    }
}

/// <summary>Reused encoding helpers shared with <see cref="HotSymbolOnPacketBenchmarks"/>.</summary>
internal static class HotSymbolOnPacketBenchmarks_EncodingHelpers
{
    public static byte[] EncodeOrder(ulong securityId, ulong orderId, long price, long qty, uint rptSeq, MDUpdateAction action, MDEntryType entryType)
    {
        const int sbeHeaderSize = 8;
        int total = sbeHeaderSize + Order_MBO_50Data.MESSAGE_SIZE;
        var buf = new byte[total];

        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)Order_MBO_50Data.MESSAGE_SIZE);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), Order_MBO_50Data.MESSAGE_ID);

        var body = buf.AsSpan(sbeHeaderSize);
        var msg = new Order_MBO_50Data
        {
            SecurityID = (SecurityID)securityId,
            MDUpdateAction = action,
            MDEntryType = entryType,
            MDEntrySize = (Quantity)qty,
            SecondaryOrderID = (OrderID)orderId,
        };
        msg.SetRptSeq(rptSeq);
        msg.TryEncode(body, out _);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(body[12..20], price);
        return buf;
    }

    public static byte[] EncodeDeleteOrder(ulong securityId, ulong orderId, uint rptSeq, MDEntryType entryType)
    {
        const int sbeHeaderSize = 8;
        int total = sbeHeaderSize + DeleteOrder_MBO_51Data.MESSAGE_SIZE;
        var buf = new byte[total];

        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)DeleteOrder_MBO_51Data.MESSAGE_SIZE);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), DeleteOrder_MBO_51Data.MESSAGE_ID);

        var body = buf.AsSpan(sbeHeaderSize);
        var msg = new DeleteOrder_MBO_51Data
        {
            SecurityID = (SecurityID)securityId,
            MDEntryType = entryType,
            SecondaryOrderID = (OrderID)orderId,
        };
        msg.SetRptSeq(rptSeq);
        msg.TryEncode(body, out _);
        return buf;
    }
}
