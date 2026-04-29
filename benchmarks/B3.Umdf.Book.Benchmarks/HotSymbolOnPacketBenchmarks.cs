using System.Buffers.Binary;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Benchmarks;

/// <summary>
/// Closes the gap between <see cref="BookSideBenchmarks"/> (single side, no
/// dispatcher) and <see cref="BookManagerOnPacketBenchmarks"/> (full dispatch
/// but spread across many symbols → small per-symbol books, BestPrice cost is
/// noise).
///
/// This bench targets the realistic "single hot symbol" scenario (think PETR4
/// or VALE3 during a busy minute): one security, both sides populated, deep
/// concentration near top-of-book, full BookManager.OnPacket dispatch path —
/// which means CheckCrossing and therefore BestPrice() fires on every
/// add/update.
///
/// Use this to validate that BookSide-level optimisations (e.g. cached
/// TotalQty/Count per price level) propagate end-to-end.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class HotSymbolOnPacketBenchmarks
{
    [Params(100_000)]
    public int MessageCount;

    private BookManager _bookManager = null!;
    private byte[][] _sbeSlices = null!;
    private ushort[] _templateIds = null!;
    private UmdfPacket _emptyPacket;

    [GlobalSetup]
    public void Setup()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        _bookManager = new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);

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

        // Mix mirrors BookSideBenchmarks (40% add / 30% update / 30% delete) but
        // alternates BID/ASK so both sides accumulate depth and CheckCrossing
        // exercises BestPrice() on both. Prices clustered tightly around 1000
        // (the natural cross point) so the top level stays deep — making
        // BestPrice's per-order foreach (pre-V2) measurably non-trivial.
        for (int i = 0; i < MessageCount; i++)
        {
            rptSeq++;
            // 50/50 BID vs ASK
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
                _sbeSlices[i] = EncodeDeleteOrder(securityId, orderId, rptSeq, entryType);
                _templateIds[i] = DeleteOrder_MBO_51Data.MESSAGE_ID;
            }
            else if (r < 0.60 && live.Count > 0)
            {
                ulong orderId = live[rng.Next(live.Count)];
                _sbeSlices[i] = EncodeOrder(securityId, orderId,
                    price: PickPrice(isBid, rng), qty: rng.Next(1, 1000),
                    rptSeq, MDUpdateAction.CHANGE, entryType);
                _templateIds[i] = Order_MBO_50Data.MESSAGE_ID;
            }
            else
            {
                ulong orderId = nextOrderId++;
                live.Add(orderId);
                _sbeSlices[i] = EncodeOrder(securityId, orderId,
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
        // Bids 990–999, asks 1001–1010 → never crossed (avoids the heavy
        // logging branch in CheckCrossing) but the top level (999 / 1001)
        // gets ~80% of orders → realistic depth concentration at top.
        double r = rng.NextDouble();
        int offset = r < 0.8 ? rng.Next(0, 3) : rng.Next(0, 10);
        return isBid ? 999 - offset : 1001 + offset;
    }

    [Benchmark]
    public int OnPacket_HotSymbol()
    {
        int processed = 0;
        for (int i = 0; i < _sbeSlices.Length; i++)
        {
            _bookManager.OnPacket(in _emptyPacket, _sbeSlices[i], _templateIds[i]);
            processed++;
        }
        return processed;
    }

    private static byte[] EncodeOrder(ulong securityId, ulong orderId, long price, long qty, uint rptSeq, MDUpdateAction action, MDEntryType entryType)
    {
        const int sbeHeaderSize = 8;
        int total = sbeHeaderSize + Order_MBO_50Data.MESSAGE_SIZE;
        var buf = new byte[total];

        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)Order_MBO_50Data.MESSAGE_SIZE);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), Order_MBO_50Data.MESSAGE_ID);

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
        BinaryPrimitives.WriteInt64LittleEndian(body[12..20], price);
        return buf;
    }

    private static byte[] EncodeDeleteOrder(ulong securityId, ulong orderId, uint rptSeq, MDEntryType entryType)
    {
        const int sbeHeaderSize = 8;
        int total = sbeHeaderSize + DeleteOrder_MBO_51Data.MESSAGE_SIZE;
        var buf = new byte[total];

        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)DeleteOrder_MBO_51Data.MESSAGE_SIZE);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), DeleteOrder_MBO_51Data.MESSAGE_ID);

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
