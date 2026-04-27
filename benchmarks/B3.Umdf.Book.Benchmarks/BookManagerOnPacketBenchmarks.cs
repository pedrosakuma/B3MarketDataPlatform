using System.Buffers.Binary;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Benchmarks;

/// <summary>
/// Profiles <see cref="BookManager.OnPacket"/> end-to-end on a healed registry
/// so that each pre-encoded MBO message follows the steady-state Apply path
/// (no buffering, no heal). The mix mirrors the rough proportions observed in
/// production PCAP characterization (~40% Order add, ~30% Order update on an
/// existing OID, ~30% Delete) but only across the three highest-frequency
/// templates — Trade/MassDelete are deliberately excluded so this benchmark
/// stays focused on the order-book hot path.
///
/// Goals:
///   1. Establish a baseline ns/op + allocations/op for OnPacket dispatch.
///   2. Confirm the BookSbeHandler struct dispatch is allocation-free per call
///      (any Allocated/Op &gt; 0 here is a hot-path regression).
///   3. Provide a stable harness for future micro-optimizations
///      (e.g. inlining RouteMbo, removing closure captures, or BookStore
///      lookup tweaks).
///
/// The harness pre-builds N <see cref="UmdfPacket"/>+sbeSlice pairs at
/// <see cref="GlobalSetup"/>, so the per-iteration loop measures only the
/// dispatch + handler cost.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class BookManagerOnPacketBenchmarks
{
    [Params(10_000, 100_000)]
    public int MessageCount;

    [Params(64, 512)]
    public int SymbolCount;

    private BookManager _bookManager = null!;
    private byte[][] _sbeSlices = null!;
    private ushort[] _templateIds = null!;
    private UmdfPacket _emptyPacket;

    // Per-symbol RptSeq counter; advanced as messages are pre-encoded so the
    // live stream is contiguous and stays on the Apply path.
    private uint[] _rptSeqPerSymbol = null!;

    [GlobalSetup]
    public void Setup()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        _bookManager = new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);

        // Pre-heal every symbol at baseline rptSeq=0 so live messages flow
        // through the Healthy/Apply path with no buffering.
        for (ulong sec = 1; sec <= (ulong)SymbolCount; sec++)
        {
            registry.EnsureRegistered(sec);
            registry.HealFromSnapshot(sec, SymbolGapKind.Mbo, snapshotRptSeq: 0);
        }

        _bookManager.FreezeBooks();

        _rptSeqPerSymbol = new uint[SymbolCount + 1]; // +1 so we can index by securityId directly
        _sbeSlices = new byte[MessageCount][];
        _templateIds = new ushort[MessageCount];

        var rng = new Random(Seed: 42);

        // Track which orderIds are alive per symbol so update/delete pick a
        // valid existing OID instead of churning through fresh adds (the
        // dictionary lookup cost differs and we want a realistic mix).
        var liveOrders = new List<ulong>[SymbolCount + 1];
        for (int i = 0; i <= SymbolCount; i++)
            liveOrders[i] = new List<ulong>();
        ulong nextOrderId = 1;

        for (int i = 0; i < MessageCount; i++)
        {
            ulong sec = (ulong)rng.Next(1, SymbolCount + 1);
            _rptSeqPerSymbol[sec]++;
            uint rptSeq = _rptSeqPerSymbol[sec];

            double r = rng.NextDouble();
            if (r < 0.30 && liveOrders[sec].Count > 0)
            {
                // Delete
                int idx = rng.Next(liveOrders[sec].Count);
                ulong orderId = liveOrders[sec][idx];
                liveOrders[sec][idx] = liveOrders[sec][^1];
                liveOrders[sec].RemoveAt(liveOrders[sec].Count - 1);
                _sbeSlices[i] = EncodeDeleteOrder(sec, orderId, rptSeq);
                _templateIds[i] = DeleteOrder_MBO_51Data.MESSAGE_ID;
            }
            else if (r < 0.60 && liveOrders[sec].Count > 0)
            {
                // Update existing OID
                ulong orderId = liveOrders[sec][rng.Next(liveOrders[sec].Count)];
                _sbeSlices[i] = EncodeOrder(sec, orderId, price: 1000 + rng.Next(-10, 11),
                    qty: rng.Next(1, 1000), rptSeq, MDUpdateAction.CHANGE);
                _templateIds[i] = Order_MBO_50Data.MESSAGE_ID;
            }
            else
            {
                // Add
                ulong orderId = nextOrderId++;
                liveOrders[sec].Add(orderId);
                _sbeSlices[i] = EncodeOrder(sec, orderId, price: 1000 + rng.Next(-10, 11),
                    qty: rng.Next(1, 1000), rptSeq, MDUpdateAction.NEW);
                _templateIds[i] = Order_MBO_50Data.MESSAGE_ID;
            }
        }

        // Empty packet — TryGetHeader will fail and OnPacket simply skips
        // setting _currentSendingTimeNs (acceptable for the order-book path).
        _emptyPacket = new UmdfPacket
        {
            Data = ReadOnlyMemory<byte>.Empty,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 0,
        };
    }

    [Benchmark]
    public int OnPacket_DispatchLoop()
    {
        int processed = 0;
        for (int i = 0; i < _sbeSlices.Length; i++)
        {
            _bookManager.OnPacket(in _emptyPacket, _sbeSlices[i], _templateIds[i]);
            processed++;
        }
        return processed;
    }

    private static byte[] EncodeOrder(ulong securityId, ulong orderId, long price, long qty, uint rptSeq, MDUpdateAction action)
    {
        const int sbeHeaderSize = 8;
        int total = sbeHeaderSize + Order_MBO_50Data.MESSAGE_SIZE;
        var buf = new byte[total];

        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)Order_MBO_50Data.MESSAGE_SIZE);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), Order_MBO_50Data.MESSAGE_ID);
        // schemaId @ 4 / version @ 6 — left as zero; SbeDispatcher only branches on templateId.

        var body = buf.AsSpan(sbeHeaderSize);
        var msg = new Order_MBO_50Data
        {
            SecurityID = (SecurityID)securityId,
            MDUpdateAction = action,
            MDEntryType = MDEntryType.BID,
            MDEntrySize = (Quantity)qty,
            SecondaryOrderID = (OrderID)orderId,
        };
        msg.SetRptSeq(rptSeq);
        msg.TryEncode(body, out _);
        // PriceOptional has no public setter (zero-copy reader struct); patch
        // the mantissa directly at FieldOffset(12).
        BinaryPrimitives.WriteInt64LittleEndian(body[12..20], price);
        return buf;
    }

    private static byte[] EncodeDeleteOrder(ulong securityId, ulong orderId, uint rptSeq)
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
            MDEntryType = MDEntryType.BID,
            SecondaryOrderID = (OrderID)orderId,
        };
        msg.SetRptSeq(rptSeq);
        msg.TryEncode(body, out _);
        return buf;
    }
}
