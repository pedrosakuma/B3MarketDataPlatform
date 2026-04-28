using System.Buffers.Binary;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Allocation-regression sentry for <see cref="BookManager.OnPacket"/> on the
/// steady Apply path. Before P11-1, the lambda inside
/// <c>BookManager.RouteMbo</c> captured <c>securityId</c> + <c>_stateRegistry</c>,
/// forcing the C# compiler to hoist a <c>&lt;&gt;c__DisplayClass96_0</c> closure
/// at method entry. The closure was allocated on EVERY invocation regardless of
/// whether the Buffer branch ran — accounting for ~35 % of all heap
/// allocations under peak load (1 208 MB / 600 s in the 5x replay capture).
///
/// The fix routes the eviction callback through a new
/// <c>StaleMboBuffer.Enqueue&lt;TState&gt;</c> overload + a <c>static</c>
/// lambda, eliminating the closure. This test asserts the steady Apply path
/// is essentially zero-allocation. The exact threshold leaves headroom for
/// transient JIT/runtime allocations on first iterations, but is far below
/// what the old code produced (which would allocate ~80 KB+ for 100 k calls).
/// </summary>
[Collection(nameof(AllocationSensitiveCollection))]
public class BookManagerOnPacketAllocationTests
{
    [Fact]
    public void OnPacket_ApplyPath_Allocates_Below_Threshold()
    {
        // Use a single symbol with a SMALL fixed pool of pre-populated orders
        // and drive only CHANGE messages (Update existing OID). This exercises
        // BookManager.OnPacket → SbeDispatcher → BookSbeHandler → HandleOrder
        // → RouteMbo on the Apply path, but avoids the BookSide dictionary /
        // list growth that contaminates a broader workload's allocation
        // accounting (those are tracked separately in P10-3 / future P11
        // phases). The allocation we are watching for is the
        // <>c__DisplayClass96_0 closure that the C# compiler used to hoist at
        // the top of RouteMbo (24 B per call before the fix).
        const ulong sec = 1;
        const int orderPoolSize = 64;
        const int messageCount = 100_000;

        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);
        registry.EnsureRegistered(sec);
        registry.HealFromSnapshot(sec, SymbolGapKind.Mbo, snapshotRptSeq: 0);
        bm.GetOrCreateBook(sec);
        bm.FreezeBooks();

        // Pre-populate with NEW orders so subsequent CHANGE messages target
        // existing OIDs (no dict insert, no list AddWithResize).
        uint rptSeq = 0;
        for (ulong oid = 1; oid <= orderPoolSize; oid++)
        {
            rptSeq++;
            var addSlice = EncodeOrder(sec, oid, price: 1000, qty: 10, rptSeq, MDUpdateAction.NEW);
            bm.OnPacket(in EmptyPacket, addSlice, Order_MBO_50Data.MESSAGE_ID);
        }

        // Build CHANGE messages cycling through the existing pool.
        var slices = new byte[messageCount][];
        for (int i = 0; i < messageCount; i++)
        {
            ulong oid = (ulong)((i % orderPoolSize) + 1);
            rptSeq++;
            // Keep price CONSTANT (1000) so BookSide.MoveOrder is never invoked
            // (price-level list rebalance allocates). Only quantity varies — the
            // same-price branch just calls SyncPriceLevelCopy which mutates the
            // existing entry in place. This isolates the closure under test
            // from BookSide dict/list churn (tracked separately in P11 follow-ups).
            slices[i] = EncodeOrder(sec, oid, price: 1000, qty: 10 + (i % 5), rptSeq, MDUpdateAction.CHANGE);
        }

        // Warmup ~1 % of the workload: triggers JIT, exercises any one-time
        // path so the measured window only sees steady-state allocations.
        var warmupCount = messageCount / 100;
        for (int i = 0; i < warmupCount; i++)
            bm.OnPacket(in EmptyPacket, slices[i], Order_MBO_50Data.MESSAGE_ID);

        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);

        long beforeBytes = GC.GetAllocatedBytesForCurrentThread();
        for (int i = warmupCount; i < messageCount; i++)
            bm.OnPacket(in EmptyPacket, slices[i], Order_MBO_50Data.MESSAGE_ID);
        long afterBytes = GC.GetAllocatedBytesForCurrentThread();

        long deltaBytes = afterBytes - beforeBytes;
        int measuredCalls = messageCount - warmupCount;

        // Pre-fix baseline on the same harness: ~28-32 B/call (the hoisted
        // <>c__DisplayClass96_0 closure). Post-fix should be effectively
        // zero. Threshold of 1 KB total leaves room for transient runtime
        // events (PollGC bookkeeping, EventSource, etc.) without admitting
        // a closure regression.
        Assert.True(deltaBytes < 1024,
            $"OnPacket Apply path allocated {deltaBytes} bytes across {measuredCalls} calls " +
            $"(={(double)deltaBytes / measuredCalls:F3} B/call). Threshold is 1024 bytes total. " +
            $"Suspect a new heap allocation in BookManager.RouteMbo / SbeDispatcher / handler dispatch " +
            $"(historically the <>c__DisplayClass96_0 closure in RouteMbo).");
    }

    private static readonly UmdfPacket EmptyPacket = new()
    {
        Data = ReadOnlyMemory<byte>.Empty,
        Channel = ChannelType.IncrementalA,
        ChannelGroup = 1,
        ReceivedTimestampTicks = 0,
    };

    private static byte[] EncodeOrder(ulong securityId, ulong orderId, long price, long qty, uint rptSeq, MDUpdateAction action)
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
