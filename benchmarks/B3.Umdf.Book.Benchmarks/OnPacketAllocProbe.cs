using System.Buffers.Binary;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Benchmarks;

/// <summary>
/// One-shot allocation probe (not a BenchmarkDotNet harness — runs once to
/// produce per-op byte counts). Useful when BenchmarkDotNet's per-batch
/// allocation diagnostic is fuzzy: this measures the EXACT bytes allocated
/// per OnPacket call by snapshotting <c>GC.GetAllocatedBytesForCurrentThread</c>
/// before and after a tight loop. Run via:
///   dotnet run -c Release -- alloc-probe
/// </summary>
internal static class OnPacketAllocProbe
{
    public static void Run()
    {
        const int symbolCount = 64;
        const int messageCount = 100_000;

        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);

        for (ulong sec = 1; sec <= (ulong)symbolCount; sec++)
        {
            registry.EnsureRegistered(sec);
            registry.HealFromSnapshot(sec, SymbolGapKind.Mbo, snapshotRptSeq: 0);
            // Materialize each book so GetOrCreate doesn't allocate inside the loop.
            bm.GetOrCreateBook(sec);
        }
        bm.FreezeBooks();

        var rng = new Random(Seed: 42);
        var slices = new byte[messageCount][];
        var templateIds = new ushort[messageCount];
        var rptSeqPerSymbol = new uint[symbolCount + 1];
        var liveOrders = new List<ulong>[symbolCount + 1];
        for (int i = 0; i <= symbolCount; i++)
            liveOrders[i] = new List<ulong>();
        ulong nextOrderId = 1;

        for (int i = 0; i < messageCount; i++)
        {
            ulong sec = (ulong)rng.Next(1, symbolCount + 1);
            rptSeqPerSymbol[sec]++;
            uint rptSeq = rptSeqPerSymbol[sec];
            double r = rng.NextDouble();
            if (r < 0.30 && liveOrders[sec].Count > 0)
            {
                int idx = rng.Next(liveOrders[sec].Count);
                ulong orderId = liveOrders[sec][idx];
                liveOrders[sec][idx] = liveOrders[sec][^1];
                liveOrders[sec].RemoveAt(liveOrders[sec].Count - 1);
                slices[i] = Encode.Delete(sec, orderId, rptSeq);
                templateIds[i] = DeleteOrder_MBO_51Data.MESSAGE_ID;
            }
            else if (r < 0.60 && liveOrders[sec].Count > 0)
            {
                ulong orderId = liveOrders[sec][rng.Next(liveOrders[sec].Count)];
                slices[i] = Encode.Order(sec, orderId, 1000 + rng.Next(-10, 11),
                    rng.Next(1, 1000), rptSeq, MDUpdateAction.CHANGE);
                templateIds[i] = Order_MBO_50Data.MESSAGE_ID;
            }
            else
            {
                ulong orderId = nextOrderId++;
                liveOrders[sec].Add(orderId);
                slices[i] = Encode.Order(sec, orderId, 1000 + rng.Next(-10, 11),
                    rng.Next(1, 1000), rptSeq, MDUpdateAction.NEW);
                templateIds[i] = Order_MBO_50Data.MESSAGE_ID;
            }
        }

        var emptyPacket = new UmdfPacket
        {
            Data = ReadOnlyMemory<byte>.Empty,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 1,
            ReceivedTimestampTicks = 0,
        };

        // Warm up — get the dictionary growth done before measurement.
        for (int i = 0; i < messageCount / 10; i++)
            bm.OnPacket(in emptyPacket, slices[i], templateIds[i]);

        // Force a full GC so we measure allocations only, not survival pressure.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long beforeBytes = GC.GetAllocatedBytesForCurrentThread();
        long beforeGen0 = GC.CollectionCount(0);
        long beforeGen1 = GC.CollectionCount(1);
        long beforeGen2 = GC.CollectionCount(2);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Measure the remaining 90% of messages.
        for (int i = messageCount / 10; i < messageCount; i++)
            bm.OnPacket(in emptyPacket, slices[i], templateIds[i]);

        sw.Stop();
        long afterBytes = GC.GetAllocatedBytesForCurrentThread();
        long afterGen0 = GC.CollectionCount(0);
        long afterGen1 = GC.CollectionCount(1);
        long afterGen2 = GC.CollectionCount(2);

        int measuredOps = messageCount - (messageCount / 10);
        double bytesPerOp = (afterBytes - beforeBytes) / (double)measuredOps;
        double nsPerOp = sw.Elapsed.TotalNanoseconds / measuredOps;

        Console.WriteLine();
        Console.WriteLine("─── OnPacket allocation probe (post-warmup) ───");
        Console.WriteLine($"  Symbols           : {symbolCount}");
        Console.WriteLine($"  Measured ops      : {measuredOps:N0}");
        Console.WriteLine($"  Order adds        : {bm.OrderAdds:N0}");
        Console.WriteLine($"  Order updates     : {bm.OrderUpdates:N0}");
        Console.WriteLine($"  Order deletes     : {bm.OrderDeletes:N0}");
        Console.WriteLine($"  Elapsed           : {sw.ElapsedMilliseconds} ms");
        Console.WriteLine($"  ns / op           : {nsPerOp:F2}");
        Console.WriteLine($"  bytes / op        : {bytesPerOp:F2}");
        Console.WriteLine($"  Gen0 collections  : {afterGen0 - beforeGen0}");
        Console.WriteLine($"  Gen1 collections  : {afterGen1 - beforeGen1}");
        Console.WriteLine($"  Gen2 collections  : {afterGen2 - beforeGen2}");
    }

    private static class Encode
    {
        public static byte[] Order(ulong securityId, ulong orderId, long price, long qty, uint rptSeq, MDUpdateAction action)
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
            BinaryPrimitives.WriteInt64LittleEndian(body[12..20], price);
            return buf;
        }

        public static byte[] Delete(ulong securityId, ulong orderId, uint rptSeq)
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
}
