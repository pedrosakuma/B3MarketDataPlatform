using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class StaleMboBufferTests
{
    private static StaleMboBuffer NewBuffer(int perSymbolCap = 1024, long globalByteCap = 64L * 1024 * 1024)
        => new(NullLogger.Instance, perSymbolCap, globalByteCap);

    [Fact]
    public void Enqueue_StoresMessageBody()
    {
        var buf = NewBuffer();
        var body = new byte[] { 1, 2, 3, 4 };
        Assert.True(buf.Enqueue(securityId: 100, templateId: 50, rptSeq: 5, sendingTimeNs: 1234, body));
        Assert.Equal(1, buf.EnqueuedCount);
        Assert.Equal(4, buf.TotalBytes);
        Assert.Equal(1, buf.DepthOf(100));
    }

    [Fact]
    public void Drain_AppliesInRptSeqOrder_RegardlessOfArrivalOrder()
    {
        var buf = NewBuffer();
        // Arrival order shuffled (simulating A/B reorder).
        buf.Enqueue(1, 50, rptSeq: 12, 0, new byte[] { 12 });
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(1, 50, rptSeq: 11, 0, new byte[] { 11 });

        var applied = new List<uint>();
        var n = buf.Drain(1, drainFrom: 10, drainTo: 12, m => applied.Add(m.RptSeq));

        Assert.Equal(3, n);
        Assert.Equal(new uint[] { 10, 11, 12 }, applied);
    }

    [Fact]
    public void Drain_DropsBelowWindow_KeepsAboveWindow()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 5, 0, new byte[] { 5 });   // below
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 }); // in
        buf.Enqueue(1, 50, rptSeq: 15, 0, new byte[] { 15 }); // above

        var applied = new List<uint>();
        var n = buf.Drain(1, drainFrom: 10, drainTo: 12, m => applied.Add(m.RptSeq));

        Assert.Equal(1, n);
        Assert.Equal(new uint[] { 10 }, applied);
        Assert.Equal(1, buf.DepthOf(1)); // 15 retained for future drain
    }

    [Fact]
    public void Drain_EmptyWindow_NoOp()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 10, 0, new byte[] { 10 });

        // drainTo < drainFrom signals nothing to drain (registry returns this when no buffered messages).
        var n = buf.Drain(1, drainFrom: 10, drainTo: 9, _ => Assert.Fail("should not apply"));
        Assert.Equal(0, n);
    }

    [Fact]
    public void Drain_FuturePreservedAcrossCalls()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 20, 0, new byte[] { 20 });
        buf.Drain(1, 10, 15, _ => { });
        // Now heal again with broader window.
        var applied = new List<uint>();
        buf.Drain(1, 16, 25, m => applied.Add(m.RptSeq));
        Assert.Equal(new uint[] { 20 }, applied);
    }

    [Fact]
    public void PerSymbolCap_EvictsOldest_RetainsNewest()
    {
        var buf = NewBuffer(perSymbolCap: 2);
        uint? evicted = null;
        Assert.True(buf.Enqueue(1, 50, 10, 0, new byte[] { 1 }));
        Assert.True(buf.Enqueue(1, 50, 11, 0, new byte[] { 2 }));
        // Cap reached: oldest (rptSeq=10) evicted, newest enqueued.
        Assert.True(buf.Enqueue(1, 50, 12, 0, new byte[] { 3 }, e => evicted = e));
        Assert.Equal((uint)10, evicted);
        Assert.Equal(1, buf.EvictedPerSymbolCapCount);
        Assert.Equal(2, buf.DepthOf(1));
        // Drain confirms only [11, 12] retained.
        var seen = new List<uint>();
        buf.Drain(1, 11, 12, m => seen.Add(m.RptSeq));
        Assert.Equal(new uint[] { 11, 12 }, seen);
    }

    [Fact]
    public void GlobalByteCap_DropsNewest()
    {
        var buf = NewBuffer(perSymbolCap: 1000, globalByteCap: 10);
        Assert.True(buf.Enqueue(1, 50, 1, 0, new byte[6]));
        Assert.False(buf.Enqueue(2, 50, 1, 0, new byte[8])); // 6+8 > 10
        Assert.Equal(1, buf.DroppedGlobalCapCount);
    }

    [Fact]
    public void Clear_DiscardsAndReleasesBytes()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 10, 0, new byte[100]);
        buf.Enqueue(1, 50, 11, 0, new byte[100]);
        Assert.Equal(200, buf.TotalBytes);

        Assert.Equal(2, buf.Clear(1));
        Assert.Equal(0, buf.TotalBytes);
        Assert.Equal(0, buf.DepthOf(1));
    }

    [Fact]
    public void ClearAll_ClearsEverySymbol()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 1, 0, new byte[10]);
        buf.Enqueue(2, 50, 1, 0, new byte[10]);
        buf.Enqueue(3, 50, 1, 0, new byte[10]);
        Assert.Equal(3, buf.ClearAll());
        Assert.Equal(0, buf.TotalBytes);
    }

    [Fact]
    public void Drain_ReleasesByteAccountingForDroppedAndApplied()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 5, 0, new byte[20]);   // below → dropped
        buf.Enqueue(1, 50, 10, 0, new byte[30]);  // applied
        buf.Enqueue(1, 50, 20, 0, new byte[40]);  // above → kept

        Assert.Equal(90, buf.TotalBytes);
        buf.Drain(1, drainFrom: 10, drainTo: 15, _ => { });
        Assert.Equal(40, buf.TotalBytes); // only the kept future entry remains
    }

    [Fact]
    public void DepthOf_ReturnsZeroForUnknownSymbol()
    {
        var buf = NewBuffer();
        Assert.Equal(0, buf.DepthOf(999));
    }
}
