using B3.Umdf.Feed;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class BookManagerSnapshotHealTests
{
    private static (BookManager bm, SymbolStateRegistry reg, StaleMboBuffer buf) CreatePerSymbol()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);
        return (bm, reg, buf);
    }

    [Fact]
    public void Header_Then_Heal_TransitionsRegistryToHealthy()
    {
        var (bm, reg, _) = CreatePerSymbol();

        // Cold-start: send Observe directly to bump high-water (without buffering bodies).
        for (uint r = 10; r <= 15; r++)
            reg.Observe(securityId: 42, SymbolGapKind.Mbo, r);
        // Cold-start MBO is Unknown (not yet Stale; that requires Healthy→gap).

        bm.RecordSnapshotHeader(42, lastRptSeq: 12);
        bm.HealAfterSnapshotForTest(42);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);
        Assert.Equal(12u, bm.Books[42].LastRptSeq); // book reflects snapshot baseline
    }

    [Fact]
    public void Heal_WithoutHeader_IncrementsMissingCounter()
    {
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 10; r <= 12; r++)
            reg.Observe(securityId: 77, SymbolGapKind.Mbo, r);

        bm.HealAfterSnapshotForTest(77);

        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(1, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void Header_NullLastRptSeq_PreventsHeal()
    {
        var (bm, reg, _) = CreatePerSymbol();
        reg.Observe(99, SymbolGapKind.Mbo, 100);

        bm.RecordSnapshotHeader(99, lastRptSeq: null);
        bm.HealAfterSnapshotForTest(99);

        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(1, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void Header_RptSeqZero_TreatedAsNull()
    {
        var (bm, _, _) = CreatePerSymbol();

        bm.RecordSnapshotHeader(50, lastRptSeq: 0);
        bm.HealAfterSnapshotForTest(50);

        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(1, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void Heal_PendingEntry_IsConsumed_NotReused()
    {
        var (bm, _, _) = CreatePerSymbol();

        bm.RecordSnapshotHeader(11, lastRptSeq: 50);
        bm.HealAfterSnapshotForTest(11);
        Assert.Equal(1, bm.SnapshotsHealed);

        // Second snapshot for same symbol with no fresh header → must NOT reuse stale 50
        bm.HealAfterSnapshotForTest(11);
        Assert.Equal(1, bm.SnapshotsHealed); // unchanged
        Assert.Equal(1, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void ChunkedSnapshot_HealsOnlyAfterAllChunks()
    {
        // Regression for production bug: a single instrument's MBO snapshot is delivered as
        // 1× Header_30 + N× Orders_71 chunks (sum entries == TotNumBids+TotNumOffers).
        // Previously each chunk consumed the cached LastRptSeq and re-cleared the book, so:
        //   - chunk 1 healed prematurely (book half-built)
        //   - chunks 2..N incremented snapshots_missing_rptseq and re-Cleared the book
        // Now: heal must only fire when received >= expected.
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 10; r <= 15; r++)
            reg.Observe(securityId: 100, SymbolGapKind.Mbo, r);

        bm.BeginChunkedSnapshotForTest(100, lastRptSeq: 12, ordersExpected: 30);
        bm.RecordSnapshotChunkForTest(100, ordersInChunk: 10);
        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);

        bm.RecordSnapshotChunkForTest(100, ordersInChunk: 10);
        Assert.Equal(0, bm.SnapshotsHealed);

        bm.RecordSnapshotChunkForTest(100, ordersInChunk: 10);
        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);
        Assert.Equal(12u, bm.Books[100].LastRptSeq);
    }

    [Fact]
    public void OrphanChunk_WithoutHeader_IncrementsCounter()
    {
        var (bm, _, _) = CreatePerSymbol();
        bm.RecordSnapshotChunkForTest(200, ordersInChunk: 5);
        Assert.Equal(1, bm.SnapshotChunksOrphaned);
        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void EmptyBookSnapshot_HealsImmediatelyOnHeader()
    {
        var (bm, reg, _) = CreatePerSymbol();
        reg.Observe(securityId: 300, SymbolGapKind.Mbo, 50);

        bm.BeginChunkedSnapshotForTest(300, lastRptSeq: 50, ordersExpected: 0);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(50u, bm.Books[300].LastRptSeq);
    }

    [Fact]
    public void NewHeader_MidSnapshot_SupersedesPriorAndResetsCounters()
    {
        var (bm, reg, _) = CreatePerSymbol();
        reg.Observe(securityId: 400, SymbolGapKind.Mbo, 5);

        bm.BeginChunkedSnapshotForTest(400, lastRptSeq: 5, ordersExpected: 30);
        bm.RecordSnapshotChunkForTest(400, ordersInChunk: 10);
        Assert.Equal(0, bm.SnapshotsHealed);

        // Fresh Header_30 supersedes the in-progress snapshot.
        bm.BeginChunkedSnapshotForTest(400, lastRptSeq: 7, ordersExpected: 4);
        bm.RecordSnapshotChunkForTest(400, ordersInChunk: 4);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(7u, bm.Books[400].LastRptSeq);
        Assert.Equal(0, bm.SnapshotChunksOrphaned);
    }
}
