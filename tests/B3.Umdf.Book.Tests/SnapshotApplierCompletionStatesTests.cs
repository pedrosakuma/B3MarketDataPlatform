using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// P1 coverage for the explicit snapshot assembly state machine: header-only,
/// zero-order, orphan-chunk, replaced-header, and mid-assembly-abort scenarios.
/// Each test asserts both the new explicit counter increments AND that the
/// live <see cref="OrderBook"/> is mutated only on successful Complete.
/// </summary>
public class SnapshotApplierCompletionStatesTests
{
    private static (BookManager bm, SymbolStateRegistry reg, StaleMboBuffer buf) CreatePerSymbol()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);
        return (bm, reg, buf);
    }

    [Fact]
    public void HeaderOnly_IlliquidEmpty_CompletesAndIncrementsCounter()
    {
        // HasRptSeq=false (LastRptSeq omitted ⇒ illiquid) + OrdersExpected=0
        // ⇒ "header-only" success path: completion fires immediately at
        // BeginHeader time with no Orders_71 chunks following.
        var (bm, reg, _) = CreatePerSymbol();

        bm.RecordSnapshotHeader(securityId: 1001, lastRptSeq: null);
        bm.HealAfterSnapshotForTest(1001);

        Assert.Equal(1, bm.SnapshotsCompleted);
        Assert.Equal(1, bm.SnapshotsHeaderOnly);
        Assert.Equal(0, bm.SnapshotsZeroOrder);
        Assert.Equal(0, bm.SnapshotsOrphanChunk);
        Assert.Equal(0, bm.SnapshotsReplacedHeader);
        Assert.Equal(0, bm.SnapshotsAborted);
        Assert.Equal(SymbolState.Healthy, reg.GetState(1001, SymbolGapKind.Mbo));
    }

    [Fact]
    public void ZeroOrder_WithRptSeq_CompletesImmediatelyAtomicSwap()
    {
        // OrdersExpected=0 + concrete LastRptSeq ⇒ atomic swap into an empty
        // book happens in BeginHeader; counter increments and the book
        // baseline is anchored at the snapshot rptSeq.
        var (bm, reg, _) = CreatePerSymbol();

        // Cold-start: bump high-water so the symbol is Stale-eligible.
        for (uint r = 10; r <= 12; r++)
            reg.Observe(securityId: 2002, SymbolGapKind.Mbo, r);

        bm.BeginChunkedSnapshotForTest(2002, lastRptSeq: 12, ordersExpected: 0);

        Assert.Equal(1, bm.SnapshotsCompleted);
        Assert.Equal(1, bm.SnapshotsZeroOrder);
        Assert.Equal(0, bm.SnapshotsHeaderOnly);
        Assert.Equal(12u, bm.Books[2002].LastRptSeq);
        Assert.Equal(0, bm.Books[2002].Bids.OrderCount);
        Assert.Equal(0, bm.Books[2002].Asks.OrderCount);
    }

    [Fact]
    public void OrphanChunk_NoHeader_DropsCleanlyAndCounts()
    {
        // Orders_71 chunk with no preceding Header_30 ⇒ silently dropped, no
        // exception, both legacy and new orphan counters tick. Live book
        // (created on demand) stays empty.
        var (bm, _, _) = CreatePerSymbol();

        bm.RecordSnapshotChunkForTest(securityId: 3003, ordersInChunk: 5);

        Assert.Equal(1, bm.SnapshotChunksOrphaned);
        Assert.Equal(1, bm.SnapshotsOrphanChunk);
        Assert.Equal(0, bm.SnapshotsCompleted);
        Assert.Equal(0, bm.SnapshotsAborted);
        // No book mutation: the on-demand created book is empty.
        Assert.True(!bm.Books.TryGetValue(3003, out var b) || (b.Bids.OrderCount == 0 && b.Asks.OrderCount == 0));
    }

    [Fact]
    public void ReplacedHeader_DiscardsStagingAndStartsFresh()
    {
        // A new Header_30 arriving for an in-flight (partially-staged)
        // assembly discards the old staging, increments the replaced
        // counter, and the new header proceeds to a clean completion.
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 1; r <= 10; r++)
            reg.Observe(securityId: 4004, SymbolGapKind.Mbo, r);

        // First (incomplete) assembly: 1 of 4 staged.
        bm.BeginChunkedSnapshotForTest(4004, lastRptSeq: 5, ordersExpected: 4);
        bm.StageSnapshotEntryForTest(4004, BookSideType.Bid, orderId: 91, price: 100, quantity: 1);

        Assert.Equal(0, bm.SnapshotsReplacedHeader);
        Assert.Equal(0, bm.Books[4004].Bids.OrderCount); // staging only — book untouched.

        // Replacement header: old staging discarded.
        bm.BeginChunkedSnapshotForTest(4004, lastRptSeq: 9, ordersExpected: 2);
        bm.StageSnapshotEntryForTest(4004, BookSideType.Bid, orderId: 92, price: 101, quantity: 2);
        bm.StageSnapshotEntryForTest(4004, BookSideType.Ask, orderId: 93, price: 105, quantity: 3);

        Assert.Equal(1, bm.SnapshotsReplacedHeader);
        Assert.Equal(1, bm.SnapshotsAbandoned); // legacy parallel counter still ticks.
        Assert.Equal(1, bm.SnapshotsCompleted);
        Assert.Equal(9u, bm.Books[4004].LastRptSeq);
        Assert.Equal(1, bm.Books[4004].Bids.OrderCount);
        Assert.Equal(1, bm.Books[4004].Asks.OrderCount);
        // Discarded predecessor's order 91 must not appear.
        Assert.False(bm.Books[4004].Bids.TryGetOrder(91, out _));
    }

    [Fact]
    public void MidAssemblyAbort_LiveBookUntouched_CounterIncrements()
    {
        // Simulate an exception thrown while applying a chunk: pending
        // staging is discarded via AbortPendingSnapshot. The live book —
        // which holds the prior healed baseline — must remain unchanged.
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 1; r <= 20; r++)
            reg.Observe(securityId: 5005, SymbolGapKind.Mbo, r);

        // First, heal the book to a known baseline.
        bm.BeginChunkedSnapshotForTest(5005, lastRptSeq: 10, ordersExpected: 1);
        bm.StageSnapshotEntryForTest(5005, BookSideType.Bid, orderId: 1, price: 99, quantity: 7);
        Assert.Equal(1, bm.SnapshotsCompleted);
        Assert.Equal(10u, bm.Books[5005].LastRptSeq);
        Assert.Equal(1, bm.Books[5005].Bids.OrderCount);

        // Begin a second assembly and stage a partial chunk.
        bm.BeginChunkedSnapshotForTest(5005, lastRptSeq: 18, ordersExpected: 5);
        bm.StageSnapshotEntryForTest(5005, BookSideType.Bid, orderId: 2, price: 100, quantity: 3);
        bm.StageSnapshotEntryForTest(5005, BookSideType.Ask, orderId: 3, price: 110, quantity: 4);

        // Mid-assembly failure — abort.
        bool aborted = bm.AbortPendingSnapshotForTest(5005);

        Assert.True(aborted);
        Assert.Equal(1, bm.SnapshotsAborted);
        // Live book MUST be untouched: still the post-first-snapshot baseline.
        Assert.Equal(10u, bm.Books[5005].LastRptSeq);
        Assert.Equal(1, bm.Books[5005].Bids.OrderCount);
        Assert.Equal(0, bm.Books[5005].Asks.OrderCount);
        Assert.True(bm.Books[5005].Bids.TryGetOrder(1, out _));
        Assert.False(bm.Books[5005].Bids.TryGetOrder(2, out _));
        // Subsequent abort is a no-op (no double count).
        Assert.False(bm.AbortPendingSnapshotForTest(5005));
        Assert.Equal(1, bm.SnapshotsAborted);
    }
}
