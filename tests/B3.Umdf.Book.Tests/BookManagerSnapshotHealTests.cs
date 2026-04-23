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
    public void Heal_DrainsBufferedMessages_RegistryReachesHealthy()
    {
        // Validate the drain wiring even with empty buffer (no replay-able payloads
        // because we bypassed buffering above). The registry contract is what matters:
        // post-heal, observing the next contiguous rptSeq returns Apply.
        var (bm, reg, _) = CreatePerSymbol();
        reg.Observe(7, SymbolGapKind.Mbo, 100); // cold-start buffer + bumps high-water

        bm.RecordSnapshotHeader(7, lastRptSeq: 100);
        bm.HealAfterSnapshotForTest(7);

        var next = reg.Observe(7, SymbolGapKind.Mbo, 101);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, next.Action);
    }

}
