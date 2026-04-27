using B3.Umdf.Book;
using B3.Umdf.Feed;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Regression coverage for recovery-path bugs identified during deep analysis:
///  - #17: illiquid (no-rpt + empty-book) snapshot must heal a symbol that was
///         flipped Stale by a cross-kind global rptSeq gap (otherwise stuck forever).
///  - #16: replacing an incomplete <c>PendingSnapshot</c> must (a) account the
///         abandoned chunks and (b) clear the per-symbol StaleMboBuffer protected
///         floor, regardless of the replacement path (normal / Healthy-skip /
///         stale-version skip).
///  - #19: when the channel has observed a SequenceVersion, snapshot Header_30
///         with an absent or mismatched LastSequenceVersion MUST be rejected
///         (otherwise an in-flight V1 snapshot can poison V2 baseline and
///         silently drop subsequent live messages until rptSeq catches up).
/// </summary>
public class RecoveryGapScenariosTests
{
    private static (BookManager bm, SymbolStateRegistry reg, StaleMboBuffer buf) CreatePerSymbol()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);
        return (bm, reg, buf);
    }

    // ── #17 — illiquid snapshot must rescue cross-kind-Stale Mbo ───────────────

    [Fact]
    public void Bug17_IlliquidSnapshot_HealsMboStaledByCrossKindGlobalGap()
    {
        var (bm, reg, buf) = CreatePerSymbol();
        const ulong secId = 4242;

        // Bootstrap MBO via a normal snapshot at rptSeq=50 → state Healthy.
        reg.HealFromSnapshot(secId, SymbolGapKind.Mbo, snapshotRptSeq: 50);

        // A stat message arrives advancing the GLOBAL rptSeq watermark with a gap
        // (50 → 100). DetectAndApplyGlobalGap forces Mbo Stale with
        // MinHealRptSeq[Mbo] = 99. From now on, only a snapshot whose rpt ≥ 99
        // would normally be accepted.
        reg.Observe(secId, SymbolGapKind.PriceBand, 100);
        Assert.Equal(SymbolState.Stale, reg.GetState(secId, SymbolGapKind.Mbo));

        // The wire publishes an illiquid snapshot for this instrument: empty book
        // + LastRptSeq absent (per spec §7.4 — instrument never received MBO
        // incrementals on its own counter). Expected behavior: this is an
        // authoritative empty-book signal — Mbo MUST become Healthy.
        bm.RecordSnapshotHeader(secId, lastRptSeq: null);
        bm.HealAfterSnapshotForTest(secId);

        Assert.Equal(SymbolState.Healthy, reg.GetState(secId, SymbolGapKind.Mbo));
        Assert.Equal(1L, bm.SnapshotsHealed);
        Assert.Equal(0L, bm.SnapshotsMissingRptSeq);
        // Buffer for this symbol must be empty (no stale tail to drain).
        Assert.Equal(0, buf.DepthOf(secId));
    }

    [Fact]
    public void Bug17_IlliquidHeal_MustNotAcceptLatePreSnapshotMbo()
    {
        // After an authoritative illiquid heal, a delayed pre-snapshot MBO
        // packet (rptSeq small relative to global high-water) MUST NOT be
        // applied — the book is empty by definition and applying a stale order
        // would resurrect ghost state.
        var (bm, reg, _) = CreatePerSymbol();
        const ulong secId = 7777;

        reg.HealFromSnapshot(secId, SymbolGapKind.Mbo, snapshotRptSeq: 50);
        reg.Observe(secId, SymbolGapKind.PriceBand, 100); // Mbo Stale, observed=100

        bm.RecordSnapshotHeader(secId, lastRptSeq: null);
        bm.HealAfterSnapshotForTest(secId);

        // Late MBO from BEFORE the global watermark — must be dropped.
        var late = reg.Observe(secId, SymbolGapKind.Mbo, receivedRptSeq: 60);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Drop, late.Action);
    }

    // ── #16 — abandoned PendingSnapshot accounting + floor cleanup ─────────────

    [Fact]
    public void Bug16_PartialPendingReplacedByNewHeader_IncrementsAbandonedCounter()
    {
        var (bm, _, _) = CreatePerSymbol();
        const ulong secId = 100;

        // Header for a snapshot expecting 10 entries; deliver only 5 (e.g. last
        // chunk lost on the wire).
        bm.BeginChunkedSnapshotForTest(secId, lastRptSeq: 200, ordersExpected: 10);
        bm.RecordSnapshotChunkForTest(secId, ordersInChunk: 5);

        Assert.Equal(0L, bm.SnapshotsAbandoned);

        // Next rotation: a new Header arrives for the same symbol BEFORE the
        // previous one finished. The 5 dangling chunks must be counted as
        // abandoned so operators can detect this loss pattern.
        bm.BeginChunkedSnapshotForTest(secId, lastRptSeq: 220, ordersExpected: 4);

        Assert.Equal(1L, bm.SnapshotsAbandoned);
    }

    [Fact]
    public void Bug16_PendingReplaced_ClearsProtectedFloor()
    {
        var (bm, _, buf) = CreatePerSymbol();
        const ulong secId = 101;

        bm.BeginChunkedSnapshotForTest(secId, lastRptSeq: 500, ordersExpected: 8);
        // BeginChunkedSnapshotForTest pins floor at lastRptSeq + 1 = 501.
        Assert.Equal(501u, buf.ProtectedFloorOf(secId));

        // A stale-version skipped header replaces the pending. The OLD floor
        // belongs to a snapshot that will never complete — it must NOT linger.
        bm.OnSequenceVersionChanged(newVersion: 5);
        bm.OnSnapshotHeaderForTest(secId, lastRptSeq: 600, ordersExpected: 0, lastSequenceVersion: 4);

        // A skipped/stale-version header should not LEAVE a floor pin from the
        // discarded predecessor. (The skipped path itself sets no new floor.)
        Assert.Equal(0u, buf.ProtectedFloorOf(secId));
    }

    [Fact]
    public void Bug16_StaleVersionReplacement_AlsoIncrementsAbandoned()
    {
        var (bm, _, _) = CreatePerSymbol();
        const ulong secId = 102;

        // Sit on V9 and start a real partial snapshot (matches current version).
        bm.OnSequenceVersionChanged(newVersion: 9);
        bm.OnSnapshotHeaderForTest(secId, lastRptSeq: 100, ordersExpected: 6, lastSequenceVersion: 9);
        bm.RecordSnapshotChunkForTest(secId, ordersInChunk: 3);

        // A stale-version V8 header arrives for the same symbol — it must be
        // skipped AND must account the predecessor as abandoned.
        bm.OnSnapshotHeaderForTest(secId, lastRptSeq: 120, ordersExpected: 0, lastSequenceVersion: 8);

        Assert.Equal(1L, bm.SnapshotsAbandoned);
        Assert.Equal(1L, bm.SnapshotsRejectedStaleVersion);
    }

    // ── #19 — strict version gate: null/0/mismatch must be rejected ────────────

    [Fact]
    public void Bug19_AfterVersionChange_NullLastSequenceVersion_MustBeRejected()
    {
        var (bm, reg, _) = CreatePerSymbol();
        const ulong secId = 555;

        // Channel observes the new SequenceVersion (V2). All books / state reset.
        bm.OnSequenceVersionChanged(newVersion: 2);

        // An in-flight V1 snapshot arrives without LastSequenceVersion populated
        // (or with the null sentinel). lastRptSeq is HUGE (V1 numbering — what
        // was current at the rollover moment). If this leaks through, baseline
        // becomes huge, and every V2 live message at small rpt will be silently
        // dropped by the Healthy-baseline >= received guard until rpt catches up.
        bm.OnSnapshotHeaderForTest(secId, lastRptSeq: 1_000_000, ordersExpected: 0, lastSequenceVersion: null);

        Assert.Equal(SymbolState.Unknown, reg.GetState(secId, SymbolGapKind.Mbo));
        Assert.Equal(1L, bm.SnapshotsRejectedStaleVersion);
        // The would-be poisoned baseline must not have been written.
        Assert.False(bm.Books.ContainsKey(secId) && bm.Books[secId].LastRptSeq == 1_000_000u);
    }

    [Fact]
    public void Bug19_AfterVersionChange_FutureLastSequenceVersion_MustBeRejected()
    {
        var (bm, reg, _) = CreatePerSymbol();
        const ulong secId = 556;

        bm.OnSequenceVersionChanged(newVersion: 2);
        // A snapshot tagged with a FUTURE version (publisher bug or out-of-order
        // version transition) cannot be trusted against our current epoch.
        bm.OnSnapshotHeaderForTest(secId, lastRptSeq: 50, ordersExpected: 0, lastSequenceVersion: 3);

        Assert.Equal(SymbolState.Unknown, reg.GetState(secId, SymbolGapKind.Mbo));
        Assert.Equal(1L, bm.SnapshotsRejectedStaleVersion);
    }

    [Fact]
    public void Bug19_AfterVersionChange_MatchingLastSequenceVersion_StillAccepted()
    {
        var (bm, reg, _) = CreatePerSymbol();
        const ulong secId = 557;

        bm.OnSequenceVersionChanged(newVersion: 2);
        bm.OnSnapshotHeaderForTest(secId, lastRptSeq: 7, ordersExpected: 0, lastSequenceVersion: 2);

        // Sanity: legitimate same-version snapshot must still heal.
        Assert.Equal(SymbolState.Healthy, reg.GetState(secId, SymbolGapKind.Mbo));
        Assert.Equal(0L, bm.SnapshotsRejectedStaleVersion);
    }
}
