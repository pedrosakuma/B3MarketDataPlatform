using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins the contract of <see cref="StaleMboBuffer.BeginDrain"/> /
/// <see cref="StaleMboBuffer.DrainTransaction"/> (the P1 transactional drain
/// primitive). Compared to the legacy <see cref="StaleMboBuffer.Drain"/>
/// method, the transaction additionally guarantees:
/// <list type="number">
///   <item>Atomic restore of the protected floor on Rollback.</item>
///   <item>Atomic restore of below-floor (snapshot-covered) entries on
///   Rollback — legacy Drain dropped them irrecoverably.</item>
///   <item>Auto-Rollback on Dispose without Commit.</item>
///   <item>Single-thread/single-owner re-entrancy guard.</item>
/// </list>
/// </summary>
public class StaleMboBufferDrainTransactionTests
{
    private static StaleMboBuffer NewBuffer() =>
        new(NullLogger.Instance, perSymbolCap: 1024, globalByteCap: 64L * 1024 * 1024, hotPerSymbolCap: 65536);

    [Fact]
    public void Commit_AppliesAllEntries_ClearsFloor_BumpsDrainedCounter()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(1, 50, rptSeq: 11, 0, new byte[] { 11 });
        buf.Enqueue(1, 50, rptSeq: 12, 0, new byte[] { 12 });
        buf.Enqueue(1, 50, rptSeq: 20, 0, new byte[] { 20, 20 }); // future
        buf.SetProtectedFloor(1, 9);
        Assert.Equal(9u, buf.ProtectedFloorOf(1));
        long preDrainBytes = buf.TotalBytes;
        Assert.Equal(5, preDrainBytes);
        long preDrained = buf.DrainedCount;

        var applied = new List<uint>();
        using (var tx = buf.BeginDrain(1, drainFrom: 10, drainTo: 12))
        {
            Assert.Equal(3, tx.MatchCount);
            Assert.False(tx.IsEmpty);
            // BeginDrain captures-and-clears the floor for the duration.
            Assert.Equal(0u, buf.ProtectedFloorOf(1));
            int n = tx.Apply(m => applied.Add(m.RptSeq));
            Assert.Equal(3, n);
            tx.Commit();
        }

        Assert.Equal(new uint[] { 10, 11, 12 }, applied);
        Assert.Equal(0u, buf.ProtectedFloorOf(1)); // Commit leaves floor cleared
        Assert.Equal(preDrained + 3, buf.DrainedCount);
        // Future entry (20) survives.
        Assert.Equal(1, buf.DepthOf(1));
        Assert.Equal(2L, buf.TotalBytes); // only the 2-byte future remains
    }

    [Fact]
    public void Rollback_OnApplyThrow_RestoresBufferAndFloorExactly()
    {
        var buf = NewBuffer();
        // Mix of below-floor (5,9), in-window (10,11,12), future (20,21).
        buf.Enqueue(1, 50, rptSeq: 5, 0, new byte[] { 5 });
        buf.Enqueue(1, 50, rptSeq: 9, 0, new byte[] { 9 });
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(1, 50, rptSeq: 11, 0, new byte[] { 11 });
        buf.Enqueue(1, 50, rptSeq: 12, 0, new byte[] { 12 });
        buf.Enqueue(1, 50, rptSeq: 20, 0, new byte[] { 20, 20 });
        buf.Enqueue(1, 50, rptSeq: 21, 0, new byte[] { 21, 21 });
        buf.SetProtectedFloor(1, 10);

        long preDrainBytes = buf.TotalBytes;
        int preDepth = buf.DepthOf(1);
        long preDrained = buf.DrainedCount;
        uint preFloor = buf.ProtectedFloorOf(1);

        Assert.Throws<InvalidOperationException>(() =>
        {
            using var tx = buf.BeginDrain(1, drainFrom: 10, drainTo: 12);
            // Floor is captured-and-cleared while tx is open.
            Assert.Equal(0u, buf.ProtectedFloorOf(1));
            tx.Apply(m =>
            {
                if (m.RptSeq == 11) throw new InvalidOperationException("simulated");
            });
            tx.Commit(); // unreachable — Apply threw
        });

        // Post-rollback: EXACTLY pre-drain state.
        Assert.Equal(preDrainBytes, buf.TotalBytes);
        Assert.Equal(preDepth, buf.DepthOf(1));
        Assert.Equal(preDrained, buf.DrainedCount); // no advance — drain rolled back
        Assert.Equal(preFloor, buf.ProtectedFloorOf(1));

        // Subsequent successful drain should see all in-window entries again.
        var applied = new List<uint>();
        using (var tx2 = buf.BeginDrain(1, drainFrom: 10, drainTo: 12))
        {
            tx2.Apply(m => applied.Add(m.RptSeq));
            tx2.Commit();
        }
        Assert.Equal(new uint[] { 10, 11, 12 }, applied);
        // Future entries still there too.
        Assert.Equal(2, buf.DepthOf(1));
    }

    [Fact]
    public void Dispose_WithoutCommit_AutoRollsBack()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(1, 50, rptSeq: 11, 0, new byte[] { 11 });
        buf.SetProtectedFloor(1, 8);

        long preDrainBytes = buf.TotalBytes;

        // No throw, no Commit — Dispose alone must rollback.
        var applied = new List<uint>();
        using (var tx = buf.BeginDrain(1, drainFrom: 10, drainTo: 11))
        {
            tx.Apply(m => applied.Add(m.RptSeq));
            // intentionally no tx.Commit()
        }

        // Apply did execute — but rollback restores buffer state.
        Assert.Equal(new uint[] { 10, 11 }, applied);
        Assert.Equal(preDrainBytes, buf.TotalBytes);
        Assert.Equal(2, buf.DepthOf(1));
        Assert.Equal(8u, buf.ProtectedFloorOf(1));
    }

    [Fact]
    public void Commit_AfterApplyThrew_IsForbidden()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });

        using var tx = buf.BeginDrain(1, 10, 10);
        Assert.Throws<InvalidOperationException>(() =>
            tx.Apply(_ => throw new InvalidOperationException("boom")));
        Assert.Throws<InvalidOperationException>(() => tx.Commit());
        // Rollback is still allowed (and Dispose will call it).
        tx.Rollback();
        Assert.Equal(1, buf.DepthOf(1));
    }

    [Fact]
    public void Commit_WithUnappliedMatches_IsForbidden()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(1, 50, rptSeq: 11, 0, new byte[] { 11 });

        using var tx = buf.BeginDrain(1, 10, 11);
        // Caller never calls Apply — Commit must refuse to silently drop matches.
        Assert.Throws<InvalidOperationException>(() => tx.Commit());
    }

    [Fact]
    public void EmptyWindow_BeginDrain_IsNoop()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.SetProtectedFloor(1, 5);

        // drainTo < drainFrom → empty.
        using (var tx = buf.BeginDrain(1, drainFrom: 5, drainTo: 4))
        {
            Assert.Equal(0, tx.MatchCount);
            Assert.True(tx.IsEmpty);
            tx.Commit();
        }

        // Buffer untouched, floor untouched (no clear-and-restore happens
        // when the tx is empty — preserves the snapshot-in-flight semantics).
        Assert.Equal(1, buf.DepthOf(1));
        Assert.Equal(5u, buf.ProtectedFloorOf(1));
    }

    [Fact]
    public void NestedBeginDrain_OnSameThread_IsRefused()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(2, 50, rptSeq: 10, 0, new byte[] { 10 });

        using var outerTx = buf.BeginDrain(1, 10, 10);
        // Even for a different securityId, the single-owner contract refuses
        // re-entrant drain — the caller is single-threaded by class contract.
        Assert.Throws<InvalidOperationException>(() => buf.BeginDrain(2, 10, 10));
        outerTx.Rollback();

        // After outer Rollback the guard is released — fresh tx works.
        using var nextTx = buf.BeginDrain(2, 10, 10);
        nextTx.Apply(_ => { });
        nextTx.Commit();
    }

    [Fact]
    public void Rollback_RestoresBelowFloorEntries_NotDroppedLikeLegacyDrain()
    {
        // This is the key behavioral upgrade vs. the legacy Drain method:
        // entries with rptSeq < drainFrom (snapshot-covered) are released by
        // legacy Drain immediately, irrecoverable on apply throw. The
        // transaction MUST hold them and restore on rollback.
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 5, 0, new byte[] { 5 });
        buf.Enqueue(1, 50, rptSeq: 6, 0, new byte[] { 6 });
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        long preDrainBytes = buf.TotalBytes;

        Assert.Throws<InvalidOperationException>(() =>
        {
            using var tx = buf.BeginDrain(1, drainFrom: 10, drainTo: 10);
            tx.Apply(_ => throw new InvalidOperationException());
            tx.Commit();
        });

        // 5 and 6 must still be enqueued (rollback restored them).
        Assert.Equal(3, buf.DepthOf(1));
        Assert.Equal(preDrainBytes, buf.TotalBytes);
    }

    [Fact]
    public void Commit_DropsBelowFloorEntries()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 5, 0, new byte[] { 5 });
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });

        using (var tx = buf.BeginDrain(1, drainFrom: 10, drainTo: 10))
        {
            tx.Apply(_ => { });
            tx.Commit();
        }

        // Below-floor entry (5) was dropped on Commit (snapshot baseline covers it).
        Assert.Equal(0, buf.DepthOf(1));
        Assert.Equal(0L, buf.TotalBytes);
    }
}
