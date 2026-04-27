using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins the <see cref="BookManager.SnapshotsAbortedByEpoch"/> counter
/// (recovery improvement #26): epoch resets that drop in-flight snapshots
/// must be observable as an operational metric, distinct from
/// <see cref="BookManager.SnapshotsAbandoned"/> which counts per-symbol
/// header-replacement loss.
/// </summary>
public class EpochAbortedSnapshotTests
{
    private static (BookManager bm, SymbolStateRegistry reg, StaleMboBuffer buf) CreatePerSymbol()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);
        return (bm, reg, buf);
    }

    [Fact]
    public void EpochReset_CountsOnlyIncomplete_NonSkippedPendings()
    {
        var (bm, _, _) = CreatePerSymbol();

        // Symbol A: header + 0/2 chunks  → INCOMPLETE → counts.
        bm.BeginSnapshotHeader(1, lastRptSeq: 100, hasRptSeq: true, ordersExpected: 2);
        // Symbol B: header + 1/2 chunks  → INCOMPLETE → counts.
        bm.BeginSnapshotHeader(2, lastRptSeq: 200, hasRptSeq: true, ordersExpected: 2);
        bm.RecordSnapshotChunkForTest(2, ordersInChunk: 1);
        // Symbol C: header + 2/2 chunks  → COMPLETE → does NOT count.
        bm.BeginSnapshotHeader(3, lastRptSeq: 300, hasRptSeq: true, ordersExpected: 2);
        bm.RecordSnapshotChunkForTest(3, ordersInChunk: 2);

        // Sanity: no aborts yet.
        Assert.Equal(0, bm.SnapshotsAbortedByEpoch);

        bm.OnSequenceReset();

        Assert.Equal(2, bm.SnapshotsAbortedByEpoch);
        Assert.Equal(1, bm.EpochResets);
    }

    [Fact]
    public void NoPendings_EpochReset_DoesNotMoveCounter()
    {
        var (bm, _, _) = CreatePerSymbol();

        bm.OnSequenceReset();
        bm.OnSequenceReset();

        Assert.Equal(0, bm.SnapshotsAbortedByEpoch);
        Assert.Equal(2, bm.EpochResets);
    }

    [Fact]
    public void SequentialEpochResets_CounterIsCumulative()
    {
        var (bm, _, _) = CreatePerSymbol();

        bm.BeginSnapshotHeader(10, lastRptSeq: 10, hasRptSeq: true, ordersExpected: 3);
        bm.OnSequenceReset();
        Assert.Equal(1, bm.SnapshotsAbortedByEpoch);

        bm.BeginSnapshotHeader(11, lastRptSeq: 11, hasRptSeq: true, ordersExpected: 3);
        bm.BeginSnapshotHeader(12, lastRptSeq: 12, hasRptSeq: true, ordersExpected: 3);
        bm.OnSequenceReset();
        Assert.Equal(3, bm.SnapshotsAbortedByEpoch);
    }
}
