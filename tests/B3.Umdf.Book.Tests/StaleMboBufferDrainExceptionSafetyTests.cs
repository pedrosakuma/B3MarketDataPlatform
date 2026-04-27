using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins exception-safety of <see cref="StaleMboBuffer.Drain"/>. If the apply
/// callback throws (e.g. a downstream replay handler bug), Drain MUST:
///   1. release every pooled buffer it took ownership of (no array leak),
///   2. preserve future entries (rptSeq &gt; drainTo) for the next drain,
///   3. keep <c>TotalBytes</c> consistent with the post-drain state,
///   4. not leave the symbol's queue in an internally inconsistent state.
/// Pre-fix: a throw mid-loop leaked the current and remaining matched
/// buffers, lost future-window entries, and corrupted TotalBytes — a single
/// bad replay could silently destroy the symbol's recovery window.
/// </summary>
public class StaleMboBufferDrainExceptionSafetyTests
{
    private static StaleMboBuffer NewBuffer() =>
        new(NullLogger.Instance, perSymbolCap: 1024, globalByteCap: 64L * 1024 * 1024, hotPerSymbolCap: 65536);

    [Fact]
    public void Drain_ApplyThrowsOnSecond_PreservesFutureEntries_AndKeepsTotalBytesConsistent()
    {
        var buf = NewBuffer();
        // 5 items: 3 in window [10..12], 2 future [20, 21].
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(1, 50, rptSeq: 11, 0, new byte[] { 11 });
        buf.Enqueue(1, 50, rptSeq: 12, 0, new byte[] { 12 });
        buf.Enqueue(1, 50, rptSeq: 20, 0, new byte[] { 20, 20 });
        buf.Enqueue(1, 50, rptSeq: 21, 0, new byte[] { 21, 21 });
        Assert.Equal(7, buf.TotalBytes);

        var applied = new List<uint>();
        Action<StaleMboBuffer.DeferredMboMsg> apply = m =>
        {
            applied.Add(m.RptSeq);
            if (m.RptSeq == 11)
                throw new InvalidOperationException("simulated replay handler bug");
        };

        Assert.Throws<InvalidOperationException>(() => buf.Drain(1, drainFrom: 10, drainTo: 12, apply));

        // The first message must have been applied; the throwing one was
        // attempted; rptSeq=12 was never invoked (apply order is 10, 11, 12).
        Assert.Equal(new uint[] { 10, 11 }, applied);

        // Future entries (rptSeq 20, 21) MUST still be available for the
        // next drain — they were never invoked.
        var laterApplied = new List<uint>();
        var n = buf.Drain(1, drainFrom: 20, drainTo: 21, m => laterApplied.Add(m.RptSeq));
        Assert.Equal(2, n);
        Assert.Equal(new uint[] { 20, 21 }, laterApplied);

        // After both drains, TotalBytes MUST be zero — every byte we ever
        // accounted for has been released. Pre-fix: leaked matched buffers
        // (rptSeq=12 never released) leave bytes pinned.
        Assert.Equal(0L, buf.TotalBytes);
        Assert.Equal(0, buf.DepthOf(1));
    }

    [Fact]
    public void Drain_ApplyThrowsOnFirst_LeavesNoOrphanedQueueState()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(1, 50, rptSeq: 11, 0, new byte[] { 11 });
        buf.Enqueue(1, 50, rptSeq: 30, 0, new byte[] { 30 }); // future
        Assert.Equal(3, buf.TotalBytes);

        Assert.Throws<InvalidOperationException>(() =>
            buf.Drain(1, drainFrom: 10, drainTo: 11, _ => throw new InvalidOperationException()));

        // Subsequent drain of the surviving future entry must work cleanly,
        // proving the queue and byte counters are internally consistent.
        var applied = new List<uint>();
        var n = buf.Drain(1, drainFrom: 30, drainTo: 30, m => applied.Add(m.RptSeq));
        Assert.Equal(1, n);
        Assert.Equal(new uint[] { 30 }, applied);
        Assert.Equal(0L, buf.TotalBytes);
    }
}
