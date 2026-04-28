using B3.Umdf.Book;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins ring-buffer semantics for <see cref="RecoveryEventLog"/>: bounded
/// capacity, newest-first snapshot order, monotonic <c>TotalRecorded</c>,
/// eviction of oldest when capacity is exceeded.
/// </summary>
public class RecoveryEventLogTests
{
    [Fact]
    public void Snapshot_NewestFirst()
    {
        var log = new RecoveryEventLog(capacity: 8);
        for (int i = 0; i < 5; i++)
            log.Record(MakeEvent(i));

        var snap = log.Snapshot();

        Assert.Equal(5, snap.Length);
        Assert.Equal(4L, snap[0].TimestampUnixMs);
        Assert.Equal(0L, snap[4].TimestampUnixMs);
        Assert.Equal(5L, log.TotalRecorded);
    }

    [Fact]
    public void Capacity_Overflow_EvictsOldest()
    {
        var log = new RecoveryEventLog(capacity: 4);
        for (int i = 0; i < 10; i++)
            log.Record(MakeEvent(i));

        var snap = log.Snapshot();

        Assert.Equal(4, snap.Length);
        // Newest is index=9, oldest retained is index=6 (i=0..5 evicted).
        Assert.Equal(9L, snap[0].TimestampUnixMs);
        Assert.Equal(6L, snap[3].TimestampUnixMs);
        Assert.Equal(10L, log.TotalRecorded);
    }

    [Fact]
    public void Snapshot_RespectsLimit()
    {
        var log = new RecoveryEventLog(capacity: 100);
        for (int i = 0; i < 50; i++)
            log.Record(MakeEvent(i));

        var snap = log.Snapshot(max: 5);

        Assert.Equal(5, snap.Length);
        Assert.Equal(49L, snap[0].TimestampUnixMs);
        Assert.Equal(45L, snap[4].TimestampUnixMs);
    }

    [Fact]
    public void Empty_SnapshotIsEmpty()
    {
        var log = new RecoveryEventLog(capacity: 8);
        Assert.Empty(log.Snapshot());
        Assert.Equal(0L, log.TotalRecorded);
    }

    private static RecoveryEvent MakeEvent(long ts) =>
        new(ts, RecoveryEventKind.InstrumentReplaced, GroupId: 1,
            SecurityId: 42UL, SnapshotRptSeq: null, PriorRptSeq: null, Detail: null);
}
