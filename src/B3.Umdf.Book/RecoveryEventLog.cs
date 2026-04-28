using System.Threading;

namespace B3.Umdf.Book;

/// <summary>
/// Categorical kind of recovery event. Stable wire enum — append only,
/// do not renumber (consumers may filter by integer value).
/// </summary>
public enum RecoveryEventKind
{
    /// <summary>FeedHandler entered Recovery (channel-level gap detected).</summary>
    ChannelRecoveryEntered = 1,
    /// <summary>FeedHandler returned to Streaming (channel-level recovery completed).</summary>
    ChannelRecoveryExited = 2,
    /// <summary>Per-symbol epoch reset due to SecurityID identity change (P12-3).</summary>
    InstrumentReplaced = 3,
    /// <summary>SequenceVersion bumped — global per-channel epoch reset.</summary>
    SequenceVersionRollover = 4,
    /// <summary>Forced/authoritative heal accepted (per-symbol stuck-Stale escape).</summary>
    ForcedHealAccepted = 5,
}

/// <summary>
/// Single immutable recovery audit-trail entry. Designed to be cheap to
/// allocate (struct-of-primitives + a single string), so the log can be
/// emitted from latency-sensitive paths.
/// </summary>
public readonly record struct RecoveryEvent(
    long TimestampUnixMs,
    RecoveryEventKind Kind,
    int GroupId,
    ulong? SecurityId,
    long? SnapshotRptSeq,
    long? PriorRptSeq,
    string? Detail);

/// <summary>
/// Bounded ring buffer of <see cref="RecoveryEvent"/> entries. Fixed-size,
/// lock-free reads, lock-protected writes. Thread-safe.
///
/// Capacity defaults to 256: large enough for ops to investigate a recent
/// incident, small enough to keep allocation quiet (4 KB at 16 B/entry).
/// </summary>
public sealed class RecoveryEventLog
{
    private readonly RecoveryEvent[] _ring;
    private readonly Lock _gate = new();
    private long _nextIndex;

    /// <summary>
    /// Total events ever recorded (monotonic). The ring may have evicted
    /// older entries — <see cref="Snapshot"/> reflects only the most recent
    /// <see cref="Capacity"/> events.
    /// </summary>
    public long TotalRecorded => Volatile.Read(ref _nextIndex);

    public int Capacity => _ring.Length;

    public RecoveryEventLog(int capacity = 256)
    {
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
        _ring = new RecoveryEvent[capacity];
    }

    public void Record(RecoveryEvent ev)
    {
        lock (_gate)
        {
            _ring[(int)(_nextIndex % _ring.Length)] = ev;
            _nextIndex++;
        }
    }

    /// <summary>
    /// Returns up to <paramref name="max"/> most recent events, newest first.
    /// Allocates a fresh array — intended for low-frequency endpoints, not
    /// the hot path.
    /// </summary>
    public RecoveryEvent[] Snapshot(int max = int.MaxValue)
    {
        lock (_gate)
        {
            int n = (int)Math.Min(_nextIndex, _ring.Length);
            int take = Math.Min(n, max);
            var result = new RecoveryEvent[take];
            // Newest first: walk the ring backward from (_nextIndex - 1).
            for (int i = 0; i < take; i++)
            {
                long idx = _nextIndex - 1 - i;
                result[i] = _ring[(int)(idx % _ring.Length)];
            }
            return result;
        }
    }
}
