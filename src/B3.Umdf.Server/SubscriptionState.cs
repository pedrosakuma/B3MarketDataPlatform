using System.Threading;

namespace B3.Umdf.Server;

/// <summary>
/// Per-client subscription metadata for one security.
///
/// <para>Hybrid mutability model:</para>
/// <list type="bullet">
///   <item><description><see cref="Flags"/> is immutable after construction. Flag changes
///     (Subscribe/Unsubscribe) replace the entire object inside a copy-on-write of the
///     containing <c>Dictionary&lt;string, SubscriptionState&gt;</c> under <c>_subLock</c>.
///     This preserves the snapshot model for membership and flag identity — readers
///     iterating a captured dictionary snapshot see a consistent flag set.</description></item>
///   <item><description>The book broadcast cutoff (<see cref="MinBroadcastSequenceExclusive"/>)
///     is a mutable cell, advanced lock-free via <see cref="AdvanceMinBroadcastSequence"/>.
///     CoW snapshots share the same <c>SubscriptionState</c> reference for unchanged
///     subscriptions, so a cutoff advance is intentionally visible to broadcasters
///     iterating any snapshot. This is required for the cutoff to act as a sequence
///     barrier after a snapshot/Get.</description></item>
/// </list>
/// </summary>
internal sealed class SubscriptionState
{
    public DataFlags Flags { get; }

    private long _minBroadcastSequenceExclusive;

    public SubscriptionState(DataFlags flags, long minBroadcastSequenceExclusive)
    {
        Flags = flags;
        _minBroadcastSequenceExclusive = minBroadcastSequenceExclusive;
    }

    public long MinBroadcastSequenceExclusive => Volatile.Read(ref _minBroadcastSequenceExclusive);

    /// <summary>
    /// Monotonically advance the broadcast cutoff. Lock-free CAS-max: only updates if
    /// <paramref name="sequence"/> is strictly greater than the current value. Multiple
    /// concurrent advances are safe; a stale <paramref name="sequence"/> is silently
    /// ignored. In practice all callers serialize through the owning group dispatch
    /// thread, but the CAS-max keeps the contract robust against accidental concurrent
    /// updates from new code paths.
    /// </summary>
    public void AdvanceMinBroadcastSequence(long sequence)
    {
        while (true)
        {
            long current = Volatile.Read(ref _minBroadcastSequenceExclusive);
            if (sequence <= current) return;
            if (Interlocked.CompareExchange(ref _minBroadcastSequenceExclusive, sequence, current) == current)
                return;
        }
    }

    public bool WantsBookBatch(long batchSequence) =>
        (Flags & DataFlags.Book) != 0 && batchSequence > Volatile.Read(ref _minBroadcastSequenceExclusive);

    /// <summary>True iff this subscription wants the MBP (price-level) stream and the
    /// given batch is past the snapshot cutoff. MBP shares the same broadcast cutoff
    /// as Book — both originate from the same per-packet batch sequence.</summary>
    public bool WantsMbpBatch(long batchSequence) =>
        (Flags & DataFlags.Mbp) != 0 && batchSequence > Volatile.Read(ref _minBroadcastSequenceExclusive);

    public bool WantsMbp => (Flags & DataFlags.Mbp) != 0;

    public bool WantsInfo => (Flags & DataFlags.Info) != 0;

    public bool WantsNews => (Flags & DataFlags.News) != 0;
}
