namespace B3.Umdf.Book;

/// <summary>
/// Reason for a <see cref="SnapshotApplier.Clear(SnapshotClearReason)"/> call.
/// Surfaced to the metrics layer so dashboards can distinguish epoch-driven
/// pending-snapshot loss from per-instrument replacement.
/// </summary>
public enum SnapshotClearReason : byte
{
    /// <summary>Caller did not specify a reason (legacy <c>Clear()</c> overload).</summary>
    Unspecified = 0,
    /// <summary>B3 spec §6.5.5.1 — SequenceVersion increment (weekly rollover or failover).</summary>
    SequenceVersionChanged = 1,
    /// <summary>ChannelReset_11 — channel-wide catastrophic reset.</summary>
    ChannelReset = 2,
    /// <summary>SequenceReset_1 — gap-recovery sequence reset.</summary>
    SequenceReset = 3,
}
