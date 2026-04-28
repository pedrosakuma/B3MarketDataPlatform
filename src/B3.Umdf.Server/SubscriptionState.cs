namespace B3.Umdf.Server;

/// <summary>
/// Per-client subscription metadata for one security.
/// </summary>
internal readonly record struct SubscriptionState(DataFlags Flags, long MinBroadcastSequenceExclusive)
{
    public bool WantsBookBatch(long batchSequence) =>
        (Flags & DataFlags.Book) != 0 && batchSequence > MinBroadcastSequenceExclusive;

    public bool WantsInfo => (Flags & DataFlags.Info) != 0;

    public bool WantsNews => (Flags & DataFlags.News) != 0;

    public SubscriptionState WithMinBroadcastSequence(long sequence) =>
        this with { MinBroadcastSequenceExclusive = sequence };
}
