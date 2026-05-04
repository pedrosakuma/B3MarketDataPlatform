namespace B3.Umdf.Feed;

/// <summary>
/// Source of an <see cref="IFeedEventHandler.OnSequenceReset(int, SequenceResetReason)"/>
/// invocation. Mirrors the two B3 UMDF mid-session reset templates so downstream
/// consumers can distinguish them in metrics and reset policies.
/// </summary>
public enum SequenceResetReason : byte
{
    /// <summary>
    /// Source not specified (legacy <see cref="IFeedEventHandler.OnSequenceReset()"/>
    /// callers that have not been migrated to the parameterised overload).
    /// </summary>
    Unspecified = 0,

    /// <summary>
    /// SBE template <c>SequenceReset_1</c> (MESSAGE_ID = 1) — gap-recovery
    /// sequence reset / restart of the instrument-definition or snapshot loop.
    /// </summary>
    SequenceReset = 1,

    /// <summary>
    /// SBE template <c>ChannelReset_11</c> (MESSAGE_ID = 11) — channel-wide
    /// catastrophic reset (remove all instruments, empty all books and
    /// statistics).
    /// </summary>
    ChannelReset = 2,
}
