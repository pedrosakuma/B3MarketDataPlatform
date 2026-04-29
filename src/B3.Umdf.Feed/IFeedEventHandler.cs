using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public interface IFeedEventHandler
{
    void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId);

    /// <summary>
    /// Channel-level catastrophic reset (SequenceReset_1 / ChannelReset_11):
    /// flips every per-symbol entry to Stale so the next snapshot cycle
    /// re-Healthifies symbols progressively.
    /// </summary>
    void OnSequenceReset();

    /// <summary>
    /// Fired once when all instrument definitions have been received and the
    /// channel transitions to Streaming. Used by managers to freeze metadata
    /// dictionaries (FreezeBooks / FreezeData).
    /// </summary>
    void OnInstrumentDefinitionsComplete(int instrumentCount);

    /// <summary>
    /// Called after all SBE messages in a UMDF packet have been dispatched.
    /// Used as a batch boundary for upstream conflation.
    /// </summary>
    void OnPacketProcessed() { }

    /// <summary>
    /// Fired when the channel observes a SequenceVersion increment in the
    /// PacketHeader (B3 spec §6.5.5.1 — weekly rollover or failover event).
    /// SequenceNumber resets to 1 in the new version, so per-symbol epoch
    /// state must be reset (books cleared, state registry reset, stat
    /// rptSeq watermarks zeroed). Implementations should treat this as
    /// equivalent to <see cref="OnSequenceReset"/>.
    /// </summary>
    void OnSequenceVersionChanged(ushort newVersion) { }

    /// <summary>
    /// Optional hook for handlers that defer wire fan-out (e.g. server-side
    /// temporal conflation window). Called by the dispatch loop on idle wakeups
    /// (no packets pending). Implementations should flush only if the configured
    /// window has elapsed since the first dirty event since the last flush.
    /// Default no-op preserves legacy behavior.
    /// </summary>
    void FlushIfDue() { }

    /// <summary>
    /// Unconditional flush invoked by the dispatch loop at shutdown to prevent
    /// silently dropping the last conflation window of buffered events. Default
    /// no-op preserves legacy behavior.
    /// </summary>
    void FlushNow() { }
}
