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
    /// Variant of <see cref="OnSequenceReset()"/> that surfaces the originating
    /// <paramref name="channelGroupId"/> and <paramref name="reason"/> so
    /// downstream consumers can tag metrics / dashboards by reset source. The
    /// default implementation forwards to the legacy zero-arg overload to keep
    /// existing implementers source-compatible. Wired by
    /// <see cref="MessageDispatcher"/> when an SBE template id matching one of
    /// the UMDF reset templates (SequenceReset_1, ChannelReset_11) is decoded
    /// on any channel.
    /// </summary>
    void OnSequenceReset(int channelGroupId, SequenceResetReason reason)
        => OnSequenceReset();

    /// <summary>
    /// Fired once when all instrument definitions have been received and the
    /// channel transitions to Streaming. Used by managers to freeze metadata
    /// dictionaries (FreezeBooks / FreezeData).
    /// </summary>
    void OnInstrumentDefinitionsComplete(int instrumentCount);

    /// <summary>
    /// Variant of <see cref="OnInstrumentDefinitionsComplete(int)"/> that
    /// distinguishes a normal end-of-replay completion (<paramref name="wasAborted"/>=false)
    /// from a forced bootstrap-abort fallback (<paramref name="wasAborted"/>=true,
    /// fired when <c>InstrDefStuckTimeoutMs</c> elapses without a real completion
    /// message). The default implementation forwards to the legacy single-arg
    /// overload so existing implementers remain source-compatible; handlers that
    /// need to flag aborted-bootstrap state to operators or downstream metrics
    /// should override this method instead.
    /// </summary>
    void OnInstrumentDefinitionsComplete(int instrumentCount, bool wasAborted)
        => OnInstrumentDefinitionsComplete(instrumentCount);

    /// <summary>
    /// Fired when a snapshot recovery batch begins for a single security on the
    /// snapshot channel (i.e. when a <c>SnapshotFullRefresh_Header_30</c> is
    /// observed). Default no-op preserves legacy behaviour for handlers that do
    /// not care about per-symbol snapshot lifecycle.
    /// </summary>
    void OnSnapshotStart(int channelGroupId, ulong securityId) { }

    /// <summary>
    /// Fired when the snapshot recovery batch for a single security finishes.
    /// The current implementation emits this once per snapshot packet that
    /// contained a <c>Header_30</c>, with the last observed securityId. Default
    /// no-op preserves legacy behaviour.
    /// </summary>
    void OnSnapshotComplete(int channelGroupId, ulong securityId) { }

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
