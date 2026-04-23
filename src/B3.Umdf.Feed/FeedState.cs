namespace B3.Umdf.Feed;

/// <summary>
/// Channel-level state machine for UMDF feed bootstrap.
///
/// WaitInstrumentDefinition → (all SecDefs received) → Streaming
///
/// Per-symbol bootstrap, gap detection, and healing are owned by the
/// SymbolStateRegistry / BookManager / MarketDataManager layer (Unknown →
/// Stale → Healthy). The channel-level state machine no longer reacts to
/// gaps or snapshots: a gap is absorbed (per-symbol routing flips affected
/// instruments to Stale), and snapshots heal Stale symbols continuously
/// without interrupting Streaming.
/// </summary>
public enum FeedState
{
    /// <summary>
    /// Initial state. Consuming Instrument Definition stream from start to end.
    /// Incremental and snapshot packets received in this window are discarded
    /// (cannot be decoded without metadata; the snapshot stream will heal each
    /// symbol once Streaming begins).
    /// </summary>
    WaitInstrumentDefinition,

    /// <summary>
    /// Universe metadata is loaded. Incremental, snapshot, and instrument
    /// definition packets are all processed. Per-symbol layer drives bootstrap
    /// (Unknown → Stale → Healthy as snapshots arrive) and gap recovery.
    /// </summary>
    Streaming,
}
