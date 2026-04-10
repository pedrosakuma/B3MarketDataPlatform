namespace B3.Umdf.Feed;

/// <summary>
/// State machine for UMDF channel synchronization.
/// 
/// WAIT_INSTRUMENT_DEF → WAIT_SNAPSHOT → CATCH_UP → REAL_TIME
///                                                      ↓ (gap)
///                                                  RECOVERY → WAIT_SNAPSHOT
/// </summary>
public enum FeedState
{
    /// <summary>
    /// Initial state. Consuming Instrument Definition stream from start to end.
    /// Incremental packets are queued but not processed.
    /// </summary>
    WaitInstrumentDefinition,

    /// <summary>
    /// Instrument definitions loaded. Consuming Snapshot Recovery stream from start to end.
    /// Incremental packets are queued but not processed.
    /// </summary>
    WaitSnapshot,

    /// <summary>
    /// Snapshot consumed. Replaying queued incrementals that have SeqNum > snapshot's LastMsgSeqNumProcessed.
    /// </summary>
    CatchUp,

    /// <summary>
    /// Fully synchronized. Processing incremental packets in real time.
    /// </summary>
    RealTime,

    /// <summary>
    /// Gap detected in incremental stream. Waiting for snapshot to resynchronize.
    /// Incremental packets are queued.
    /// </summary>
    Recovery
}
