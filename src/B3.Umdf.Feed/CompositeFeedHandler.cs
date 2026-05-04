using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

/// <summary>
/// Multiplexes feed events to multiple handlers.
/// Generic fan-out via IFeedEventHandler[]; specialized hot-path composites
/// (e.g. for the canonical book + market-data + symbol-registry triple) live in
/// the consuming layer (B3.Umdf.Book.OptimizedFeedComposite) to avoid pulling
/// concrete handler types into this lower layer.
/// </summary>
public sealed class CompositeFeedHandler : IFeedEventHandler
{
    private readonly IFeedEventHandler[] _handlers;
    private long _delegateExceptionCount;

    public CompositeFeedHandler(params IFeedEventHandler[] handlers)
    {
        _handlers = handlers;
    }

    /// <summary>
    /// Total number of exceptions thrown by any delegate handler across all
    /// fan-out methods. A misbehaving delegate must NOT prevent its peers from
    /// observing the same event, so each invocation is isolated in try/catch.
    /// </summary>
    public long DelegateExceptionCount => Volatile.Read(ref _delegateExceptionCount);

    private void RecordFailure(Exception ex, string method, ref bool firstReported)
    {
        Interlocked.Increment(ref _delegateExceptionCount);
        if (!firstReported)
        {
            firstReported = true;
            // First failure per fan-out cycle is surfaced via Trace so a host
            // that wires console redirection sees it; downstream consumers that
            // care attach to the counter for monitoring. Avoid pulling in ILogger
            // here to keep this lower-layer composite dependency-free.
            System.Diagnostics.Trace.TraceWarning(
                "CompositeFeedHandler delegate threw in {0}: {1}", method, ex);
        }
    }

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.OnPacket(in packet, sbePayload, templateId); }
            catch (Exception ex) { RecordFailure(ex, nameof(OnPacket), ref reported); }
        }
    }

    public void OnSequenceReset()
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.OnSequenceReset(); }
            catch (Exception ex) { RecordFailure(ex, nameof(OnSequenceReset), ref reported); }
        }
    }

    public void OnInstrumentDefinitionsComplete(int instrumentCount)
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.OnInstrumentDefinitionsComplete(instrumentCount); }
            catch (Exception ex) { RecordFailure(ex, nameof(OnInstrumentDefinitionsComplete), ref reported); }
        }
    }

    public void OnInstrumentDefinitionsComplete(int instrumentCount, bool wasAborted)
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.OnInstrumentDefinitionsComplete(instrumentCount, wasAborted); }
            catch (Exception ex) { RecordFailure(ex, nameof(OnInstrumentDefinitionsComplete), ref reported); }
        }
    }

    public void OnSnapshotStart(int channelGroupId, ulong securityId)
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.OnSnapshotStart(channelGroupId, securityId); }
            catch (Exception ex) { RecordFailure(ex, nameof(OnSnapshotStart), ref reported); }
        }
    }

    public void OnSnapshotComplete(int channelGroupId, ulong securityId)
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.OnSnapshotComplete(channelGroupId, securityId); }
            catch (Exception ex) { RecordFailure(ex, nameof(OnSnapshotComplete), ref reported); }
        }
    }

    public void OnPacketProcessed()
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.OnPacketProcessed(); }
            catch (Exception ex) { RecordFailure(ex, nameof(OnPacketProcessed), ref reported); }
        }
    }

    public void OnSequenceVersionChanged(ushort newVersion)
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.OnSequenceVersionChanged(newVersion); }
            catch (Exception ex) { RecordFailure(ex, nameof(OnSequenceVersionChanged), ref reported); }
        }
    }

    public void FlushIfDue()
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.FlushIfDue(); }
            catch (Exception ex) { RecordFailure(ex, nameof(FlushIfDue), ref reported); }
        }
    }

    public void FlushNow()
    {
        bool reported = false;
        foreach (var handler in _handlers)
        {
            try { handler.FlushNow(); }
            catch (Exception ex) { RecordFailure(ex, nameof(FlushNow), ref reported); }
        }
    }
}
