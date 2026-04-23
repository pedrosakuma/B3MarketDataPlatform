using System.Runtime.InteropServices;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Feed;

public sealed class FeedHandler : IDisposable
{
    private readonly IPacketSource? _source;
    private readonly ChannelHandler _incrementalHandler;
    private readonly IFeedEventHandler _eventHandler;
    private readonly IFeedEventHandler? _marketDataHandler;
    private readonly ILogger<FeedHandler> _logger;
    private CancellationTokenSource? _cts;
    private Task? _runTask;

    private volatile FeedState _state = FeedState.WaitInstrumentDefinition;
    private readonly Queue<UmdfPacket> _incrementalQueue = new();
    /// <summary>
    /// Max incrementals deferred during recovery before we start dropping the oldest.
    /// Default 50,000 packets (≈ 75 MB pinned per FeedHandler) was chosen to bound the
    /// pinned footprint when recovery storms spin in tight loops; the original
    /// 500,000 cap (≈ 750 MB per group, 1.5 GB across both groups) was enough to OOM a
    /// 4 GB container when the publisher sustained > consumer drain rate.
    /// At 30k pkt/s the default covers ~1.6 s of incrementals — enough to bridge a
    /// typical snapshot recovery cycle for the 18k-symbol universe.
    /// Operators that publish at extreme rates (e.g. PCAP replay at SpeedMultiplier=0)
    /// can raise this via the constructor / UMDF_INCREMENTAL_RECOVERY_QUEUE_CAPACITY env.
    /// </summary>
    public const int DefaultIncrementalRecoveryQueueCapacity = 50_000;
    private readonly int _maxIncrementalQueueSize;
    private long _packetCount;
    private long _lastPacketTicks;
    private long _incrementalQueueDroppedPackets;

    // Instrument Definition tracking
    private uint _instrDefTotalExpected;
    private uint _instrDefReceived;

    // Snapshot tracking — use MIN(LastMsgSeqNumProcessed) across all instruments
    // so NO instrument loses incrementals that weren't in its snapshot.
    private uint _snapshotMinSeqNum;
    private uint _snapshotMaxSeqNum;
    private bool _snapshotCycleStarted;
    private bool _snapshotBoundaryFound;   // true after we've seen at least one seqVer change
    private ushort _snapshotSeqVer;

    // Snapshot reset debounce: when a catch-up fails and we transition back to
    // Recovery, fully resetting the snapshot tracker forces us to skip another
    // (potentially partial) cycle before collecting a clean one — costing up to
    // ~100s for an 18k-symbol universe. If the previous Recovery was very recent
    // we keep _snapshotBoundaryFound=true so the next seqVer flip is treated as
    // a fresh cycle boundary directly.
    private const long SnapshotResetDebounceMs = 30_000;
    private long _lastRecoveryEnteredTicks;
    private long _snapshotResetsDebounced;

    public FeedState State => _state;
    public long PacketCount => _packetCount;
    public long InstrDefPacketCount => _instrDefPacketCount;
    public uint InstrDefReceived => _instrDefReceived;
    public uint InstrDefTotalExpected => _instrDefTotalExpected;
    public ChannelHandler IncrementalHandler => _incrementalHandler;
    public long IncrementalQueueDroppedPackets => Volatile.Read(ref _incrementalQueueDroppedPackets);
    /// <summary>
    /// Number of times we transitioned back to Recovery soon after a previous
    /// Recovery and avoided resetting the snapshot cycle tracker, so the
    /// already-in-flight snapshot stream could be reused for catch-up.
    /// </summary>
    public long SnapshotResetsDebounced => Volatile.Read(ref _snapshotResetsDebounced);

    /// <summary>Fires after the state machine transitions. Args are (oldState, newState). Invoked synchronously on the worker thread.</summary>
    public event Action<FeedState, FeedState>? StateChanged;

    /// <summary>TickCount64 (milliseconds since process start) of the last packet processed. 0 if none yet.</summary>
    public long LastPacketTicks => Volatile.Read(ref _lastPacketTicks);

    public FeedHandler(IPacketSource source, IFeedEventHandler eventHandler, ILogger<FeedHandler>? logger = null, IFeedEventHandler? marketDataHandler = null, int incrementalRecoveryQueueCapacity = DefaultIncrementalRecoveryQueueCapacity, RecoveryMode recoveryMode = RecoveryMode.Channel)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(incrementalRecoveryQueueCapacity, 1);
        _source = source;
        _eventHandler = eventHandler;
        _marketDataHandler = marketDataHandler;
        _logger = logger ?? NullLogger<FeedHandler>.Instance;
        _incrementalHandler = new ChannelHandler(eventHandler);
        _maxIncrementalQueueSize = incrementalRecoveryQueueCapacity;
        _recoveryMode = recoveryMode;
    }

    /// <summary>
    /// Creates a FeedHandler for external feeding (no owned source).
    /// Use FeedPacket() to push packets from an external loop.
    /// </summary>
    public FeedHandler(IFeedEventHandler eventHandler, ILogger<FeedHandler>? logger = null, IFeedEventHandler? marketDataHandler = null, int incrementalRecoveryQueueCapacity = DefaultIncrementalRecoveryQueueCapacity, RecoveryMode recoveryMode = RecoveryMode.Channel)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(incrementalRecoveryQueueCapacity, 1);
        _source = null;
        _eventHandler = eventHandler;
        _marketDataHandler = marketDataHandler;
        _logger = logger ?? NullLogger<FeedHandler>.Instance;
        _incrementalHandler = new ChannelHandler(eventHandler);
        _maxIncrementalQueueSize = incrementalRecoveryQueueCapacity;
        _recoveryMode = recoveryMode;
    }

    private readonly RecoveryMode _recoveryMode;
    private long _perSymbolGapsAbsorbed;

    /// <summary>The recovery mode this FeedHandler is operating in.</summary>
    public RecoveryMode RecoveryMode => _recoveryMode;

    /// <summary>
    /// PerSymbol mode only: number of channel-level gaps that were absorbed
    /// by advancing past the missing SeqNum without entering Recovery state.
    /// Per-symbol routing is responsible for healing affected instruments.
    /// </summary>
    public long PerSymbolGapsAbsorbed => Volatile.Read(ref _perSymbolGapsAbsorbed);

    public Task StartAsync(CancellationToken ct = default)
    {
        if (_source is null)
            throw new InvalidOperationException("Cannot start a FeedHandler without a packet source. Use FeedPacket() for externally-fed handlers.");

        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        if (_source is ISyncPacketSource syncSource)
            _runTask = Task.Run(() => RunSyncLoop(syncSource, _cts.Token));
        else
            _runTask = RunLoop(_cts.Token);

        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _cts?.Cancel();
        if (_runTask is not null)
        {
            try { await _runTask; }
            catch (OperationCanceledException) { }
        }
    }

    /// <summary>
    /// Waits for the run loop to complete (e.g. when the packet source is exhausted).
    /// </summary>
    public async Task WaitForCompletionAsync()
    {
        if (_runTask is not null)
            await _runTask;
    }

    private async Task RunLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var packet = await _source!.ReceiveAsync(ct);
                _packetCount++;
                ProcessOwnedPacket(in packet);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (System.Threading.Channels.ChannelClosedException)
            {
                break;
            }
        }
    }

    private void RunSyncLoop(ISyncPacketSource source, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && source.TryReceive(out var packet))
        {
            _packetCount++;
            ProcessOwnedPacket(in packet);
        }
    }

    /// <summary>
    /// Push a packet from an external loop (used with MultiFeedManager).
    /// </summary>
    public void FeedPacket(in UmdfPacket packet)
    {
        _packetCount++;
        ProcessOwnedPacket(in packet);
    }

    private void ProcessOwnedPacket(in UmdfPacket packet)
    {
        try
        {
            HandlePacket(in packet);
        }
        finally
        {
            packet.Release();
        }
    }

    private void HandlePacket(in UmdfPacket packet)
    {
        Volatile.Write(ref _lastPacketTicks, packet.ReceivedTimestampTicks);

        switch (_state)
        {
            case FeedState.WaitInstrumentDefinition:
                HandleWaitInstrumentDefinition(in packet);
                break;

            case FeedState.WaitSnapshot:
                HandleWaitSnapshot(in packet);
                break;

            case FeedState.CatchUp:
                break;

            case FeedState.RealTime:
                HandleRealTime(in packet);
                break;

            case FeedState.Recovery:
                HandleRecovery(in packet);
                break;
        }
    }

    private void HandleWaitInstrumentDefinition(in UmdfPacket packet)
    {
        switch (packet.Channel)
        {
            case ChannelType.InstrumentDefinition:
                DispatchAndTrackInstrDef(in packet);
                break;

            case ChannelType.IncrementalA:
            case ChannelType.IncrementalB:
                DispatchMarketDataPassthrough(in packet);
                EnqueueCopy(in packet);
                break;
        }
    }

    private void HandleWaitSnapshot(in UmdfPacket packet)
    {
        switch (packet.Channel)
        {
            case ChannelType.SnapshotRecovery:
                DispatchAndTrackSnapshot(in packet);
                break;

            case ChannelType.IncrementalA:
            case ChannelType.IncrementalB:
                DispatchMarketDataPassthrough(in packet);
                EnqueueCopy(in packet);
                break;
        }
    }

    private void HandleRealTime(in UmdfPacket packet)
    {
        switch (packet.Channel)
        {
            case ChannelType.IncrementalA:
            case ChannelType.IncrementalB:
                var result = _incrementalHandler.HandlePacket(in packet);
                if (result == GapResult.Gap)
                {
                    if (_recoveryMode == RecoveryMode.PerSymbol)
                    {
                        // Channel-level gap is absorbed: dispatch the packet so its
                        // contents reach the per-symbol gating layer, then advance
                        // past the missing SeqNum. Symbols affected by the missing
                        // packet will go Stale via SymbolStateRegistry and self-heal
                        // when the next snapshot for them arrives. Channel state
                        // remains RealTime (continuous snapshot consumption below).
                        _incrementalHandler.AcceptGapAndAdvance(in packet);
                        Interlocked.Increment(ref _perSymbolGapsAbsorbed);
                        if (_logger.IsEnabled(LogLevel.Information))
                            _logger.LogInformation(
                                "PerSymbol: absorbed channel gap on {Channel} (expected={Expected} received={Received}); per-symbol routing will heal affected instruments.",
                                packet.Channel,
                                _incrementalHandler.LastGapExpected,
                                _incrementalHandler.LastGapReceived);
                        break;
                    }

                    EnqueueCopy(in packet);
                    // Reorder buffer was holding future packets that now belong
                    // to the catch-up window; transfer them into the deferred
                    // queue so the snapshot+catch-up cycle replays them.
                    foreach (var buffered in _incrementalHandler.DrainReorderBuffer())
                        EnqueueOwned(in buffered);
                    _logger.LogWarning(
                        "Gap detected on {Channel}: expected SeqNum {Expected}, received {Received}. Deferring packet and entering recovery.",
                        packet.Channel,
                        _incrementalHandler.LastGapExpected,
                        _incrementalHandler.LastGapReceived);
                    TransitionTo(FeedState.Recovery);
                }
                break;

            case ChannelType.InstrumentDefinition:
                // Keep processing instrument definitions for symbol/market data updates
                MessageDispatcher.Dispatch(in packet, _eventHandler);
                _eventHandler.OnPacketProcessed();
                break;

            case ChannelType.SnapshotRecovery:
                // PerSymbol mode keeps the snapshot stream always-on so individual
                // Stale instruments can heal without triggering a channel-wide
                // Recovery cycle. Channel mode ignores snapshots while in RealTime
                // (incremental stream is the source of truth).
                if (_recoveryMode == RecoveryMode.PerSymbol)
                    DispatchAndTrackSnapshot(in packet);
                break;
        }
    }

    private void HandleRecovery(in UmdfPacket packet)
    {
        switch (packet.Channel)
        {
            case ChannelType.SnapshotRecovery:
                DispatchAndTrackSnapshot(in packet);
                break;

            case ChannelType.IncrementalA:
            case ChannelType.IncrementalB:
                DispatchMarketDataPassthrough(in packet);
                EnqueueCopy(in packet);
                break;
        }
    }

    private long _instrDefPacketCount;

    /// <summary>
    /// Dispatches an incremental packet to the market data passthrough handler.
    /// Used during WaitInstrumentDefinition, WaitSnapshot, and catch-up to preserve
    /// market statistics (OpeningPrice, TradeVolume, etc.) that the B3 snapshot does not contain.
    /// </summary>
    private void DispatchMarketDataPassthrough(in UmdfPacket packet)
    {
        if (_marketDataHandler is null)
            return;
        MessageDispatcher.Dispatch(in packet, _marketDataHandler);
    }

    /// <summary>
    /// Enqueues an incremental packet for later replay during catch-up.
    /// Drops oldest if the queue exceeds the capacity limit to bound memory usage.
    /// </summary>
    private void EnqueueCopy(in UmdfPacket packet)
    {
        if (_incrementalQueue.Count >= _maxIncrementalQueueSize
            && _incrementalQueue.TryDequeue(out var dropped))
        {
            dropped.Release();
            var droppedCount = Interlocked.Increment(ref _incrementalQueueDroppedPackets);
            if (droppedCount == 1 || droppedCount % 10_000 == 0)
            {
                _logger.LogWarning(
                    "Incremental recovery queue overflow: dropped {DroppedPackets} packets (capacity {Capacity})",
                    droppedCount,
                    _maxIncrementalQueueSize);
            }
        }

        packet.Retain();
        _incrementalQueue.Enqueue(packet);
    }

    /// <summary>
    /// Like <see cref="EnqueueCopy"/> but for packets the caller already owns
    /// (e.g. drained from the reorder buffer where Retain() was called on stash).
    /// Does NOT retain again.
    /// </summary>
    private void EnqueueOwned(in UmdfPacket packet)
    {
        if (_incrementalQueue.Count >= _maxIncrementalQueueSize
            && _incrementalQueue.TryDequeue(out var dropped))
        {
            dropped.Release();
            var droppedCount = Interlocked.Increment(ref _incrementalQueueDroppedPackets);
            if (droppedCount == 1 || droppedCount % 10_000 == 0)
            {
                _logger.LogWarning(
                    "Incremental recovery queue overflow: dropped {DroppedPackets} packets (capacity {Capacity})",
                    droppedCount,
                    _maxIncrementalQueueSize);
            }
        }

        _incrementalQueue.Enqueue(packet);
    }

    private void DispatchAndTrackInstrDef(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (!PacketHeader.TryParse(span, out var pktHeader, out _))
            return;

        _instrDefPacketCount++;

        int offset = PacketHeader.MESSAGE_SIZE;

        while (offset + FramingHeader.MESSAGE_SIZE + MessageDispatcher.SbeHeaderSize <= span.Length)
        {
            var framingSlice = span[offset..];
            if (!FramingHeader.TryParse(framingSlice, out var framing, out _))
                break;

            if (framing.MessageLength < FramingHeader.MESSAGE_SIZE + MessageDispatcher.SbeHeaderSize)
                break;

            if (offset + framing.MessageLength > span.Length)
                break;

            var sbeSlice = span[(offset + FramingHeader.MESSAGE_SIZE)..];
            ushort templateId = MemoryMarshal.Read<ushort>(sbeSlice[2..]);

            _eventHandler.OnPacket(in packet, sbeSlice, templateId);

            if (templateId == SecurityDefinition_12Data.MESSAGE_ID)
            {
                var body = sbeSlice[MessageDispatcher.SbeHeaderSize..];
                TrackSecurityDefinition(body);
            }

            offset += framing.MessageLength;
        }
    }

    private void TrackSecurityDefinition(ReadOnlySpan<byte> body)
    {
        if (!SecurityDefinition_12Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        _instrDefReceived++;

        if (_instrDefTotalExpected == 0)
            _instrDefTotalExpected = msg.TotNoRelatedSym;

        if (_instrDefReceived >= _instrDefTotalExpected && _instrDefTotalExpected > 0)
        {
            _logger.LogInformation("Instrument definitions complete: {Received}/{Total}", _instrDefReceived, _instrDefTotalExpected);
            _eventHandler.OnInstrumentDefinitionsComplete((int)_instrDefTotalExpected);
            TransitionTo(FeedState.WaitSnapshot);
        }
    }

    private void DispatchAndTrackSnapshot(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (!PacketHeader.TryParse(span, out var pktHeader, out _))
            return;

        // B3 UMDF snapshot uses SequenceVersion to identify cycles.
        // We must skip the first cycle (potentially partial if we joined mid-stream)
        // and process the SECOND complete cycle from boundary to boundary.

        if (!_snapshotCycleStarted)
        {
            // First packet ever — record seqVer and wait for boundary
            _snapshotCycleStarted = true;
            _snapshotSeqVer = pktHeader.SequenceVersion;
            return;
        }

        if (!_snapshotBoundaryFound)
        {
            if (pktHeader.SequenceVersion == _snapshotSeqVer)
                return; // Still in first (potentially partial) cycle — skip

            // SequenceVersion changed — this is a clean cycle boundary.
            _snapshotBoundaryFound = true;
            _snapshotSeqVer = pktHeader.SequenceVersion;
            _snapshotMinSeqNum = 0;
            _snapshotMaxSeqNum = 0;
            _eventHandler.OnSnapshotStart();
            // Fall through to process this packet (start of new complete cycle)
        }
        else if (pktHeader.SequenceVersion != _snapshotSeqVer)
        {
            // Processing a cycle and seqVer changed — cycle boundary
            if (_snapshotMaxSeqNum > 0)
            {
                // Previous cycle had snapshot data — it's complete
                if (CompleteSnapshotCycle())
                    return;

                _snapshotSeqVer = pktHeader.SequenceVersion;
                _snapshotMinSeqNum = 0;
                _snapshotMaxSeqNum = 0;
                _eventHandler.OnSnapshotStart();
            }
            else
            {
                // Empty cycle (heartbeats only) — reset for next cycle
                _snapshotSeqVer = pktHeader.SequenceVersion;
                _snapshotMinSeqNum = 0;
                _snapshotMaxSeqNum = 0;
                _eventHandler.OnSnapshotStart();
            }
        }

        // Process all SBE messages in this packet
        int offset = PacketHeader.MESSAGE_SIZE;

        while (offset + FramingHeader.MESSAGE_SIZE + MessageDispatcher.SbeHeaderSize <= span.Length)
        {
            var framingSlice = span[offset..];
            if (!FramingHeader.TryParse(framingSlice, out var framing, out _))
                break;

            if (framing.MessageLength < FramingHeader.MESSAGE_SIZE + MessageDispatcher.SbeHeaderSize)
                break;

            if (offset + framing.MessageLength > span.Length)
                break;

            var sbeSlice = span[(offset + FramingHeader.MESSAGE_SIZE)..];
            ushort templateId = MemoryMarshal.Read<ushort>(sbeSlice[2..]);

            _eventHandler.OnPacket(in packet, sbeSlice, templateId);

            if (templateId == SnapshotFullRefresh_Header_30Data.MESSAGE_ID)
            {
                var body = sbeSlice[MessageDispatcher.SbeHeaderSize..];
                TrackSnapshotHeader(body);
            }

            offset += framing.MessageLength;
        }
    }

    private void TrackSnapshotHeader(ReadOnlySpan<byte> body)
    {
        if (!SnapshotFullRefresh_Header_30Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        uint lastProcessed = (uint)msg.LastMsgSeqNumProcessed;

        if (_snapshotMinSeqNum == 0 || lastProcessed < _snapshotMinSeqNum)
            _snapshotMinSeqNum = lastProcessed;
        if (lastProcessed > _snapshotMaxSeqNum)
            _snapshotMaxSeqNum = lastProcessed;
    }

    /// <summary>
    /// Called when a snapshot cycle completes (SeqNum wraps back to 1).
    /// Transitions to CatchUp → RealTime.
    /// </summary>
    public bool CompleteSnapshotCycle()
    {
        if (_state is not (FeedState.WaitSnapshot or FeedState.Recovery))
            return false;

        uint catchUpFrom = _snapshotMinSeqNum;
        uint nextExpected = catchUpFrom + 1;

        if (TryGetEarliestQueuedSequence(out var earliestQueuedSeq) && earliestQueuedSeq > nextExpected)
        {
            _logger.LogWarning(
                "Snapshot complete but earliest retained incremental SeqNum is {EarliestQueuedSeq} while catch-up needs {NextExpected}. Waiting for a newer snapshot cycle.",
                earliestQueuedSeq,
                nextExpected);
            return false;
        }

        _logger.LogInformation(
            "Snapshot complete. MinSeq={MinSeq}, MaxSeq={MaxSeq}. Catching up from SeqNum > {CatchUpFrom}",
            _snapshotMinSeqNum, _snapshotMaxSeqNum, catchUpFrom);

        _eventHandler.OnSnapshotComplete(catchUpFrom);

        TransitionTo(FeedState.CatchUp);

        _incrementalHandler.CompleteRecovery(nextExpected);

        int discarded = 0;
        int applied = 0;

        while (_incrementalQueue.TryDequeue(out var queued))
        {
            bool requeuedForRecovery = false;
            try
            {
                if (!queued.TryGetHeader(out var hdr))
                    continue;

                if (hdr.SequenceNumber <= catchUpFrom)
                {
                    // Snapshot only contains order book state, not market statistics.
                    // Dispatch discarded packets to market data handler so OpeningPrice,
                    // ExecutionStatistics (TradeVolume), HighPrice etc. are not lost.
                    DispatchMarketDataPassthrough(in queued);
                    discarded++;
                    continue;
                }

                var result = _incrementalHandler.HandlePacket(in queued);
                if (result == GapResult.Gap)
                {
                    RequeueIncrementalForRecovery(in queued);
                    requeuedForRecovery = true;
                    _logger.LogWarning(
                        "Gap detected during catch-up on {Channel}: expected SeqNum {Expected}, received {Received}. Restarting recovery with pending incrementals preserved.",
                        queued.Channel,
                        _incrementalHandler.LastGapExpected,
                        _incrementalHandler.LastGapReceived);
                    TransitionTo(FeedState.Recovery);
                    return false;
                }

                applied++;
            }
            finally
            {
                if (!requeuedForRecovery)
                    queued.Release();
            }
        }

        _logger.LogInformation("Catch-up done: {Applied} applied, {Discarded} discarded", applied, discarded);
        TransitionTo(FeedState.RealTime);
        return true;
    }

    private void RequeueIncrementalForRecovery(in UmdfPacket current)
    {
        var pending = new List<UmdfPacket>();
        while (_incrementalQueue.TryDequeue(out var queued))
            pending.Add(queued);

        _incrementalQueue.Enqueue(current);

        foreach (var queued in pending)
            _incrementalQueue.Enqueue(queued);
    }

    private bool TryGetEarliestQueuedSequence(out uint sequenceNumber)
    {
        while (_incrementalQueue.Count > 0)
        {
            var packet = _incrementalQueue.Peek();
            if (packet.TryGetHeader(out var header))
            {
                sequenceNumber = header.SequenceNumber;
                return true;
            }

            _incrementalQueue.Dequeue().Release();
            _logger.LogWarning("Dropped queued incremental with invalid packet header while preparing catch-up.");
        }

        sequenceNumber = 0;
        return false;
    }

    private void TransitionTo(FeedState newState)
    {
        var oldState = _state;
        _logger.LogInformation("{OldState} → {NewState}", oldState, newState);
        _state = newState;

        if (newState == FeedState.Recovery)
        {
            long now = Environment.TickCount64;
            long elapsedSinceLast = _lastRecoveryEnteredTicks > 0
                ? now - _lastRecoveryEnteredTicks
                : long.MaxValue;
            _lastRecoveryEnteredTicks = now;

            // Always clear the in-progress sequence range so the next cycle is
            // collected freshly. Keep the cycle/boundary flags only if we just
            // came out of Recovery very recently AND had already locked onto a
            // boundary — that lets the next seqVer flip serve as a clean
            // cycle start without paying for a skipped partial cycle.
            _snapshotMinSeqNum = 0;
            _snapshotMaxSeqNum = 0;

            if (elapsedSinceLast <= SnapshotResetDebounceMs && _snapshotBoundaryFound)
            {
                Interlocked.Increment(ref _snapshotResetsDebounced);
                _logger.LogInformation(
                    "Recovery debounce: keeping snapshot boundary lock (elapsed={ElapsedMs}ms, seqVer={SeqVer}). Next cycle boundary will be reused.",
                    elapsedSinceLast, _snapshotSeqVer);
            }
            else
            {
                _snapshotCycleStarted = false;
                _snapshotBoundaryFound = false;
                _snapshotSeqVer = 0;
            }
        }

        try { StateChanged?.Invoke(oldState, newState); }
        catch (Exception ex) { _logger.LogError(ex, "StateChanged handler threw"); }
    }

    /// <summary>For testing: force the state machine into a specific state.</summary>
    internal void SetStateForTesting(FeedState state) => _state = state;

    /// <summary>For testing: invoke the real state transition (with all side effects).</summary>
    internal void TransitionToForTesting(FeedState state) => TransitionTo(state);

    /// <summary>For testing: seed snapshot tracking so catch-up can be exercised deterministically.</summary>
    internal void SetSnapshotRangeForTesting(uint minSeqNum, uint maxSeqNum)
    {
        _snapshotMinSeqNum = minSeqNum;
        _snapshotMaxSeqNum = maxSeqNum;
    }

    /// <summary>For testing: simulate the snapshot tracker having locked onto a clean cycle boundary.</summary>
    internal void SetSnapshotBoundaryFoundForTesting()
    {
        _snapshotCycleStarted = true;
        _snapshotBoundaryFound = true;
        _snapshotSeqVer = 1;
    }

    internal bool SnapshotBoundaryFoundForTesting => _snapshotBoundaryFound;

    public void Dispose()
    {
        while (_incrementalQueue.TryDequeue(out var packet))
            packet.Release();

        _incrementalHandler.Dispose();

        _cts?.Cancel();
        _cts?.Dispose();
    }
}
