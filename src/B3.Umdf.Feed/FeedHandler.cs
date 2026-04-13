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
    private readonly ILogger<FeedHandler> _logger;
    private CancellationTokenSource? _cts;
    private Task? _runTask;

    private volatile FeedState _state = FeedState.WaitInstrumentDefinition;
    private readonly Queue<UmdfPacket> _incrementalQueue = new();
    private const int MaxIncrementalQueueSize = 500_000;
    private long _packetCount;
    private long _lastPacketTicks;

    // Instrument Definition tracking
    private uint _instrDefTotalExpected;
    private uint _instrDefReceived;

    // Snapshot tracking
    private uint _snapshotLastSeqNum;
    private bool _snapshotCycleStarted;

    public FeedState State => _state;
    public long PacketCount => _packetCount;
    public long InstrDefPacketCount => _instrDefPacketCount;
    public uint InstrDefReceived => _instrDefReceived;
    public uint InstrDefTotalExpected => _instrDefTotalExpected;
    public ChannelHandler IncrementalHandler => _incrementalHandler;

    /// <summary>Ticks (DateTime.UtcNow.Ticks) of the last packet processed. 0 if none yet.</summary>
    public long LastPacketTicks => Volatile.Read(ref _lastPacketTicks);

    public FeedHandler(IPacketSource source, IFeedEventHandler eventHandler, ILogger<FeedHandler>? logger = null)
    {
        _source = source;
        _eventHandler = eventHandler;
        _logger = logger ?? NullLogger<FeedHandler>.Instance;
        _incrementalHandler = new ChannelHandler(eventHandler);
    }

    /// <summary>
    /// Creates a FeedHandler for external feeding (no owned source).
    /// Use FeedPacket() to push packets from an external loop.
    /// </summary>
    public FeedHandler(IFeedEventHandler eventHandler, ILogger<FeedHandler>? logger = null)
    {
        _source = null;
        _eventHandler = eventHandler;
        _logger = logger ?? NullLogger<FeedHandler>.Instance;
        _incrementalHandler = new ChannelHandler(eventHandler);
    }

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
                HandlePacket(in packet);
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
            HandlePacket(in packet);
        }
    }

    /// <summary>
    /// Push a packet from an external loop (used with MultiFeedManager).
    /// </summary>
    public void FeedPacket(in UmdfPacket packet)
    {
        _packetCount++;
        HandlePacket(in packet);
    }

    private void HandlePacket(in UmdfPacket packet)
    {
        Volatile.Write(ref _lastPacketTicks, DateTime.UtcNow.Ticks);

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
                    TransitionTo(FeedState.Recovery);
                break;

            case ChannelType.InstrumentDefinition:
            case ChannelType.SnapshotRecovery:
                MessageDispatcher.Dispatch(in packet, _eventHandler);
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
                EnqueueCopy(in packet);
                break;
        }
    }

    private long _instrDefPacketCount;

    /// <summary>
    /// Enqueues an incremental packet for later replay during catch-up.
    /// Drops oldest if the queue exceeds the capacity limit to bound memory usage.
    /// </summary>
    private void EnqueueCopy(in UmdfPacket packet)
    {
        if (_incrementalQueue.Count >= MaxIncrementalQueueSize)
            _incrementalQueue.Dequeue();
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

        // Snapshot stream cycles continuously. SeqNum=1 marks start of a cycle.
        if (pktHeader.SequenceNumber == 1)
        {
            if (_snapshotCycleStarted && _snapshotLastSeqNum > 0)
            {
                CompleteSnapshotCycle();
                return;
            }
            _snapshotCycleStarted = true;
            _eventHandler.OnSnapshotStart();
        }

        if (!_snapshotCycleStarted)
            return;

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
        if (lastProcessed > _snapshotLastSeqNum)
            _snapshotLastSeqNum = lastProcessed;
    }

    /// <summary>
    /// Called when a snapshot cycle completes (SeqNum wraps back to 1).
    /// Transitions to CatchUp → RealTime.
    /// </summary>
    public void CompleteSnapshotCycle()
    {
        if (_state is not (FeedState.WaitSnapshot or FeedState.Recovery))
            return;

        uint catchUpFrom = _snapshotLastSeqNum;
        _logger.LogInformation("Snapshot complete. Catching up from SeqNum > {CatchUpFrom}", catchUpFrom);

        _eventHandler.OnSnapshotComplete(catchUpFrom);

        TransitionTo(FeedState.CatchUp);

        _incrementalHandler.CompleteRecovery(catchUpFrom + 1);

        int discarded = 0;
        int applied = 0;

        while (_incrementalQueue.TryDequeue(out var queued))
        {
            if (!queued.TryGetHeader(out var hdr))
                continue;

            if (hdr.SequenceNumber <= catchUpFrom)
            {
                discarded++;
                continue;
            }

            _incrementalHandler.HandlePacket(in queued);
            applied++;
        }

        _logger.LogInformation("Catch-up done: {Applied} applied, {Discarded} discarded", applied, discarded);
        TransitionTo(FeedState.RealTime);
    }

    private void TransitionTo(FeedState newState)
    {
        _logger.LogInformation("{OldState} → {NewState}", _state, newState);
        _state = newState;

        if (newState == FeedState.Recovery)
        {
            _snapshotCycleStarted = false;
            _snapshotLastSeqNum = 0;
        }
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
    }
}
