using System.Runtime.InteropServices;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public sealed class FeedHandler : IDisposable
{
    private readonly IPacketSource _source;
    private readonly ChannelHandler _incrementalHandler;
    private readonly IFeedEventHandler _eventHandler;
    private CancellationTokenSource? _cts;
    private Task? _runTask;

    private FeedState _state = FeedState.WaitInstrumentDefinition;
    private readonly Queue<UmdfPacket> _incrementalQueue = new();
    private long _packetCount;

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

    public FeedHandler(IPacketSource source, IFeedEventHandler eventHandler)
    {
        _source = source;
        _eventHandler = eventHandler;
        _incrementalHandler = new ChannelHandler(eventHandler);
    }

    public Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
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
                var packet = await _source.ReceiveAsync(ct);
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

    private void HandlePacket(in UmdfPacket packet)
    {
        switch (_state)
        {
            case FeedState.WaitInstrumentDefinition:
                HandleWaitInstrumentDefinition(in packet);
                break;

            case FeedState.WaitSnapshot:
                HandleWaitSnapshot(in packet);
                break;

            case FeedState.CatchUp:
                // Should not receive new packets in catch-up (it's a drain of the queue)
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
                _incrementalQueue.Enqueue(packet);
                break;

            // Ignore snapshot during this phase
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
                _incrementalQueue.Enqueue(packet);
                break;

            // Ignore instrument definition during this phase
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
                // In real-time, we still dispatch these for the event handler
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
                _incrementalQueue.Enqueue(packet);
                break;
        }
    }

    private long _instrDefPacketCount;
    private long _instrDefMsgCount;
    private long _instrDefParseFailCount;

    private void DispatchAndTrackInstrDef(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (span.Length < UmdfPacketHeader.Size)
            return;

        _instrDefPacketCount++;

        ref readonly var header = ref UmdfPacketHeader.Read(span);
        int offset = UmdfPacketHeader.Size;

        if (_instrDefPacketCount <= 3)
            Console.WriteLine($"[InstrDef] Packet #{_instrDefPacketCount}: SeqNum={header.SequenceNumber} MsgCount={header.MessageCount} DataLen={span.Length}");

        for (int i = 0; i < header.MessageCount; i++)
        {
            if (offset + MessageDispatcher.SbeHeaderSize > span.Length)
                break;

            ushort blockLength = MemoryMarshal.Read<ushort>(span[offset..]);
            ushort templateId = MemoryMarshal.Read<ushort>(span[(offset + 2)..]);
            ushort schemaId = MemoryMarshal.Read<ushort>(span[(offset + 4)..]);
            ushort version = MemoryMarshal.Read<ushort>(span[(offset + 6)..]);

            _instrDefMsgCount++;

            if (_instrDefMsgCount <= 5)
                Console.WriteLine($"[InstrDef]   Msg #{_instrDefMsgCount}: templateId={templateId} blockLen={blockLength} schemaId={schemaId} version={version} offset={offset}");

            var messageSpan = span[offset..];
            _eventHandler.OnPacket(in packet, messageSpan, templateId);

            // Track SecurityDefinition_12 to detect end of cycle
            if (templateId == 12) // SecurityDefinition
            {
                var body = messageSpan[MessageDispatcher.SbeHeaderSize..];
                TrackSecurityDefinition(body);
            }

            offset += MessageDispatcher.SbeHeaderSize + blockLength;
        }
    }

    private void TrackSecurityDefinition(ReadOnlySpan<byte> body)
    {
        if (!B3.Umdf.Mbo.Sbe.V16.SecurityDefinition_12Data.TryParse(body, out var reader))
        {
            _instrDefParseFailCount++;
            if (_instrDefParseFailCount <= 3)
                Console.WriteLine($"[InstrDef] TryParse FAILED (bodyLen={body.Length})");
            return;
        }

        ref readonly var msg = ref reader.Data;

        _instrDefReceived++;

        if (_instrDefReceived <= 3)
            Console.WriteLine($"[InstrDef] Parsed #{_instrDefReceived}: SecurityID={(ulong)msg.SecurityID} TotNoRelatedSym={msg.TotNoRelatedSym}");

        if (_instrDefTotalExpected == 0)
            _instrDefTotalExpected = msg.TotNoRelatedSym;

        if (_instrDefReceived >= _instrDefTotalExpected && _instrDefTotalExpected > 0)
        {
            Console.WriteLine($"[FeedHandler] Instrument definitions complete: {_instrDefReceived}/{_instrDefTotalExpected}");
            TransitionTo(FeedState.WaitSnapshot);
        }
    }

    private void DispatchAndTrackSnapshot(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (span.Length < UmdfPacketHeader.Size)
            return;

        ref readonly var header = ref UmdfPacketHeader.Read(span);

        // Snapshot stream cycles continuously. SeqNum=1 marks start of a cycle.
        // When we see SeqNum=1 after having already consumed a cycle, it's complete.
        if (header.SequenceNumber == 1)
        {
            if (_snapshotCycleStarted && _snapshotLastSeqNum > 0)
            {
                // We've consumed a full cycle — transition
                CompleteSnapshotCycle();
                // Don't return — also process this packet as part of the new cycle (or ignore)
                return;
            }
            _snapshotCycleStarted = true;
            _eventHandler.OnSnapshotStart();
        }

        if (!_snapshotCycleStarted)
            return; // Wait for start of cycle

        int offset = UmdfPacketHeader.Size;

        for (int i = 0; i < header.MessageCount; i++)
        {
            if (offset + MessageDispatcher.SbeHeaderSize > span.Length)
                break;

            ushort blockLength = MemoryMarshal.Read<ushort>(span[offset..]);
            ushort templateId = MemoryMarshal.Read<ushort>(span[(offset + 2)..]);

            var messageSpan = span[offset..];
            _eventHandler.OnPacket(in packet, messageSpan, templateId);

            // Track SnapshotFullRefresh_Header_30 for LastMsgSeqNumProcessed
            if (templateId == 30) // SnapshotFullRefresh_Header
            {
                var body = messageSpan[MessageDispatcher.SbeHeaderSize..];
                TrackSnapshotHeader(body);
            }

            offset += MessageDispatcher.SbeHeaderSize + blockLength;
        }
    }

    private void TrackSnapshotHeader(ReadOnlySpan<byte> body)
    {
        if (!B3.Umdf.Mbo.Sbe.V16.SnapshotFullRefresh_Header_30Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;

        // Track the highest LastMsgSeqNumProcessed from the snapshot.
        // This tells us which incremental SeqNum the snapshot is consistent with.
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
        Console.WriteLine($"[FeedHandler] Snapshot complete. Catching up from SeqNum > {catchUpFrom}");

        _eventHandler.OnSnapshotComplete(catchUpFrom);

        TransitionTo(FeedState.CatchUp);

        // Reset incremental gap detector to expect SeqNum after the snapshot
        _incrementalHandler.CompleteRecovery(catchUpFrom + 1);

        // Drain the queue: discard packets with SeqNum <= catchUpFrom, process the rest
        int discarded = 0;
        int applied = 0;

        while (_incrementalQueue.TryDequeue(out var queued))
        {
            var span = queued.Data.Span;
            if (span.Length < UmdfPacketHeader.Size)
                continue;

            ref readonly var hdr = ref UmdfPacketHeader.Read(span);

            if (hdr.SequenceNumber <= catchUpFrom)
            {
                discarded++;
                continue;
            }

            _incrementalHandler.HandlePacket(in queued);
            applied++;
        }

        Console.WriteLine($"[FeedHandler] Catch-up done: {applied} applied, {discarded} discarded");
        TransitionTo(FeedState.RealTime);
    }

    private void TransitionTo(FeedState newState)
    {
        Console.WriteLine($"[FeedHandler] {_state} → {newState}");
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
