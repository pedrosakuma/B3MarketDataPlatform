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
    private long _packetCount;
    private long _lastPacketTicks;

    // Instrument Definition tracking
    private uint _instrDefTotalExpected;
    private uint _instrDefReceived;

    public FeedState State => _state;
    public long PacketCount => _packetCount;
    public long InstrDefPacketCount => _instrDefPacketCount;
    public uint InstrDefReceived => _instrDefReceived;
    public uint InstrDefTotalExpected => _instrDefTotalExpected;
    public ChannelHandler IncrementalHandler => _incrementalHandler;

    /// <summary>Fires after the state machine transitions. Args are (oldState, newState). Invoked synchronously on the worker thread.</summary>
    public event Action<FeedState, FeedState>? StateChanged;

    /// <summary>TickCount64 (milliseconds since process start) of the last packet processed. 0 if none yet.</summary>
    public long LastPacketTicks => Volatile.Read(ref _lastPacketTicks);

    public FeedHandler(IPacketSource source, IFeedEventHandler eventHandler, ILogger<FeedHandler>? logger = null, IFeedEventHandler? marketDataHandler = null)
    {
        _source = source;
        _eventHandler = eventHandler;
        _marketDataHandler = marketDataHandler;
        _logger = logger ?? NullLogger<FeedHandler>.Instance;
        _incrementalHandler = new ChannelHandler(eventHandler);
    }

    /// <summary>
    /// Creates a FeedHandler for external feeding (no owned source).
    /// Use FeedPacket() to push packets from an external loop.
    /// </summary>
    public FeedHandler(IFeedEventHandler eventHandler, ILogger<FeedHandler>? logger = null, IFeedEventHandler? marketDataHandler = null)
    {
        _source = null;
        _eventHandler = eventHandler;
        _marketDataHandler = marketDataHandler;
        _logger = logger ?? NullLogger<FeedHandler>.Instance;
        _incrementalHandler = new ChannelHandler(eventHandler);
    }

    private long _perSymbolGapsAbsorbed;

    /// <summary>
    /// Number of channel-level gaps that were absorbed by advancing past the
    /// missing SeqNum without dropping the FeedHandler out of Streaming.
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

            case FeedState.Streaming:
                HandleStreaming(in packet);
                break;
        }
    }

    private void HandleWaitInstrumentDefinition(in UmdfPacket packet)
    {
        // Only InstrumentDefinition is meaningful before metadata is loaded.
        // Incrementals and Snapshots arriving in this window are discarded:
        // each symbol bootstraps independently from the snapshot stream once
        // we transition to Streaming. Pre-bootstrap incrementals cannot be
        // decoded (no SecDef → no symbol/decimals/multipliers).
        if (packet.Channel == ChannelType.InstrumentDefinition)
            DispatchAndTrackInstrDef(in packet);
    }

    private void HandleStreaming(in UmdfPacket packet)
    {
        switch (packet.Channel)
        {
            case ChannelType.IncrementalA:
            case ChannelType.IncrementalB:
                var result = _incrementalHandler.HandlePacket(in packet);
                if (result == GapResult.Gap)
                {
                    // Channel-level gap: dispatch the packet through per-symbol
                    // gating (rptSeq detects the per-instrument gap on
                    // BookManager / MarketDataManager) and advance past the
                    // missing SeqNum. Affected symbols self-heal when the next
                    // matching snapshot arrives.
                    _incrementalHandler.AcceptGapAndAdvance(in packet);
                    Interlocked.Increment(ref _perSymbolGapsAbsorbed);
                    if (_logger.IsEnabled(LogLevel.Information))
                        _logger.LogInformation(
                            "Absorbed channel gap on {Channel} (expected={Expected} received={Received}); per-symbol routing will heal affected instruments.",
                            packet.Channel,
                            _incrementalHandler.LastGapExpected,
                            _incrementalHandler.LastGapReceived);
                }
                break;

            case ChannelType.InstrumentDefinition:
                // Mid-session new listings: continue tracking SecDefs so the
                // dispatcher can decode them. New symbols enter the registry as
                // Unknown → Stale → Healthy via the normal per-symbol flow.
                MessageDispatcher.Dispatch(in packet, _eventHandler);
                _eventHandler.OnPacketProcessed();
                break;

            case ChannelType.SnapshotRecovery:
                // Always-on snapshot consumption: heals individual Stale symbols
                // (and bootstraps newly-Unknown symbols) without ever blocking
                // the channel. Each (Header_30, Orders_71) pair is self-contained
                // and applies independently — no cycle gating required.
                DispatchSnapshotMessages(in packet);
                break;
        }
    }

    private long _instrDefPacketCount;

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

        if (_instrDefReceived >= _instrDefTotalExpected && _instrDefTotalExpected > 0
            && _state == FeedState.WaitInstrumentDefinition)
        {
            _logger.LogInformation("Instrument definitions complete: {Received}/{Total}", _instrDefReceived, _instrDefTotalExpected);
            // Notifies BookManager / MarketDataManager so they can FreezeBooks /
            // FreezeData (universe metadata is now stable). After this transition
            // the feed is fully Streaming; per-symbol heal drives all bootstrap
            // and gap-recovery work.
            _eventHandler.OnInstrumentDefinitionsComplete((int)_instrDefTotalExpected);
            TransitionTo(FeedState.Streaming);
        }
    }

    /// <summary>
    /// Dispatch every SBE message in a snapshot packet directly to the event
    /// handler. Per-symbol heal does not depend on cycle boundaries — each
    /// (Header_30, Orders_71) pair is self-contained and applies independently
    /// to one instrument at a time.
    /// </summary>
    private void DispatchSnapshotMessages(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (!PacketHeader.TryParse(span, out _, out _))
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

            offset += framing.MessageLength;
        }
    }

    private void TransitionTo(FeedState newState)
    {
        var oldState = _state;
        _logger.LogInformation("{OldState} → {NewState}", oldState, newState);
        _state = newState;

        try { StateChanged?.Invoke(oldState, newState); }
        catch (Exception ex) { _logger.LogError(ex, "StateChanged handler threw"); }
    }

    /// <summary>For testing: force the state machine into a specific state.</summary>
    internal void SetStateForTesting(FeedState state) => _state = state;

    /// <summary>For testing: invoke the real state transition (with all side effects).</summary>
    internal void TransitionToForTesting(FeedState state) => TransitionTo(state);

    public void Dispose()
    {
        _incrementalHandler.Dispose();

        _cts?.Cancel();
        _cts?.Dispose();
    }
}
