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
    private long _instrDefDuplicateCount;
    private long _instrDefMismatchedTotalCount;
    // Set of unique SecurityIDs already counted toward _instrDefReceived.
    // Bootstrap completion is gated on UNIQUE securities, not raw message
    // count, so a duplicate SecDef cannot prematurely complete and leave the
    // tail of the universe permanently unknown to downstream consumers.
    private readonly HashSet<ulong> _instrDefSeenSymbols = new();

    // Receive-time of the first SecurityDefinition_12 observed during bootstrap.
    // Powers the escape valve below; 0 until the first SecDef arrives.
    private long _instrDefFirstSeenTicks;
    private long _instrDefStuckEscapeFiredCount;

    /// <summary>
    /// Maximum time (in milliseconds, measured against
    /// <see cref="UmdfPacket.ReceivedTimestampTicks"/>) we will wait inside
    /// <see cref="FeedState.WaitInstrumentDefinition"/> after the first
    /// SecurityDefinition_12 arrives. Past this window we force-transition to
    /// <see cref="FeedState.Streaming"/> so that an upstream pathology (e.g.
    /// <c>TotNoRelatedSym=0</c> on every SecDef, or a SecDef stream that never
    /// completes) cannot stall the consumer forever. The bootstrap is then
    /// driven entirely by per-symbol heal once snapshots arrive.
    /// </summary>
    public const long InstrDefStuckTimeoutMs = 30_000;

    public FeedState State => _state;
    public long PacketCount => _packetCount;
    public long InstrDefPacketCount => _instrDefPacketCount;
    public uint InstrDefReceived => _instrDefReceived;
    public uint InstrDefTotalExpected => _instrDefTotalExpected;
    /// <summary>Number of times the bootstrap escape valve forced WaitInstrumentDefinition → Streaming because <see cref="InstrDefStuckTimeoutMs"/> elapsed without completion.</summary>
    public long InstrDefStuckEscapeFiredCount => Volatile.Read(ref _instrDefStuckEscapeFiredCount);
    /// <summary>Repeated SecurityDefinition_12 messages observed during bootstrap (deduplicated by SecurityID).</summary>
    public long InstrDefDuplicateCount => Volatile.Read(ref _instrDefDuplicateCount);
    /// <summary>SecurityDefinition_12 messages whose <c>TotNoRelatedSym</c> contradicted the first observed value (the original total wins).</summary>
    public long InstrDefMismatchedTotalCount => Volatile.Read(ref _instrDefMismatchedTotalCount);
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
    /// Forward an idle-flush signal from the dispatch loop down to handlers that
    /// implement deferred conflation (e.g. <c>GroupConflationHandler</c> in the
    /// server). Default <see cref="IFeedEventHandler.FlushIfDue"/> is a no-op so
    /// non-server handlers stay unchanged.
    /// </summary>
    public void FlushIfDue()
    {
        _eventHandler.FlushIfDue();
        _marketDataHandler?.FlushIfDue();
    }

    /// <summary>
    /// Forward an unconditional shutdown drain so the last conflation window of
    /// buffered events is published instead of silently dropped at process exit.
    /// </summary>
    public void FlushNow()
    {
        _eventHandler.FlushNow();
        _marketDataHandler?.FlushNow();
    }

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

        // Escape valve: B3 spec violations (TotNoRelatedSym=0 on every SecDef,
        // or a SecDef stream that simply never completes) would otherwise pin
        // us in WaitInstrumentDefinition forever. Once we have observed at
        // least one SecDef and the stuck-timeout has elapsed, force-transition
        // to Streaming. Per-symbol heal then bootstraps every symbol that
        // actually exists from the snapshot feed; symbols whose SecDef never
        // arrived stay Unknown until one does.
        if (_state == FeedState.WaitInstrumentDefinition
            && _instrDefFirstSeenTicks != 0
            && packet.ReceivedTimestampTicks - _instrDefFirstSeenTicks >= InstrDefStuckTimeoutMs)
        {
            Interlocked.Increment(ref _instrDefStuckEscapeFiredCount);
            _logger.LogError(
                "InstrumentDefinition bootstrap stuck for {ElapsedMs}ms (received={Received}, expectedTotal={Expected}); forcing transition to Streaming.",
                packet.ReceivedTimestampTicks - _instrDefFirstSeenTicks,
                _instrDefReceived,
                _instrDefTotalExpected);
            _eventHandler.OnInstrumentDefinitionsComplete((int)_instrDefReceived);
            TransitionTo(FeedState.Streaming);
        }
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
                // Snapshot apply can mutate book state (clears, market-tier
                // changes, stale→healthy transitions). Fire OnPacketProcessed
                // so downstream conflation buffers (e.g. GroupConflationHandler)
                // flush promptly instead of waiting for an unrelated incremental.
                _eventHandler.OnPacketProcessed();
                break;
        }
    }

    private long _instrDefPacketCount;

    private void DispatchAndTrackInstrDef(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (!PacketHeader.TryParse(span, out _, out _))
            return;

        _instrDefPacketCount++;
        if (_instrDefFirstSeenTicks == 0)
            _instrDefFirstSeenTicks = packet.ReceivedTimestampTicks;

        int offset = PacketHeader.MESSAGE_SIZE;
        while (TryReadNextSbe(span, ref offset, out var sbeSlice, out var templateId))
        {
            _eventHandler.OnPacket(in packet, sbeSlice, templateId);
            if (templateId == SecurityDefinition_12Data.MESSAGE_ID)
                TrackSecurityDefinition(sbeSlice[MessageDispatcher.SbeHeaderSize..]);
        }
    }

    private void TrackSecurityDefinition(ReadOnlySpan<byte> body)
    {
        if (!SecurityDefinition_12Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;

        // Dedupe by SecurityID: B3 may resend SecDefs (snapshot retry, slow
        // listeners) and counting raw messages would prematurely satisfy
        // TotNoRelatedSym, leaving symbols at the tail permanently unknown.
        ulong securityId = msg.SecurityID.Value;
        if (!_instrDefSeenSymbols.Add(securityId))
        {
            Interlocked.Increment(ref _instrDefDuplicateCount);
            return;
        }
        _instrDefReceived++;

        if (_instrDefTotalExpected == 0)
        {
            _instrDefTotalExpected = msg.TotNoRelatedSym;
        }
        else if (msg.TotNoRelatedSym != 0 && msg.TotNoRelatedSym != _instrDefTotalExpected)
        {
            // Contradictory total — log once per occurrence; the first
            // observed value remains authoritative to keep completion
            // deterministic.
            Interlocked.Increment(ref _instrDefMismatchedTotalCount);
            _logger.LogWarning(
                "InstrumentDefinition reported mismatched TotNoRelatedSym: first={Expected} now={Observed} (SecurityID={SecurityId}); first value wins",
                _instrDefTotalExpected, msg.TotNoRelatedSym, securityId);
        }

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
        while (TryReadNextSbe(span, ref offset, out var sbeSlice, out var templateId))
            _eventHandler.OnPacket(in packet, sbeSlice, templateId);
    }

    /// <summary>
    /// Advance <paramref name="offset"/> past one framed SBE message. Centralizes
    /// the framing validation (length checks + bounds) shared by the snapshot and
    /// instrument-definition dispatch loops. Returns false at end-of-packet or on
    /// any malformed frame (matching the pre-existing "best-effort, never throw"
    /// contract — partial packets are silently truncated).
    /// </summary>
    private static bool TryReadNextSbe(
        ReadOnlySpan<byte> packetSpan, ref int offset,
        out ReadOnlySpan<byte> sbeSlice, out ushort templateId)
    {
        sbeSlice = default;
        templateId = 0;
        if (offset + FramingHeader.MESSAGE_SIZE + MessageDispatcher.SbeHeaderSize > packetSpan.Length)
            return false;
        var framingSlice = packetSpan[offset..];
        if (!FramingHeader.TryParse(framingSlice, out var framing, out _))
            return false;
        if (framing.MessageLength < FramingHeader.MESSAGE_SIZE + MessageDispatcher.SbeHeaderSize)
            return false;
        if (offset + framing.MessageLength > packetSpan.Length)
            return false;

        sbeSlice = packetSpan[(offset + FramingHeader.MESSAGE_SIZE)..];
        templateId = MemoryMarshal.Read<ushort>(sbeSlice[2..]);
        offset += framing.MessageLength;
        return true;
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
