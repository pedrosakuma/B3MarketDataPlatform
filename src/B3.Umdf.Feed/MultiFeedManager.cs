using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Feed;

/// <summary>
/// Manages multiple FeedHandlers, one per channel group.
///
/// Inline-dispatch architecture: receive threads call <see cref="PushPacket(in UmdfPacket)"/>
/// directly, which acquires a per-group lock and feeds the packet straight into the group's
/// FeedHandler on the calling thread. There is no intermediate queue, no async/await
/// machinery and no dedicated worker thread per group.
///
/// Threading contract:
/// - Multiple receive threads (one per (group, channel)) may call PushPacket concurrently.
/// - Per-group serialization is provided by an internal lock object: only one thread at a
///   time runs the FeedHandler dispatch path for any given group, so BookManager and
///   MarketDataManager remain free of locks on the hot path.
/// - Cross-group calls run fully in parallel (one lock per group).
///
/// Backpressure: there is no application-level queue between the kernel UDP socket and
/// the FeedHandler. If processing for a group can't keep up, peer receive threads in the
/// same group block on the lock, which causes the kernel SO_RCVBUF to fill and eventually
/// drop datagrams. The A/B reorder buffer + snapshot recovery in the FeedHandler handle
/// the resulting sequence gaps.
/// </summary>
public sealed class MultiFeedManager : IDisposable, IAsyncDisposable
{
    private readonly IPacketSource? _source;
    private readonly Dictionary<int, FeedHandler> _handlers = new();
    private readonly Dictionary<int, object> _groupLocks = new();
    private readonly Dictionary<int, int> _groupIndex = new();
    private volatile bool[] _groupReady = [];
    private CancellationTokenSource? _cts;
    private Task? _dispatchTask;
    private int _anyReadyFired;
    private readonly ILogger<MultiFeedManager> _logger;

    public event Action? AllGroupsReady;

    /// <summary>Fired when the first group reaches RealTime.</summary>
    public event Action? AnyGroupReady;

    /// <summary>All feed handlers by group index.</summary>
    public IReadOnlyDictionary<int, FeedHandler> Handlers => _handlers;

    /// <summary>True when all groups have reached RealTime at least once.</summary>
    public bool IsAllReady
    {
        get
        {
            var ready = _groupReady;
            for (int i = 0; i < ready.Length; i++)
                if (!Volatile.Read(ref ready[i]))
                    return false;
            return ready.Length > 0;
        }
    }

    /// <summary>Total packets processed across all groups.</summary>
    public long TotalPacketCount
    {
        get
        {
            long total = 0;
            foreach (var h in _handlers.Values)
                total += h.PacketCount;
            return total;
        }
    }

    public MultiFeedManager(IPacketSource source, IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger = null, IFeedEventHandler? marketDataHandler = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int incrementalRecoveryQueueCapacity = FeedHandler.DefaultIncrementalRecoveryQueueCapacity)
        : this(groupIds, eventHandler, feedLogger, marketDataHandler, logger, source, incrementalRecoveryQueueCapacity)
    {
        _ = feedChannelCapacity; // accepted for API compatibility; no longer used
    }

    /// <summary>
    /// Live-push constructor: no source / no internal dispatcher. Receive threads (e.g. one per
    /// MulticastPacketSource) must call <see cref="PushPacket(in UmdfPacket)"/> directly.
    /// </summary>
    public MultiFeedManager(IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger = null, IFeedEventHandler? marketDataHandler = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int incrementalRecoveryQueueCapacity = FeedHandler.DefaultIncrementalRecoveryQueueCapacity)
        : this(groupIds, eventHandler, feedLogger, marketDataHandler, logger, source: null, incrementalRecoveryQueueCapacity)
    {
        _ = feedChannelCapacity;
    }

    private MultiFeedManager(IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger, IFeedEventHandler? marketDataHandler, ILogger<MultiFeedManager>? logger, IPacketSource? source, int incrementalRecoveryQueueCapacity)
    {
        _source = source;
        _logger = logger ?? NullLogger<MultiFeedManager>.Instance;
        int idx = 0;
        foreach (var gid in groupIds)
        {
            _handlers[gid] = new FeedHandler(eventHandler, feedLogger, marketDataHandler: marketDataHandler, incrementalRecoveryQueueCapacity: incrementalRecoveryQueueCapacity);
            _groupLocks[gid] = new object();
            _groupIndex[gid] = idx++;
        }
        _groupReady = new bool[idx];
    }

    /// <summary>
    /// Creates a MultiFeedManager where each group has its own event handler.
    /// Optionally accepts per-group market data handlers for passthrough during non-RealTime states.
    /// </summary>
    public MultiFeedManager(IPacketSource source, IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger = null, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int incrementalRecoveryQueueCapacity = FeedHandler.DefaultIncrementalRecoveryQueueCapacity)
        : this(groupHandlers, feedLogger, marketDataHandlers, logger, source, incrementalRecoveryQueueCapacity)
    {
        _ = feedChannelCapacity;
    }

    /// <summary>Live-push constructor with per-group handlers (no internal dispatcher).</summary>
    public MultiFeedManager(IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger = null, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int incrementalRecoveryQueueCapacity = FeedHandler.DefaultIncrementalRecoveryQueueCapacity)
        : this(groupHandlers, feedLogger, marketDataHandlers, logger, source: null, incrementalRecoveryQueueCapacity)
    {
        _ = feedChannelCapacity;
    }

    private MultiFeedManager(IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers, ILogger<MultiFeedManager>? logger, IPacketSource? source, int incrementalRecoveryQueueCapacity)
    {
        _source = source;
        _logger = logger ?? NullLogger<MultiFeedManager>.Instance;
        int idx = 0;
        foreach (var (gid, handler) in groupHandlers)
        {
            IFeedEventHandler? mdHandler = null;
            marketDataHandlers?.TryGetValue(gid, out mdHandler);
            _handlers[gid] = new FeedHandler(handler, feedLogger, marketDataHandler: mdHandler, incrementalRecoveryQueueCapacity: incrementalRecoveryQueueCapacity);
            _groupLocks[gid] = new object();
            _groupIndex[gid] = idx++;
        }
        _groupReady = new bool[idx];
    }

    public Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // No source -> live push mode: receive threads will call PushPacket directly.
        if (_source is null)
        {
            _dispatchTask = null;
            return Task.CompletedTask;
        }

        // Source-driven mode (used by tests and by replay drivers): a single dispatch
        // thread pulls from the source and inline-dispatches via PushPacket.
        if (_source is ISyncPacketSource syncSource)
            _dispatchTask = Task.Factory.StartNew(
                () => RunSyncDispatch(syncSource, _cts.Token),
                _cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        else
            _dispatchTask = RunAsyncDispatch(_cts.Token);

        return Task.CompletedTask;
    }

    public async Task WaitForCompletionAsync()
    {
        if (_dispatchTask is not null)
        {
            await _dispatchTask;
            return;
        }

        // Live-push mode: receive threads are owned externally and there is no internal
        // dispatcher task. Block until the manager is stopped (StopAsync) or the linked
        // cancellation token fires; otherwise the host process would fall through to
        // shutdown immediately.
        var cts = _cts;
        if (cts is null)
            return;
        try { await Task.Delay(Timeout.Infinite, cts.Token); }
        catch (OperationCanceledException) { }
    }

    public async Task StopAsync()
    {
        _cts?.Cancel();
        if (_dispatchTask is not null)
        {
            try { await _dispatchTask; }
            catch (OperationCanceledException) { }
        }
    }

    private void RunSyncDispatch(ISyncPacketSource source, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && source.TryReceive(out var packet))
            PushPacket(in packet);
    }

    private async Task RunAsyncDispatch(CancellationToken ct)
    {
        var source = _source!;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var packet = await source.ReceiveAsync(ct);
                PushPacket(in packet);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    /// <summary>
    /// Direct push entry point for live receive threads. Acquires the per-group lock and
    /// runs the FeedHandler dispatch on the calling thread. Concurrent callers for the
    /// same group are serialized by the lock; cross-group callers run in parallel.
    /// </summary>
    public void PushPacket(in UmdfPacket packet)
    {
        if (!_handlers.TryGetValue(packet.ChannelGroup, out var handler))
        {
            packet.Release();
            return;
        }

        var lockObj = _groupLocks[packet.ChannelGroup];
        // Hold the lock for the full FeedHandler.FeedPacket call. The critical section is
        // small (microseconds — book/MD updates measured at <0.1% of total CPU) so peer
        // receive threads in the same group rarely actually contend.
        lock (lockObj)
        {
            handler.FeedPacket(in packet);
            CheckReadyTransition(packet.ChannelGroup, handler);
        }
    }

    /// <summary>
    /// Bulk variant of <see cref="PushPacket(in UmdfPacket)"/>: acquires the per-group
    /// lock once for the whole batch instead of once per packet. This dramatically
    /// reduces lock acquire/release pressure when receive threads use recvmmsg-style
    /// batching, and — more importantly — gives the other receive threads in the same
    /// group (snapshot, instrument-definition, incremental-B) a fair shot at the lock
    /// between batches instead of competing for it on every single packet.
    ///
    /// All packets in the batch must belong to the same channel group; the group is
    /// determined from the first packet. Packets that route to an unknown group are
    /// released individually (defensive).
    /// </summary>
    public void PushPacketBatch(ReadOnlySpan<UmdfPacket> packets)
    {
        if (packets.Length == 0)
            return;

        // Fast path: all packets in the batch share the same group (always true when
        // called from a per-socket receive loop). Acquire the lock once and dispatch
        // them in order.
        int firstGroup = packets[0].ChannelGroup;
        if (!_handlers.TryGetValue(firstGroup, out var handler))
        {
            // Unknown group on the lead packet — fall back to per-packet routing so
            // each lease still gets released exactly once.
            for (int i = 0; i < packets.Length; i++)
                PushPacket(in packets[i]);
            return;
        }

        var lockObj = _groupLocks[firstGroup];
        lock (lockObj)
        {
            for (int i = 0; i < packets.Length; i++)
            {
                ref readonly var packet = ref packets[i];
                if (packet.ChannelGroup == firstGroup)
                {
                    handler.FeedPacket(in packet);
                }
                else if (_handlers.TryGetValue(packet.ChannelGroup, out var otherHandler))
                {
                    // Cross-group packet snuck into the batch (shouldn't happen with the
                    // standard per-socket receive loop). Re-route under its own group lock
                    // by recursing through PushPacket; this releases firstGroup's lock for
                    // the duration via Monitor reentrance only if the lock is the same
                    // object — different groups have different lock objects, so we drop
                    // back to PushPacket which acquires the correct lock.
                    var otherLock = _groupLocks[packet.ChannelGroup];
                    lock (otherLock)
                    {
                        otherHandler.FeedPacket(in packet);
                        CheckReadyTransition(packet.ChannelGroup, otherHandler);
                    }
                }
                else
                {
                    packet.Release();
                }
            }

            CheckReadyTransition(firstGroup, handler);
        }
    }

    private void CheckReadyTransition(int groupId, FeedHandler handler)
    {
        int idx = _groupIndex[groupId];
        var ready = _groupReady;
        if (Volatile.Read(ref ready[idx]) || handler.State != FeedState.RealTime)
            return;

        Volatile.Write(ref ready[idx], true);

        if (Interlocked.Exchange(ref _anyReadyFired, 1) == 0)
        {
            try { AnyGroupReady?.Invoke(); }
            catch (Exception ex) { _logger.LogError(ex, "AnyGroupReady handler threw"); }
        }

        if (IsAllReady)
        {
            try { AllGroupsReady?.Invoke(); }
            catch (Exception ex) { _logger.LogError(ex, "AllGroupsReady handler threw"); }
        }
    }

    /// <summary>
    /// Returns per-group queue stats. With the inline-dispatch architecture there is no
    /// application-level queue, so depth and dropped are always zero. Kept for API
    /// compatibility with metrics consumers; the meaningful backpressure signal now comes
    /// from kernel SO_RCVBUF + sequence gap counters.
    /// </summary>
    public IEnumerable<(int GroupId, int Depth, long DroppedPackets)> GetChannelStats()
    {
        foreach (var gid in _handlers.Keys)
            yield return (gid, 0, 0L);
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _cts?.Dispose();
        foreach (var h in _handlers.Values)
            h.Dispose();
    }
}
