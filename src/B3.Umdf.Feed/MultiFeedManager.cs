using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Feed;

/// <summary>
/// Manages multiple FeedHandlers, one per channel group.
///
/// Lock-free MPSC dispatch: each group owns a bounded ring buffer
/// (<see cref="MpscPacketRing"/>) and a dedicated dispatch thread that drains it and runs
/// the FeedHandler pipeline. Receive threads enqueue into the ring without locks via
/// <see cref="PushPacket(in UmdfPacket)"/> / <see cref="PushPacketBatch(ReadOnlySpan{UmdfPacket})"/>
/// and return immediately to <c>recvmmsg</c>.
///
/// Threading contract:
/// - Multiple receive threads (one per (group, channel)) may concurrently enqueue packets
///   for any group; enqueues are wait-free under no contention and use a single
///   <see cref="Interlocked.CompareExchange(ref long, long, long)"/> per packet.
/// - Exactly one dispatch thread per group reads from that group's ring and calls
///   <see cref="FeedHandler.FeedPacket(in UmdfPacket)"/>; BookManager and
///   MarketDataManager remain single-threaded per group with zero locks on the hot path.
/// - Cross-group dispatch runs fully in parallel (one ring + one thread per group).
///
/// Backpressure: when a ring fills (consumer can't keep up with producers) the producer
/// drops the packet and increments a counter; downstream sequence gaps trigger snapshot
/// recovery as usual. The ring capacity defaults to 65 536 slots per group and is
/// configurable via the constructor.
/// </summary>
public sealed class MultiFeedManager : IDisposable, IAsyncDisposable
{
    public const int DefaultGroupRingCapacity = 65_536;

    private readonly IPacketSource? _source;
    private readonly Dictionary<int, FeedHandler> _handlers = new();
    private readonly Dictionary<int, MpscPacketRing> _rings = new();
    private readonly Dictionary<int, Thread> _dispatchThreads = new();
    private readonly Dictionary<int, int> _groupIndex = new();
    private volatile bool[] _groupReady = [];
    private CancellationTokenSource? _cts;
    private Task? _dispatchTask;
    private int _anyReadyFired;
    private readonly ILogger<MultiFeedManager> _logger;
    private readonly int _ringCapacity;

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

    public MultiFeedManager(IPacketSource source, IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger = null, IFeedEventHandler? marketDataHandler = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int incrementalRecoveryQueueCapacity = FeedHandler.DefaultIncrementalRecoveryQueueCapacity, int groupRingCapacity = DefaultGroupRingCapacity, RecoveryMode recoveryMode = RecoveryMode.Channel)
        : this(groupIds, eventHandler, feedLogger, marketDataHandler, logger, source, incrementalRecoveryQueueCapacity, groupRingCapacity, recoveryMode)
    {
        _ = feedChannelCapacity; // accepted for API compatibility; no longer used
    }

    /// <summary>
    /// Live-push constructor: no source / no internal dispatcher. Receive threads (e.g. one per
    /// MulticastPacketSource) must call <see cref="PushPacket(in UmdfPacket)"/> directly.
    /// </summary>
    public MultiFeedManager(IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger = null, IFeedEventHandler? marketDataHandler = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int incrementalRecoveryQueueCapacity = FeedHandler.DefaultIncrementalRecoveryQueueCapacity, int groupRingCapacity = DefaultGroupRingCapacity, RecoveryMode recoveryMode = RecoveryMode.Channel)
        : this(groupIds, eventHandler, feedLogger, marketDataHandler, logger, source: null, incrementalRecoveryQueueCapacity, groupRingCapacity, recoveryMode)
    {
        _ = feedChannelCapacity;
    }

    private MultiFeedManager(IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger, IFeedEventHandler? marketDataHandler, ILogger<MultiFeedManager>? logger, IPacketSource? source, int incrementalRecoveryQueueCapacity, int groupRingCapacity, RecoveryMode recoveryMode = RecoveryMode.Channel)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(groupRingCapacity, 2);
        _source = source;
        _logger = logger ?? NullLogger<MultiFeedManager>.Instance;
        _ringCapacity = groupRingCapacity;
        int idx = 0;
        foreach (var gid in groupIds)
        {
            _handlers[gid] = new FeedHandler(eventHandler, feedLogger, marketDataHandler: marketDataHandler, incrementalRecoveryQueueCapacity: incrementalRecoveryQueueCapacity, recoveryMode: recoveryMode);
            _rings[gid] = new MpscPacketRing(_ringCapacity);
            _groupIndex[gid] = idx++;
        }
        _groupReady = new bool[idx];
    }

    /// <summary>
    /// Creates a MultiFeedManager where each group has its own event handler.
    /// Optionally accepts per-group market data handlers for passthrough during non-RealTime states.
    /// </summary>
    public MultiFeedManager(IPacketSource source, IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger = null, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int incrementalRecoveryQueueCapacity = FeedHandler.DefaultIncrementalRecoveryQueueCapacity, int groupRingCapacity = DefaultGroupRingCapacity, RecoveryMode recoveryMode = RecoveryMode.Channel)
        : this(groupHandlers, feedLogger, marketDataHandlers, logger, source, incrementalRecoveryQueueCapacity, groupRingCapacity, recoveryMode)
    {
        _ = feedChannelCapacity;
    }

    /// <summary>Live-push constructor with per-group handlers (no internal dispatcher).</summary>
    public MultiFeedManager(IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger = null, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int incrementalRecoveryQueueCapacity = FeedHandler.DefaultIncrementalRecoveryQueueCapacity, int groupRingCapacity = DefaultGroupRingCapacity, RecoveryMode recoveryMode = RecoveryMode.Channel)
        : this(groupHandlers, feedLogger, marketDataHandlers, logger, source: null, incrementalRecoveryQueueCapacity, groupRingCapacity, recoveryMode)
    {
        _ = feedChannelCapacity;
    }

    private MultiFeedManager(IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers, ILogger<MultiFeedManager>? logger, IPacketSource? source, int incrementalRecoveryQueueCapacity, int groupRingCapacity, RecoveryMode recoveryMode = RecoveryMode.Channel)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(groupRingCapacity, 2);
        _source = source;
        _logger = logger ?? NullLogger<MultiFeedManager>.Instance;
        _ringCapacity = groupRingCapacity;
        int idx = 0;
        foreach (var (gid, handler) in groupHandlers)
        {
            IFeedEventHandler? mdHandler = null;
            marketDataHandlers?.TryGetValue(gid, out mdHandler);
            _handlers[gid] = new FeedHandler(handler, feedLogger, marketDataHandler: mdHandler, incrementalRecoveryQueueCapacity: incrementalRecoveryQueueCapacity, recoveryMode: recoveryMode);
            _rings[gid] = new MpscPacketRing(_ringCapacity);
            _groupIndex[gid] = idx++;
        }
        _groupReady = new bool[idx];
    }

    public Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // Spin up one dispatch thread per group. These are LongRunning, foreground-style
        // threads so they keep the process alive and run on dedicated OS threads (not
        // borrowed from the thread pool).
        foreach (var (gid, ring) in _rings)
        {
            int groupId = gid;
            var thread = new Thread(() => RunGroupDispatch(groupId, ring, _cts.Token))
            {
                IsBackground = true,
                Name = $"FeedDispatch-G{groupId}",
            };
            _dispatchThreads[gid] = thread;
            thread.Start();
        }

        // No source -> live push mode: receive threads will call PushPacket directly.
        if (_source is null)
        {
            _dispatchTask = null;
            return Task.CompletedTask;
        }

        // Source-driven mode (used by tests and by replay drivers): a single feeder
        // thread pulls from the source and enqueues into the per-group rings. The
        // FeedHandler pipeline still runs on the dispatch threads above.
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
            // The feeder finished pulling from the source, but per-group rings may still
            // hold packets that the dispatch threads haven't processed yet. Wait for them
            // to drain so callers (tests, replay drivers) observe a stable end state.
            var cts = _cts;
            while (cts is not null && !cts.IsCancellationRequested)
            {
                bool allDrained = true;
                foreach (var ring in _rings.Values)
                {
                    if (ring.ApproximateDepth > 0)
                    {
                        allDrained = false;
                        break;
                    }
                }
                if (allDrained)
                    return;
                await Task.Delay(2, cts.Token).ConfigureAwait(false);
            }
            return;
        }

        // Live-push mode: receive threads are owned externally and there is no internal
        // dispatcher task. Block until the manager is stopped (StopAsync) or the linked
        // cancellation token fires; otherwise the host process would fall through to
        // shutdown immediately.
        var liveCts = _cts;
        if (liveCts is null)
            return;
        try { await Task.Delay(Timeout.Infinite, liveCts.Token); }
        catch (OperationCanceledException) { }
    }

    public async Task StopAsync()
    {
        _cts?.Cancel();
        // Wake any dispatch threads blocked on WaitForItems.
        foreach (var ring in _rings.Values)
            ring.SignalShutdown();
        if (_dispatchTask is not null)
        {
            try { await _dispatchTask; }
            catch (OperationCanceledException) { }
        }
        foreach (var thread in _dispatchThreads.Values)
            thread.Join(TimeSpan.FromSeconds(5));
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
    /// Per-group dispatch loop. Runs on a dedicated OS thread; the only consumer of the
    /// group's ring. Tight drain loop with brief spin + block fallback when the ring
    /// goes empty — gives optimal throughput under load and low CPU when idle.
    /// </summary>
    private void RunGroupDispatch(int groupId, MpscPacketRing ring, CancellationToken ct)
    {
        var handler = _handlers[groupId];
        const int spinIterations = 64;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (ring.TryDequeue(out var packet))
                {
                    handler.FeedPacket(in packet);
                    CheckReadyTransition(groupId, handler);
                    continue;
                }

                // Brief spin to absorb tiny producer gaps without paying a syscall.
                bool gotItem = false;
                for (int i = 0; i < spinIterations; i++)
                {
                    if (ring.TryDequeue(out packet))
                    {
                        handler.FeedPacket(in packet);
                        CheckReadyTransition(groupId, handler);
                        gotItem = true;
                        break;
                    }
                    Thread.SpinWait(1 << Math.Min(i, 6));
                }

                if (!gotItem)
                {
                    try { ring.WaitForItems(ct); }
                    catch (OperationCanceledException) { break; }
                }
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            _logger.LogError(ex, "Dispatch thread for group {GroupId} crashed", groupId);
        }
        finally
        {
            // Drain any remaining packets so leases aren't leaked.
            while (ring.TryDequeue(out var packet))
                packet.Release();
        }
    }

    /// <summary>
    /// Direct push entry point for live receive threads. Lock-free enqueue into the
    /// per-group ring; the dispatch thread for that group will pick it up.
    /// </summary>
    public void PushPacket(in UmdfPacket packet)
    {
        if (!_rings.TryGetValue(packet.ChannelGroup, out var ring))
        {
            packet.Release();
            return;
        }

        if (!ring.TryEnqueue(in packet))
        {
            // Ring full: drop and release. Producer counter inside the ring tracks drops.
            packet.Release();
        }
    }

    /// <summary>
    /// Bulk variant of <see cref="PushPacket(in UmdfPacket)"/>: enqueues each packet from
    /// a recvmmsg-style batch into the appropriate per-group ring. The receive thread
    /// returns to the kernel as quickly as possible — there is no lock and no dispatch
    /// work performed on the caller.
    /// </summary>
    public void PushPacketBatch(ReadOnlySpan<UmdfPacket> packets)
    {
        for (int i = 0; i < packets.Length; i++)
            PushPacket(in packets[i]);
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
    /// Returns per-group ring stats: current depth and total dropped packets. Depth is
    /// an approximate snapshot of <c>producerSeq - consumerSeq</c>; drops are exact
    /// (incremented atomically inside <see cref="MpscPacketRing.TryEnqueue"/>).
    /// </summary>
    public IEnumerable<(int GroupId, int Depth, long DroppedPackets)> GetChannelStats()
    {
        foreach (var (gid, ring) in _rings)
            yield return (gid, ring.ApproximateDepth, ring.DroppedPackets);
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _cts?.Dispose();
        foreach (var ring in _rings.Values)
            ring.Dispose();
        foreach (var h in _handlers.Values)
            h.Dispose();
    }
}
