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
    private readonly Dictionary<int, long> _dispatchExceptionCounts = new();
    // Per-group last-logged drop count. Used to rate-limit drop warnings:
    // log on the first drop, then once per power-of-two threshold (1, 2, 4, 8, ...).
    // Power-of-two cadence preserves visibility on bursts without flooding logs
    // when the dispatch thread permanently lags.
    private readonly Dictionary<int, long> _lastLoggedDropMilestone = new();
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

    /// <summary>
    /// True iff the per-group dispatch thread is alive (started and not
    /// exited). Becomes false after StopAsync joins the thread, or if the
    /// thread crashes for an unrecoverable reason. Returns false for an
    /// unknown group.
    /// </summary>
    public bool IsDispatchThreadAlive(int groupId)
        => _dispatchThreads.TryGetValue(groupId, out var t) && t.IsAlive;

    /// <summary>
    /// Number of handler exceptions absorbed by the per-group dispatch loop
    /// since startup (DispatchOne isolation). Sustained growth means a
    /// handler bug is silently dropping packets — alarm on rate.
    /// </summary>
    public long DispatchHandlerExceptionCount(int groupId)
        => _dispatchExceptionCounts.TryGetValue(groupId, out var c) ? Volatile.Read(ref c) : 0L;

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

    /// <summary>
    /// Aggregate packets dropped across every per-group ring since startup.
    /// Drops happen when the receive thread enqueues faster than the dispatch
    /// thread drains — surfaces app-level backpressure overflow distinct from
    /// kernel SO_RCVBUF overruns. Sustained growth indicates the dispatch
    /// thread (or a downstream handler under it) cannot keep up; alarm on
    /// rate, not magnitude.
    /// </summary>
    public long DroppedPacketsTotal
    {
        get
        {
            long total = 0;
            foreach (var ring in _rings.Values)
                total += ring.DroppedPackets;
            return total;
        }
    }

    /// <summary>
    /// Source-driven constructor with a single shared event handler. The internal dispatcher loop
    /// pulls packets from <paramref name="source"/> and routes them to one <see cref="FeedHandler"/>
    /// per channel group. Use this overload for offline replay (PCAP files) where one source feeds
    /// every group; for live multicast prefer the <em>live-push</em> overload below so each receive
    /// thread can write directly without an extra hop through the ring on the dispatcher thread.
    /// </summary>
    /// <param name="feedChannelCapacity">
    /// No-op. Retained only so existing callers (notably the console app and a handful of tests)
    /// keep compiling without source edits. The per-group ring capacity is now controlled by
    /// <paramref name="groupRingCapacity"/>; this parameter will be removed in a future major bump.
    /// </param>
    public MultiFeedManager(IPacketSource source, IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger = null, IFeedEventHandler? marketDataHandler = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int groupRingCapacity = DefaultGroupRingCapacity)
        : this(groupIds, eventHandler, feedLogger, marketDataHandler, logger, source, groupRingCapacity)
    {
        _ = feedChannelCapacity;
    }

    /// <summary>
    /// Live-push constructor with a single shared event handler: no internal dispatcher loop.
    /// Receive threads (e.g. one per <c>MulticastPacketSource</c>) must call
    /// <see cref="PushPacket(in UmdfPacket)"/> directly; the call routes through the per-group
    /// ring to the appropriate <see cref="FeedHandler"/> on the same calling thread (no async hop).
    /// </summary>
    /// <param name="feedChannelCapacity">No-op; see source-driven overload.</param>
    public MultiFeedManager(IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger = null, IFeedEventHandler? marketDataHandler = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int groupRingCapacity = DefaultGroupRingCapacity)
        : this(groupIds, eventHandler, feedLogger, marketDataHandler, logger, source: null, groupRingCapacity)
    {
        _ = feedChannelCapacity;
    }

    private MultiFeedManager(IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger, IFeedEventHandler? marketDataHandler, ILogger<MultiFeedManager>? logger, IPacketSource? source, int groupRingCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(groupRingCapacity, 2);
        _source = source;
        _logger = logger ?? NullLogger<MultiFeedManager>.Instance;
        _ringCapacity = groupRingCapacity;
        int idx = 0;
        foreach (var gid in groupIds)
        {
            _handlers[gid] = new FeedHandler(eventHandler, feedLogger, marketDataHandler: marketDataHandler);
            _rings[gid] = new MpscPacketRing(_ringCapacity);
            _dispatchExceptionCounts[gid] = 0L;
            _lastLoggedDropMilestone[gid] = 0L;
            _groupIndex[gid] = idx++;
        }
        _groupReady = new bool[idx];
    }

    /// <summary>
    /// Source-driven constructor with a per-group event handler map. Use when each channel group
    /// must dispatch into a distinct downstream subscriber (e.g. separating MBO from MBP). The
    /// <paramref name="marketDataHandlers"/> overlay receives passthrough events while a group is
    /// not in <see cref="FeedState.Streaming"/>; pass <c>null</c> if not needed.
    /// </summary>
    /// <param name="feedChannelCapacity">No-op; see single-handler overload.</param>
    public MultiFeedManager(IPacketSource source, IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger = null, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int groupRingCapacity = DefaultGroupRingCapacity)
        : this(groupHandlers, feedLogger, marketDataHandlers, logger, source, groupRingCapacity)
    {
        _ = feedChannelCapacity;
    }

    /// <summary>Live-push constructor with per-group handlers (no internal dispatcher); see live-push overload above for the push-mode contract.</summary>
    /// <param name="feedChannelCapacity">No-op; see single-handler overload.</param>
    public MultiFeedManager(IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger = null, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers = null, ILogger<MultiFeedManager>? logger = null, int feedChannelCapacity = 0, int groupRingCapacity = DefaultGroupRingCapacity)
        : this(groupHandlers, feedLogger, marketDataHandlers, logger, source: null, groupRingCapacity)
    {
        _ = feedChannelCapacity;
    }

    private MultiFeedManager(IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger, IReadOnlyDictionary<int, IFeedEventHandler>? marketDataHandlers, ILogger<MultiFeedManager>? logger, IPacketSource? source, int groupRingCapacity)
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
            _handlers[gid] = new FeedHandler(handler, feedLogger, marketDataHandler: mdHandler);
            _rings[gid] = new MpscPacketRing(_ringCapacity);
            _dispatchExceptionCounts[gid] = 0L;
            _lastLoggedDropMilestone[gid] = 0L;
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
                    DispatchOne(groupId, handler, in packet);
                    CheckReadyTransition(groupId, handler);
                    continue;
                }

                // Brief spin to absorb tiny producer gaps without paying a syscall.
                bool gotItem = false;
                for (int i = 0; i < spinIterations; i++)
                {
                    if (ring.TryDequeue(out packet))
                    {
                        DispatchOne(groupId, handler, in packet);
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
            // Unreachable in normal operation: DispatchOne isolates handler
            // failures so the dispatch loop survives. Anything reaching here
            // is a bug in the dispatcher itself (ring corruption, OOM).
            _logger.LogCritical(ex, "Dispatch loop for group {GroupId} died unexpectedly; channel will silently drop packets until restart", groupId);
        }
        finally
        {
            // Drain any remaining packets so leases aren't leaked.
            while (ring.TryDequeue(out var packet))
                packet.Release();
        }
    }

    /// <summary>
    /// Dispatch a single packet to the handler with per-packet exception
    /// isolation. A misbehaving handler MUST NOT kill the dispatch thread —
    /// the previous behavior caught at the loop level and silently exited,
    /// after which the producer kept enqueueing until the ring filled and
    /// dropped packets unobserved.
    /// </summary>
    private void DispatchOne(int groupId, FeedHandler handler, in UmdfPacket packet)
    {
        try
        {
            handler.FeedPacket(in packet);
        }
        catch (Exception ex)
        {
            // Increment first so even logging failures don't mask the metric.
            // Key is guaranteed to exist (pre-populated in ctor), and the dict
            // is not mutated after StartAsync, so the ref is stable.
            ref var counter = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrNullRef(
                _dispatchExceptionCounts, groupId);
            if (!System.Runtime.CompilerServices.Unsafe.IsNullRef(ref counter))
                Interlocked.Increment(ref counter);
            _logger.LogError(ex, "Handler exception in dispatch for group {GroupId}; packet skipped, dispatch loop continuing", groupId);
        }
    }

    /// <summary>
    /// Direct push entry point for live receive threads. Lock-free enqueue into the
    /// per-group ring; the dispatch thread for that group will pick it up.
    /// </summary>
    public void PushPacket(in UmdfPacket packet)
    {
        int groupId = packet.ChannelGroup;
        if (!_rings.TryGetValue(groupId, out var ring))
        {
            packet.Release();
            return;
        }

        if (!ring.TryEnqueue(in packet))
        {
            // Ring full: drop and release. Producer counter inside the ring tracks drops.
            packet.Release();
            LogDropRateLimited(groupId, ring.DroppedPackets);
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

    private void LogDropRateLimited(int groupId, long currentDropCount)
    {
        // Power-of-two milestone cadence: log at 1, 2, 4, 8, 16, ... drops per group.
        // Hot path is single-producer per group in practice (one receive thread per
        // channel pushes for the group), so the non-atomic read/write here is safe;
        // worst case is a duplicated log line, never a missed escalation.
        if (!_lastLoggedDropMilestone.TryGetValue(groupId, out var last))
            return;
        if (currentDropCount <= last)
            return;
        // Round currentDropCount down to the nearest power of two ≥ 1.
        long milestone = 1L << (63 - System.Numerics.BitOperations.LeadingZeroCount((ulong)currentDropCount));
        if (milestone <= last)
            return;
        _lastLoggedDropMilestone[groupId] = milestone;
        _logger.LogWarning(
            "MultiFeedManager group {GroupId} dropped {DropCount} packets (ring capacity {Capacity}); dispatch thread cannot keep up with producers",
            groupId, currentDropCount, _ringCapacity);
    }

    private void CheckReadyTransition(int groupId, FeedHandler handler)
    {
        int idx = _groupIndex[groupId];
        var ready = _groupReady;
        if (Volatile.Read(ref ready[idx]) || handler.State != FeedState.Streaming)
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

    /// <summary>
    /// Per-group, per-channel drop breakdown. Sum of per-channel drops within a
    /// group equals the corresponding entry's <c>DroppedPackets</c> in
    /// <see cref="GetChannelStats"/>. Use this to attribute ring overflow to a
    /// specific channel (Inc A/B vs Snap vs InstrumentDefinition) — critical
    /// because Inc dominates traffic at ~100x the Snap rate, so an undisclosed
    /// overflow could come from either path.
    /// </summary>
    public IEnumerable<(int GroupId, ChannelType Channel, long DroppedPackets)> GetChannelDropBreakdown()
    {
        foreach (var (gid, ring) in _rings)
        {
            yield return (gid, ChannelType.IncrementalA, ring.DroppedFor(ChannelType.IncrementalA));
            yield return (gid, ChannelType.IncrementalB, ring.DroppedFor(ChannelType.IncrementalB));
            yield return (gid, ChannelType.InstrumentDefinition, ring.DroppedFor(ChannelType.InstrumentDefinition));
            yield return (gid, ChannelType.SnapshotRecovery, ring.DroppedFor(ChannelType.SnapshotRecovery));
        }
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
