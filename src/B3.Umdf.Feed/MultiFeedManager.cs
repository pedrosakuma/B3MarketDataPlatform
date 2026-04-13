using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Feed;

/// <summary>
/// Manages multiple FeedHandlers, one per channel group.
/// Routes packets from a single merged source to the correct handler by ChannelGroup.
/// Tracks readiness: all groups must reach RealTime before signaling ready.
/// </summary>
public sealed class MultiFeedManager : IDisposable
{
    private readonly IPacketSource _source;
    private readonly Dictionary<int, FeedHandler> _handlers = new();
    private readonly HashSet<int> _readyGroups = new();
    private CancellationTokenSource? _cts;
    private Task? _runTask;

    public event Action? AllGroupsReady;

    /// <summary>Fired when the first group reaches RealTime.</summary>
    public event Action? AnyGroupReady;

    /// <summary>All feed handlers by group index.</summary>
    public IReadOnlyDictionary<int, FeedHandler> Handlers => _handlers;

    /// <summary>True when all groups have reached RealTime at least once.</summary>
    public bool IsAllReady => _readyGroups.Count == _handlers.Count && _handlers.Count > 0;

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

    public MultiFeedManager(IPacketSource source, IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger = null)
    {
        _source = source;
        foreach (var gid in groupIds)
            _handlers[gid] = new FeedHandler(eventHandler, feedLogger);
    }

    /// <summary>
    /// Creates a MultiFeedManager where each group has its own event handler.
    /// </summary>
    public MultiFeedManager(IPacketSource source, IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger = null)
    {
        _source = source;
        foreach (var (gid, handler) in groupHandlers)
            _handlers[gid] = new FeedHandler(handler, feedLogger);
    }

    public Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        if (_source is ISyncPacketSource syncSource)
            _runTask = Task.Run(() => RunSyncLoop(syncSource, _cts.Token));
        else
            _runTask = RunAsyncLoop(_cts.Token);

        return Task.CompletedTask;
    }

    public async Task WaitForCompletionAsync()
    {
        if (_runTask is not null)
            await _runTask;
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

    private void RunSyncLoop(ISyncPacketSource source, CancellationToken ct)
    {
        bool wasAllReady = false;
        while (!ct.IsCancellationRequested && source.TryReceive(out var packet))
        {
            RoutePacket(in packet);
            if (!wasAllReady)
                wasAllReady = CheckReady();
        }
    }

    private async Task RunAsyncLoop(CancellationToken ct)
    {
        bool wasAllReady = false;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var packet = await _source.ReceiveAsync(ct);
                RoutePacket(in packet);
                if (!wasAllReady)
                    wasAllReady = CheckReady();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (System.Threading.Channels.ChannelClosedException) { break; }
        }
    }

    private void RoutePacket(in UmdfPacket packet)
    {
        if (_handlers.TryGetValue(packet.ChannelGroup, out var handler))
            handler.FeedPacket(in packet);
    }

    private bool CheckReady()
    {
        bool anyNew = false;
        foreach (var (gid, handler) in _handlers)
        {
            if (handler.State == FeedState.RealTime && _readyGroups.Add(gid))
                anyNew = true;
        }

        if (anyNew && _readyGroups.Count == 1)
            AnyGroupReady?.Invoke();

        if (IsAllReady)
        {
            AllGroupsReady?.Invoke();
            return true;
        }
        return false;
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        foreach (var h in _handlers.Values)
            h.Dispose();
    }
}
