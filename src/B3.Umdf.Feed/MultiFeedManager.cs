using System.Threading.Channels;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Feed;

/// <summary>
/// Manages multiple FeedHandlers, one per channel group.
/// A dispatcher thread reads from the packet source and routes to per-group
/// bounded channels. Each group has a dedicated worker thread that drains
/// its channel and feeds its FeedHandler — giving true per-channel parallelism.
/// </summary>
public sealed class MultiFeedManager : IDisposable
{
    private readonly IPacketSource _source;
    private readonly Dictionary<int, FeedHandler> _handlers = new();
    private readonly Dictionary<int, Channel<UmdfPacket>> _channels = new();
    private readonly Dictionary<int, int> _groupIndex = new();
    private volatile bool[] _groupReady = [];
    private CancellationTokenSource? _cts;
    private Task? _dispatchTask;
    private readonly List<Task> _workerTasks = new();
    private int _anyReadyFired;

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

    public MultiFeedManager(IPacketSource source, IReadOnlyList<int> groupIds, IFeedEventHandler eventHandler, ILogger<FeedHandler>? feedLogger = null)
    {
        _source = source;
        int idx = 0;
        foreach (var gid in groupIds)
        {
            _handlers[gid] = new FeedHandler(eventHandler, feedLogger);
            _channels[gid] = CreateGroupChannel();
            _groupIndex[gid] = idx++;
        }
        _groupReady = new bool[idx];
    }

    /// <summary>
    /// Creates a MultiFeedManager where each group has its own event handler.
    /// </summary>
    public MultiFeedManager(IPacketSource source, IReadOnlyDictionary<int, IFeedEventHandler> groupHandlers, ILogger<FeedHandler>? feedLogger = null)
    {
        _source = source;
        int idx = 0;
        foreach (var (gid, handler) in groupHandlers)
        {
            _handlers[gid] = new FeedHandler(handler, feedLogger);
            _channels[gid] = CreateGroupChannel();
            _groupIndex[gid] = idx++;
        }
        _groupReady = new bool[idx];
    }

    private static Channel<UmdfPacket> CreateGroupChannel() =>
        Channel.CreateBounded<UmdfPacket>(new BoundedChannelOptions(8192)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });

    public Task StartAsync(CancellationToken ct = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // Start a dedicated worker thread per group
        foreach (var (gid, channel) in _channels)
        {
            var handler = _handlers[gid];
            var groupId = gid;
            var workerTask = Task.Factory.StartNew(
                () => RunGroupWorker(groupId, handler, channel.Reader, _cts.Token),
                _cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
            _workerTasks.Add(workerTask);
        }

        // Start dispatcher that reads from source and routes to group channels
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
            await _dispatchTask;
        await Task.WhenAll(_workerTasks);
    }

    public async Task StopAsync()
    {
        _cts?.Cancel();
        if (_dispatchTask is not null)
        {
            try { await _dispatchTask; }
            catch (OperationCanceledException) { }
        }
        foreach (var t in _workerTasks)
        {
            try { await t; }
            catch (OperationCanceledException) { }
        }
    }

    private void RunSyncDispatch(ISyncPacketSource source, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && source.TryReceive(out var packet))
        {
            if (_channels.TryGetValue(packet.ChannelGroup, out var channel))
                channel.Writer.TryWrite(packet);
        }

        // Signal completion to all group channels
        foreach (var ch in _channels.Values)
            ch.Writer.TryComplete();
    }

    private async Task RunAsyncDispatch(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var packet = await _source.ReceiveAsync(ct);
                if (_channels.TryGetValue(packet.ChannelGroup, out var channel))
                    await channel.Writer.WriteAsync(packet, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (ChannelClosedException) { }
        finally
        {
            foreach (var ch in _channels.Values)
                ch.Writer.TryComplete();
        }
    }

    private async Task RunGroupWorker(int groupId, FeedHandler handler, ChannelReader<UmdfPacket> reader, CancellationToken ct)
    {
        var idx = _groupIndex[groupId];
        var ready = _groupReady;
        try
        {
            await foreach (var packet in reader.ReadAllAsync(ct))
            {
                handler.FeedPacket(in packet);

                if (!Volatile.Read(ref ready[idx]) && handler.State == FeedState.RealTime)
                {
                    Volatile.Write(ref ready[idx], true);

                    if (Interlocked.Exchange(ref _anyReadyFired, 1) == 0)
                        AnyGroupReady?.Invoke();

                    if (IsAllReady)
                        AllGroupsReady?.Invoke();
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    public void Dispose()
    {
        _cts?.Cancel();
        foreach (var ch in _channels.Values)
            ch.Writer.TryComplete();
        _cts?.Dispose();
        foreach (var h in _handlers.Values)
            h.Dispose();
    }
}
