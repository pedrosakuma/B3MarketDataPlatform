using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Transport;

/// <summary>
/// Merges multiple <see cref="MulticastPacketSource"/> instances into a single
/// <see cref="IPacketSource"/> stream. Each source runs its own receive loop on
/// the thread pool; packets are funnelled into a bounded channel for ordered consumption.
/// </summary>
public sealed class MulticastChannelMerger : IPacketSource, IAsyncDisposable
{
    private readonly IPacketSource[] _sources;
    private readonly Channel<UmdfPacket> _channel;
    private readonly CancellationTokenSource _cts = new();
    private readonly Thread[] _receiveThreads;
    private readonly ILogger _logger;
    private readonly int _capacity;
    private int _queueDepth;
    private long _droppedPackets;

    /// <param name="sources">Multicast sources to merge (one per UDP socket).</param>
    /// <param name="capacity">Bounded channel capacity. Oldest packets are dropped on overflow.</param>
    public MulticastChannelMerger(IReadOnlyList<IPacketSource> sources, int capacity = 500_000, ILogger<MulticastChannelMerger>? logger = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(sources.Count, 1);

        _sources = sources.ToArray();
        _capacity = capacity;
        _logger = logger ?? NullLogger<MulticastChannelMerger>.Instance;
        _channel = Channel.CreateBounded<UmdfPacket>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

        // One dedicated foreground-priority thread per source. Blocking sync receive
        // avoids async state-machine and thread-pool scheduling per datagram, which is
        // what allows the kernel UDP buffer to stay drained during bursts.
        _receiveThreads = new Thread[_sources.Length];
        for (int i = 0; i < _sources.Length; i++)
        {
            var src = _sources[i];
            int idx = i;
            var t = new Thread(() => RunReceiveLoop(src, _cts.Token))
            {
                Name = $"MulticastRecv-{idx}",
                IsBackground = true,
            };
            try { t.Priority = ThreadPriority.AboveNormal; } catch { /* not always supported */ }
            _receiveThreads[i] = t;
        }
        // Start after construction so receive threads see fully initialised state.
        for (int i = 0; i < _receiveThreads.Length; i++)
            _receiveThreads[i].Start();
    }

    public int QueueDepth => Math.Max(Volatile.Read(ref _queueDepth), 0);
    public long DroppedPackets => Volatile.Read(ref _droppedPackets);
    public int Capacity => _capacity;

    public async ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
    {
        var packet = await _channel.Reader.ReadAsync(ct);
        Interlocked.Decrement(ref _queueDepth);
        return packet;
    }

    private void RunReceiveLoop(IPacketSource source, CancellationToken ct)
    {
        try
        {
            if (source is MulticastPacketSource mps)
                SyncReceiveLoop(mps, ct);
            else
                AsyncReceiveLoop(source, ct).GetAwaiter().GetResult();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (ObjectDisposedException) { }
        catch (System.Net.Sockets.SocketException) when (ct.IsCancellationRequested) { }
    }

    private void SyncReceiveLoop(MulticastPacketSource source, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            UmdfPacket packet;
            try { packet = source.Receive(); }
            catch (ObjectDisposedException) { return; }
            catch (System.Net.Sockets.SocketException) when (ct.IsCancellationRequested) { return; }
            TryEnqueue(packet);
        }
    }

    private async Task AsyncReceiveLoop(IPacketSource source, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var packet = await source.ReceiveAsync(ct);
                TryEnqueue(packet);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (ObjectDisposedException) { }
    }

    private bool TryEnqueue(in UmdfPacket packet)
    {
        if (_channel.Writer.TryWrite(packet))
        {
            Interlocked.Increment(ref _queueDepth);
            return true;
        }

        if (_channel.Reader.TryRead(out var dropped))
        {
            Interlocked.Decrement(ref _queueDepth);
            dropped.Release();
            var droppedCount = Interlocked.Increment(ref _droppedPackets);
            if (droppedCount == 1 || droppedCount % 10_000 == 0)
                _logger.LogWarning("Multicast merge queue overflow: dropped {DroppedPackets} packets (capacity {Capacity})", droppedCount, _capacity);

            if (_channel.Writer.TryWrite(packet))
            {
                Interlocked.Increment(ref _queueDepth);
                return true;
            }
        }

        if (_channel.Writer.TryWrite(packet))
        {
            Interlocked.Increment(ref _queueDepth);
            return true;
        }

        packet.Release();
        return false;
    }

    private void DrainQueuedPackets()
    {
        while (_channel.Reader.TryRead(out var packet))
        {
            Interlocked.Decrement(ref _queueDepth);
            packet.Release();
        }
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();

        // Disposing sockets first unblocks any thread parked in Socket.Receive.
        foreach (var src in _sources)
        {
            try { src.Dispose(); } catch { /* best-effort */ }
        }

        // Join receive threads off the disposal thread to avoid blocking it directly.
        await Task.Run(() =>
        {
            foreach (var t in _receiveThreads)
            {
                try { t.Join(TimeSpan.FromSeconds(2)); } catch { /* best-effort */ }
            }
        });

        _channel.Writer.TryComplete();
        DrainQueuedPackets();

        _cts.Dispose();
    }
}
