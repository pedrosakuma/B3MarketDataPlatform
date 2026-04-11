using System.Threading.Channels;

namespace B3.Umdf.Transport;

/// <summary>
/// Merges multiple <see cref="MulticastPacketSource"/> instances into a single
/// <see cref="IPacketSource"/> stream. Each source runs its own receive loop on
/// the thread pool; packets are funnelled into a bounded channel for ordered consumption.
/// </summary>
public sealed class MulticastChannelMerger : IPacketSource, IAsyncDisposable
{
    private readonly MulticastPacketSource[] _sources;
    private readonly Channel<UmdfPacket> _channel;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task[] _receiveTasks;

    /// <param name="sources">Multicast sources to merge (one per UDP socket).</param>
    /// <param name="capacity">Bounded channel capacity. Oldest packets are dropped on overflow.</param>
    public MulticastChannelMerger(IReadOnlyList<MulticastPacketSource> sources, int capacity = 500_000)
    {
        _sources = sources.ToArray();
        _channel = Channel.CreateBounded<UmdfPacket>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

        _receiveTasks = new Task[_sources.Length];
        for (int i = 0; i < _sources.Length; i++)
        {
            var src = _sources[i];
            _receiveTasks[i] = Task.Run(() => ReceiveLoop(src, _cts.Token));
        }
    }

    public async ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        return await _channel.Reader.ReadAsync(linked.Token);
    }

    private async Task ReceiveLoop(MulticastPacketSource source, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var packet = await source.ReceiveAsync(ct);
                _channel.Writer.TryWrite(packet);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (ObjectDisposedException) { }
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();

        try { await Task.WhenAll(_receiveTasks); }
        catch { /* already cancelled */ }

        _channel.Writer.TryComplete();

        foreach (var src in _sources)
            src.Dispose();

        _cts.Dispose();
    }
}
