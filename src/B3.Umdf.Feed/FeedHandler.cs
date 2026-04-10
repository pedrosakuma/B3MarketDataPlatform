using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public sealed class FeedHandler : IDisposable
{
    private readonly IPacketSource _source;
    private readonly ChannelHandler _incrementalHandler;
    private readonly IFeedEventHandler _eventHandler;
    private CancellationTokenSource? _cts;
    private Task? _runTask;

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

    private async Task RunLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var packet = await _source.ReceiveAsync(ct);

                switch (packet.Channel)
                {
                    case ChannelType.IncrementalA:
                    case ChannelType.IncrementalB:
                        _incrementalHandler.HandlePacket(in packet);
                        break;

                    case ChannelType.InstrumentDefinition:
                    case ChannelType.SnapshotRecovery:
                        MessageDispatcher.Dispatch(in packet, _eventHandler);
                        break;
                }
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

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
    }
}
