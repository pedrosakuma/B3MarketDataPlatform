using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Feed;

public enum ChannelEpochObservation : byte
{
    Invalid = 0,
    Initialized = 1,
    Current = 2,
    Advanced = 3,
    Stale = 4,
}

public enum ChannelEpochSource : byte
{
    Incremental = 0,
    Snapshot = 1,
}

/// <summary>
/// Channel-scoped source of truth for the active incremental SequenceVersion.
/// Both incremental packets and snapshot LastSequenceVersion values may advance
/// the epoch; every real advance resets downstream state exactly once.
/// </summary>
public sealed class ChannelEpochCoordinator
{
    private readonly object _sync = new();
    private readonly ILogger _logger;
    private IFeedEventHandler? _eventHandler;
    private Action<ushort>? _channelStateReset;
    private int _currentVersion;
    private long _epochAdvances;
    private long _handlerExceptionCount;

    public ChannelEpochCoordinator(ILogger? logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
    }

    public ushort CurrentVersion => (ushort)Volatile.Read(ref _currentVersion);
    public long EpochAdvances => Volatile.Read(ref _epochAdvances);
    public long HandlerExceptionCount => Volatile.Read(ref _handlerExceptionCount);

    public void RegisterEventHandler(IFeedEventHandler eventHandler)
    {
        ArgumentNullException.ThrowIfNull(eventHandler);
        lock (_sync)
        {
            if (_eventHandler is not null && !ReferenceEquals(_eventHandler, eventHandler))
                throw new InvalidOperationException("An event handler is already registered for this channel epoch coordinator.");
            _eventHandler = eventHandler;
        }
    }

    public void RegisterChannelStateReset(Action<ushort> channelStateReset)
    {
        ArgumentNullException.ThrowIfNull(channelStateReset);
        lock (_sync)
        {
            if (_channelStateReset is not null && _channelStateReset != channelStateReset)
                throw new InvalidOperationException("A channel state reset callback is already registered.");
            _channelStateReset = channelStateReset;
        }
    }

    public ChannelEpochObservation Observe(ushort version, ChannelEpochSource source)
    {
        if (version == 0)
            return ChannelEpochObservation.Invalid;

        Action<ushort>? channelStateReset = null;
        IFeedEventHandler? eventHandler = null;
        ChannelEpochObservation result;

        lock (_sync)
        {
            ushort current = (ushort)_currentVersion;
            if (current == 0)
            {
                Volatile.Write(ref _currentVersion, version);
                return ChannelEpochObservation.Initialized;
            }

            if (version < current)
                return ChannelEpochObservation.Stale;
            if (version == current)
                return ChannelEpochObservation.Current;

            Volatile.Write(ref _currentVersion, version);
            Interlocked.Increment(ref _epochAdvances);
            channelStateReset = _channelStateReset;
            eventHandler = _eventHandler;
            result = ChannelEpochObservation.Advanced;
        }

        channelStateReset?.Invoke(version);
        if (eventHandler is not null)
        {
            try
            {
                eventHandler.OnSequenceVersionChanged(version);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _handlerExceptionCount);
                _logger.LogWarning(ex,
                    "Downstream handler threw while advancing channel epoch to SequenceVersion={SequenceVersion} from {Source}",
                    version, source);
            }
        }

        return result;
    }

    /// <summary>
    /// Keeps the coordinator synchronized when a downstream handler is invoked
    /// directly (primarily tests and legacy integration surfaces).
    /// </summary>
    public void SynchronizeFromNotification(ushort version)
    {
        if (version == 0)
            return;

        lock (_sync)
        {
            if (version > _currentVersion)
                Volatile.Write(ref _currentVersion, version);
        }
    }
}
