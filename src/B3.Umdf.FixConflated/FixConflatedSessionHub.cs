using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.FixConflated;

public sealed class FixConflatedSessionHub : IFixApplicationMessageSink
{
    private readonly ConcurrentDictionary<long, FixTcpClientSession> _sessions = new();
    private readonly ILogger<FixConflatedSessionHub> _logger;

    public FixConflatedSessionHub(ILogger<FixConflatedSessionHub>? logger = null)
    {
        _logger = logger ?? NullLogger<FixConflatedSessionHub>.Instance;
    }

    public int ActiveSessionCount => _sessions.Count;

    public void Register(FixTcpClientSession session)
    {
        ArgumentNullException.ThrowIfNull(session);
        _sessions[session.Id] = session;
    }

    public void Unregister(long sessionId)
    {
        _sessions.TryRemove(sessionId, out _);
    }

    public void OnMessage(ReadOnlyMemory<byte> message)
        => BroadcastApplication(FixApplicationMessageAdapter.FromEncodedFrame(message.Span));

    public void BroadcastApplication(FixMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        foreach (KeyValuePair<long, FixTcpClientSession> entry in _sessions)
        {
            try
            {
                entry.Value.TrySendApplication(message);
            }
            catch (ObjectDisposedException)
            {
                _logger.LogDebug("Skipped disposed FIX session {SessionId} during broadcast", entry.Key);
            }
        }
    }
}
