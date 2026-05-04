using System.Collections.Concurrent;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Item C: when a global broadcast (Rankings / RecoveryProgress) cannot be
/// enqueued because the per-session outbound ring is full, the publisher MUST
/// (a) increment <see cref="ClientSession.BroadcastDropCount"/> on the session
/// and (b) emit a warning at power-of-two thresholds (so chronically slow
/// clients are visible without log spam from a one-off drop).
/// </summary>
public class BroadcastBackpressureVisibilityTests
{
    private sealed class CapturingLogger : ILogger
    {
        public List<string> Warnings { get; } = new();
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
                lock (Warnings) Warnings.Add(formatter(state, exception));
        }
        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }

    [Fact]
    public void RankingsPublisher_Drop_IncrementsSessionCounter_AndLogsAtPowerOfTwo()
    {
        var logger = new CapturingLogger();

        // ChannelCapacity=1 + maxPendingBytes=1 => guaranteed drop on any
        // payload of more than one byte. Use a session that will reject every
        // TryEnqueue immediately, which is what we need to trigger the
        // drop-handling path inside the publisher.
        var ws = new FakeWebSocket();
        var session = new ClientSession(
            ws,
            channelCapacity: 1,
            maxPendingBytes: 1, // any non-trivial broadcast exceeds this immediately
            logger: NullLogger<ClientSession>.Instance);

        var clients = new ConcurrentDictionary<string, ClientSession>();
        clients[session.Id] = session;

        var publisher = new RankingsPublisher(
            marketDataManagers: () => System.Array.Empty<B3.Umdf.Book.MarketDataManager>(),
            symbolRegistry: () => new B3.Umdf.Book.SymbolRegistry(),
            clients: clients,
            logger: logger);

        // Drive Push() directly by reflection — Push is private but the
        // behaviour we're testing is the for-each at the bottom which only
        // depends on a non-empty payload. We pre-fill the session's outbound
        // so even an empty rankings payload exceeds maxPendingBytes=1.
        var pushMi = typeof(RankingsPublisher).GetMethod(
            "Push",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;

        // Call Push 8 times — even an "empty top-N" rankings frame is several
        // bytes, so every call should drop. Power-of-two threshold log lines
        // expected at totals 1, 2, 4, 8.
        for (int i = 0; i < 8; i++) pushMi.Invoke(publisher, null);

        Assert.Equal(8, session.BroadcastDropCount);

        // Expect exactly four power-of-two warnings (1, 2, 4, 8).
        var rankingsWarnings = logger.Warnings.FindAll(w => w.Contains("rankings"));
        Assert.Equal(4, rankingsWarnings.Count);

        session.Dispose();
    }
}
