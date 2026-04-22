using B3.Umdf.Book;
using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

public class SubscriptionManagerTests
{
    [Fact]
    public void RegisterClient_IncrementsClientCount()
    {
        var sm = new SubscriptionManager();
        Assert.Equal(0, sm.ClientCount);

        var ws = new FakeWebSocket();
        var session = new ClientSession(ws);
        sm.RegisterClient(session);
        Assert.Equal(1, sm.ClientCount);
    }

    [Fact]
    public void UnregisterClient_DecrementsClientCount()
    {
        var sm = new SubscriptionManager();
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws);
        sm.RegisterClient(session);
        Assert.Equal(1, sm.ClientCount);

        sm.UnregisterClient(session.Id);
        Assert.Equal(0, sm.ClientCount);
    }

    [Fact]
    public void SetReady_MakesIsReadyTrue()
    {
        var sm = new SubscriptionManager();
        Assert.False(sm.IsReady);
        sm.SetReady();
        Assert.True(sm.IsReady);
    }

    [Fact]
    public void ClientSession_QueueDepth_StartsAtZero()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws, channelCapacity: 100);
        Assert.Equal(0, session.QueueDepth);
        Assert.Equal(100, session.ChannelCapacity);
    }

    [Fact]
    public void ClientSession_TryEnqueue_AlwaysSucceeds()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws, channelCapacity: 10);
        var msg = new byte[] { 1, 2, 3 };
        Assert.True(session.TryEnqueue(msg));
        Assert.Equal(1, session.QueueDepth);
    }

    [Fact]
    public void ClientSession_BoundedChannel_DisconnectsWhenFull()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws, channelCapacity: 2);
        var msg = new byte[] { 1, 2, 3 };

        Assert.True(session.TryEnqueue(msg));
        Assert.True(session.TryEnqueue(msg));
        Assert.False(session.TryEnqueue(msg)); // hard capacity reached -> disconnect

        Assert.Equal(2, session.QueueDepth);
        Assert.True(session.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public void ClientSession_PendingBytesBudget_DisconnectsBeforeQueueDepth()
    {
        var ws = new FakeWebSocket();
        // Large queue capacity but tiny byte budget: bytes guard must trip first.
        var session = new ClientSession(
            ws,
            channelCapacity: 1024,
            maxPendingBytes: 16);
        var msg = new byte[10];

        Assert.True(session.TryEnqueue(msg));
        Assert.Equal(10, session.PendingBytes);
        // Second enqueue would push pending to 20 > 16: disconnect.
        Assert.False(session.TryEnqueue(msg));
        Assert.True(session.CancellationToken.IsCancellationRequested);
        // Bytes counter is unchanged when the new payload is rejected.
        Assert.Equal(10, session.PendingBytes);
    }

    [Fact]
    public void ClientSession_PendingBytesBudget_Disabled_AllowsAnySize()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(
            ws,
            channelCapacity: 1024,
            maxPendingBytes: 0); // disabled
        var msg = new byte[1_000_000];

        Assert.True(session.TryEnqueue(msg));
        Assert.True(session.TryEnqueue(msg));
        Assert.False(session.CancellationToken.IsCancellationRequested);
        // With the guard disabled PendingBytes stays at 0 (no tracking).
        Assert.Equal(0, session.PendingBytes);
    }

    [Fact]
    public void ClientSession_NotifyInfoAvailable_CoalescesWakeSignals()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws, channelCapacity: 4);

        Assert.True(session.NotifyInfoAvailable());
        Assert.True(session.NotifyInfoAvailable());

        Assert.Equal(1, session.QueueDepth);
    }

    [Fact]
    public async Task OutlierSweep_DoesNotDisconnect_WhenAggregatePressureBelowGate()
    {
        // 4 clients × 1 MiB hard cap = 4 MiB budget. Each client has 100 KiB pending
        // (aggregate 400 KiB ≈ 10% of budget) — well below the 50% pressure gate.
        // Even though one client is way above the median, the sweep should leave it
        // alone: a few mildly-slow clients are not a fairness/memory threat.
        using var sm = new SubscriptionManager(
            maxSnapshotRequestsPerBatch: 0,
            clientMaxPendingBytes: 1L * 1024 * 1024,
            outlierMultiplier: 4.0,
            outlierMinBytes: 4096,
            outlierPressurePct: 0.50,
            outlierIntervalMs: 50);
        var sessions = new List<ClientSession>();
        for (int i = 0; i < 4; i++)
        {
            var s = new ClientSession(new FakeWebSocket(), channelCapacity: 1024, maxPendingBytes: 1L * 1024 * 1024);
            s.TryEnqueue(new byte[100 * 1024]);
            sm.RegisterClient(s);
            sessions.Add(s);
        }

        await Task.Delay(200);
        foreach (var s in sessions)
            Assert.False(s.CancellationToken.IsCancellationRequested, $"Session {s.Id} should not have been disconnected");
    }

    [Fact]
    public async Task OutlierSweep_DisconnectsOutliers_UnderPressure()
    {
        // 4 clients × 1 MiB cap. Three sit at 600 KiB (≈ aggregate 1.8 MiB = 45% of budget).
        // Adding a 950 KiB outlier pushes aggregate to ~2.75 MiB > 50% gate. Median is
        // 600 KiB; threshold = max(600 KiB × 4, 4 KiB) = 2.4 MiB → still safe... so
        // raise multiplier sensitivity by setting a high pressure pct exceeded test.
        // Use multiplier=1.5: threshold = max(600 KiB × 1.5, 4 KiB) = 900 KiB. The
        // 950 KiB client trips, the 600 KiB ones don't.
        using var sm = new SubscriptionManager(
            maxSnapshotRequestsPerBatch: 0,
            clientMaxPendingBytes: 1L * 1024 * 1024,
            outlierMultiplier: 1.5,
            outlierMinBytes: 4096,
            outlierPressurePct: 0.40,
            outlierIntervalMs: 50);

        var fast1 = new ClientSession(new FakeWebSocket(), channelCapacity: 1024, maxPendingBytes: 1L * 1024 * 1024);
        var fast2 = new ClientSession(new FakeWebSocket(), channelCapacity: 1024, maxPendingBytes: 1L * 1024 * 1024);
        var fast3 = new ClientSession(new FakeWebSocket(), channelCapacity: 1024, maxPendingBytes: 1L * 1024 * 1024);
        var slow = new ClientSession(new FakeWebSocket(), channelCapacity: 1024, maxPendingBytes: 1L * 1024 * 1024);
        fast1.TryEnqueue(new byte[600 * 1024]);
        fast2.TryEnqueue(new byte[600 * 1024]);
        fast3.TryEnqueue(new byte[600 * 1024]);
        slow.TryEnqueue(new byte[950 * 1024]);
        sm.RegisterClient(fast1);
        sm.RegisterClient(fast2);
        sm.RegisterClient(fast3);
        sm.RegisterClient(slow);

        // Wait for at least one sweep tick (interval=50ms) plus margin.
        await Task.Delay(300);

        Assert.True(slow.CancellationToken.IsCancellationRequested, "Outlier should have been disconnected");
        Assert.False(fast1.CancellationToken.IsCancellationRequested, "Median client must not be disconnected");
        Assert.False(fast2.CancellationToken.IsCancellationRequested, "Median client must not be disconnected");
        Assert.False(fast3.CancellationToken.IsCancellationRequested, "Median client must not be disconnected");
    }

    [Fact]
    public async Task OutlierSweep_RespectsMinBytesFloor_EvenUnderPressure()
    {
        // Two clients at 200 KiB and one at 10 KiB. Aggregate well above pressure gate
        // because we lower the gate. Median = 200 KiB → 1.5× = 300 KiB threshold, but
        // the 10 KiB client is the outlier on the LOW side; we're testing the floor:
        // configure min_bytes = 1 MiB so even the high-pending clients aren't outliers.
        using var sm = new SubscriptionManager(
            maxSnapshotRequestsPerBatch: 0,
            clientMaxPendingBytes: 256L * 1024,
            outlierMultiplier: 1.0,
            outlierMinBytes: 1L * 1024 * 1024,
            outlierPressurePct: 0.01,
            outlierIntervalMs: 50);

        var a = new ClientSession(new FakeWebSocket(), channelCapacity: 1024, maxPendingBytes: 256L * 1024);
        var b = new ClientSession(new FakeWebSocket(), channelCapacity: 1024, maxPendingBytes: 256L * 1024);
        a.TryEnqueue(new byte[200 * 1024]);
        b.TryEnqueue(new byte[200 * 1024]);
        sm.RegisterClient(a);
        sm.RegisterClient(b);

        await Task.Delay(200);
        Assert.False(a.CancellationToken.IsCancellationRequested);
        Assert.False(b.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public void AppSettings_DefaultValues()
    {
        var settings = new AppSettings();
        Assert.Null(settings.WsPort);
        Assert.Equal(0.0, settings.Speed);
        Assert.False(settings.ReplayToMulticast);
        Assert.Equal(4096, settings.ClientChannelCapacity);
        Assert.Equal(0.75, settings.SlowClientThreshold);
        Assert.Equal(100, settings.SlowClientMaxTicks);
        Assert.Equal(32L * 1024 * 1024, settings.ClientMaxPendingBytes);
        Assert.Equal(4.0, settings.ClientOutlierMultiplier);
        Assert.Equal(256L * 1024, settings.ClientOutlierMinBytes);
        Assert.Equal(0.50, settings.ClientOutlierPressurePct);
        Assert.Equal(1000, settings.ClientOutlierIntervalMs);
        Assert.Equal(10, settings.ClientCoalesceWindowMs);
        Assert.Equal(5, settings.ShutdownDrainSeconds);
        Assert.Equal(1_000_000, settings.MulticastMergeCapacity);
        Assert.Equal(250_000, settings.FeedChannelCapacity);
        Assert.Equal("Information", settings.LogLevel);
    }

    [Fact]
    public void AppSettings_ApplyEnvironment_ParsesReplayToMulticastFlag()
    {
        const string key = "UMDF_REPLAY_TO_MULTICAST";
        var previous = Environment.GetEnvironmentVariable(key);

        try
        {
            Environment.SetEnvironmentVariable(key, "true");
            var settings = new AppSettings();

            settings.ApplyEnvironment();

            Assert.True(settings.ReplayToMulticast);
        }
        finally
        {
            Environment.SetEnvironmentVariable(key, previous);
        }
    }
}

/// <summary>Minimal WebSocket stub for unit tests.</summary>
internal class FakeWebSocket : System.Net.WebSockets.WebSocket
{
    public override System.Net.WebSockets.WebSocketCloseStatus? CloseStatus => null;
    public override string? CloseStatusDescription => null;
    public override System.Net.WebSockets.WebSocketState State => System.Net.WebSockets.WebSocketState.Open;
    public override string? SubProtocol => null;

    public override void Abort() { }
    public override Task CloseAsync(System.Net.WebSockets.WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;
    public override Task CloseOutputAsync(System.Net.WebSockets.WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;
    public override void Dispose() { }
    public override Task<System.Net.WebSockets.WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken) => Task.FromResult(new System.Net.WebSockets.WebSocketReceiveResult(0, System.Net.WebSockets.WebSocketMessageType.Close, true));
    public override Task SendAsync(ArraySegment<byte> buffer, System.Net.WebSockets.WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken) => Task.CompletedTask;
}
