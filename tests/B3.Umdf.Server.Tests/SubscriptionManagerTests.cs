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
        session.TryEnqueue(msg);
        Assert.Equal(1, session.QueueDepth);
    }

    [Fact]
    public void ClientSession_UnboundedChannel_NeverDrops()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws, channelCapacity: 2);
        var msg = new byte[] { 1, 2, 3 };

        session.TryEnqueue(msg);
        session.TryEnqueue(msg);
        session.TryEnqueue(msg); // exceeds soft capacity — still enqueued

        Assert.Equal(3, session.QueueDepth);
    }

    [Fact]
    public void AppSettings_DefaultValues()
    {
        var settings = new AppSettings();
        Assert.Null(settings.WsPort);
        Assert.Equal(0.0, settings.Speed);
        Assert.Equal(4096, settings.ClientChannelCapacity);
        Assert.Equal(0.75, settings.SlowClientThreshold);
        Assert.Equal(100, settings.SlowClientMaxTicks);
        Assert.Equal(5, settings.ShutdownDrainSeconds);
        Assert.Equal("Information", settings.LogLevel);
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
