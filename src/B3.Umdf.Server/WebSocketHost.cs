using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Server;

/// <summary>
/// Kestrel-based WebSocket server for market data subscriptions.
/// </summary>
public sealed class WebSocketHost : IDisposable
{
    private readonly SubscriptionManager _subscriptionManager;
    private WebApplication? _app;

    public WebSocketHost(SubscriptionManager subscriptionManager)
    {
        _subscriptionManager = subscriptionManager;
    }

    public async Task StartAsync(int port, CancellationToken ct = default)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
        builder.Logging.ClearProviders();

        _app = builder.Build();
        _app.UseWebSockets();

        _app.Map("/ws", async context =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = 400;
                return;
            }

            var ws = await context.WebSockets.AcceptWebSocketAsync();
            var session = new ClientSession(ws);
            _subscriptionManager.RegisterClient(session);

            Console.WriteLine($"[WS] Client {session.Id} connected");

            // Start write loop
            var writeTask = Task.Run(() => session.RunWriteLoopAsync());

            // Read loop
            try
            {
                await foreach (var (type, payload) in session.ReadMessagesAsync(ct))
                {
                    switch (type)
                    {
                        case MessageType.Subscribe:
                        {
                            var (symbol, flags) = WireProtocol.ReadSubscribe(payload.Span);
                            _subscriptionManager.RequestSubscribe(session.Id, symbol, flags);
                            break;
                        }

                        case MessageType.Get:
                        {
                            var (symbol, flags) = WireProtocol.ReadSubscribe(payload.Span);
                            _subscriptionManager.RequestGet(session.Id, symbol, flags);
                            break;
                        }

                        case MessageType.Unsubscribe:
                            var securityId = WireProtocol.ReadUnsubscribe(payload.Span);
                            _subscriptionManager.RequestUnsubscribe(session.Id, securityId);
                            break;
                    }
                }
            }
            finally
            {
                Console.WriteLine($"[WS] Client {session.Id} disconnected");
                _subscriptionManager.UnregisterClient(session.Id);
                session.Dispose();
                await writeTask;
            }
        });

        await _app.StartAsync(ct);
        Console.WriteLine($"[WS] WebSocket server listening on port {port}");
    }

    public async Task StopAsync()
    {
        if (_app is not null)
            await _app.StopAsync();
    }

    public void Dispose()
    {
        _app?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
