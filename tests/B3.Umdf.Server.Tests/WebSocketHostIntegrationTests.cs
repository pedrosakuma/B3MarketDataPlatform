using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// End-to-end tests for <see cref="WebSocketHost"/>: spins up Kestrel on a real
/// loopback ephemeral port, connects with HttpClient / ClientWebSocket, and
/// verifies the host wiring (HTTP endpoints, /ws accept, max-connections,
/// shutdown rejection).
/// </summary>
public class WebSocketHostIntegrationTests
{
    private static int FindFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    private static async Task<(WebSocketHost host, SubscriptionManager sm, int port, CancellationTokenSource cts)>
        StartHostAsync(int maxConnections = 0, long maxPendingBytes = 0)
    {
        var sm = new SubscriptionManager(clientMaxPendingBytes: maxPendingBytes);
        var host = new WebSocketHost(
            sm,
            maxConnections: maxConnections,
            clientMaxPendingBytes: maxPendingBytes);
        var port = FindFreePort();
        var cts = new CancellationTokenSource();
        await host.StartAsync(port, cts.Token);
        return (host, sm, port, cts);
    }

    [Fact]
    public async Task Live_Endpoint_ReturnsOk()
    {
        var (host, sm, port, cts) = await StartHostAsync();
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var resp = await http.GetAsync($"http://127.0.0.1:{port}/live");
            Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task Ready_Endpoint_Returns503_BeforeReady_AndOk_After()
    {
        var (host, sm, port, cts) = await StartHostAsync();
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };

            var before = await http.GetAsync($"http://127.0.0.1:{port}/ready");
            Assert.Equal(HttpStatusCode.ServiceUnavailable, before.StatusCode);

            sm.SetReady();

            var after = await http.GetAsync($"http://127.0.0.1:{port}/ready");
            Assert.Equal(HttpStatusCode.OK, after.StatusCode);
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task WebSocket_ConnectAndCleanClose_RegistersAndUnregistersClient()
    {
        var (host, sm, port, cts) = await StartHostAsync();
        try
        {
            using var ws = new ClientWebSocket();
            await ws.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/ws"), CancellationToken.None);

            // Wait briefly for the server-side RegisterClient call.
            await WaitUntil(() => sm.ClientCount == 1, TimeSpan.FromSeconds(3));
            Assert.Equal(1, sm.ClientCount);

            await ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);

            await WaitUntil(() => sm.ClientCount == 0, TimeSpan.FromSeconds(3));
            Assert.Equal(0, sm.ClientCount);
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task WebSocket_Connection_RejectedWith503_AfterShutdownSignaled()
    {
        var (host, sm, port, cts) = await StartHostAsync();
        try
        {
            // Trigger shutdown — host should immediately start refusing /ws requests.
            cts.Cancel();
            // Give the cancellation registration a moment to flip _isShuttingDown.
            await Task.Delay(50);

            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            // Plain GET on /ws (not a WS upgrade) — handler short-circuits with 503.
            var resp = await http.GetAsync($"http://127.0.0.1:{port}/ws");
            Assert.Equal(HttpStatusCode.ServiceUnavailable, resp.StatusCode);
        }
        finally
        {
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task WebSocket_MaxConnections_RejectsExtraClients()
    {
        var (host, sm, port, cts) = await StartHostAsync(maxConnections: 1);
        ClientWebSocket? ws1 = null;
        ClientWebSocket? ws2 = null;
        try
        {
            ws1 = new ClientWebSocket();
            await ws1.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/ws"), CancellationToken.None);
            await WaitUntil(() => sm.ClientCount == 1, TimeSpan.FromSeconds(3));

            ws2 = new ClientWebSocket();
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                await ws2.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/ws"), CancellationToken.None));
            // ClientWebSocket wraps the 503 in a WebSocketException; just confirm it failed.
            Assert.NotNull(ex);
            Assert.Equal(1, sm.ClientCount);
        }
        finally
        {
            if (ws1 is not null && ws1.State == WebSocketState.Open)
                await ws1.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
            ws1?.Dispose();
            ws2?.Dispose();
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task Health_Endpoint_ReportsFeedStateProvider_Output()
    {
        var sm = new SubscriptionManager();
        var host = new WebSocketHost(sm);
        host.FeedStateProvider = () => new Dictionary<string, string> { ["G0"] = "RealTime" };
        var port = FindFreePort();
        var cts = new CancellationTokenSource();
        await host.StartAsync(port, cts.Token);
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
            var body = await http.GetStringAsync($"http://127.0.0.1:{port}/health");
            Assert.Contains("\"G0\"", body);
            Assert.Contains("RealTime", body);
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task StopAsync_SendsCloseFrameWith1001ToActiveClients()
    {
        var (host, sm, port, cts) = await StartHostAsync();
        var ws = new ClientWebSocket();
        try
        {
            await ws.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/ws"), CancellationToken.None);
            await WaitUntil(() => sm.ClientCount == 1, TimeSpan.FromSeconds(3));

            // Trigger graceful shutdown — should send WebSocket Close (1001) to active clients
            // before tearing down Kestrel. Kicked off in the background so we can drain frames.
            var stopTask = host.StopAsync(TimeSpan.FromSeconds(5));

            // Drain frames until we observe the Close frame. The server may emit a
            // ServerStatus message on connect, so skip non-Close frames.
            var buffer = new byte[1024];
            WebSocketReceiveResult? result = null;
            using var receiveCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (!receiveCts.IsCancellationRequested)
            {
                result = await ws.ReceiveAsync(buffer, receiveCts.Token);
                if (result.MessageType == WebSocketMessageType.Close) break;
            }

            Assert.NotNull(result);
            Assert.Equal(WebSocketMessageType.Close, result!.MessageType);
            Assert.Equal(WebSocketCloseStatus.EndpointUnavailable, ws.CloseStatus);

            await stopTask;
        }
        finally
        {
            cts.Cancel();
            await host.DisposeAsync();
            sm.Dispose();
            ws.Dispose();
        }
    }

    [Fact]
    public async Task WebSocket_ServerHello_IsFirstFrame_AndCarriesProtocolVersion()
    {
        var (host, sm, port, cts) = await StartHostAsync();
        using var ws = new ClientWebSocket();
        try
        {
            await ws.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/ws"), CancellationToken.None);

            // Read the first WebSocket frame: it MUST be the binary ServerHello,
            // emitted by RegisterClient before ServerStatus.
            var buffer = new byte[1024];
            using var receiveCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            int filled = 0;
            WebSocketReceiveResult result;
            do
            {
                result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer, filled, buffer.Length - filled), receiveCts.Token);
                filled += result.Count;
            } while (!result.EndOfMessage);

            Assert.Equal(WebSocketMessageType.Binary, result.MessageType);

            // Frame 0: ServerHello (length-prefixed, type at offset 2).
            ushort len = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(buffer);
            ushort type = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(buffer.AsSpan(2));
            Assert.Equal((ushort)MessageType.ServerHello, type);
            Assert.True(filled >= len, $"frame should be at least {len} bytes (got {filled})");

            var (protocolVersion, capabilities, buildVersion) =
                WireProtocol.ReadServerHello(buffer.AsSpan(WireProtocol.FramingHeaderSize, len - WireProtocol.FramingHeaderSize));

            Assert.Equal(WireProtocol.ProtocolVersion, protocolVersion);
            Assert.False(string.IsNullOrEmpty(buildVersion), "ServerHello.buildVersion must be non-empty");
            Assert.True(capabilities.HasFlag(ServerCapabilities.SnapshotOnSubscribe));
            Assert.True(capabilities.HasFlag(ServerCapabilities.SymbolDelistedNotification));

            await ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task WebSocket_ClientHello_AcceptedForCurrentProtocolVersion()
    {
        var (host, sm, port, cts) = await StartHostAsync();
        using var ws = new ClientWebSocket();
        try
        {
            await ws.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/ws"), CancellationToken.None);

            // Send ClientHello at v=ProtocolVersion. Server must NOT close.
            var helloBuf = new byte[WireProtocol.ClientHelloSize];
            int helloLen = WireProtocol.WriteClientHello(helloBuf, WireProtocol.ProtocolVersion);
            await ws.SendAsync(new ArraySegment<byte>(helloBuf, 0, helloLen),
                WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);

            // Drain server frames briefly. We don't expect a Close frame.
            var buf = new byte[1024];
            using var rcvCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            bool sawClose = false;
            try
            {
                while (!rcvCts.IsCancellationRequested)
                {
                    var r = await ws.ReceiveAsync(buf, rcvCts.Token);
                    if (r.MessageType == WebSocketMessageType.Close)
                    {
                        sawClose = true;
                        break;
                    }
                }
            }
            catch (OperationCanceledException) { /* expected: server kept the socket open */ }

            Assert.False(sawClose,
                $"Server unexpectedly closed: {ws.CloseStatus} {ws.CloseStatusDescription}");
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    [Fact]
    public async Task WebSocket_ClientHello_RejectsUnsupportedVersion_With1003()
    {
        var (host, sm, port, cts) = await StartHostAsync();
        using var ws = new ClientWebSocket();
        try
        {
            await ws.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/ws"), CancellationToken.None);

            // ClientHello with a version far above SupportedProtocolVersionMax.
            uint badVersion = WireProtocol.SupportedProtocolVersionMax + 999u;
            var helloBuf = new byte[WireProtocol.ClientHelloSize];
            int helloLen = WireProtocol.WriteClientHello(helloBuf, badVersion);
            await ws.SendAsync(new ArraySegment<byte>(helloBuf, 0, helloLen),
                WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);

            // Drain frames until we observe Close. ServerHello/ServerStatus may arrive first.
            var buf = new byte[1024];
            using var rcvCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            WebSocketReceiveResult? r = null;
            while (!rcvCts.IsCancellationRequested)
            {
                r = await ws.ReceiveAsync(buf, rcvCts.Token);
                if (r.MessageType == WebSocketMessageType.Close) break;
            }

            Assert.NotNull(r);
            Assert.Equal(WebSocketMessageType.Close, r!.MessageType);
            // WS code 1003 => InvalidMessageType in System.Net.WebSockets.
            Assert.Equal(WebSocketCloseStatus.InvalidMessageType, ws.CloseStatus);
            Assert.NotNull(ws.CloseStatusDescription);
            Assert.Contains("protocol_version_unsupported", ws.CloseStatusDescription!);
            Assert.Contains($"client {badVersion}", ws.CloseStatusDescription!);
        }
        finally
        {
            cts.Cancel();
            await host.StopAsync();
            await host.DisposeAsync();
            sm.Dispose();
        }
    }

    private static async Task WaitUntil(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(20);
        }
    }
}
