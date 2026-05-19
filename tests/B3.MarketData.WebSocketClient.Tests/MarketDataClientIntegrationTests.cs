using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using B3.MarketData.WebSocketClient;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// End-to-end test of <see cref="MarketDataClient"/> against a tiny
/// in-process WebSocket server that speaks the B3MarketDataPlatform
/// binary wire format. Covers the typical reference-price flow:
/// <c>ServerStatus → SubscribeOk → Trade → InfoSnapshot</c>, plus
/// <c>SubscribeError</c>.
/// </summary>
public class MarketDataClientIntegrationTests
{
    private static int FindFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try { return ((IPEndPoint)listener.LocalEndpoint).Port; }
        finally { listener.Stop(); }
    }

    [Fact]
    public async Task SubscribeOk_Then_Trade_DecodesAndScalesPrice()
    {
        var port = FindFreePort();
        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            // Wait for the SDK's Subscribe frame, then push back
            // SubscribeOk + Trade.
            var sym = await TestWsServer.ReadSubscribeAsync(ws, ct);
            await TestWsServer.SendSubscribeOkAsync(ws, securityId: 12345, sym, ct);
            await TestWsServer.SendTradeAsync(ws, securityId: 12345, price: 36_7800, qty: 100, tradeId: 1, ct);

            // Hold the socket open until the test cancels.
            await Task.Delay(Timeout.Infinite, ct);
        });

        var receivedTrade = new TaskCompletionSource<TradeEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
        });
        client.Trade += t => receivedTrade.TrySetResult(t);

        await client.ConnectAsync();
        await WaitUntil(() => client.State == ConnectionState.Connected, TimeSpan.FromSeconds(5));
        await client.SubscribeAsync("PETR4");

        var trade = await receivedTrade.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(12345UL, trade.SecurityId);
        Assert.Equal("PETR4", trade.Symbol);
        Assert.Equal(36.78m, trade.Price);
        Assert.Equal(100L, trade.Qty);
        Assert.Equal(1L, trade.TradeId);
        Assert.True(client.TryGetSecurityId("PETR4", out var resolved));
        Assert.Equal(12345UL, resolved);
    }

    [Fact]
    public async Task SubscribeError_IsSurfacedAsTypedEvent()
    {
        var port = FindFreePort();
        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            var sym = await TestWsServer.ReadSubscribeAsync(ws, ct);
            await TestWsServer.SendSubscribeErrorAsync(ws, sym, errorCode: 0x01, ct); // UnknownSymbol
            await Task.Delay(Timeout.Infinite, ct);
        });

        var got = new TaskCompletionSource<SubscribeErrorEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
        });
        client.SubscribeError += e => got.TrySetResult(e);

        await client.ConnectAsync();
        await WaitUntil(() => client.State == ConnectionState.Connected, TimeSpan.FromSeconds(5));
        await client.SubscribeAsync("UNKWN");

        var err = await got.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("UNKWN", err.Symbol);
        Assert.Equal(SubscribeErrorCode.UnknownSymbol, err.ErrorCode);
    }

    [Fact]
    public async Task Reconnect_TransparentlyReSubscribes()
    {
        var port = FindFreePort();
        int subscribesSeen = 0;
        var firstSubscribeReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondSubscribeReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            var sym = await TestWsServer.ReadSubscribeAsync(ws, ct);
            int n = Interlocked.Increment(ref subscribesSeen);
            if (n == 1) firstSubscribeReceived.TrySetResult(true);
            else if (n == 2) secondSubscribeReceived.TrySetResult(true);

            await TestWsServer.SendSubscribeOkAsync(ws, securityId: 7, sym, ct);

            if (n == 1)
            {
                // Force the SDK to reconnect by closing the socket.
                await ws.CloseAsync(WebSocketCloseStatus.EndpointUnavailable, "test reconnect", ct);
                return;
            }

            await Task.Delay(Timeout.Infinite, ct);
        });

        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
            ReconnectInitialDelay = TimeSpan.FromMilliseconds(50),
            ReconnectMaxDelay = TimeSpan.FromMilliseconds(200),
        });

        await client.ConnectAsync();
        await WaitUntil(() => client.State == ConnectionState.Connected, TimeSpan.FromSeconds(5));
        await client.SubscribeAsync("PETR4");

        await firstSubscribeReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        // After the server closes, the SDK should reconnect and re-issue Subscribe.
        await secondSubscribeReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(subscribesSeen >= 2, $"expected ≥ 2 subscribes after reconnect, saw {subscribesSeen}");
    }

    [Fact]
    public async Task ServerHello_IsParsed_AndExposedOnLastServerHello()
    {
        var port = FindFreePort();
        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            // Push a ServerHello as the first frame, then idle. Mirrors what the
            // real server's RegisterClient does on connect.
            await TestWsServer.SendServerHelloAsync(
                ws,
                protocolVersion: 1,
                capabilities: 0x03, // SnapshotOnSubscribe | SymbolDelistedNotification
                buildVersion: "1.2.3-test",
                ct);
            await Task.Delay(Timeout.Infinite, ct);
        });

        var helloReceived = new TaskCompletionSource<ServerHelloEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
        });
        client.ServerHello += h => helloReceived.TrySetResult(h);

        await client.ConnectAsync();
        await WaitUntil(() => client.State == ConnectionState.Connected, TimeSpan.FromSeconds(5));

        var hello = await helloReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1U, hello.ProtocolVersion);
        Assert.Equal(ServerCapabilities.SnapshotOnSubscribe | ServerCapabilities.SymbolDelistedNotification, hello.Capabilities);
        Assert.Equal("1.2.3-test", hello.BuildVersion);

        // LastServerHello property snapshots the most-recently-received hello so
        // late subscribers can still inspect the negotiation result.
        var snap = client.LastServerHello;
        Assert.NotNull(snap);
        Assert.Equal(1U, snap!.Value.ProtocolVersion);
        Assert.Equal("1.2.3-test", snap.Value.BuildVersion);

        // NegotiatedProtocolVersion convenience accessor mirrors LastServerHello.
        Assert.Equal(1U, client.NegotiatedProtocolVersion);
    }

    [Fact]
    public async Task UnknownMessageType_IsCounted_AndRaisesEvent_AndSubsequentFramesStillDecode()
    {
        var port = FindFreePort();
        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            // First read+discard the SDK's ClientHello so the in-process server doesn't
            // confuse it with anything else.
            var buf = new byte[1024];
            try { _ = await ws.ReceiveAsync(buf, ct); } catch { }

            // Send an unknown opcode (0xFFFE) — payload is 4 trailing bytes — then a
            // valid Trade frame to assert the decoder kept going.
            const ushort unknownTotal = 4 + 4;
            var unknown = new byte[unknownTotal];
            BinaryPrimitives.WriteUInt16LittleEndian(unknown, unknownTotal);
            BinaryPrimitives.WriteUInt16LittleEndian(unknown.AsSpan(2), 0xFFFE);
            BinaryPrimitives.WriteUInt32LittleEndian(unknown.AsSpan(4), 0xDEADBEEF);
            await ws.SendAsync(unknown, WebSocketMessageType.Binary, true, ct);

            await TestWsServer.SendTradeAsync(ws, securityId: 42, price: 12345, qty: 100, tradeId: 1, ct);

            await Task.Delay(Timeout.Infinite, ct);
        });

        var unknownTcs = new TaskCompletionSource<ushort>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tradeTcs = new TaskCompletionSource<TradeEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
        });
        client.UnknownMessageReceived += op => unknownTcs.TrySetResult(op);
        client.Trade += t => tradeTcs.TrySetResult(t);

        await client.ConnectAsync();

        var op = await unknownTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal((ushort)0xFFFE, op);

        var trade = await tradeTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(42UL, trade.SecurityId);

        Assert.True(client.UnknownMessageCount >= 1);
    }

    [Fact]
    public async Task News_BeginChunkEnd_AreReassembledIntoSingleEvent()
    {
        var port = FindFreePort();
        var headline = Encoding.UTF8.GetBytes("Petrobras anuncia dividendos");
        var body = Encoding.UTF8.GetBytes("Lorem ipsum dolor sit amet, consectetur.");
        var url = Encoding.UTF8.GetBytes("https://example.com/n/1");
        const ulong newsId = 9876UL;

        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            await TestWsServer.SendNewsBeginAsync(ws, securityIdOrZero: 0, newsId: newsId,
                source: 0, language: 1, origTimeNanos: 1_000,
                headlineLen: (uint)headline.Length, textLen: (uint)body.Length, urlLen: (uint)url.Length, ct);
            await TestWsServer.SendNewsChunkAsync(ws, newsId, fieldByte: 0, headline, isFinal: false, ct);
            await TestWsServer.SendNewsChunkAsync(ws, newsId, fieldByte: 1, body, isFinal: false, ct);
            await TestWsServer.SendNewsChunkAsync(ws, newsId, fieldByte: 2, url, isFinal: true, ct);
            await Task.Delay(Timeout.Infinite, ct);
        });

        var got = new TaskCompletionSource<NewsEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
        });
        client.News += n => got.TrySetResult(n);

        await client.ConnectAsync();
        var ev = await got.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(newsId, ev.NewsId);
        Assert.Equal("Petrobras anuncia dividendos", ev.Headline);
        Assert.Equal("Lorem ipsum dolor sit amet, consectetur.", ev.Text);
        Assert.Equal("https://example.com/n/1", ev.Url);
    }

    private static async Task WaitUntil(Func<bool> pred, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (pred()) return;
            await Task.Delay(20);
        }
    }
}

/// <summary>
/// Minimal in-process WebSocket server used to drive the SDK from tests.
/// Hosts a single <c>/ws</c> endpoint that hands the open socket to the
/// caller-supplied handler. The handler is responsible for the
/// per-test conversation; cancellation is passed through on dispose.
/// </summary>
internal sealed class TestWsServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly CancellationTokenSource _cts = new();

    private TestWsServer(WebApplication app) { _app = app; }

    public static async Task<TestWsServer> StartAsync(int port, Func<WebSocket, CancellationToken, Task> handler)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        builder.Logging.ClearProviders();
        var app = builder.Build();
        app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(30) });
        var server = new TestWsServer(app);
        app.Map("/ws", async (HttpContext ctx) =>
        {
            if (!ctx.WebSockets.IsWebSocketRequest) { ctx.Response.StatusCode = 400; return; }
            using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
            try { await handler(ws, server._cts.Token); }
            catch (OperationCanceledException) { /* shutdown */ }
            catch { /* swallow — server side */ }
        });
        await app.StartAsync();
        return server;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _app.StopAsync(); } catch { }
        await _app.DisposeAsync();
        _cts.Dispose();
    }

    // ── Helpers ──────────────────────────────────────────────────────

    public static async Task<string> ReadSubscribeAsync(WebSocket ws, CancellationToken ct)
    {
        // The SDK sends a ClientHello (0x00A1) as its first frame. Skip any
        // non-Subscribe frames so existing tests need no changes.
        while (true)
        {
            var buffer = new byte[1024];
            int filled = 0;
            WebSocketReceiveResult result;
            do
            {
                result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer, filled, buffer.Length - filled), ct);
                filled += result.Count;
            } while (!result.EndOfMessage);

            // [len u16][type u16=0x0001][flags u8][symLen u8][symbol]
            ushort len = BinaryPrimitives.ReadUInt16LittleEndian(buffer);
            ushort type = BinaryPrimitives.ReadUInt16LittleEndian(buffer.AsSpan(2));
            if (type == 0x00A1) continue; // ClientHello — ignore
            if (type != 0x0001) throw new InvalidOperationException($"expected Subscribe, got 0x{type:X4}");
            byte symLen = buffer[5];
            return Encoding.UTF8.GetString(buffer, 6, symLen);
        }
    }

    public static Task SendSubscribeOkAsync(WebSocket ws, ulong securityId, string symbol, CancellationToken ct)
    {
        // [len u16][type u16=0x0010][secId u64][flags u8][symLen u8][symbol]
        var symBytes = Encoding.UTF8.GetBytes(symbol);
        ushort total = (ushort)(4 + 8 + 1 + 1 + symBytes.Length);
        var buf = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, total);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 0x0010);
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(4), securityId);
        buf[12] = (byte)0x10; // Trades flag
        buf[13] = (byte)symBytes.Length;
        symBytes.CopyTo(buf, 14);
        return ws.SendAsync(buf, WebSocketMessageType.Binary, true, ct);
    }

    public static Task SendSubscribeErrorAsync(WebSocket ws, string symbol, byte errorCode, CancellationToken ct)
    {
        // [len u16][type u16=0x0011][errorCode u8][symLen u8][symbol]
        var symBytes = Encoding.UTF8.GetBytes(symbol);
        ushort total = (ushort)(4 + 1 + 1 + symBytes.Length);
        var buf = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, total);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 0x0011);
        buf[4] = errorCode;
        buf[5] = (byte)symBytes.Length;
        symBytes.CopyTo(buf, 6);
        return ws.SendAsync(buf, WebSocketMessageType.Binary, true, ct);
    }

    public static Task SendTradeAsync(WebSocket ws, ulong securityId, long price, long qty, long tradeId, CancellationToken ct)
    {
        // [len u16][type u16=0x0033][secId u64][price i64][qty i64][tradeId i64]
        const ushort total = 4 + 8 + 8 + 8 + 8;
        var buf = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, total);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 0x0033);
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(4), securityId);
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(12), price);
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(20), qty);
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(28), tradeId);
        return ws.SendAsync(buf, WebSocketMessageType.Binary, true, ct);
    }

    public static Task SendServerHelloAsync(WebSocket ws, uint protocolVersion, uint capabilities, string buildVersion, CancellationToken ct)
    {
        // [len u16][type u16=0x00A0][protocolVersion u32][capabilities u32][buildVerLen u8][buildVer UTF-8…]
        var buildBytes = Encoding.UTF8.GetBytes(buildVersion);
        ushort total = (ushort)(4 + 4 + 4 + 1 + buildBytes.Length);
        var buf = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, total);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 0x00A0);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), protocolVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(8), capabilities);
        buf[12] = (byte)buildBytes.Length;
        buildBytes.CopyTo(buf, 13);
        return ws.SendAsync(buf, WebSocketMessageType.Binary, true, ct);
    }

    public static Task SendNewsBeginAsync(WebSocket ws, ulong securityIdOrZero, ulong newsId,
        byte source, ushort language, long origTimeNanos,
        uint headlineLen, uint textLen, uint urlLen, CancellationToken ct)
    {
        // [len u16][type u16=0x0090][version u8][secId u64][newsId u64][source u8][lang u16][origTime i64]
        // [hLen u32][tLen u32][uLen u32]
        ushort total = (ushort)(4 + 1 + 8 + 8 + 1 + 2 + 8 + 4 + 4 + 4);
        var buf = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, total);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 0x0090);
        int o = 4;
        buf[o++] = 1; // version
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(o), securityIdOrZero); o += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(o), newsId); o += 8;
        buf[o++] = source;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), language); o += 2;
        BinaryPrimitives.WriteInt64LittleEndian(buf.AsSpan(o), origTimeNanos); o += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(o), headlineLen); o += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(o), textLen); o += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(o), urlLen);
        return ws.SendAsync(buf, WebSocketMessageType.Binary, true, ct);
    }

    public static Task SendNewsChunkAsync(WebSocket ws, ulong newsId, byte fieldByte,
        byte[] fragment, bool isFinal, CancellationToken ct)
    {
        // [len u16][type u16][version u8][newsId u64][field u8][fragLen u16][bytes]
        ushort opcode = isFinal ? (ushort)0x0092 : (ushort)0x0091;
        ushort total = (ushort)(4 + 1 + 8 + 1 + 2 + fragment.Length);
        var buf = new byte[total];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, total);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), opcode);
        int o = 4;
        buf[o++] = 1; // version
        BinaryPrimitives.WriteUInt64LittleEndian(buf.AsSpan(o), newsId); o += 8;
        buf[o++] = fieldByte;
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(o), (ushort)fragment.Length); o += 2;
        fragment.CopyTo(buf, o);
        return ws.SendAsync(buf, WebSocketMessageType.Binary, true, ct);
    }
}
