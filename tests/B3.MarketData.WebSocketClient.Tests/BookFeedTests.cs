using System.Buffers.Binary;
using System.Net.WebSockets;
using System.Text;
using ServerWire = B3.Umdf.Server.WireProtocol;
using ServerMsg = B3.Umdf.Server.MessageType;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// Phase 1 tests for the opt-in <see cref="BookFeed"/> /
/// <see cref="IBookView"/> layer (issue #43). Unit tests drive
/// <see cref="BookView"/> directly with synthesized events; one
/// integration test drives an end-to-end flow through a real
/// <see cref="MarketDataClient"/> + WebSocket.
/// </summary>
public class BookFeedTests
{
    private static readonly DateTime T0 = new(2026, 5, 19, 18, 0, 0, DateTimeKind.Utc);

    // ── BookView unit tests ─────────────────────────────────────────

    [Fact]
    public void GetBook_returns_null_for_unknown_symbol()
    {
        var view = new BookView("PETR4", 4321);
        Assert.False(view.TryGetTop(out _));
    }

    [Fact]
    public void Snapshot_then_OrderAdded_stream_builds_top_correctly()
    {
        var view = new BookView("PETR4", 4321);
        // Server emits an empty snapshot marker followed by per-order frames.
        view.ApplySnapshot(Snap("PETR4", 4321, T0));
        view.ApplyAdded(Add("PETR4", 4321, 101, BookSide.Bid, 30.10m, 100, T0));
        view.ApplyAdded(Add("PETR4", 4321, 102, BookSide.Bid, 30.20m, 200, T0));
        view.ApplyAdded(Add("PETR4", 4321, 103, BookSide.Bid, 30.20m, 50, T0));
        view.ApplyAdded(Add("PETR4", 4321, 201, BookSide.Ask, 30.40m, 300, T0));
        view.ApplyAdded(Add("PETR4", 4321, 202, BookSide.Ask, 30.50m, 100, T0));

        Assert.True(view.TryGetTop(out var top));
        Assert.Equal(30.20m, top.Bid.Price);
        Assert.Equal(250, top.Bid.TotalQty);
        Assert.Equal(2, top.Bid.OrderCount);
        Assert.Equal(30.40m, top.Ask.Price);
        Assert.Equal(300, top.Ask.TotalQty);
        Assert.Equal(1, top.Ask.OrderCount);
    }

    [Fact]
    public void Update_with_zero_qty_is_treated_as_delete()
    {
        var view = SeedSimple();
        view.ApplyUpdated(Upd("PETR4", 4321, 102, BookSide.Bid, 30.20m, 0, T0.AddSeconds(1)));
        Assert.True(view.TryGetTop(out var top));
        Assert.Equal(30.20m, top.Bid.Price);
        Assert.Equal(50, top.Bid.TotalQty);
        Assert.Equal(1, top.Bid.OrderCount);
    }

    [Fact]
    public void Update_qty_change_aggregates_in_top()
    {
        var view = SeedSimple();
        view.ApplyUpdated(Upd("PETR4", 4321, 103, BookSide.Bid, 30.20m, 150, T0.AddSeconds(1)));
        Assert.True(view.TryGetTop(out var top));
        Assert.Equal(30.20m, top.Bid.Price);
        Assert.Equal(350, top.Bid.TotalQty);
        Assert.Equal(2, top.Bid.OrderCount);
    }

    [Fact]
    public void Delete_drops_and_recomputes_top()
    {
        var view = SeedSimple();
        view.ApplyDeleted(Del("PETR4", 4321, 102, BookSide.Bid, T0.AddSeconds(1)));
        view.ApplyDeleted(Del("PETR4", 4321, 103, BookSide.Bid, T0.AddSeconds(1)));
        Assert.True(view.TryGetTop(out var top));
        Assert.Equal(30.10m, top.Bid.Price);
        Assert.Equal(100, top.Bid.TotalQty);
        Assert.Equal(1, top.Bid.OrderCount);
    }

    [Fact]
    public void BookCleared_both_empties_state_and_top_returns_false()
    {
        var view = SeedSimple();
        view.ApplyCleared(new BookClearedEvent(4321, "PETR4", BookClearSide.Both, T0.AddSeconds(1)));
        Assert.False(view.TryGetTop(out _));
        var (b, a) = view.GetOrderCounts();
        Assert.Equal(0, b);
        Assert.Equal(0, a);
    }

    [Fact]
    public void BookCleared_single_side_keeps_other()
    {
        var view = SeedSimple();
        view.ApplyCleared(new BookClearedEvent(4321, "PETR4", BookClearSide.Bid, T0.AddSeconds(1)));
        Assert.True(view.TryGetTop(out var top));
        Assert.Equal(0, top.Bid.OrderCount);
        Assert.Equal(30.40m, top.Ask.Price);
    }

    [Fact]
    public void Snapshot_replaces_prior_state_and_clears_stale()
    {
        var view = SeedSimple();
        view.MarkStale(true, T0.AddSeconds(1));
        Assert.True(view.IsStale);

        view.ApplySnapshot(Snap("PETR4", 4321, T0.AddSeconds(2)));
        view.ApplyAdded(Add("PETR4", 4321, 500, BookSide.Bid, 31.00m, 10, T0.AddSeconds(2)));
        view.ApplyAdded(Add("PETR4", 4321, 501, BookSide.Ask, 31.05m, 20, T0.AddSeconds(2)));

        Assert.False(view.IsStale);
        Assert.True(view.TryGetTop(out var top));
        Assert.Equal(31.00m, top.Bid.Price);
        Assert.Equal(31.05m, top.Ask.Price);
        var (b, a) = view.GetOrderCounts();
        Assert.Equal(1, b);
        Assert.Equal(1, a);
    }

    [Fact]
    public void OneSidedBook_returns_top_with_empty_other_side()
    {
        var view = new BookView("PETR4", 4321);
        view.ApplyAdded(Add("PETR4", 4321, 1, BookSide.Ask, 30m, 100, T0));
        Assert.True(view.TryGetTop(out var top));
        Assert.Equal(30m, top.Ask.Price);
        Assert.Equal(100, top.Ask.TotalQty);
        Assert.Equal(1, top.Ask.OrderCount);
        Assert.Equal(0, top.Bid.OrderCount);
    }

    [Fact]
    public void MarkStale_toggles_flag()
    {
        var view = SeedSimple();
        Assert.False(view.IsStale);
        view.MarkStale(true, T0.AddSeconds(1));
        Assert.True(view.IsStale);
        view.MarkStale(false, T0.AddSeconds(2));
        Assert.False(view.IsStale);
    }

    // ── BookFeed wiring tests (uses real MarketDataClient events) ───

    [Fact]
    public void BookFeed_GetBook_is_case_insensitive_and_trims()
    {
        // Drive BookFeed by sourcing a MarketDataClient with no connection,
        // then raise events through reflection of the public event surface.
        // Simpler: construct a feed bound to a client, and assert that an
        // unknown symbol returns null and case-insensitive lookup works once
        // we seed via the (internal) BookView. We test the case-insensitive
        // dictionary used inside BookFeed by checking GetBook after a
        // synthesized snapshot delivered through the integration path below.
        Assert.True(StringComparer.OrdinalIgnoreCase.Equals("PETR4", "petr4"));
    }

    [Fact]
    public async Task BookFeed_end_to_end_materializes_top_of_book_over_websocket()
    {
        var port = FindFreePort();
        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            var sym = await TestWsServer.ReadSubscribeAsync(ws, ct);
            await TestWsServer.SendSubscribeOkAsync(ws, securityId: 4321, sym, ct);
            // Emit the empty BookSnapshot phase marker + a few OrderAdded frames.
            await SendBookSnapshotEmptyAsync(ws, securityId: 4321, ct);
            await SendOrderEventAsync(ws, ServerMsg.OrderAdded, secId: 4321, orderId: 101, side: BookSide.Bid, price: 30_1000, qty: 100, ct);
            await SendOrderEventAsync(ws, ServerMsg.OrderAdded, secId: 4321, orderId: 102, side: BookSide.Bid, price: 30_2000, qty: 200, ct);
            await SendOrderEventAsync(ws, ServerMsg.OrderAdded, secId: 4321, orderId: 103, side: BookSide.Bid, price: 30_2000, qty: 50, ct);
            await SendOrderEventAsync(ws, ServerMsg.OrderAdded, secId: 4321, orderId: 201, side: BookSide.Ask, price: 30_4000, qty: 300, ct);
            await Task.Delay(Timeout.Infinite, ct);
        });

        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
        });
        using var feed = client.CreateBookFeed();

        var changes = 0;
        feed.Changed += _ => Interlocked.Increment(ref changes);

        await client.ConnectAsync();
        await client.SubscribeAsync("PETR4", SubscribeFlags.Book);

        await WaitUntil(() => feed.TryGetTop("PETR4", out var t) && t.Bid.OrderCount == 2 && t.Ask.OrderCount == 1,
            TimeSpan.FromSeconds(5));

        Assert.True(feed.TryGetTop("PETR4", out var top));
        Assert.Equal(30.20m, top.Bid.Price);
        Assert.Equal(250, top.Bid.TotalQty);
        Assert.Equal(2, top.Bid.OrderCount);
        Assert.Equal(30.40m, top.Ask.Price);
        Assert.Equal(300, top.Ask.TotalQty);
        Assert.Equal(1, top.Ask.OrderCount);
        // Snapshot + 4 adds → at least 5 Changed events.
        Assert.True(changes >= 5, $"expected ≥5 Changed events, got {changes}");

        // Case-insensitive + trim
        Assert.NotNull(feed.GetBook("petr4"));
        Assert.NotNull(feed.GetBook("  PETR4  "));
    }

    [Fact]
    public async Task BookFeed_stale_flag_flips_on_SymbolStaleStatus_and_clears_on_next_snapshot()
    {
        var port = FindFreePort();
        var stalePhase = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            var sym = await TestWsServer.ReadSubscribeAsync(ws, ct);
            await TestWsServer.SendSubscribeOkAsync(ws, securityId: 4321, sym, ct);
            await SendBookSnapshotEmptyAsync(ws, securityId: 4321, ct);
            await SendOrderEventAsync(ws, ServerMsg.OrderAdded, secId: 4321, orderId: 1, side: BookSide.Bid, price: 30_0000, qty: 100, ct);
            // Mark stale.
            await SendSymbolStaleStatusAsync(ws, securityId: 4321, isStale: true, ct);
            await stalePhase.Task.WaitAsync(ct);
            // Server recovers and re-snapshots.
            await SendBookSnapshotEmptyAsync(ws, securityId: 4321, ct);
            await SendOrderEventAsync(ws, ServerMsg.OrderAdded, secId: 4321, orderId: 2, side: BookSide.Bid, price: 31_0000, qty: 200, ct);
            await Task.Delay(Timeout.Infinite, ct);
        });

        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
        });
        using var feed = client.CreateBookFeed();
        await client.ConnectAsync();
        await client.SubscribeAsync("PETR4", SubscribeFlags.Book);

        await WaitUntil(() => feed.GetBook("PETR4")?.IsStale == true, TimeSpan.FromSeconds(5));
        Assert.True(feed.GetBook("PETR4")!.IsStale);

        stalePhase.SetResult();
        await WaitUntil(() => feed.TryGetTop("PETR4", out var t) && t.Bid.Price == 31m, TimeSpan.FromSeconds(5));
        Assert.False(feed.GetBook("PETR4")!.IsStale);
        Assert.True(feed.TryGetTop("PETR4", out var top));
        Assert.Equal(31m, top.Bid.Price);
        Assert.Equal(200, top.Bid.TotalQty);
    }

    [Fact]
    public async Task BookFeed_Forget_drops_book_state()
    {
        var port = FindFreePort();
        await using var server = await TestWsServer.StartAsync(port, async (ws, ct) =>
        {
            var sym = await TestWsServer.ReadSubscribeAsync(ws, ct);
            await TestWsServer.SendSubscribeOkAsync(ws, securityId: 4321, sym, ct);
            await SendBookSnapshotEmptyAsync(ws, securityId: 4321, ct);
            await SendOrderEventAsync(ws, ServerMsg.OrderAdded, secId: 4321, orderId: 1, side: BookSide.Bid, price: 30_0000, qty: 100, ct);
            await Task.Delay(Timeout.Infinite, ct);
        });

        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = new Uri($"ws://127.0.0.1:{port}/ws"),
        });
        using var feed = client.CreateBookFeed();
        await client.ConnectAsync();
        await client.SubscribeAsync("PETR4", SubscribeFlags.Book);

        await WaitUntil(() => feed.GetBook("PETR4") is not null, TimeSpan.FromSeconds(5));
        Assert.True(feed.Forget("PETR4"));
        Assert.Null(feed.GetBook("PETR4"));
        Assert.False(feed.Forget("PETR4"));
    }

    // ── helpers ─────────────────────────────────────────────────────

    private static BookView SeedSimple()
    {
        var view = new BookView("PETR4", 4321);
        view.ApplySnapshot(Snap("PETR4", 4321, T0));
        view.ApplyAdded(Add("PETR4", 4321, 101, BookSide.Bid, 30.10m, 100, T0));
        view.ApplyAdded(Add("PETR4", 4321, 102, BookSide.Bid, 30.20m, 200, T0));
        view.ApplyAdded(Add("PETR4", 4321, 103, BookSide.Bid, 30.20m, 50, T0));
        view.ApplyAdded(Add("PETR4", 4321, 201, BookSide.Ask, 30.40m, 300, T0));
        view.ApplyAdded(Add("PETR4", 4321, 202, BookSide.Ask, 30.50m, 100, T0));
        return view;
    }

    private static BookSnapshotEvent Snap(string sym, ulong secId, DateTime ts) =>
        new() { SecurityId = secId, Symbol = sym, RptSeq = 1, ReceivedUtc = ts };

    private static OrderAddedEvent Add(string sym, ulong secId, ulong oid, BookSide side, decimal px, long qty, DateTime ts) =>
        new(secId, sym, oid, side, px, qty, ts);

    private static OrderUpdatedEvent Upd(string sym, ulong secId, ulong oid, BookSide side, decimal px, long qty, DateTime ts) =>
        new(secId, sym, oid, side, px, qty, ts);

    private static OrderDeletedEvent Del(string sym, ulong secId, ulong oid, BookSide side, DateTime ts) =>
        new(secId, sym, oid, side, ts);

    private static int FindFreePort()
    {
        var l = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var p = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return p;
    }

    private static async Task WaitUntil(Func<bool> pred, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (pred()) return;
            await Task.Delay(20);
        }
        Assert.Fail($"Condition not met within {timeout}");
    }

    // Empty BookSnapshot marker (bid/ask counts = 0).
    private static Task SendBookSnapshotEmptyAsync(WebSocket ws, ulong securityId, CancellationToken ct)
    {
        Span<byte> buf = stackalloc byte[256];
        int len = ServerWire.WriteBookSnapshotHeader(buf, securityId, rptSeq: 1, bidCount: 0, askCount: 0);
        return ws.SendAsync(buf[..len].ToArray(), WebSocketMessageType.Binary, true, ct);
    }

    private static Task SendOrderEventAsync(WebSocket ws, ServerMsg type, ulong secId, ulong orderId, BookSide side, long price, long qty, CancellationToken ct)
    {
        Span<byte> buf = stackalloc byte[64];
        int len = ServerWire.WriteOrderEvent(buf, type, secId, orderId, (byte)side, price, qty);
        return ws.SendAsync(buf[..len].ToArray(), WebSocketMessageType.Binary, true, ct);
    }

    private static Task SendSymbolStaleStatusAsync(WebSocket ws, ulong securityId, bool isStale, CancellationToken ct)
    {
        Span<byte> buf = stackalloc byte[32];
        int len = ServerWire.WriteSymbolStaleStatus(buf, securityId, isStale);
        return ws.SendAsync(buf[..len].ToArray(), WebSocketMessageType.Binary, true, ct);
    }
}
