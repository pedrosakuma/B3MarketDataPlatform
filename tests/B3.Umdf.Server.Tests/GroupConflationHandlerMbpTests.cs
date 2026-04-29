using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Reflection;
using B3.Umdf.Book;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// MBP (Market-By-Price) wire fan-out and conflation behavior. Pins:
/// <list type="bullet">
///   <item>Subscribers must receive <c>LevelSnapshot</c> on subscribe and incremental
///   <c>LevelUpdate</c>/<c>LevelDeleted</c> on book mutations.</item>
///   <item>MBO-only subscribers must NOT receive level frames; conversely MBP-only
///   subscribers must NOT receive raw order frames but MUST receive shared frames
///   like Trade / BookCleared.</item>
///   <item>Multiple <c>OnPriceLevelChanged</c> at the same key collapse into a
///   single emission per batch.</item>
///   <item>A drained level emits <c>LevelDeleted</c> (not <c>LevelUpdate qty=0</c>).</item>
/// </list>
/// </summary>
public class GroupConflationHandlerMbpTests
{
    private const ulong SecurityId = 3001;
    private const string Symbol = "MBP1";

    [Fact]
    public async Task Subscribe_WithMbpFlag_EnqueuesLevelSnapshot()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            // Pre-populate the book so the snapshot has content.
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            book.Bids.Add(NewEntry(orderId: 1, price: 1000, qty: 5));
            book.Bids.Add(NewEntry(orderId: 2, price: 999, qty: 3));
            book.Asks.Add(NewEntry(orderId: 3, price: 1010, qty: 4, side: BookSideType.Ask));

            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session); _ = Task.Run(() => session.RunWriteLoopAsync());

            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Mbp,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));
            // Must NOT send BookSnapshot for an Mbp-only subscriber.
            Assert.False(rec.HasMessageType(MessageType.BookSnapshot));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task OnPriceLevelChanged_LiveLevel_EmitsLevelUpdateToMbpSubscriber()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);

            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session); _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Mbp,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));
            int snapshotCount = rec.CountByType(MessageType.LevelUpdate);

            // Mutate book + signal level changed.
            book.Bids.Add(NewEntry(orderId: 10, price: 1000, qty: 7));
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            // Same key again in the same batch — must conflate.
            book.Bids.Add(NewEntry(orderId: 11, price: 1000, qty: 3));
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.CountByType(MessageType.LevelUpdate) > snapshotCount, TimeSpan.FromSeconds(2));

            // Exactly one LevelUpdate emitted despite two signals.
            Assert.Equal(snapshotCount + 1, rec.CountByType(MessageType.LevelUpdate));

            var lu = rec.LastFrame(MessageType.LevelUpdate);
            Assert.NotNull(lu);
            // Layout: header(4) + secId(8) + side(1) + price(8) + totalQty(8) + count(4)
            Assert.Equal(SecurityId, BinaryPrimitives.ReadUInt64LittleEndian(lu.AsSpan(4)));
            Assert.Equal((byte)BookSideType.Bid, lu[12]);
            Assert.Equal(1000L, BinaryPrimitives.ReadInt64LittleEndian(lu.AsSpan(13)));
            Assert.Equal(10L, BinaryPrimitives.ReadInt64LittleEndian(lu.AsSpan(21))); // 7 + 3
            Assert.Equal(2u, BinaryPrimitives.ReadUInt32LittleEndian(lu.AsSpan(29)));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task OnPriceLevelChanged_DrainedLevel_EmitsLevelDeleted()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            book.Bids.Add(NewEntry(orderId: 10, price: 1000, qty: 5));

            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session); _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Mbp,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);
            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));

            // Drain the level then signal.
            book.Bids.Remove(10);
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.HasMessageType(MessageType.LevelDeleted), TimeSpan.FromSeconds(2));
            Assert.Equal(0, rec.CountByType(MessageType.LevelUpdate));

            var ld = rec.LastFrame(MessageType.LevelDeleted)!;
            Assert.Equal(SecurityId, BinaryPrimitives.ReadUInt64LittleEndian(ld.AsSpan(4)));
            Assert.Equal((byte)BookSideType.Bid, ld[12]);
            Assert.Equal(1000L, BinaryPrimitives.ReadInt64LittleEndian(ld.AsSpan(13)));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task MboOnlySubscriber_DoesNotReceiveLevelFrames()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);

            var mboRec = new RecordingWebSocket();
            var mboSession = new ClientSession(mboRec, channelCapacity: 64);
            w.Manager.RegisterClient(mboSession); _ = Task.Run(() => mboSession.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(mboSession.Id, Symbol, DataFlags.Book,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            // Trigger a level change with no MBP subscriber present.
            book.Bids.Add(NewEntry(orderId: 1, price: 1000, qty: 5));
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            w.Group.OnBatchComplete();

            await Task.Delay(150);

            Assert.False(mboRec.HasMessageType(MessageType.LevelUpdate));
            Assert.False(mboRec.HasMessageType(MessageType.LevelDeleted));
            Assert.False(mboRec.HasMessageType(MessageType.LevelSnapshot));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task MbpOnlySubscriber_ReceivesSharedFrames_ButNotOrderFrames()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);

            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session); _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Mbp,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);
            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));

            // Order frames must NOT reach an MBP-only subscriber. We push them
            // directly through the group as if the BookManager fired them.
            var entry = NewEntry(orderId: 10, price: 1000, qty: 5);
            w.Group.OnOrderAdded(book, in entry);

            // Shared frame (Trade) MUST reach the MBP-only subscriber.
            w.Group.OnTrade(SecurityId, price: 1000, quantity: 1, tradeId: 42, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.HasMessageType(MessageType.Trade), TimeSpan.FromSeconds(2));
            Assert.False(rec.HasMessageType(MessageType.OrderAdded));
            Assert.False(rec.HasMessageType(MessageType.OrderUpdated));
            Assert.False(rec.HasMessageType(MessageType.OrderDeleted));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task BothFlags_ReceivesOrderAndLevelStreams()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);

            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 128);
            w.Manager.RegisterClient(session); _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Book | DataFlags.Mbp,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);

            await WaitUntil(() => rec.HasMessageType(MessageType.BookSnapshot)
                                  && rec.HasMessageType(MessageType.LevelSnapshot),
                            TimeSpan.FromSeconds(2));

            book.Bids.Add(NewEntry(orderId: 10, price: 1000, qty: 5));
            var entry = NewEntry(orderId: 10, price: 1000, qty: 5);
            w.Group.OnOrderAdded(book, in entry);
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.HasMessageType(MessageType.OrderAdded)
                                  && rec.HasMessageType(MessageType.LevelUpdate),
                            TimeSpan.FromSeconds(2));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    // ── helpers ──

    private static OrderBookEntry NewEntry(ulong orderId, long price, long qty,
        BookSideType side = BookSideType.Bid) => new()
    {
        OrderId = orderId,
        Price = price,
        Quantity = qty,
        SecurityId = SecurityId,
        Side = side,
    };

    private static (SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager) NewWiring()
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var book = new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);
        group.SetBookManager(book);

        var symbols = new SymbolRegistry();
        RegisterSymbol(symbols, Symbol, SecurityId);

        manager.SetDataSources(
            new[] { book },
            new[] { new MarketDataManager(stateRegistry: registry) },
            symbols,
            new[] { group });
        manager.SetReady();

        return (manager, group, book);
    }

    private static void RegisterSymbol(SymbolRegistry registry, string symbol, ulong securityId)
    {
        var bySymbol = (ConcurrentDictionary<string, ulong>)typeof(SymbolRegistry)
            .GetField("_bySymbol", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;
        var byId = (ConcurrentDictionary<ulong, string>)typeof(SymbolRegistry)
            .GetField("_byId", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;
        bySymbol[symbol] = securityId;
        byId[securityId] = symbol;
    }

    private static async Task WaitUntil(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(20);
        }
        Assert.True(predicate(), "Timed out waiting for condition.");
    }
}

/// <summary>
/// Minimal WebSocket stub that records every payload sent so tests can scan
/// for specific <see cref="MessageType"/> framings and assert on conflation.
/// </summary>
internal sealed class RecordingWebSocket : WebSocket
{
    private readonly List<byte[]> _frames = new();
    private readonly object _lock = new();

    public override WebSocketCloseStatus? CloseStatus => null;
    public override string? CloseStatusDescription => null;
    public override WebSocketState State => WebSocketState.Open;
    public override string? SubProtocol => null;

    public override void Abort() { }
    public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;
    public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;
    public override void Dispose() { }
    public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        => Task.FromResult(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true));

    public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
    {
        // A single SendAsync may carry several length-prefixed frames coalesced
        // by the broadcaster — split them here.
        var bytes = new byte[buffer.Count];
        Buffer.BlockCopy(buffer.Array!, buffer.Offset, bytes, 0, buffer.Count);
        lock (_lock)
        {
            int o = 0;
            while (o + 4 <= bytes.Length)
            {
                int frameLen = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(o));
                if (frameLen < 4 || o + frameLen > bytes.Length) break;
                var frame = new byte[frameLen];
                Buffer.BlockCopy(bytes, o, frame, 0, frameLen);
                _frames.Add(frame);
                o += frameLen;
            }
        }
        return Task.CompletedTask;
    }

    public bool HasMessageType(MessageType t) => CountByType(t) > 0;

    public int CountByType(MessageType t)
    {
        lock (_lock)
        {
            int n = 0;
            foreach (var f in _frames)
            {
                if (f.Length >= 4 && (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(f.AsSpan(2)) == t)
                    n++;
            }
            return n;
        }
    }

    public byte[]? LastFrame(MessageType t)
    {
        lock (_lock)
        {
            for (int i = _frames.Count - 1; i >= 0; i--)
            {
                var f = _frames[i];
                if (f.Length >= 4 && (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(f.AsSpan(2)) == t)
                    return f;
            }
            return null;
        }
    }
}
