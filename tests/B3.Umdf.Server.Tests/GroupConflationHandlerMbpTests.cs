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
            // v2 layout: header(8) + secId(8) + price(8) + totalQty(8) + count u32 + side u8
            Assert.Equal(SecurityId, BinaryPrimitives.ReadUInt64LittleEndian(lu.AsSpan(8)));
            Assert.Equal((byte)BookSideType.Bid, lu[36]);
            Assert.Equal(1000L, BinaryPrimitives.ReadInt64LittleEndian(lu.AsSpan(16)));
            Assert.Equal(10L, BinaryPrimitives.ReadInt64LittleEndian(lu.AsSpan(24))); // 7 + 3
            Assert.Equal(2u, BinaryPrimitives.ReadUInt32LittleEndian(lu.AsSpan(32)));
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
            Assert.Equal(SecurityId, BinaryPrimitives.ReadUInt64LittleEndian(ld.AsSpan(8)));
            Assert.Equal((byte)BookSideType.Bid, ld[24]);
            Assert.Equal(1000L, BinaryPrimitives.ReadInt64LittleEndian(ld.AsSpan(16)));
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
            // Mbp + Trades: Mbp-only excludes order frames, Trades opts in to live prints.
            w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Mbp | DataFlags.Trades,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);
            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));

            // Order frames must NOT reach an MBP-only subscriber. We push them
            // directly through the group as if the BookManager fired them.
            var entry = NewEntry(orderId: 10, price: 1000, qty: 5);
            w.Group.OnOrderAdded(book, in entry);

            // Trade frame MUST reach the subscriber because they opted in via DataFlags.Trades.
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

    [Fact]
    public async Task ConflatedMbp_SnapshotIsImmediate_AndLevelsFlushLastValueAtCadence()
    {
        var w = NewWiring([500]);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);

            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session); _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(
                session.Id,
                Symbol,
                DataFlags.ConflatedMbp,
                conflationIntervalMs: 500,
                w.BookManager,
                w.Group,
                bookBatchCutoffSequence: 0);

            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));

            book.Bids.Add(NewEntry(orderId: 1, price: 1000, qty: 4));
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            w.Group.OnBatchComplete();
            book.Bids.Add(NewEntry(orderId: 2, price: 1000, qty: 6));
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            w.Group.OnBatchComplete();

            await Task.Delay(100);
            Assert.Equal(0, rec.CountByType(MessageType.LevelUpdate));
            Assert.True(w.Group.CadenceFramesBuffered > 0);

            await WaitUntil(
                () => w.Group.CadenceFramesEmitted > 0,
                TimeSpan.FromSeconds(2));
            await WaitUntil(() => rec.HasMessageType(MessageType.LevelUpdate), TimeSpan.FromSeconds(1));
            Assert.Equal(1, rec.CountByType(MessageType.LevelUpdate));
            var update = rec.LastFrame(MessageType.LevelUpdate)!;
            Assert.Equal(10L, BinaryPrimitives.ReadInt64LittleEndian(update.AsSpan(24)));
            Assert.Equal(2u, BinaryPrimitives.ReadUInt32LittleEndian(update.AsSpan(32)));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task ConflatedMbp_DoesNotDelayExistingMbpSubscribers()
    {
        var w = NewWiring([500]);
        w.Group.StartBroadcaster(0);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            var live = new ClientSession(new FakeWebSocket(), channelCapacity: 64);
            var conflated = new ClientSession(new FakeWebSocket(), channelCapacity: 64);
            w.Manager.RegisterClient(live);
            w.Manager.RegisterClient(conflated);
            w.Manager.HandleSubscribe(live.Id, Symbol, DataFlags.Mbp,
                w.BookManager, w.Group, bookBatchCutoffSequence: 0);
            w.Manager.HandleSubscribe(
                conflated.Id,
                Symbol,
                DataFlags.ConflatedMbp,
                conflationIntervalMs: 500,
                w.BookManager,
                w.Group,
                bookBatchCutoffSequence: 0);

            int liveBefore = live.QueueDepth;
            int conflatedBefore = conflated.QueueDepth;

            // Pin the cadence deadline to a full interval after the delta is buffered
            // so the test observes immediate MBP enqueueing instead of racing the
            // broadcaster timer's current phase.
            typeof(CadenceConflationBuffer)
                .GetField("_nextFlushTicks", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(GetCadenceBuffer(w.Group), Environment.TickCount64 + 500);

            book.Bids.Add(NewEntry(orderId: 1, price: 1000, qty: 7));
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            w.Group.OnBatchComplete();

            await WaitUntil(() => live.QueueDepth > liveBefore, TimeSpan.FromSeconds(2));
            Assert.Equal(conflatedBefore, conflated.QueueDepth);
            await WaitUntil(() => conflated.QueueDepth > conflatedBefore, TimeSpan.FromSeconds(2));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task ConflatedMbp_RejectsUnsupportedCadence()
    {
        var w = NewWiring();
        var rec = new RecordingWebSocket();
        var session = new ClientSession(rec, channelCapacity: 64);
        w.Manager.RegisterClient(session); _ = Task.Run(() => session.RunWriteLoopAsync());

        w.Manager.HandleSubscribe(
            session.Id,
            Symbol,
            DataFlags.ConflatedMbp,
            conflationIntervalMs: 50,
            w.BookManager,
            w.Group,
            bookBatchCutoffSequence: 0);

        await WaitUntil(() => rec.HasMessageType(MessageType.SubscribeError), TimeSpan.FromSeconds(2));
        Assert.False(rec.HasMessageType(MessageType.LevelSnapshot));
        Assert.Equal(
            (byte)SubscribeErrorCode.InvalidCadence,
            rec.LastFrame(MessageType.SubscribeError)![8]);
        w.Manager.Dispose();
    }

    [Fact]
    public async Task ConflatedMbp_TradesRemainSeparateAndImmediate()
    {
        var w = NewWiring([500]);
        w.Group.StartBroadcaster(0);
        try
        {
            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session); _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(
                session.Id,
                Symbol,
                DataFlags.ConflatedMbp | DataFlags.Trades,
                conflationIntervalMs: 500,
                w.BookManager,
                w.Group,
                bookBatchCutoffSequence: 0);
            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));

            w.Group.OnTrade(SecurityId, price: 1000, quantity: 2, tradeId: 9, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            await WaitUntil(() => rec.HasMessageType(MessageType.Trade), TimeSpan.FromSeconds(1));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task ConflatedMbp_EventTimeBuffering_IsPerCadenceNotPerConsumer()
    {
        var w = NewWiring([500]);
        w.Group.StartBroadcaster(0);
        try
        {
            for (int i = 0; i < 256; i++)
            {
                w.Manager.AddSubscriptionForTest(
                    $"cadence-{i}",
                    SecurityId,
                    DataFlags.ConflatedMbp,
                    conflationIntervalMs: 500);
            }

            Assert.Null(w.Manager.GetImmediateMbpSubscribers(SecurityId));
            Assert.Equal(
                256,
                w.Manager.GetConflatedMbpSubscribers(SecurityId, 500)!.Count);

            var book = w.BookManager.GetOrCreateBook(SecurityId);
            book.Bids.Add(NewEntry(orderId: 1, price: 1000, qty: 7));
            w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
            w.Group.OnBatchComplete();

            await WaitUntil(() => w.Group.CadenceFramesBuffered > 0, TimeSpan.FromSeconds(1));
            Assert.Equal(1, w.Group.CadenceFramesBuffered);
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task ConflatedMbp_SustainedBacklog_ReleasesCadenceBeforeRingDrains()
    {
        var w = NewWiring([100]);
        try
        {
            var book = w.BookManager.GetOrCreateBook(SecurityId);
            book.Bids.Add(NewEntry(orderId: 1, price: 1000, qty: 7));

            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 256);
            w.Manager.RegisterClient(session);
            _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(
                session.Id,
                Symbol,
                DataFlags.ConflatedMbp,
                conflationIntervalMs: 100,
                w.BookManager,
                w.Group,
                bookBatchCutoffSequence: 0);
            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));

            // Seed a pending cadence frame and force its deadline due before starting
            // the broadcaster. This avoids timing races while the queued batches model
            // a producer that keeps the ring continuously non-empty.
            var cadenceBuffer = GetCadenceBuffer(w.Group);
            byte[] frame = new byte[WireProtocol.LevelUpdateSize];
            int frameLength = WireProtocol.WriteLevelUpdate(
                frame,
                SecurityId,
                (byte)BookSideType.Bid,
                price: 1000,
                totalQty: 7,
                orderCount: 1);
            cadenceBuffer.Buffer(SecurityId, frame.AsSpan(0, frameLength), batchSequence: 1, epoch: 0);
            typeof(CadenceConflationBuffer)
                .GetField("_nextFlushTicks", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(cadenceBuffer, Environment.TickCount64 - 1);

            const int queuedBatches = 128;
            for (int i = 0; i < queuedBatches; i++)
            {
                w.Group.OnPriceLevelChanged(book, BookSideType.Bid, 1000);
                w.Group.OnBatchComplete();
            }
            Assert.Equal(queuedBatches, w.Group.BroadcastRingDepth);

            w.Group.StartBroadcaster(0);

            await WaitUntil(
                () => w.Group.CadenceFlushesWhileBacklogged > 0,
                TimeSpan.FromSeconds(2));
            await WaitUntil(
                () => rec.HasMessageType(MessageType.LevelUpdate),
                TimeSpan.FromSeconds(1));
            Assert.True(w.Group.CadenceFramesEmitted > 0);
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task GetConflatedMbp_DoesNotAdvanceTradeCutoff()
    {
        var w = NewWiring([500]);
        try
        {
            w.BookManager.GetOrCreateBook(SecurityId);
            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 64);
            w.Manager.RegisterClient(session);
            _ = Task.Run(() => session.RunWriteLoopAsync());
            w.Manager.HandleSubscribe(
                session.Id,
                Symbol,
                DataFlags.ConflatedMbp | DataFlags.Trades,
                conflationIntervalMs: 500,
                w.BookManager,
                w.Group,
                bookBatchCutoffSequence: 0);
            await WaitUntil(() => rec.HasMessageType(MessageType.LevelSnapshot), TimeSpan.FromSeconds(2));

            w.Group.OnTrade(SecurityId, price: 1000, quantity: 2, tradeId: 9, sendingTimeNs: 0);
            w.Group.OnBatchComplete(); // queued batch sequence 1; broadcaster is not started yet

            w.Manager.HandleGet(
                session.Id,
                Symbol,
                DataFlags.ConflatedMbp,
                conflationIntervalMs: 500,
                w.BookManager,
                w.Group,
                bookBatchCutoffSequence: 1);

            w.Group.StartBroadcaster(0);
            await WaitUntil(() => rec.HasMessageType(MessageType.Trade), TimeSpan.FromSeconds(2));
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task ConflatedMbp_BootstrapIncludesMarketTierAndStaleStatus()
    {
        var w = NewWiring([500]);
        var book = w.BookManager.GetOrCreateBook(SecurityId);
        book.UpsertMarketOrder(
            orderId: 900,
            side: BookSideType.Bid,
            quantity: 123,
            enteringFirm: 7);
        w.BookManager.StateRegistry.HealFromSnapshot(SecurityId, SymbolGapKind.Mbo, 100);
        w.BookManager.StateRegistry.Observe(SecurityId, SymbolGapKind.Mbo, 102);
        Assert.True(w.BookManager.StateRegistry.IsAnyStale(SecurityId));

        var rec = new RecordingWebSocket();
        var session = new ClientSession(rec, channelCapacity: 64);
        w.Manager.RegisterClient(session);
        _ = Task.Run(() => session.RunWriteLoopAsync());
        w.Manager.HandleSubscribe(
            session.Id,
            Symbol,
            DataFlags.ConflatedMbp,
            conflationIntervalMs: 500,
            w.BookManager,
            w.Group,
            bookBatchCutoffSequence: 0);

        await WaitUntil(
            () => rec.HasMessageType(MessageType.LevelSnapshot) &&
                  rec.HasMessageType(MessageType.MarketTierUpdate) &&
                  rec.HasMessageType(MessageType.SymbolStaleStatus),
            TimeSpan.FromSeconds(2));

        var tiers = rec.AllFrames(MessageType.MarketTierUpdate);
        Assert.Equal(2, tiers.Count);
        var bidTier = Assert.Single(tiers, frame => frame[28] == (byte)BookSideType.Bid);
        var askTier = Assert.Single(tiers, frame => frame[28] == (byte)BookSideType.Ask);
        Assert.Equal(123L, BinaryPrimitives.ReadInt64LittleEndian(bidTier.AsSpan(16)));
        Assert.Equal(1u, BinaryPrimitives.ReadUInt32LittleEndian(bidTier.AsSpan(24)));
        Assert.Equal(0L, BinaryPrimitives.ReadInt64LittleEndian(askTier.AsSpan(16)));
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(askTier.AsSpan(24)));
        Assert.Equal(1, rec.LastFrame(MessageType.SymbolStaleStatus)![16]);
        w.Manager.Dispose();
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

    private static (SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager) NewWiring(
        IReadOnlyCollection<int>? cadences = null)
    {
        var manager = new SubscriptionManager(allowedConflatedCadencesMs: cadences);
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

    private static CadenceConflationBuffer GetCadenceBuffer(GroupConflationHandler group)
    {
        var buffers = (CadenceConflationBuffer[])typeof(GroupConflationHandler)
            .GetField("_cadenceBuffers", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(group)!;
        return Assert.Single(buffers);
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
            while (o + 8 <= bytes.Length)
            {
                int frameLen = (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(o));
                if (frameLen < 8 || o + frameLen > bytes.Length) break;
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
                if (f.Length >= 8 && (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(f.AsSpan(4)) == t)
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
                if (f.Length >= 8 && (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(f.AsSpan(4)) == t)
                    return f;
            }
            return null;
        }
    }

    public List<byte[]> AllFrames(MessageType t)
    {
        lock (_lock)
        {
            var result = new List<byte[]>();
            foreach (var f in _frames)
            {
                if (f.Length >= 8 && (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(f.AsSpan(4)) == t)
                    result.Add(f);
            }
            return result;
        }
    }
}
