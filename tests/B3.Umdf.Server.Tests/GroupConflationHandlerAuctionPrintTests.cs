using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Reflection;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Pins the AuctionPrint derivation (issue #46): a trade is flagged with
/// <see cref="TradeFlags.AuctionPrint"/> when either
/// <list type="bullet">
///   <item>the upstream SBE message carried <c>TradeCondition.OpeningPrice</c>
///     (BookManager.HandleTrade emits the flag), or</item>
///   <item>the security's current TradingStatus is <c>RESERVED</c> /
///     <c>FINAL_CLOSING_CALL</c> (GroupConflationHandler augments the flag).</item>
/// </list>
/// Verified end-to-end through the BufferTrade → FlushTradeBuffer → WriteTrade
/// pipeline so the flag survives all the way to the wire.
/// </summary>
public class GroupConflationHandlerAuctionPrintTests
{
    private const ulong SecurityId = 5001;
    private const string Symbol = "AUC";

    [Fact]
    public void OnTrade_FlagFromBookManager_PropagatesToWireFrame()
    {
        var w = NewWiring();
        try
        {
            SubscribeOne(w);

            // Simulate BookManager calling the flagged overload because the
            // SBE TradeCondition.OpeningPrice bit was set on the source message.
            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1,
                sendingTimeNs: 0, flags: TradeFlags.AuctionPrint);
            w.Group.OnBatchComplete();

            Assert.Equal(TradeFlags.AuctionPrint, LastWireFlags(w));
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void OnTrade_RegularPhase_NoFlag()
    {
        var w = NewWiring();
        try
        {
            SubscribeOne(w);
            // Default phase: status unknown → IsAuctionPhase returns false.
            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            Assert.Equal(TradeFlags.None, LastWireFlags(w));
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Theory]
    [InlineData((int)TradingSessionSubID.RESERVED)]
    [InlineData((int)TradingSessionSubID.FINAL_CLOSING_CALL)]
    public void OnTrade_AuctionPhase_AddsFlag(int status)
    {
        var w = NewWiring();
        try
        {
            SubscribeOne(w);
            SetTradingStatus(w, status);

            // Legacy overload, no upstream flag — handler must derive it from phase.
            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            Assert.Equal(TradeFlags.AuctionPrint, LastWireFlags(w));
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void OnTrade_OpenPhase_NoFlag()
    {
        var w = NewWiring();
        try
        {
            SubscribeOne(w);
            SetTradingStatus(w, (int)TradingSessionSubID.OPEN);

            w.Group.OnTrade(SecurityId, price: 100, quantity: 5, tradeId: 1, sendingTimeNs: 0);
            w.Group.OnBatchComplete();

            Assert.Equal(TradeFlags.None, LastWireFlags(w));
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void Conflation_AnyAuctionFlag_StickyAfterCoalescing()
    {
        var w = NewWiring();
        try
        {
            SubscribeOne(w);

            // Three trades at same (secId, price): middle one is auction; the
            // OR-merge in BufferTrade must keep the flag set after coalescing.
            w.Group.OnTrade(SecurityId, 100, 1, 1, 0, TradeFlags.None);
            w.Group.OnTrade(SecurityId, 100, 1, 2, 0, TradeFlags.AuctionPrint);
            w.Group.OnTrade(SecurityId, 100, 1, 3, 0, TradeFlags.None);
            w.Group.OnBatchComplete();

            Assert.Equal(TradeFlags.AuctionPrint, LastWireFlags(w));
        }
        finally
        {
            w.Manager.Dispose();
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static TradeFlags LastWireFlags((SubscriptionManager Manager, GroupConflationHandler Group,
        BookManager BookManager, RecordingWebSocket Recorder) w)
    {
        // Wait for the broadcast — fall back to spinning on Test thread; the
        // broadcaster runs in the background.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        byte[]? frame = null;
        while (DateTime.UtcNow < deadline)
        {
            frame = w.Recorder.LastFrame(MessageType.Trade);
            if (frame is not null) break;
            Thread.Sleep(10);
        }
        Assert.NotNull(frame);
        Assert.Equal(37, frame!.Length);
        return (TradeFlags)frame[36];
    }

    private static void SetTradingStatus((SubscriptionManager Manager, GroupConflationHandler Group,
        BookManager BookManager, RecordingWebSocket Recorder) w, int status)
    {
        var info = new InstrumentInfo { TradingStatus = status };
        w.Group.OnMarketDataUpdated(SecurityId, info);
    }

    private static (SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager,
        RecordingWebSocket Recorder) NewWiring()
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();
        group.StartBroadcaster(0);
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

        return (manager, group, book, new RecordingWebSocket());
    }

    private static void SubscribeOne((SubscriptionManager Manager, GroupConflationHandler Group,
        BookManager BookManager, RecordingWebSocket Recorder) w)
    {
        var session = new ClientSession(w.Recorder, channelCapacity: 64);
        w.Manager.RegisterClient(session);
        _ = Task.Run(() => session.RunWriteLoopAsync());
        w.Manager.HandleSubscribe(session.Id, Symbol, DataFlags.Trades,
            w.BookManager, w.Group, bookBatchCutoffSequence: 0);
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
}
