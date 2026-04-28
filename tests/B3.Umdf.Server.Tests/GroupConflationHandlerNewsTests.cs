using System.Collections.Concurrent;
using System.Reflection;
using B3.Umdf.Book;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// P13-4 fan-out coverage. Validates the per-client News-subscription counter
/// (used by the global news fast-path) and that <see cref="GroupConflationHandler.OnNews"/>
/// enqueues serialized wire frames to the right subscriber set.
/// </summary>
public class GroupConflationHandlerNewsTests
{
    private const ulong SecurityId = 2001;
    private const ulong OtherSecurityId = 2002;
    private const string Symbol = "NEWS1";
    private const string OtherSymbol = "NEWS2";

    [Fact]
    public void Subscribe_WithNewsFlag_IncrementsHasAnyNewsSubscription()
    {
        var w = NewWiring();
        try
        {
            Assert.False(w.Session.HasAnyNewsSubscription);

            w.Manager.HandleSubscribe(w.Session.Id, Symbol,
                DataFlags.Book | DataFlags.News, w.BookManager, w.Group, 0);

            Assert.True(w.Session.HasAnyNewsSubscription);
        }
        finally { w.Manager.Dispose(); }
    }

    [Fact]
    public void Subscribe_WithoutNewsFlag_DoesNotChangeCounter()
    {
        var w = NewWiring();
        try
        {
            w.Manager.HandleSubscribe(w.Session.Id, Symbol, DataFlags.Book, w.BookManager, w.Group, 0);
            Assert.False(w.Session.HasAnyNewsSubscription);
        }
        finally { w.Manager.Dispose(); }
    }

    [Fact]
    public void Subscribe_TogglingNewsFlag_OnSameSecurity_DoesNotDoubleCount()
    {
        var w = NewWiring();
        try
        {
            w.Manager.HandleSubscribe(w.Session.Id, Symbol, DataFlags.News, w.BookManager, w.Group, 0);
            w.Manager.HandleSubscribe(w.Session.Id, Symbol, DataFlags.News | DataFlags.Book, w.BookManager, w.Group, 0);
            Assert.True(w.Session.HasAnyNewsSubscription);

            // Drop News bit on second subscription update -> counter goes back to 0.
            w.Manager.HandleSubscribe(w.Session.Id, Symbol, DataFlags.Book, w.BookManager, w.Group, 0);
            Assert.False(w.Session.HasAnyNewsSubscription);
        }
        finally { w.Manager.Dispose(); }
    }

    [Fact]
    public void Subscribe_NewsOnTwoSecurities_RefcountsBoth()
    {
        var w = NewWiring();
        try
        {
            w.Manager.HandleSubscribe(w.Session.Id, Symbol, DataFlags.News, w.BookManager, w.Group, 0);
            w.Manager.HandleSubscribe(w.Session.Id, OtherSymbol, DataFlags.News, w.BookManager, w.Group, 0);
            Assert.True(w.Session.HasAnyNewsSubscription);

            // Drop news on one of them: counter still > 0 thanks to the other.
            w.Manager.HandleSubscribe(w.Session.Id, Symbol, DataFlags.Book, w.BookManager, w.Group, 0);
            Assert.True(w.Session.HasAnyNewsSubscription);

            // Drop news on the second: counter goes to 0.
            w.Manager.HandleSubscribe(w.Session.Id, OtherSymbol, DataFlags.Book, w.BookManager, w.Group, 0);
            Assert.False(w.Session.HasAnyNewsSubscription);
        }
        finally { w.Manager.Dispose(); }
    }

    [Fact]
    public async Task OnNews_PerSymbol_DeliversOnlyToNewsSubscribers()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            // Two clients on the same symbol: one has News, the other doesn't.
            var bookOnly = new ClientSession(new FakeWebSocket(),
                channelCapacity: 64);
            w.Manager.RegisterClient(bookOnly);
            w.Manager.HandleSubscribe(bookOnly.Id, Symbol, DataFlags.Book, w.BookManager, w.Group, 0);
            w.Manager.HandleSubscribe(w.Session.Id, Symbol, DataFlags.News, w.BookManager, w.Group, 0);

            int bookOnlyBefore = bookOnly.QueueDepth;
            int newsBefore = w.Session.QueueDepth;

            w.Group.OnNews(SecurityId, newsId: 42, source: 1, language: 0,
                origTimeNanos: 0,
                headline: System.Text.Encoding.UTF8.GetBytes("HEAD"),
                text: System.Text.Encoding.UTF8.GetBytes("BODY"),
                url: System.Text.Encoding.UTF8.GetBytes(""));
            w.Group.OnBatchComplete();

            await WaitUntil(() => w.Session.QueueDepth > newsBefore, TimeSpan.FromSeconds(2));

            // Book-only client must NOT receive any news bytes.
            await Task.Delay(100);
            Assert.Equal(bookOnlyBefore, bookOnly.QueueDepth);
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public async Task OnNews_Global_DeliversToAllClientsWithNewsFlag()
    {
        var w = NewWiring();
        w.Group.StartBroadcaster(0);
        try
        {
            var withNews1 = w.Session;
            var withNews2 = new ClientSession(new FakeWebSocket(), channelCapacity: 64);
            var withoutNews = new ClientSession(new FakeWebSocket(), channelCapacity: 64);
            w.Manager.RegisterClient(withNews2);
            w.Manager.RegisterClient(withoutNews);

            // Subscribe each client to ANY symbol with appropriate flags. The
            // securityId chosen is irrelevant — for global news the only check
            // is HasAnyNewsSubscription on the session.
            w.Manager.HandleSubscribe(withNews1.Id, Symbol, DataFlags.News, w.BookManager, w.Group, 0);
            w.Manager.HandleSubscribe(withNews2.Id, OtherSymbol, DataFlags.News, w.BookManager, w.Group, 0);
            w.Manager.HandleSubscribe(withoutNews.Id, Symbol, DataFlags.Book, w.BookManager, w.Group, 0);

            int q1 = withNews1.QueueDepth, q2 = withNews2.QueueDepth, q3 = withoutNews.QueueDepth;

            // securityId = 0 → global broadcast.
            w.Group.OnNews(0, 7, 1, 0, 0,
                headline: System.Text.Encoding.UTF8.GetBytes("ALERT"),
                text: System.Text.Encoding.UTF8.GetBytes(""),
                url: System.Text.Encoding.UTF8.GetBytes(""));
            w.Group.OnBatchComplete();

            await WaitUntil(() => withNews1.QueueDepth > q1 && withNews2.QueueDepth > q2,
                TimeSpan.FromSeconds(2));
            await Task.Delay(100);
            Assert.Equal(q3, withoutNews.QueueDepth);
        }
        finally
        {
            w.Group.StopBroadcaster();
            w.Manager.Dispose();
        }
    }

    [Fact]
    public void OnNews_NoSubscribers_ShortCircuits()
    {
        var w = NewWiring();
        try
        {
            int before = w.Session.QueueDepth;
            // No client has the News flag → OnNews must not enqueue any bytes.
            w.Group.OnNews(SecurityId, 1, 0, 0, 0,
                System.Text.Encoding.UTF8.GetBytes("h"),
                System.Text.Encoding.UTF8.GetBytes("t"),
                System.Text.Encoding.UTF8.GetBytes("u"));
            w.Group.OnNews(0, 1, 0, 0, 0,
                System.Text.Encoding.UTF8.GetBytes("h"),
                System.Text.Encoding.UTF8.GetBytes("t"),
                System.Text.Encoding.UTF8.GetBytes("u"));
            Assert.Equal(before, w.Session.QueueDepth);
        }
        finally { w.Manager.Dispose(); }
    }

    private static (SubscriptionManager Manager, GroupConflationHandler Group, BookManager BookManager, ClientSession Session) NewWiring()
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var book = new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);
        group.SetBookManager(book);

        var symbols = new SymbolRegistry();
        RegisterSymbol(symbols, Symbol, SecurityId);
        RegisterSymbol(symbols, OtherSymbol, OtherSecurityId);

        manager.SetDataSources(
            new[] { book },
            new[] { new MarketDataManager(stateRegistry: registry) },
            symbols,
            new[] { group });
        manager.SetReady();

        var session = new ClientSession(new FakeWebSocket(), channelCapacity: 64);
        manager.RegisterClient(session);
        return (manager, group, book, session);
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
