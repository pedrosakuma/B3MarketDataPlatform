using System.Diagnostics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.PcapReplay;
using B3.Umdf.Server;
using B3.Umdf.Transport;

// Parse named arguments
int? wsPort = null;
var positionalArgs = new List<string>();

for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--ws-port" && i + 1 < args.Length)
    {
        if (!int.TryParse(args[++i], out var p) || p < 1 || p > 65535)
        {
            Console.Error.WriteLine("Invalid --ws-port value.");
            return 1;
        }
        wsPort = p;
    }
    else
    {
        positionalArgs.Add(args[i]);
    }
}

if (positionalArgs.Count < 1)
{
    Console.WriteLine("Usage: B3.Umdf.ConsoleApp <incremental-a.pcap> [incremental-b.pcap] [instrdef.pcap] [snapshot.pcap] [--ws-port <port>]");
    Console.WriteLine();
    Console.WriteLine("Replays B3 UMDF Binary PCAP files and prints decoded market data.");
    Console.WriteLine("Files are merged by timestamp for accurate cross-channel ordering.");
    Console.WriteLine();
    Console.WriteLine("Options:");
    Console.WriteLine("  --ws-port <port>   Start WebSocket subscription server on the given port");
    return 1;
}

var sources = new List<PcapChannelSource>();
var channelTypes = new[] { ChannelType.IncrementalA, ChannelType.IncrementalB, ChannelType.InstrumentDefinition, ChannelType.SnapshotRecovery };

for (int i = 0; i < positionalArgs.Count && i < channelTypes.Length; i++)
{
    if (!File.Exists(positionalArgs[i]))
    {
        Console.Error.WriteLine($"File not found: {positionalArgs[i]}");
        return 1;
    }
    sources.Add(new PcapChannelSource(positionalArgs[i], channelTypes[i]));
    Console.WriteLine($"  {channelTypes[i],-25} <- {positionalArgs[i]}");
}

Console.WriteLine();

var stats = new Stats();

// Wire up subscription manager if WebSocket port is specified
SubscriptionManager? subscriptionManager = null;
WebSocketHost? wsHost = null;
var symbolRegistry = new SymbolRegistry();

IBookEventHandler bookHandler;
IMarketDataEventHandler mdHandler;

if (wsPort is not null)
{
    subscriptionManager = new SubscriptionManager();
    bookHandler = new CompositeBookEventHandler(stats, subscriptionManager);
    mdHandler = new CompositeMarketDataEventHandler(stats, subscriptionManager);
}
else
{
    bookHandler = stats;
    mdHandler = stats;
}

var bookManager = new BookManager(bookHandler);
var marketDataManager = new MarketDataManager(mdHandler);
var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0 });

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var composite = new CompositeFeedHandler(bookManager, marketDataManager, symbolRegistry);
using var feedHandler = new FeedHandler(replayer, composite);

if (subscriptionManager is not null)
{
    subscriptionManager.SetDataSources(bookManager, marketDataManager, symbolRegistry);
    subscriptionManager.Start();
    wsHost = new WebSocketHost(subscriptionManager);
    await wsHost.StartAsync(wsPort!.Value, cts.Token);
}

// Periodic stats timer
var sw = Stopwatch.StartNew();
var lastState = FeedState.WaitInstrumentDefinition;
using var statsTimer = new Timer(_ =>
{
    var state = feedHandler.State;
    var stateChanged = state != lastState;
    lastState = state;

    if (stateChanged && state == FeedState.RealTime)
        subscriptionManager?.SetReady();

    Console.WriteLine();
    Console.WriteLine($"── [{sw.Elapsed:hh\\:mm\\:ss}] State: {state} ──");
    Console.WriteLine($"   Packets: {feedHandler.PacketCount:N0}  |  Orders: {stats.OrderCount:N0}  |  Trades: {stats.TradeCount:N0}  |  MktData: {stats.MarketDataCount:N0}  |  Books: {bookManager.Books.Count:N0}  |  Instruments: {marketDataManager.InstrumentData.Count:N0}");
    if (state == FeedState.WaitInstrumentDefinition)
        Console.WriteLine($"   InstrDef: {feedHandler.InstrDefReceived:N0}/{feedHandler.InstrDefTotalExpected:N0} parsed  ({feedHandler.InstrDefPacketCount:N0} packets)");

    if (state == FeedState.RealTime && bookManager.Books.Count > 0)
        PrintBookSample(bookManager);

}, null, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5));

Console.WriteLine("Starting replay...");
await feedHandler.StartAsync(cts.Token);

try
{
    await feedHandler.WaitForCompletionAsync();
}
catch (OperationCanceledException)
{
    Console.WriteLine("Cancelled.");
}

statsTimer.Change(Timeout.Infinite, Timeout.Infinite);
sw.Stop();

if (wsHost is not null)
    await wsHost.StopAsync();
subscriptionManager?.Dispose();
wsHost?.Dispose();

Console.WriteLine();
Console.WriteLine($"═══ Replay complete ({sw.Elapsed:hh\\:mm\\:ss}) ═══");
Console.WriteLine($"  Final state:  {feedHandler.State}");
Console.WriteLine($"  Packets:      {feedHandler.PacketCount:N0}");
Console.WriteLine($"  Orders:       {stats.OrderCount:N0}");
Console.WriteLine($"  Trades:       {stats.TradeCount:N0}");
Console.WriteLine($"  Deletes:      {stats.DeleteCount:N0}");
Console.WriteLine($"  MarketData:   {stats.MarketDataCount:N0}");
Console.WriteLine($"  StatusChg:    {stats.StatusChangeCount:N0}");
Console.WriteLine($"  FwdTrades:    {stats.ForwardTradeCount:N0}");
Console.WriteLine($"  TradeBusts:   {stats.TradeBustCount:N0}");
Console.WriteLine($"  ExecSummary:  {stats.ExecSummaryCount:N0}");
Console.WriteLine($"  Books:        {bookManager.Books.Count:N0}");
Console.WriteLine($"  Instruments:  {marketDataManager.InstrumentData.Count:N0}");
Console.WriteLine($"  Symbols:      {symbolRegistry.Count:N0}");

if (bookManager.Books.Count > 0)
    PrintBookSample(bookManager);

return 0;

static void PrintBookSample(BookManager bookManager)
{
    try
    {
        // Pick the book with the most orders as a representative sample
        OrderBook? best = null;
        int bestCount = 0;
        foreach (var book in bookManager.Books.Values)
        {
            int count = book.Bids.Orders.Count + book.Asks.Orders.Count;
            if (count > bestCount)
            {
                bestCount = count;
                best = book;
            }
        }

        if (best is null || bestCount == 0)
            return;

        Console.WriteLine($"   ── Book sample: SecurityId={best.SecurityId} ({best.Bids.Orders.Count} bids, {best.Asks.Orders.Count} asks) ──");

        var topBids = best.Bids.PriceLevels.Take(5)
            .Select(kv => (Price: kv.Key, Qty: kv.Value.Sum(o => o.Quantity), Count: kv.Value.Count))
            .ToList();
        var topAsks = best.Asks.PriceLevels.Take(5)
            .Select(kv => (Price: kv.Key, Qty: kv.Value.Sum(o => o.Quantity), Count: kv.Value.Count))
            .ToList();

        Console.WriteLine("       BIDS                        ASKS");
        Console.WriteLine("       Price        Qty  Count     Price        Qty  Count");

        int rows = Math.Max(topBids.Count, topAsks.Count);

        for (int i = 0; i < rows; i++)
        {
            string bidStr = i < topBids.Count
                ? $"  {topBids[i].Price,12:N0} {topBids[i].Qty,10:N0} {topBids[i].Count,5}"
                : new string(' ', 30);
            string askStr = i < topAsks.Count
                ? $"  {topAsks[i].Price,12:N0} {topAsks[i].Qty,10:N0} {topAsks[i].Count,5}"
                : "";
            Console.WriteLine($"    {bidStr}  |{askStr}");
        }
    }
    catch (InvalidOperationException)
    {
        // Collection was modified during enumeration (timer vs feed handler race)
    }
}

sealed class Stats : IBookEventHandler, IMarketDataEventHandler
{
    public long OrderCount;
    public long TradeCount;
    public long DeleteCount;
    public long MarketDataCount;
    public long StatusChangeCount;
    public long ForwardTradeCount;
    public long TradeBustCount;
    public long ExecSummaryCount;

    public void OnOrderAdded(OrderBook book, OrderBookEntry entry)
    {
        Interlocked.Increment(ref OrderCount);
    }

    public void OnOrderUpdated(OrderBook book, OrderBookEntry entry)
    {
    }

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
        Interlocked.Increment(ref DeleteCount);
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        Interlocked.Increment(ref TradeCount);
    }

    public void OnBookCleared(ulong securityId)
    {
    }

    public void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info)
    {
        Interlocked.Increment(ref StatusChangeCount);
    }

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        Interlocked.Increment(ref MarketDataCount);
    }

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        Interlocked.Increment(ref ForwardTradeCount);
    }

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
    {
        Interlocked.Increment(ref TradeBustCount);
    }

    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty)
    {
        Interlocked.Increment(ref ExecSummaryCount);
    }
}
