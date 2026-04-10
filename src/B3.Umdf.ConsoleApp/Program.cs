using System.Diagnostics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.PcapReplay;
using B3.Umdf.Transport;

if (args.Length < 1)
{
    Console.WriteLine("Usage: B3.Umdf.ConsoleApp <incremental-a.pcap> [incremental-b.pcap] [instrdef.pcap] [snapshot.pcap]");
    Console.WriteLine();
    Console.WriteLine("Replays B3 UMDF Binary PCAP files and prints decoded market data.");
    Console.WriteLine("Files are merged by timestamp for accurate cross-channel ordering.");
    return 1;
}

var sources = new List<PcapChannelSource>();
var channelTypes = new[] { ChannelType.IncrementalA, ChannelType.IncrementalB, ChannelType.InstrumentDefinition, ChannelType.SnapshotRecovery };

for (int i = 0; i < args.Length && i < channelTypes.Length; i++)
{
    if (!File.Exists(args[i]))
    {
        Console.Error.WriteLine($"File not found: {args[i]}");
        return 1;
    }
    sources.Add(new PcapChannelSource(args[i], channelTypes[i]));
    Console.WriteLine($"  {channelTypes[i],-25} <- {args[i]}");
}

Console.WriteLine();

var stats = new Stats();
var bookManager = new BookManager(stats);
var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0 });

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

using var feedHandler = new FeedHandler(replayer, bookManager);

// Periodic stats timer
var sw = Stopwatch.StartNew();
var lastState = FeedState.WaitInstrumentDefinition;
using var statsTimer = new Timer(_ =>
{
    var state = feedHandler.State;
    var stateChanged = state != lastState;
    lastState = state;

    Console.WriteLine();
    Console.WriteLine($"── [{sw.Elapsed:hh\\:mm\\:ss}] State: {state} ──");
    Console.WriteLine($"   Packets: {feedHandler.PacketCount:N0}  |  Orders: {stats.OrderCount:N0}  |  Trades: {stats.TradeCount:N0}  |  Books: {bookManager.Books.Count:N0}");
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

Console.WriteLine();
Console.WriteLine($"═══ Replay complete ({sw.Elapsed:hh\\:mm\\:ss}) ═══");
Console.WriteLine($"  Final state:  {feedHandler.State}");
Console.WriteLine($"  Packets:      {feedHandler.PacketCount:N0}");
Console.WriteLine($"  Orders:       {stats.OrderCount:N0}");
Console.WriteLine($"  Trades:       {stats.TradeCount:N0}");
Console.WriteLine($"  Deletes:      {stats.DeleteCount:N0}");
Console.WriteLine($"  Books:        {bookManager.Books.Count:N0}");

if (bookManager.Books.Count > 0)
    PrintBookSample(bookManager);

return 0;

static void PrintBookSample(BookManager bookManager)
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
        .Select(kv => (Price: kv.Key, Qty: kv.Value.Sum(o => o.Quantity), Count: kv.Value.Count));
    var topAsks = best.Asks.PriceLevels.Take(5)
        .Select(kv => (Price: kv.Key, Qty: kv.Value.Sum(o => o.Quantity), Count: kv.Value.Count));

    Console.WriteLine("       BIDS                        ASKS");
    Console.WriteLine("       Price        Qty  Count     Price        Qty  Count");

    var bidList = topBids.ToList();
    var askList = topAsks.ToList();
    int rows = Math.Max(bidList.Count, askList.Count);

    for (int i = 0; i < rows; i++)
    {
        string bidStr = i < bidList.Count
            ? $"  {bidList[i].Price,12:N0} {bidList[i].Qty,10:N0} {bidList[i].Count,5}"
            : new string(' ', 30);
        string askStr = i < askList.Count
            ? $"  {askList[i].Price,12:N0} {askList[i].Qty,10:N0} {askList[i].Count,5}"
            : "";
        Console.WriteLine($"    {bidStr}  |{askStr}");
    }
}

sealed class Stats : IBookEventHandler
{
    public long OrderCount;
    public long TradeCount;
    public long DeleteCount;

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
}
