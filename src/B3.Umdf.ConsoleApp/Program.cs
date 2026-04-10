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
Console.WriteLine("Starting replay...");

var bookHandler = new ConsoleBookEventHandler();
var bookManager = new BookManager(bookHandler);
var replayer = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = 0 }); // burst mode

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

using var feedHandler = new FeedHandler(replayer, bookManager);
await feedHandler.StartAsync(cts.Token);

try
{
    // Wait for the replay to finish (replayer closes the channel when done)
    await feedHandler.WaitForCompletionAsync();
}
catch (OperationCanceledException)
{
    Console.WriteLine("Cancelled.");
}

Console.WriteLine();
Console.WriteLine("Replay complete.");
Console.WriteLine($"  State: {feedHandler.State}");
Console.WriteLine($"  Books tracked: {bookManager.Books.Count}");

return 0;

sealed class ConsoleBookEventHandler : IBookEventHandler
{
    private int _tradeCount;
    private int _orderCount;

    public void OnOrderAdded(OrderBook book, OrderBookEntry entry)
    {
        _orderCount++;
        if (_orderCount <= 10 || _orderCount % 100_000 == 0)
            Console.WriteLine($"  Order #{_orderCount}: SecurityId={entry.SecurityId} {entry.Side} Price={entry.Price} Qty={entry.Quantity}");
    }

    public void OnOrderUpdated(OrderBook book, OrderBookEntry entry)
    {
    }

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        _tradeCount++;
        if (_tradeCount <= 100 || _tradeCount % 10_000 == 0)
        {
            Console.WriteLine($"  Trade #{_tradeCount}: SecurityId={securityId} Price={price} Qty={quantity} TradeId={tradeId}");
        }
    }

    public void OnBookCleared(ulong securityId)
    {
        Console.WriteLine($"  Book cleared: SecurityId={securityId}");
    }
}
