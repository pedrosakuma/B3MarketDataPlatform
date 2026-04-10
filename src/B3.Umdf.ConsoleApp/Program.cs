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

using var feedHandler = new FeedHandler(replayer, bookManager);
await feedHandler.StartAsync();

try
{
    // Wait for the replay to finish (replayer will close the channel)
    await Task.Delay(Timeout.Infinite, CancellationToken.None);
}
catch (OperationCanceledException)
{
}

Console.WriteLine();
Console.WriteLine("Replay complete.");
Console.WriteLine($"  Books tracked: {bookManager.Books.Count}");

return 0;

sealed class ConsoleBookEventHandler : IBookEventHandler
{
    private int _tradeCount;

    public void OnOrderAdded(OrderBook book, OrderBookEntry entry)
    {
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
        if (_tradeCount <= 100 || _tradeCount % 1000 == 0)
        {
            Console.WriteLine($"  Trade #{_tradeCount}: SecurityId={securityId} Price={price} Qty={quantity} TradeId={tradeId}");
        }
    }

    public void OnBookCleared(ulong securityId)
    {
        Console.WriteLine($"  Book cleared: SecurityId={securityId}");
    }
}
