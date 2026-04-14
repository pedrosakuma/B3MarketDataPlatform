using System.Diagnostics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.PcapReplay;
using B3.Umdf.Server;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;

// Parse named arguments
// Quick health check mode for Docker HEALTHCHECK
if (args.Length == 1 && args[0] == "--health-check")
{
    try
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        var port = Environment.GetEnvironmentVariable("WS_PORT") ?? "8080";
        var resp = await http.GetAsync($"http://localhost:{port}/live");
        return resp.IsSuccessStatusCode ? 0 : 1;
    }
    catch { return 1; }
}

int? wsPort = null;
double speed = 0;
var pcapPrefixes = new List<string>();
var positionalArgs = new List<string>();
string? multicastConfig = null;

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
    else if (args[i] == "--speed" && i + 1 < args.Length)
    {
        if (!double.TryParse(args[++i], System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out speed) || speed < 0)
        {
            Console.Error.WriteLine("Invalid --speed value. Use 0 for max speed, 1 for real-time, >1 for accelerated.");
            return 1;
        }
    }
    else if (args[i] == "--pcap-prefix" && i + 1 < args.Length)
    {
        pcapPrefixes.Add(args[++i]);
    }
    else if (args[i] == "--multicast-config" && i + 1 < args.Length)
    {
        multicastConfig = args[++i];
    }
    else
    {
        positionalArgs.Add(args[i]);
    }
}

// Fallback to environment variables (for shell-less Docker images)
if (pcapPrefixes.Count == 0 && positionalArgs.Count == 0 && multicastConfig is null)
{
    var envPcapDir = Environment.GetEnvironmentVariable("PCAP_DIR") ?? "/app/pcap";
    var envPcapPrefix = Environment.GetEnvironmentVariable("PCAP_PREFIX") ?? "";
    if (!string.IsNullOrEmpty(envPcapPrefix))
    {
        foreach (var prefix in envPcapPrefix.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            pcapPrefixes.Add(Path.Combine(envPcapDir, prefix));
    }
    if (wsPort is null && int.TryParse(Environment.GetEnvironmentVariable("WS_PORT"), out var envPort))
        wsPort = envPort;
    if (speed == 0 && double.TryParse(Environment.GetEnvironmentVariable("REPLAY_SPEED"),
            System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var envSpeed))
        speed = envSpeed;
}

// Build packet source — either from multicast config, --pcap-prefix, or positional args
IPacketSource packetSource;
var groupIds = new List<int>();
MulticastChannelMerger? multicastMerger = null;

if (multicastConfig is not null)
{
    // Live multicast mode
    if (!File.Exists(multicastConfig))
    {
        Console.Error.WriteLine($"Multicast config not found: {multicastConfig}");
        return 1;
    }

    var feedConfig = MulticastFeedConfig.Load(multicastConfig);
    groupIds = feedConfig.GetGroupIds();
    var channelConfigs = feedConfig.ToChannelConfigs();

    Console.WriteLine("  Mode: Live multicast");
    foreach (var group in feedConfig.ChannelGroups)
    {
        int g = feedConfig.ChannelGroups.IndexOf(group);
        Console.WriteLine($"  Channel group {g}: {group.Name}");
        foreach (var ch in group.Channels)
            Console.WriteLine($"    {ch.Type,-25} <- {ch.MulticastGroup}:{ch.Port}");
    }

    var multicastSources = channelConfigs
        .Select(c => new MulticastPacketSource(c))
        .ToList();

    multicastMerger = new MulticastChannelMerger(multicastSources);
    packetSource = multicastMerger;
}
else
{
    // PCAP replay mode
    var sources = new List<PcapChannelSource>();

    var channelSuffixes = new[]
    {
        ("Incremental_FeedA", ChannelType.IncrementalA),
        ("Incremental_FeedB", ChannelType.IncrementalB),
        ("InstrumentDefinition", ChannelType.InstrumentDefinition),
        ("SnapshotRecovery", ChannelType.SnapshotRecovery),
    };

    if (pcapPrefixes.Count > 0)
    {
        for (int g = 0; g < pcapPrefixes.Count; g++)
        {
            var prefix = pcapPrefixes[g];
            Console.WriteLine($"  Channel group {g}: {Path.GetFileName(prefix)}");
            groupIds.Add(g);

            foreach (var (suffix, channelType) in channelSuffixes)
            {
                var filePath = $"{prefix}_{suffix}.pcap";
                if (!File.Exists(filePath))
                {
                    Console.Error.WriteLine($"  File not found: {filePath}");
                    return 1;
                }
                sources.Add(new PcapChannelSource(filePath, channelType, g));
                Console.WriteLine($"    {channelType,-25} <- {Path.GetFileName(filePath)}");
            }
        }
    }
    else if (positionalArgs.Count >= 1)
    {
        var channelTypes = new[] { ChannelType.IncrementalA, ChannelType.IncrementalB, ChannelType.InstrumentDefinition, ChannelType.SnapshotRecovery };
        groupIds.Add(0);

        for (int i = 0; i < positionalArgs.Count && i < channelTypes.Length; i++)
        {
            if (!File.Exists(positionalArgs[i]))
            {
                Console.Error.WriteLine($"File not found: {positionalArgs[i]}");
                return 1;
            }
            sources.Add(new PcapChannelSource(positionalArgs[i], channelTypes[i], 0));
            Console.WriteLine($"  {channelTypes[i],-25} <- {positionalArgs[i]}");
        }
    }
    else
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  B3.Umdf.ConsoleApp --pcap-prefix <prefix> [--pcap-prefix <prefix2>] [options]");
        Console.WriteLine("  B3.Umdf.ConsoleApp --multicast-config <config.json> [options]");
        Console.WriteLine("  B3.Umdf.ConsoleApp <incrA.pcap> [incrB.pcap] [instrdef.pcap] [snapshot.pcap] [options]");
        Console.WriteLine();
        Console.WriteLine("The --pcap-prefix mode auto-discovers files by naming convention:");
        Console.WriteLine("  <prefix>_Incremental_FeedA.pcap, <prefix>_Incremental_FeedB.pcap,");
        Console.WriteLine("  <prefix>_InstrumentDefinition.pcap, <prefix>_SnapshotRecovery.pcap");
        Console.WriteLine();
        Console.WriteLine("The --multicast-config mode reads channel configs from a JSON file.");
        Console.WriteLine("  See config/multicast-sample.json for the format.");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --ws-port <port>              Start WebSocket subscription server on the given port");
        Console.WriteLine("  --speed <mult>                Replay speed: 0=max, 1=real-time, 2=2x, etc. (default: 0)");
        Console.WriteLine("  --pcap-prefix <prefix>        Channel group PCAP prefix (repeatable for multi-channel)");
        Console.WriteLine("  --multicast-config <file>     JSON config with multicast group addresses/ports");
        return 1;
    }

    Console.WriteLine($"  Mode: PCAP replay");

    packetSource = new TimestampMergedReplayer(sources, new ReplayOptions { SpeedMultiplier = speed });
}

Console.WriteLine($"  Speed: {(speed == 0 ? "max" : $"{speed}x")}");
Console.WriteLine($"  Channel groups: {groupIds.Count}");
Console.WriteLine();

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddSimpleConsole(opts =>
    {
        opts.SingleLine = true;
        opts.TimestampFormat = "HH:mm:ss.fff ";
    });
    builder.SetMinimumLevel(LogLevel.Information);
});

var stats = new Stats();

// Wire up subscription manager if WebSocket port is specified
SubscriptionManager? subscriptionManager = null;
WebSocketHost? wsHost = null;
var symbolRegistry = new SymbolRegistry();

IBookEventHandler bookHandler;
IMarketDataEventHandler mdHandler;

if (wsPort is not null)
{
    subscriptionManager = new SubscriptionManager(loggerFactory.CreateLogger<SubscriptionManager>());
    bookHandler = new CompositeBookEventHandler(stats, subscriptionManager);
    mdHandler = new CompositeMarketDataEventHandler(stats, subscriptionManager);
}
else
{
    bookHandler = stats;
    mdHandler = stats;
}

var bookManager = new BookManager(bookHandler, loggerFactory.CreateLogger<BookManager>());
var marketDataManager = new MarketDataManager(mdHandler, loggerFactory.CreateLogger<MarketDataManager>());

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// Handle SIGTERM for graceful shutdown in containers
AppDomain.CurrentDomain.ProcessExit += (_, _) =>
{
    if (!cts.IsCancellationRequested)
        cts.Cancel();
};

var composite = new CompositeFeedHandler(bookManager, marketDataManager, symbolRegistry);

// Use MultiFeedManager for multi-channel, single FeedHandler for single-channel
MultiFeedManager? multiFeed = null;
FeedHandler? singleFeed = null;

var feedLogger = loggerFactory.CreateLogger<FeedHandler>();

if (groupIds.Count > 1)
{
    multiFeed = new MultiFeedManager(packetSource, groupIds, composite, feedLogger);
    if (subscriptionManager is not null)
        multiFeed.AnyGroupReady += () => subscriptionManager.SetReady();
}
else
{
    singleFeed = new FeedHandler(packetSource, composite, feedLogger);
}

if (subscriptionManager is not null)
{
    subscriptionManager.SetDataSources(bookManager, marketDataManager, symbolRegistry);
    wsHost = new WebSocketHost(subscriptionManager, loggerFactory.CreateLogger<WebSocketHost>());

    // Wire up feed state and last-packet providers for /health endpoint
    if (multiFeed is not null)
    {
        var mf = multiFeed;
        wsHost.FeedStateProvider = () =>
        {
            var dict = new Dictionary<string, string>();
            foreach (var (gid, handler) in mf.Handlers)
                dict[$"G{gid}"] = handler.State.ToString();
            return dict;
        };
        wsHost.LastPacketTimestampProvider = () =>
        {
            var dict = new Dictionary<string, long>();
            foreach (var (gid, handler) in mf.Handlers)
                dict[$"G{gid}"] = handler.LastPacketTicks;
            return dict;
        };
    }
    else if (singleFeed is not null)
    {
        var sf = singleFeed;
        wsHost.FeedStateProvider = () => new Dictionary<string, string> { ["G0"] = sf.State.ToString() };
        wsHost.LastPacketTimestampProvider = () => new Dictionary<string, long> { ["G0"] = sf.LastPacketTicks };
    }

    await wsHost.StartAsync(wsPort!.Value, cts.Token);
}

// Periodic stats timer
var sw = Stopwatch.StartNew();
var lastReady = false;
using var statsTimer = new Timer(_ =>
{
    bool ready;
    long packets;
    string stateStr;

    if (multiFeed is not null)
    {
        ready = multiFeed.IsAllReady;
        packets = multiFeed.TotalPacketCount;
        var states = string.Join(", ", multiFeed.Handlers.Select(h => $"G{h.Key}:{h.Value.State}"));
        stateStr = states;
    }
    else if (singleFeed is not null)
    {
        ready = singleFeed.State == FeedState.RealTime;
        packets = singleFeed.PacketCount;
        stateStr = singleFeed.State.ToString();
    }
    else return;

    if (ready && !lastReady)
    {
        if (singleFeed is not null)
            subscriptionManager?.SetReady();
        lastReady = true;
    }

    Console.WriteLine();
    Console.WriteLine($"── [{sw.Elapsed:hh\\:mm\\:ss}] {stateStr} ──");
    Console.WriteLine($"   Packets: {packets:N0}  |  Orders: {stats.OrderCount:N0}  |  Trades: {stats.TradeCount:N0}  |  MktData: {stats.MarketDataCount:N0}  |  Books: {bookManager.Books.Count:N0}  |  Instruments: {marketDataManager.InstrumentData.Count:N0}  |  Symbols: {symbolRegistry.Count:N0}");

    if (subscriptionManager is not null)
    {
        foreach (var (id, depth) in subscriptionManager.GetClientStats())
            Console.WriteLine($"   {id}: queue={depth:N0}");
        if (subscriptionManager.UpstreamConflated > 0)
            Console.WriteLine($"   upstream conflated: {subscriptionManager.UpstreamConflated:N0}");
    }

    if (!ready && singleFeed is not null && singleFeed.State == FeedState.WaitInstrumentDefinition)
        Console.WriteLine($"   InstrDef: {singleFeed.InstrDefReceived:N0}/{singleFeed.InstrDefTotalExpected:N0} parsed  ({singleFeed.InstrDefPacketCount:N0} packets)");

    if (!ready && multiFeed is not null)
    {
        foreach (var (gid, h) in multiFeed.Handlers)
        {
            if (h.State == FeedState.WaitInstrumentDefinition)
                Console.WriteLine($"   G{gid} InstrDef: {h.InstrDefReceived:N0}/{h.InstrDefTotalExpected:N0} parsed  ({h.InstrDefPacketCount:N0} idef pkts, {h.PacketCount:N0} total pkts)");
        }
    }

}, null, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5));

Console.WriteLine($"Starting {(multicastMerger is not null ? "live feed" : "replay")}...");

if (multiFeed is not null)
{
    await multiFeed.StartAsync(cts.Token);
    try { await multiFeed.WaitForCompletionAsync(); }
    catch (OperationCanceledException) { Console.WriteLine("Cancelled."); }
}
else
{
    await singleFeed!.StartAsync(cts.Token);
    try { await singleFeed.WaitForCompletionAsync(); }
    catch (OperationCanceledException) { Console.WriteLine("Cancelled."); }
}

statsTimer.Change(Timeout.Infinite, Timeout.Infinite);
sw.Stop();

// Graceful shutdown: stop accepting new connections, drain, then stop
Console.WriteLine("Shutting down gracefully...");

// Stop feed first (stop producing new data)
if (multiFeed is not null)
    await multiFeed.StopAsync();
if (singleFeed is not null)
    await singleFeed.StopAsync();

// Brief drain period for in-flight WebSocket writes
if (wsHost is not null)
{
    await Task.Delay(TimeSpan.FromSeconds(2));
    await wsHost.StopAsync();
    await wsHost.DisposeAsync();
}

multiFeed?.Dispose();
singleFeed?.Dispose();
if (multicastMerger is not null)
    await multicastMerger.DisposeAsync();
else
    packetSource.Dispose();

Console.WriteLine();
Console.WriteLine($"═══ Complete ({sw.Elapsed:hh\\:mm\\:ss}) ═══");
Console.WriteLine($"  Channel groups: {groupIds.Count}");
Console.WriteLine($"  Packets:      {(multiFeed?.TotalPacketCount ?? singleFeed?.PacketCount ?? 0):N0}");
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

return 0;

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
        OrderCount++;
    }

    public void OnOrderUpdated(OrderBook book, OrderBookEntry entry)
    {
    }

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
        DeleteCount++;
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        TradeCount++;
    }

    public void OnBookCleared(ulong securityId, BookClearSide side)
    {
    }

    public void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info)
    {
        StatusChangeCount++;
    }

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        MarketDataCount++;
    }

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId)
    {
        ForwardTradeCount++;
    }

    public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
    {
        TradeBustCount++;
    }

    public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty)
    {
        ExecSummaryCount++;
    }
}
