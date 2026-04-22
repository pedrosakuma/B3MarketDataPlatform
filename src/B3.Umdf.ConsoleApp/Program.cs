using System.Diagnostics;
using System.Runtime.InteropServices;
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
        var port = Environment.GetEnvironmentVariable("UMDF_WS_PORT")
            ?? Environment.GetEnvironmentVariable("WS_PORT")
            ?? "8080";
        var resp = await http.GetAsync($"http://localhost:{port}/live");
        return resp.IsSuccessStatusCode ? 0 : 1;
    }
    catch { return 1; }
}

var settings = AppSettings.LoadDefault();
settings.ApplyEnvironment();

int? wsPort = settings.WsPort;
double speed = settings.Speed;
bool replayToMulticast = settings.ReplayToMulticast;
var pcapPrefixes = new List<string>();
var positionalArgs = new List<string>();
string? multicastConfig = settings.MulticastConfig;
int maxConnections = settings.MaxConnections;
int clientChannelCapacity = settings.ClientChannelCapacity;
double slowClientThreshold = settings.SlowClientThreshold;
int slowClientMaxTicks = settings.SlowClientMaxTicks;
long clientMaxPendingBytes = settings.ClientMaxPendingBytes;
int clientCoalesceWindowMs = settings.ClientCoalesceWindowMs;
int maxSnapshotRequestsPerBatch = settings.MaxSnapshotRequestsPerBatch;
int shutdownDrainSeconds = settings.ShutdownDrainSeconds;
int multicastMergeCapacity = settings.MulticastMergeCapacity;
int feedChannelCapacity = settings.FeedChannelCapacity;
int incrementalRecoveryQueueCapacity = settings.IncrementalRecoveryQueueCapacity;
int groupRingCapacity = settings.GroupRingCapacity;
var logLevel = ParseLogLevel(settings.LogLevel);

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
    else if (args[i] == "--replay-to-multicast")
    {
        replayToMulticast = true;
    }
    else
    {
        positionalArgs.Add(args[i]);
    }
}

if (pcapPrefixes.Count == 0 && settings.PcapPrefixes.Count > 0)
    pcapPrefixes.AddRange(settings.PcapPrefixes);

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddSimpleConsole(opts =>
    {
        opts.SingleLine = true;
        opts.TimestampFormat = "HH:mm:ss.fff ";
    });
    builder.SetMinimumLevel(logLevel);
});

using var cts = new CancellationTokenSource();
var shutdownLogger = loggerFactory.CreateLogger("Shutdown");
var shutdownStopwatch = new System.Diagnostics.Stopwatch();

void TriggerShutdown(string source)
{
    if (cts.IsCancellationRequested)
    {
        shutdownLogger.LogWarning("Shutdown re-requested via {Source} (already in progress, elapsed={Elapsed}ms)",
            source, shutdownStopwatch.ElapsedMilliseconds);
        return;
    }
    shutdownStopwatch.Start();
    shutdownLogger.LogInformation("Shutdown requested via {Source}. Cancelling pipelines...", source);
    cts.Cancel();
}

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    TriggerShutdown("Console.CancelKeyPress (SIGINT/Ctrl-C)");
};

// Posix signal handlers — these surface *which* signal triggered the shutdown so a
// SIGTERM from `docker stop` is distinguishable from SIGINT (Ctrl-C), SIGHUP, etc.
// Without these we only see ProcessExit, which fires for *every* termination cause.
using var sigTerm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, ctx =>
{
    ctx.Cancel = true;
    TriggerShutdown($"PosixSignal SIGTERM (signo={(int)ctx.Signal})");
});
using var sigInt = PosixSignalRegistration.Create(PosixSignal.SIGINT, ctx =>
{
    ctx.Cancel = true;
    TriggerShutdown($"PosixSignal SIGINT (signo={(int)ctx.Signal})");
});
using var sigHup = PosixSignalRegistration.Create(PosixSignal.SIGHUP, ctx =>
{
    ctx.Cancel = true;
    TriggerShutdown($"PosixSignal SIGHUP (signo={(int)ctx.Signal})");
});
using var sigQuit = PosixSignalRegistration.Create(PosixSignal.SIGQUIT, ctx =>
{
    ctx.Cancel = true;
    TriggerShutdown($"PosixSignal SIGQUIT (signo={(int)ctx.Signal})");
});

// AppDomain.ProcessExit fires for any termination path that didn't already cancel —
// last-resort visibility (will not run on SIGKILL).
AppDomain.CurrentDomain.ProcessExit += (_, _) =>
{
    TriggerShutdown("AppDomain.ProcessExit");
    shutdownLogger.LogInformation("ProcessExit reached after {Elapsed}ms of draining.",
        shutdownStopwatch.ElapsedMilliseconds);
};

// Unhandled exceptions on background threads will tear the process down; surface them
// so they are not silently lost in the SIGKILL noise.
AppDomain.CurrentDomain.UnhandledException += (_, e) =>
{
    var ex = e.ExceptionObject as Exception;
    shutdownLogger.LogCritical(ex,
        "Unhandled exception on background thread (terminating={Terminating}). Process will exit.",
        e.IsTerminating);
};

// Fallback to environment variables (for shell-less Docker images)
if (pcapPrefixes.Count == 0 && positionalArgs.Count == 0 && (multicastConfig is null || replayToMulticast))
{
    var envPcapDir = Environment.GetEnvironmentVariable("PCAP_DIR") ?? "/app/pcap";
    var envPcapPrefix = Environment.GetEnvironmentVariable("PCAP_PREFIX") ?? "";
    if (!string.IsNullOrEmpty(envPcapPrefix))
    {
        foreach (var prefix in envPcapPrefix.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            pcapPrefixes.Add(Path.Combine(envPcapDir, prefix));
    }
    if (wsPort is null
        && Environment.GetEnvironmentVariable("UMDF_WS_PORT") is null
        && int.TryParse(Environment.GetEnvironmentVariable("WS_PORT"), out var envPort))
        wsPort = envPort;
    if (Environment.GetEnvironmentVariable("UMDF_SPEED") is null
        && speed == settings.Speed
        && double.TryParse(Environment.GetEnvironmentVariable("REPLAY_SPEED"),
            System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var envSpeed))
        speed = envSpeed;
}

// Build packet source — either from multicast config, --pcap-prefix, or positional args.
// In live multicast mode, packetSource is left null and receive threads push directly into the
// MultiFeedManager via PushPacket(), eliminating the merger queue and the async dispatcher loop.
IPacketSource? packetSource = null;
var groupIds = new List<int>();
List<MulticastPacketSource>? liveMulticastSources = null;
List<PcapChannelSource>? replaySources = null;

if (replayToMulticast || multicastConfig is null)
{
    if (!TryBuildReplaySources(out var builtSources, out groupIds))
        return 1;

    replaySources = builtSources;
}

if (replayToMulticast)
{
    if (wsPort is not null)
    {
        Console.Error.WriteLine("--ws-port is not supported with --replay-to-multicast. Run the publisher and consumer as separate processes.");
        return 1;
    }

    if (multicastConfig is null)
    {
        Console.Error.WriteLine("--replay-to-multicast requires --multicast-config <file>.");
        return 1;
    }

    if (!File.Exists(multicastConfig))
    {
        Console.Error.WriteLine($"Multicast config not found: {multicastConfig}");
        return 1;
    }

    var feedConfig = MulticastFeedConfig.Load(multicastConfig);
    var publishConfigs = feedConfig.ToPublishChannelConfigs();

    if (feedConfig.ChannelGroups.Count != groupIds.Count)
    {
        Console.Error.WriteLine(
            $"--replay-to-multicast requires the multicast config to contain exactly {groupIds.Count} channel group(s) in the same order as the replay inputs.");
        return 1;
    }

    var publishRoutes = publishConfigs
        .Select(c => (c.ChannelGroup, c.Type))
        .ToHashSet();
    var missingRoutes = replaySources!
        .Select(s => (s.Group, s.Channel))
        .Distinct()
        .Where(route => !publishRoutes.Contains(route))
        .ToList();

    if (missingRoutes.Count > 0)
    {
        Console.Error.WriteLine(
            $"--replay-to-multicast is missing multicast routes for: {string.Join(", ", missingRoutes.Select(r => $"G{r.Group}-{r.Channel}"))}");
        return 1;
    }

    Console.WriteLine("  Mode: PCAP replay -> multicast publisher");
    for (int g = 0; g < feedConfig.ChannelGroups.Count; g++)
    {
        var group = feedConfig.ChannelGroups[g];
        Console.WriteLine($"  Channel group {g}: {group.Name}");
        foreach (var ch in group.Channels)
            Console.WriteLine($"    {ch.Type,-25} -> {ch.MulticastGroup}:{ch.Port}");
    }

    // Batching is only safe when timing fidelity is irrelevant (speed=0 / max). At any
    // throttled speed (>0) we want every datagram to hit the wire as soon as it's produced,
    // otherwise inter-packet pacing would be distorted by batch buffering.
    int publishBatchSize = (speed == 0 && OperatingSystem.IsLinux())
        ? MulticastPacketPublisher.DefaultBatchSize
        : 1;
    Console.WriteLine($"  Speed: {(speed == 0 ? "max" : $"{speed}x")}");
    Console.WriteLine($"  Channel groups: {groupIds.Count}");
    Console.WriteLine($"  Publish batching: {(publishBatchSize > 1 ? $"sendmmsg, batch={publishBatchSize}" : "off (per-datagram sendto)")}");
    Console.WriteLine();

    using var replay = new TimestampMergedReplayer(replaySources!, new ReplayOptions { SpeedMultiplier = speed });
    using var publisher = new MulticastPacketPublisher(
        publishConfigs,
        loggerFactory.CreateLogger<MulticastPacketPublisher>(),
        publishBatchSize);

    var publishSw = Stopwatch.StartNew();
    long prevPublishedPackets = 0;
    long prevPublishedBytes = 0;
    long prevPublishStatsTicks = 0;
    // When batching is enabled the last few datagrams from each route can sit in the per-socket
    // staging buffer indefinitely if the replay stalls; periodic flushes guarantee bounded
    // staleness (≤ 1 s) even when the inbound rate temporarily falls below the batch size.
    using var publishStatsTimer = new Timer(_ => PrintPublisherProgress(), null, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5));
    using var publishFlushTimer = publishBatchSize > 1
        ? new Timer(_ => { try { publisher.Flush(); } catch { /* surfaced on next Publish */ } }, null, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50))
        : null;

    Console.WriteLine("Starting replay multicast publisher...");

    int publisherExitCode = 0;
    try
    {
        while (!cts.IsCancellationRequested && replay.TryReceive(out var packet))
            publisher.Publish(in packet);

        if (cts.IsCancellationRequested)
            Console.WriteLine("Cancelled.");

        // Drain partial batches so the consumer sees every datagram before EOF.
        publisher.Flush();
    }
    catch (NetworkInterfaceLostException ex)
    {
        // Publisher's network namespace went away (e.g. shared with consumer that just died).
        // Exit cleanly with a non-zero code so the orchestrator does not restart us into the
        // same broken namespace.
        Console.Error.WriteLine($"Publisher aborting: {ex.Message}");
        publisherExitCode = 75; // EX_TEMPFAIL — transient infra failure
    }
    finally
    {
        publishStatsTimer.Change(Timeout.Infinite, Timeout.Infinite);
        publishFlushTimer?.Change(Timeout.Infinite, Timeout.Infinite);
        publishSw.Stop();
    }

    Console.WriteLine();
    PrintPublisherSummary(publishSw, groupIds.Count, publisher.PublishedPackets, publisher.PublishedBytes);
    return publisherExitCode;

    void PrintPublisherProgress()
    {
        long packets = publisher.PublishedPackets;
        long bytes = publisher.PublishedBytes;
        long nowTicks = publishSw.ElapsedTicks;
        double secs = prevPublishStatsTicks > 0
            ? (double)(nowTicks - prevPublishStatsTicks) / Stopwatch.Frequency
            : publishSw.Elapsed.TotalSeconds;
        if (secs < 0.5) secs = 0.5;

        long pktRate = (long)((packets - prevPublishedPackets) / secs);
        long byteRate = (long)((bytes - prevPublishedBytes) / secs);
        prevPublishedPackets = packets;
        prevPublishedBytes = bytes;
        prevPublishStatsTicks = nowTicks;

        long lastPublishTicks = publisher.LastPublishTimestampTicks;
        double secondsSinceLastPublish = lastPublishTicks > 0
            ? Math.Max((Environment.TickCount64 - lastPublishTicks) / 1000.0, 0.0)
            : -1.0;

        Console.WriteLine();
        Console.WriteLine($"── [publish {publishSw.Elapsed:hh\\:mm\\:ss}] ──");
        Console.WriteLine(
            $"   Packets: {packets:N0} ({pktRate:N0}/s)  |  Bytes: {bytes:N0} ({byteRate:N0}/s)  |  Since last publish: {(secondsSinceLastPublish >= 0 ? $"{secondsSinceLastPublish:N1}s" : "n/a")}");
    }
}
else if (multicastConfig is not null)
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

    var multicastSources = new List<MulticastPacketSource>();
    foreach (var c in channelConfigs)
    {
        int replicas = Math.Max(1, c.ReceiveSocketCount);
        for (int r = 0; r < replicas; r++)
        {
            multicastSources.Add(new MulticastPacketSource(
                c,
                loggerFactory.CreateLogger<MulticastPacketSource>()));
        }
    }

    // Live mode: receive threads push packets directly into the MultiFeedManager (no merger).
    liveMulticastSources = multicastSources;
    packetSource = null;
}
else
{
    Console.WriteLine($"  Mode: PCAP replay");

    packetSource = new TimestampMergedReplayer(replaySources!, new ReplayOptions { SpeedMultiplier = speed });
}

Console.WriteLine($"  Speed: {(speed == 0 ? "max" : $"{speed}x")}");
Console.WriteLine($"  Channel groups: {groupIds.Count}");
Console.WriteLine();

var stats = new Stats();

// Wire up subscription manager if WebSocket port is specified
SubscriptionManager? subscriptionManager = null;
WebSocketHost? wsHost = null;
var symbolRegistry = new SymbolRegistry();

if (wsPort is not null)
    subscriptionManager = new SubscriptionManager(loggerFactory.CreateLogger<SubscriptionManager>(), maxSnapshotRequestsPerBatch);

// Create per-group BookManager + MarketDataManager + FeedHandler
var bookManagers = new List<BookManager>();
var marketDataManagers = new List<MarketDataManager>();
var groupHandlers = new List<GroupConflationHandler>();
var groupFeedHandlers = new Dictionary<int, IFeedEventHandler>();
var groupMdHandlers = new Dictionary<int, IFeedEventHandler>();

var bmLogger = loggerFactory.CreateLogger<BookManager>();
var mdmLogger = loggerFactory.CreateLogger<MarketDataManager>();

foreach (var gid in groupIds)
{
    IBookEventHandler bookHandler;
    IMarketDataEventHandler mdHandler = stats;

    if (subscriptionManager is not null)
    {
        var gh = subscriptionManager.CreateGroupHandler();
        bookHandler = new CompositeBookEventHandler(stats, gh);
        var bm = new BookManager(bookHandler, bmLogger);
        mdHandler = new CompositeMarketDataEventHandler(stats, gh, bm);
        gh.SetBookManager(bm);
        gh.StartBroadcaster(gid);
        groupHandlers.Add(gh);
        bookManagers.Add(bm);
    }
    else
    {
        bookHandler = stats;
        var bm = new BookManager(bookHandler, bmLogger);
        mdHandler = new CompositeMarketDataEventHandler(stats, bm);
        bookManagers.Add(bm);
    }

    var mm = new MarketDataManager(mdHandler, mdmLogger);
    marketDataManagers.Add(mm);

    var composite = new CompositeFeedHandler(bookManagers[^1], mm, symbolRegistry);
    groupFeedHandlers[gid] = composite;
    groupMdHandlers[gid] = mm;
}

// Use MultiFeedManager for multi-channel, single FeedHandler for single-channel
MultiFeedManager? multiFeed = null;
FeedHandler? singleFeed = null;

var feedLogger = loggerFactory.CreateLogger<FeedHandler>();

if (groupIds.Count > 1)
{
    multiFeed = packetSource is null
        ? new MultiFeedManager(
            groupFeedHandlers,
            feedLogger,
            marketDataHandlers: groupMdHandlers,
            logger: loggerFactory.CreateLogger<MultiFeedManager>(),
            feedChannelCapacity: feedChannelCapacity,
            incrementalRecoveryQueueCapacity: incrementalRecoveryQueueCapacity,
            groupRingCapacity: groupRingCapacity)
        : new MultiFeedManager(
            packetSource,
            groupFeedHandlers,
            feedLogger,
            marketDataHandlers: groupMdHandlers,
            logger: loggerFactory.CreateLogger<MultiFeedManager>(),
            feedChannelCapacity: feedChannelCapacity,
            incrementalRecoveryQueueCapacity: incrementalRecoveryQueueCapacity,
            groupRingCapacity: groupRingCapacity);
    if (subscriptionManager is not null)
        multiFeed.AnyGroupReady += () => subscriptionManager.SetReady();
}
else
{
    if (packetSource is null)
        throw new InvalidOperationException("Single-group live multicast mode is not supported; use multiple groups.");
    singleFeed = new FeedHandler(packetSource, groupFeedHandlers[groupIds[0]], feedLogger, marketDataHandler: groupMdHandlers[groupIds[0]], incrementalRecoveryQueueCapacity: incrementalRecoveryQueueCapacity);
}

if (subscriptionManager is not null)
{
    subscriptionManager.SetDataSources(
        bookManagers.ToArray(),
        marketDataManagers.ToArray(),
        symbolRegistry,
        groupHandlers.ToArray());

    // Suppress per-client fanout while the feed for a group is in Recovery/CatchUp
    // (book is being rebuilt; broadcasting partial state both wastes per-client
    // bandwidth and risks shipping inconsistent updates). On RealTime entry the
    // GroupConflationHandler schedules a fresh book snapshot for every Book subscriber.
    var groupHandlersByGid = new Dictionary<int, GroupConflationHandler>();
    for (int i = 0; i < groupIds.Count && i < groupHandlers.Count; i++)
        groupHandlersByGid[groupIds[i]] = groupHandlers[i];

    void WireFanoutSuppression(int gid, FeedHandler fh)
    {
        if (!groupHandlersByGid.TryGetValue(gid, out var gh)) return;
        // Initialize from current state in case we attach after the first transition.
        gh.SetFanoutSuppressed(fh.State != FeedState.RealTime);
        fh.StateChanged += (_, newState) =>
            gh.SetFanoutSuppressed(newState != FeedState.RealTime);
    }

    if (multiFeed is not null)
        foreach (var (gid, handler) in multiFeed.Handlers) WireFanoutSuppression(gid, handler);
    else if (singleFeed is not null)
        WireFanoutSuppression(groupIds[0], singleFeed);

    wsHost = new WebSocketHost(
        subscriptionManager,
        loggerFactory.CreateLogger<WebSocketHost>(),
        maxConnections: maxConnections,
        clientChannelCapacity: clientChannelCapacity,
        slowClientThreshold: slowClientThreshold,
        slowClientMaxTicks: slowClientMaxTicks,
        clientMaxPendingBytes: clientMaxPendingBytes,
        clientCoalesceWindowMs: clientCoalesceWindowMs);

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
long prevPackets = 0, prevTotalEvents = 0;
long prevStatsTicks = 0;

// Register OTEL-compatible metrics (System.Diagnostics.Metrics)
AppMetrics.Register(stats, bookManagers, marketDataManagers, groupIds,
    multiFeed, singleFeed, multicastMerger: null, subscriptionManager, groupHandlers, symbolRegistry,
    multicastSources: liveMulticastSources);

using var statsTimer = new Timer(_ => PrintPeriodicStats(), null, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5));

Console.WriteLine($"Starting {(liveMulticastSources is not null ? "live feed" : "replay")}...");

// Live multicast: spawn one dedicated foreground receive thread per UDP socket.
// Each thread does a blocking sync recv() and pushes the packet straight into MultiFeedManager,
// removing the merger queue and the async dispatcher from the hot path.
Thread[]? liveReceiveThreads = null;
if (liveMulticastSources is not null && multiFeed is not null)
{
    var manager = multiFeed;
    var sources = liveMulticastSources;

    // Build per-group lookup of "recovery-only" sources (Snap + InstrDef) so we can leave/rejoin
    // their multicast memberships in sync with the feed state machine. In RealTime these channels
    // are useless and just consume kernel buffer + CPU; B3 publishes new instruments mid-day via
    // the incremental stream, so InstrDef is also dispensable once we've reached RealTime.
    var recoverySourcesByGroup = sources
        .Where(s => s.ChannelType is ChannelType.SnapshotRecovery or ChannelType.InstrumentDefinition)
        .GroupBy(s => s.ChannelGroup)
        .ToDictionary(g => g.Key, g => g.ToArray());

    foreach (var (gid, handler) in manager.Handlers)
    {
        if (!recoverySourcesByGroup.TryGetValue(gid, out var groupRecoverySources))
            continue;
        var capturedSources = groupRecoverySources;
        var capturedGid = gid;
        handler.StateChanged += (oldState, newState) =>
        {
            if (newState == FeedState.RealTime && oldState != FeedState.RealTime)
            {
                foreach (var src in capturedSources) src.LeaveMulticastGroup();
            }
            else if (oldState == FeedState.RealTime && newState != FeedState.RealTime)
            {
                foreach (var src in capturedSources) src.RejoinMulticastGroup();
            }
        };
    }

    liveReceiveThreads = new Thread[sources.Count];
    for (int i = 0; i < sources.Count; i++)
    {
        var src = sources[i];
        var idx = i;
        var t = new Thread(() =>
        {
            try
            {
                if (MulticastPacketSource.IsBatchReceiveSupported)
                {
                    var batch = new UmdfPacket[MulticastPacketSource.MaxBatchSize];
                    while (!cts.IsCancellationRequested)
                    {
                        int n;
                        try { n = src.ReceiveBatch(batch); }
                        catch (ObjectDisposedException) { return; }
                        catch (System.Net.Sockets.SocketException) when (cts.IsCancellationRequested) { return; }
                        if (n > 0)
                            manager.PushPacketBatch(batch.AsSpan(0, n));
                    }
                }
                else
                {
                    while (!cts.IsCancellationRequested)
                    {
                        UmdfPacket packet;
                        try { packet = src.Receive(); }
                        catch (ObjectDisposedException) { return; }
                        catch (System.Net.Sockets.SocketException) when (cts.IsCancellationRequested) { return; }
                        manager.PushPacket(in packet);
                    }
                }
            }
            catch (OperationCanceledException) { }
        })
        {
            IsBackground = false,
            Name = $"MulticastRecv-{idx}",
            Priority = ThreadPriority.AboveNormal,
        };
        liveReceiveThreads[i] = t;
    }
}

if (multiFeed is not null)
{
    await multiFeed.StartAsync(cts.Token);
    if (liveReceiveThreads is not null)
        foreach (var t in liveReceiveThreads) t.Start();
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
subscriptionManager?.StopRankingsTimer();

// Brief drain period for in-flight WebSocket writes
if (wsHost is not null)
{
    await Task.Delay(TimeSpan.FromSeconds(shutdownDrainSeconds));
    await wsHost.StopAsync();
    await wsHost.DisposeAsync();
}

multiFeed?.Dispose();
singleFeed?.Dispose();

// Stop broadcaster threads after feed sources are disposed so no more batches can be
// published. StopBroadcaster signals the ring and joins the thread.
foreach (var gh in groupHandlers) gh.StopBroadcaster();

if (liveMulticastSources is not null)
{
    // Dispose sockets first to unblock the blocking Receive() calls, then join threads.
    foreach (var s in liveMulticastSources) s.Dispose();
    if (liveReceiveThreads is not null)
        foreach (var t in liveReceiveThreads) t.Join(TimeSpan.FromSeconds(2));
}
else
{
    packetSource?.Dispose();
}

Console.WriteLine();
PrintFinalSummary();

return 0;

// ── Local functions ──

static LogLevel ParseLogLevel(string value) =>
    Enum.TryParse<LogLevel>(value, ignoreCase: true, out var parsed)
        ? parsed
        : LogLevel.Information;

bool TryBuildReplaySources(out List<PcapChannelSource> sources, out List<int> replayGroupIds)
{
    sources = new List<PcapChannelSource>();
    replayGroupIds = new List<int>();

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
            replayGroupIds.Add(g);

            foreach (var (suffix, channelType) in channelSuffixes)
            {
                var filePath = $"{prefix}_{suffix}.pcap";
                if (!File.Exists(filePath))
                {
                    Console.Error.WriteLine($"  File not found: {filePath}");
                    return false;
                }

                sources.Add(new PcapChannelSource(filePath, channelType, g));
                Console.WriteLine($"    {channelType,-25} <- {Path.GetFileName(filePath)}");
            }
        }

        return true;
    }

    if (positionalArgs.Count >= 1)
    {
        var channelTypes = new[]
        {
            ChannelType.IncrementalA,
            ChannelType.IncrementalB,
            ChannelType.InstrumentDefinition,
            ChannelType.SnapshotRecovery
        };

        replayGroupIds.Add(0);

        for (int i = 0; i < positionalArgs.Count && i < channelTypes.Length; i++)
        {
            if (!File.Exists(positionalArgs[i]))
            {
                Console.Error.WriteLine($"File not found: {positionalArgs[i]}");
                return false;
            }

            sources.Add(new PcapChannelSource(positionalArgs[i], channelTypes[i], 0));
            Console.WriteLine($"  {channelTypes[i],-25} <- {positionalArgs[i]}");
        }

        return true;
    }

    PrintUsage();
    return false;
}

void PrintUsage()
{
    Console.WriteLine("Usage:");
    Console.WriteLine("  B3.Umdf.ConsoleApp --pcap-prefix <prefix> [--pcap-prefix <prefix2>] [options]");
    Console.WriteLine("  B3.Umdf.ConsoleApp --multicast-config <config.json> [options]");
    Console.WriteLine("  B3.Umdf.ConsoleApp --replay-to-multicast --multicast-config <config.json> --pcap-prefix <prefix> [--pcap-prefix <prefix2>] [options]");
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
    Console.WriteLine("  --replay-to-multicast         Publish replayed PCAP payloads to multicast instead of consuming them");
}

void PrintPublisherSummary(Stopwatch publishSw, int channelGroupCount, long publishedPackets, long publishedBytes)
{
    double totalSecs = publishSw.Elapsed.TotalSeconds;
    Console.WriteLine($"═══ Publish complete ({publishSw.Elapsed:hh\\:mm\\:ss}) ═══");
    Console.WriteLine($"  Channel groups: {channelGroupCount}");
    Console.WriteLine($"  Packets:        {publishedPackets:N0}  ({(totalSecs > 0 ? (long)(publishedPackets / totalSecs) : 0):N0}/s avg)");
    Console.WriteLine($"  Bytes:          {publishedBytes:N0}");
}

void PrintPeriodicStats()
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

    // Compute rates
    long nowTicks = sw.ElapsedTicks;
    double secs = prevStatsTicks > 0
        ? (double)(nowTicks - prevStatsTicks) / Stopwatch.Frequency
        : sw.Elapsed.TotalSeconds;
    if (secs < 0.5) secs = 0.5;

    long totalEvents = stats.OrderCount + stats.TradeCount + stats.DeleteCount +
                       stats.MarketDataCount + stats.StatusChangeCount +
                       stats.ForwardTradeCount + stats.TradeBustCount + stats.ExecSummaryCount;

    long pktRate = (long)((packets - prevPackets) / secs);
    long evtRate = (long)((totalEvents - prevTotalEvents) / secs);
    prevPackets = packets;
    prevTotalEvents = totalEvents;
    prevStatsTicks = nowTicks;

    Console.WriteLine();
    Console.WriteLine($"── [{sw.Elapsed:hh\\:mm\\:ss}] {stateStr} ──");
    Console.WriteLine($"   Packets: {packets:N0} ({pktRate:N0}/s)  |  Events: {totalEvents:N0} ({evtRate:N0}/s)  |  Books: {bookManagers.Sum(bm => bm.Books.Count):N0}  |  Instruments: {marketDataManagers.Sum(m => m.InstrumentData.Count):N0}  |  Symbols: {symbolRegistry.Count:N0}");

    if (subscriptionManager is not null)
    {
        foreach (var (id, depth, pendingBytes, sent, _) in subscriptionManager.GetClientStats())
            Console.WriteLine($"   {id}: queue={depth:N0}  pending={pendingBytes:N0}B  sent={sent:N0}");
        if (subscriptionManager.UpstreamConflated > 0)
            Console.WriteLine($"   upstream conflated (total): {subscriptionManager.UpstreamConflated:N0}");
    }

    // Per-group feed queue stats removed: inline dispatch (Option A) eliminated the bounded
    // Channel<UmdfPacket>; backpressure now manifests as kernel SO_RCVBUF drops -> sequence
    // gaps -> recovery, which is already reported below.

    var recoveryParts = new List<string>();
    if (singleFeed is not null)
    {
        if (singleFeed.State == FeedState.Recovery || singleFeed.IncrementalQueueDroppedPackets > 0)
        {
            recoveryParts.Add(
                $"G{groupIds[0]}={singleFeed.State} gap={singleFeed.IncrementalHandler.LastGapExpected}->{singleFeed.IncrementalHandler.LastGapReceived} catchupDropped={singleFeed.IncrementalQueueDroppedPackets:N0}");
        }
    }
    else if (multiFeed is not null)
    {
        foreach (var (gid, handler) in multiFeed.Handlers.OrderBy(h => h.Key))
        {
            if (handler.State == FeedState.Recovery || handler.IncrementalQueueDroppedPackets > 0)
            {
                recoveryParts.Add(
                    $"G{gid}={handler.State} gap={handler.IncrementalHandler.LastGapExpected}->{handler.IncrementalHandler.LastGapReceived} catchupDropped={handler.IncrementalQueueDroppedPackets:N0}");
            }
        }
    }

    if (recoveryParts.Count > 0)
        Console.WriteLine($"   recovery: {string.Join("  ", recoveryParts)}");

    var crossedParts = new List<string>();
    for (int i = 0; i < bookManagers.Count; i++)
    {
        long crossed = bookManagers[i].CurrentlyCrossedBooks;
        long auction = bookManagers[i].CurrentlyCrossedAuction;
        long locked = bookManagers[i].CurrentlyLockedBooks;
        long transitions = bookManagers[i].CrossingTransitions;
        if (crossed > 0 || auction > 0 || transitions > 0)
            crossedParts.Add($"G{groupIds[i]}=trading:{crossed} auction:{auction} locked:{locked} (transitions={transitions:N0})");
    }
    if (crossedParts.Count > 0)
        Console.WriteLine($"   crossed books: {string.Join("  ", crossedParts)}");

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
}

void PrintFinalSummary()
{
    double totalSecs = sw.Elapsed.TotalSeconds;
    long packets = multiFeed?.TotalPacketCount ?? singleFeed?.PacketCount ?? 0;
    long totalEvents = stats.OrderCount + stats.TradeCount + stats.DeleteCount +
                       stats.MarketDataCount + stats.StatusChangeCount +
                       stats.ForwardTradeCount + stats.TradeBustCount + stats.ExecSummaryCount;

    Console.WriteLine($"═══ Complete ({sw.Elapsed:hh\\:mm\\:ss}) ═══");
    Console.WriteLine($"  Channel groups: {groupIds.Count}");
    Console.WriteLine($"  Packets:      {packets:N0}  ({(totalSecs > 0 ? (long)(packets / totalSecs) : 0):N0}/s avg)");
    Console.WriteLine($"  Events:       {totalEvents:N0}  ({(totalSecs > 0 ? (long)(totalEvents / totalSecs) : 0):N0}/s avg)");
    Console.WriteLine($"    Orders:     {stats.OrderCount:N0}");
    Console.WriteLine($"    Trades:     {stats.TradeCount:N0}");
    Console.WriteLine($"    Deletes:    {stats.DeleteCount:N0}");
    Console.WriteLine($"    MarketData: {stats.MarketDataCount:N0}");
    Console.WriteLine($"    StatusChg:  {stats.StatusChangeCount:N0}");
    Console.WriteLine($"    FwdTrades:  {stats.ForwardTradeCount:N0}");
    Console.WriteLine($"    TradeBusts: {stats.TradeBustCount:N0}");
    Console.WriteLine($"    ExecSumm:   {stats.ExecSummaryCount:N0}");
    Console.WriteLine($"  Books:        {bookManagers.Sum(bm => bm.Books.Count):N0}");
    Console.WriteLine($"  Instruments:  {marketDataManagers.Sum(m => m.InstrumentData.Count):N0}");
    Console.WriteLine($"  Symbols:      {symbolRegistry.Count:N0}");
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

    public void OnOrderAdded(OrderBook book, in OrderBookEntry entry)
    {
        OrderCount++;
    }

    public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry)
    {
    }

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
    {
        DeleteCount++;
    }

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
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
