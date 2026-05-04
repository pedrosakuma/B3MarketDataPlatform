using System.Diagnostics;
using System.Runtime.InteropServices;
using B3.Umdf.Book;
using B3.Umdf.ConsoleApp;
using B3.Umdf.Feed;
using B3.Umdf.PcapReplay;
using B3.Umdf.Server;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;

var healthExit = await HealthCheckCommand.TryRunAsync(args);
if (healthExit is int code) return code;

var settings = AppSettings.LoadDefault();
settings.ApplyEnvironment();

var positionalArgs = new List<string>();
if (!CliArgs.TryApply(args, settings, positionalArgs, out var cliError))
{
    Console.Error.WriteLine(cliError);
    return 1;
}

int? wsPort = settings.WsPort;
double speed = settings.Speed;
bool replayToMulticast = settings.ReplayToMulticast;
var pcapPrefixes = new List<string>(settings.PcapPrefixes);
string? multicastConfig = settings.MulticastConfig;
int maxConnections = settings.MaxConnections;
int clientChannelCapacity = settings.ClientChannelCapacity;
double slowClientThreshold = settings.SlowClientThreshold;
int slowClientMaxTicks = settings.SlowClientMaxTicks;
long clientMaxPendingBytes = settings.ClientMaxPendingBytes;
int clientCoalesceWindowMs = settings.ClientCoalesceWindowMs;
int maxSnapshotRequestsPerBatch = settings.MaxSnapshotRequestsPerBatch;
int serverFlushWindowMs = settings.ServerFlushWindowMs;
int shutdownDrainSeconds = settings.ShutdownDrainSeconds;
int multicastMergeCapacity = settings.MulticastMergeCapacity;
int feedChannelCapacity = settings.FeedChannelCapacity;
int groupRingCapacity = settings.GroupRingCapacity;
var logLevel = LogLevelParser.Parse(settings.LogLevel);

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
    // CTS may already be disposed by the `using` in Main when the runtime
    // fires AppDomain.ProcessExit during late-stage shutdown. Guard the Cancel
    // call so a benign post-disposal signal doesn't surface as an unhandled
    // ObjectDisposedException on the finalizer/exit path.
    try
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
    catch (ObjectDisposedException)
    {
        // Process is already past the using-scope of `cts`; nothing left to cancel.
    }
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

// Build packet source — either from multicast config, --pcap-prefix, or positional args.
// In live multicast mode, packetSource is left null and receive threads push directly into the
// MultiFeedManager via PushPacket(), eliminating the merger queue and the async dispatcher loop.
// Legacy env vars (PCAP_DIR / PCAP_PREFIX / WS_PORT / REPLAY_SPEED) are folded into AppSettings
// by ApplyEnvironment above; CLI / UMDF_* values continue to take precedence over them.
IPacketSource? packetSource = null;
var groupIds = new List<int>();
List<MulticastPacketSource>? liveMulticastSources = null;
List<PcapChannelSource>? replaySources = null;

if (replayToMulticast || multicastConfig is null)
{
    if (!ReplaySourcesBuilder.TryBuild(pcapPrefixes, positionalArgs, out var builtSources, out groupIds))
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

    var lossPolicy = LossPolicyFactory.FromSettings(settings);
    if (lossPolicy is not null)
        Console.WriteLine($"  Loss injection: {LossPolicyFactory.Describe(lossPolicy)}");

    using var replay = new TimestampMergedReplayer(replaySources!, new ReplayOptions { SpeedMultiplier = speed, Loss = lossPolicy });
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
    PublisherSummary.Print(publishSw, groupIds.Count, publisher.PublishedPackets, publisher.PublishedBytes);
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
        {
            var transport = MulticastFeedConfig.ParseTransport(ch.Transport);
            Console.WriteLine(transport == TransportKind.Unicast
                ? $"    {ch.Type,-25} <- unicast {ch.MulticastGroup}:{ch.Port}"
                : $"    {ch.Type,-25} <- {ch.MulticastGroup}:{ch.Port}");
        }
    }

    var multicastSources = new List<MulticastPacketSource>();
    var clampLogger = loggerFactory.CreateLogger("Multicast.ReplicaClamp");
    foreach (var c in channelConfigs)
    {
        // SO_REUSEPORT does NOT load-balance multicast on Linux: every joined
        // socket receives a full copy of each datagram. Replicas would only
        // multiply CPU/memory cost without throughput benefit. Clamp to 1 and
        // warn loudly so an operator who set this hoping for headroom finds out.
        // See docs/perf/reuseport-validation.md for the empirical proof.
        if (c.ReceiveSocketCount > 1)
        {
            clampLogger.LogWarning(
                "Multicast channel {Type} group {Group} has receiveSocketCount={Count}; clamping to 1. " +
                "SO_REUSEPORT does not load-balance multicast on Linux (every socket gets a copy). " +
                "See docs/perf/reuseport-validation.md.",
                c.Type, c.ChannelGroup, c.ReceiveSocketCount);
        }
        multicastSources.Add(new MulticastPacketSource(
            c,
            loggerFactory.CreateLogger<MulticastPacketSource>()));
    }

    // Live mode: receive threads push packets directly into the MultiFeedManager (no merger).
    liveMulticastSources = multicastSources;
    packetSource = null;
}
else
{
    Console.WriteLine($"  Mode: PCAP replay");

    var lossPolicy = LossPolicyFactory.FromSettings(settings);
    if (lossPolicy is not null)
        Console.WriteLine($"  Loss injection: {LossPolicyFactory.Describe(lossPolicy)}");

    packetSource = new TimestampMergedReplayer(replaySources!, new ReplayOptions { SpeedMultiplier = speed, Loss = lossPolicy });
}

Console.WriteLine($"  Speed: {(speed == 0 ? "max" : $"{speed}x")}");
Console.WriteLine($"  Channel groups: {groupIds.Count}");
Console.WriteLine();

var stats = new Stats();

// Bounded ring buffer of recovery audit-trail events. Capacity sized for
// recent-incident triage (256 entries, ~24 KB total). Surfaced via
// /api/recovery/recent and fed by FeedHandler/SymbolStateRegistry/MDM hooks
// wired below.
var recoveryEventLog = new RecoveryEventLog(capacity: 256);

// Wire up subscription manager if WebSocket port is specified
SubscriptionManager? subscriptionManager = null;
WebSocketHost? wsHost = null;
var symbolRegistry = new SymbolRegistry();

if (wsPort is not null)
    subscriptionManager = new SubscriptionManager(
        loggerFactory.CreateLogger<SubscriptionManager>(),
        maxSnapshotRequestsPerBatch,
        clientMaxPendingBytes: clientMaxPendingBytes,
        outlierMultiplier: settings.ClientOutlierMultiplier,
        outlierMinBytes: settings.ClientOutlierMinBytes,
        outlierPressurePct: settings.ClientOutlierPressurePct,
        outlierIntervalMs: settings.ClientOutlierIntervalMs,
        serverFlushWindowMs: serverFlushWindowMs);

// Create per-group BookManager + MarketDataManager + FeedHandler
var bookManagers = new List<BookManager>();
var marketDataManagers = new List<MarketDataManager>();
var groupHandlers = new List<GroupConflationHandler>();
var groupFeedHandlers = new Dictionary<int, IFeedEventHandler>();
var groupMdHandlers = new Dictionary<int, IFeedEventHandler>();

var bmLogger = loggerFactory.CreateLogger<BookManager>();
var mdmLogger = loggerFactory.CreateLogger<MarketDataManager>();
var registryLogger = loggerFactory.CreateLogger<SymbolStateRegistry>();
var staleBufferLogger = loggerFactory.CreateLogger<StaleMboBuffer>();

// Per-group state — each B3 channel group is an independent feed with its
// own incremental sequence space and snapshot stream, so each gets its own
// SymbolStateRegistry + StaleMboBuffer.
var registries = new Dictionary<int, SymbolStateRegistry>();
var staleBuffers = new Dictionary<int, StaleMboBuffer>();

foreach (var gid in groupIds)
{
    IBookEventHandler bookHandler;
    IMarketDataEventHandler mdHandler = stats;

    var groupRegistry = new SymbolStateRegistry(registryLogger)
    {
        StaleEscapeTimeoutMs = settings.StaleEscapeTimeoutMs,
    };
    // Capture forced/authoritative-reset events into the audit ring. Cheap:
    // fires only when a stuck-Stale snapshot is accepted (rare, ops-relevant).
    int gidForCallback = gid;
    groupRegistry.SetAuthoritativeResetCallback((securityId, snapRptSeq, unsafeDelta, discardedTailDelta) =>
    {
        recoveryEventLog.Record(new RecoveryEvent(
            TimestampUnixMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Kind: RecoveryEventKind.ForcedHealAccepted,
            GroupId: gidForCallback,
            SecurityId: securityId,
            SnapshotRptSeq: (long)snapRptSeq,
            PriorRptSeq: null,
            Detail: $"unsafeDelta={unsafeDelta} discardedTail={discardedTailDelta}"));
    });
    // Multi-tier dynamic-grow cap ladder + global byte cap come from AppSettings.
    // Defaults: [8k, 64k, 256k, 1M] per-symbol ladder, 512 MB global per group.
    var groupStaleBuffer = new StaleMboBuffer(staleBufferLogger,
        capLevels: settings.StaleBufferCapLevels,
        globalByteCap: (long)settings.StaleBufferGlobalMib * 1024L * 1024L);
    registries[gid] = groupRegistry;
    staleBuffers[gid] = groupStaleBuffer;

    if (subscriptionManager is not null)
    {
        var gh = subscriptionManager.CreateGroupHandler();
        bookHandler = new CompositeBookEventHandler(stats, gh);
        var bm = new BookManager(bookHandler, bmLogger,
            stateRegistry: groupRegistry, staleBuffer: groupStaleBuffer);
        mdHandler = new CompositeMarketDataEventHandler(stats, gh, bm,
            new RecoveryEventLoggerHandler(recoveryEventLog, gid));
        gh.SetBookManager(bm);
        gh.StartBroadcaster(gid);
        groupHandlers.Add(gh);
        bookManagers.Add(bm);
    }
    else
    {
        bookHandler = stats;
        var bm = new BookManager(bookHandler, bmLogger,
            stateRegistry: groupRegistry, staleBuffer: groupStaleBuffer);
        mdHandler = new CompositeMarketDataEventHandler(stats, bm,
            new RecoveryEventLoggerHandler(recoveryEventLog, gid));
        bookManagers.Add(bm);
    }

    var mm = new MarketDataManager(mdHandler, mdmLogger,
        stateRegistry: groupRegistry);
    marketDataManagers.Add(mm);

    var composite = new OptimizedFeedComposite(bookManagers[^1], mm, symbolRegistry);
    groupFeedHandlers[gid] = composite;
    groupMdHandlers[gid] = mm;
}

// Use MultiFeedManager for multi-channel, single FeedHandler for single-channel
MultiFeedManager? multiFeed = null;
FeedHandler? singleFeed = null;

var feedLogger = loggerFactory.CreateLogger<FeedHandler>();

if (groupIds.Count > 1 || packetSource is null)
{
    // Live push-mode (multicast/unicast, packetSource == null): receive threads call
    // PushPacket/PushPacketBatch directly — works for any group count, including 1
    // (e.g. matching-platform's single-EQT bridge deployment, see issue #13).
    // Source-driven mode (packetSource != null) keeps the multi-group MultiFeedManager
    // it always used.
    multiFeed = packetSource is null
        ? new MultiFeedManager(
            groupFeedHandlers,
            feedLogger,
            marketDataHandlers: groupMdHandlers,
            logger: loggerFactory.CreateLogger<MultiFeedManager>(),
            feedChannelCapacity: feedChannelCapacity,
            groupRingCapacity: groupRingCapacity)
        : new MultiFeedManager(
            packetSource,
            groupFeedHandlers,
            feedLogger,
            marketDataHandlers: groupMdHandlers,
            logger: loggerFactory.CreateLogger<MultiFeedManager>(),
            feedChannelCapacity: feedChannelCapacity,
            groupRingCapacity: groupRingCapacity);
    if (subscriptionManager is not null)
        multiFeed.AnyGroupReady += () => subscriptionManager.SetReady();
    multiFeed.FlushWindowMs = serverFlushWindowMs;
}
else
{
    // Source-driven single-group: replay path. FeedHandler avoids the per-group
    // dispatcher overhead since there's exactly one source feeding one group.
    singleFeed = new FeedHandler(packetSource, groupFeedHandlers[groupIds[0]], feedLogger, marketDataHandler: groupMdHandlers[groupIds[0]]);
}

if (subscriptionManager is not null)
{
    subscriptionManager.SetDataSources(
        bookManagers.ToArray(),
        marketDataManagers.ToArray(),
        symbolRegistry,
        groupHandlers.ToArray());

    // Suppress per-client fanout while the feed for a group is in Recovery/CatchUp.
    // Also engages a market-wide stale-ratio gate (see PerSymbolFanoutGate).
    FanoutSuppressionWiring.Wire(groupIds, groupHandlers, bookManagers, settings, multiFeed, singleFeed);

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

    // Recovery audit-trail surface — newest-first snapshot of the in-memory
    // ring buffer. Capped per-call (default 50, max 1000) by the endpoint.
    wsHost.RecoveryEventProvider = max => recoveryEventLog.Snapshot(max);
    wsHost.RecoveryEventTotalProvider = () => recoveryEventLog.TotalRecorded;

    // Expose the ConsoleApp's MetricsBinder meter through the host's
    // /metrics endpoint alongside the Server's own "B3.Umdf" meter.
    wsHost.AdditionalMeterNames = new[] { MetricsBinder.Meter.Name };

    // Wire the active-symbol gauge to SubscriptionManager. Captured once;
    // SubscriptionManager outlives the host.
    var sm = subscriptionManager;
    MetricsRegistry.ActiveSubscribedSymbolsProvider = () => sm.ActiveSymbolCount;

    await wsHost.StartAsync(wsPort!.Value, cts.Token);
}

// Periodic stats timer
var sw = Stopwatch.StartNew();
var statsPrinter = new StatsPrinter(sw, stats, bookManagers, marketDataManagers, symbolRegistry,
    groupIds, multiFeed, singleFeed, subscriptionManager, groupHandlers);

// Register OTEL-compatible metrics (System.Diagnostics.Metrics)
MetricsBinder.Register(stats, bookManagers, marketDataManagers, groupIds,
    multiFeed, singleFeed, multicastMerger: null, subscriptionManager, groupHandlers, symbolRegistry,
    multicastSources: liveMulticastSources);

// Scheduler jitter probe — detects CPU contention from noisy neighbours,
// GC pauses, IRQ storms, cgroup CFS throttling. Detection only.
// Disable via UMDF_SCHEDULER_JITTER_PROBE=0.
SchedulerJitterProbe? jitterProbe = null;
var jitterProbeEnv = Environment.GetEnvironmentVariable("UMDF_SCHEDULER_JITTER_PROBE");
if (!string.Equals(jitterProbeEnv, "0", StringComparison.Ordinal) &&
    !string.Equals(jitterProbeEnv, "false", StringComparison.OrdinalIgnoreCase))
{
    jitterProbe = new SchedulerJitterProbe();
    jitterProbe.Start();
}

using var statsTimer = new Timer(_ => statsPrinter.PrintPeriodic(), null, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5));

Console.WriteLine($"Starting {(liveMulticastSources is not null ? "live feed" : "replay")}...");

// Live multicast: spawn one dedicated foreground receive thread per UDP socket.
// Each thread does a blocking sync recv() and pushes the packet straight into MultiFeedManager,
// removing the merger queue and the async dispatcher from the hot path.
Thread[]? liveReceiveThreads = null;
if (liveMulticastSources is not null && multiFeed is not null)
{
    var manager = multiFeed;
    var sources = liveMulticastSources;

    // NOTE: previously we left the SnapshotRecovery and InstrumentDefinition
    // multicast groups when the channel reached RealTime to save kernel buffer
    // / CPU. With the unified per-symbol design those streams must stay joined
    // for the entire session: snapshots heal individual Stale symbols on
    // demand, and instrument-definition packets carry mid-session new listings.

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

jitterProbe?.Dispose();

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
statsPrinter.PrintFinal();

return 0;

// ── Local functions ──


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
    public long EpochResetCount;
    public SnapshotClearReason LastEpochResetReason;

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

    public void OnEpochReset(SnapshotClearReason reason)
    {
        EpochResetCount++;
        LastEpochResetReason = reason;
    }
}

/// <summary>
/// IMarketDataEventHandler that records OnInstrumentReplaced events into
/// the process-wide <see cref="RecoveryEventLog"/>. Allocation per event
/// is one boxed string + one Record struct — only fires on identity-change
/// arrivals which are expected to be ~zero in normal sessions.
/// </summary>
sealed class RecoveryEventLoggerHandler : IMarketDataEventHandler
{
    private readonly RecoveryEventLog _log;
    private readonly int _groupId;

    public RecoveryEventLoggerHandler(RecoveryEventLog log, int groupId)
    {
        _log = log;
        _groupId = groupId;
    }

    public void OnInstrumentReplaced(ulong securityId, string? oldSymbol, string newSymbol)
    {
        _log.Record(new RecoveryEvent(
            TimestampUnixMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Kind: RecoveryEventKind.InstrumentReplaced,
            GroupId: _groupId,
            SecurityId: securityId,
            SnapshotRptSeq: null,
            PriorRptSeq: null,
            Detail: $"{oldSymbol ?? "<null>"}→{newSymbol}"));
    }
}
