using System.Diagnostics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.PcapReplay;
using B3.Umdf.Server;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.ConsoleApp;

internal static class CliArgs
{
    /// <summary>
    /// Applies CLI argument overrides to <paramref name="settings"/> in place and
    /// collects any positional arguments. Returns false on a validation error and
    /// sets <paramref name="error"/> with a user-facing message.
    /// </summary>
    public static bool TryApply(
        string[] args,
        AppSettings settings,
        List<string> positionalArgs,
        out string? error)
    {
        var cliPrefixes = new List<string>();

        // Flags that take a value as the next token. Used to detect missing
        // values (flag at end of args, or followed by another --flag) and
        // surface a clear validation error instead of silently consuming the
        // next flag as the value.
        var valueFlags = new HashSet<string>(StringComparer.Ordinal)
        {
            "--ws-port", "--speed", "--pcap-prefix", "--multicast-config",
            "--loss-targets", "--loss-rate", "--loss-mode", "--loss-burst",
            "--loss-seed",
        };

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i];

            if (valueFlags.Contains(arg) && !TryReadValue(args, ref i, arg, out var readErr))
            {
                error = readErr;
                return false;
            }

            switch (arg)
            {
                case "--ws-port":
                    if (!int.TryParse(args[i], out var p) || p < 1 || p > 65535)
                    { error = "Invalid --ws-port value."; return false; }
                    settings.WsPort = p;
                    break;

                case "--speed":
                    if (!double.TryParse(args[i],
                            System.Globalization.NumberStyles.Any,
                            System.Globalization.CultureInfo.InvariantCulture,
                            out var sp) || sp < 0)
                    {
                        error = "Invalid --speed value. Use 0 for max speed, 1 for real-time, >1 for accelerated.";
                        return false;
                    }
                    settings.Speed = sp;
                    break;

                case "--pcap-prefix":
                    cliPrefixes.Add(args[i]);
                    break;

                case "--multicast-config":
                    settings.MulticastConfig = args[i];
                    break;

                case "--replay-to-multicast":
                    settings.ReplayToMulticast = true;
                    break;

                case "--loss-targets":
                    settings.LossTargets = args[i];
                    break;

                case "--loss-rate":
                    if (!double.TryParse(args[i],
                            System.Globalization.NumberStyles.Any,
                            System.Globalization.CultureInfo.InvariantCulture,
                            out var lr) || lr < 0 || lr > 1)
                    {
                        error = "Invalid --loss-rate value. Must be in [0, 1].";
                        return false;
                    }
                    settings.LossRate = lr;
                    break;

                case "--loss-mode":
                    settings.LossMode = args[i];
                    break;

                case "--loss-burst":
                    if (!int.TryParse(args[i], out var lb) || lb < 1)
                    { error = "Invalid --loss-burst value. Must be >= 1."; return false; }
                    settings.LossBurstSize = lb;
                    break;

                case "--loss-correlated":
                    settings.LossCorrelated = true;
                    break;

                case "--loss-seed":
                    if (!int.TryParse(args[i], out var ls))
                    { error = "Invalid --loss-seed value."; return false; }
                    settings.LossSeed = ls;
                    break;

                default:
                    positionalArgs.Add(arg);
                    break;
            }
        }

        // CLI prefixes win over env/json prefixes when present; otherwise leave
        // settings.PcapPrefixes (env/json) untouched as the default.
        if (cliPrefixes.Count > 0)
        {
            settings.PcapPrefixes.Clear();
            settings.PcapPrefixes.AddRange(cliPrefixes);
        }

        error = null;
        return true;
    }

    private static bool TryReadValue(string[] args, ref int i, string flag, out string? error)
    {
        if (i + 1 >= args.Length)
        {
            error = $"{flag} requires a value (got: <EOF>)";
            return false;
        }
        var next = args[i + 1];
        if (next.StartsWith("--", StringComparison.Ordinal))
        {
            error = $"{flag} requires a value (got: {next})";
            return false;
        }
        i++;
        error = null;
        return true;
    }
}

internal static class HealthCheckCommand
{
    public static async Task<int?> TryRunAsync(string[] args)
    {
        if (args.Length != 1) return null;
        string mode;
        if (args[0] == "--health-check") mode = "live";
        else if (args[0] == "--health-check=full" || args[0] == "--health-check=health") mode = "health";
        else if (args[0] == "--health-check=live") mode = "live";
        else return null;
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
            var port = Environment.GetEnvironmentVariable("UMDF_WS_PORT")
                ?? Environment.GetEnvironmentVariable("WS_PORT")
                ?? "8080";
            // "live" probes /live (liveness, always 200 unless the process is dead).
            // "full"/"health" probes /health which returns 503 when feed groups
            // are stuck in non-Streaming state past UMDF_HEALTH_MAX_STALE_SECONDS.
            var path = mode == "health" ? "/health" : "/live";
            var resp = await http.GetAsync($"http://localhost:{port}{path}");
            return resp.IsSuccessStatusCode ? 0 : 1;
        }
        catch { return 1; }
    }
}

internal static class LogLevelParser
{
    public static LogLevel Parse(string value) =>
        Enum.TryParse<LogLevel>(value, ignoreCase: true, out var parsed)
            ? parsed
            : LogLevel.Information;
}

internal static class UsageBanner
{
    public static void Print()
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
        Console.WriteLine("  --loss-targets <list>         Channel classes to drop on (A,B,AB,Snap,InstrDef,All). Enables loss injection");
        Console.WriteLine("  --loss-rate <0..1>            Drop probability per eligible packet (default 0)");
        Console.WriteLine("  --loss-mode <random|burst>    Loss pattern (default random)");
        Console.WriteLine("  --loss-burst <n>              Consecutive packets dropped per burst trigger (burst mode, default 1)");
        Console.WriteLine("  --loss-correlated             A and B drop the SAME SeqNum (worst case for arbitration)");
        Console.WriteLine("  --loss-seed <int>             RNG seed for reproducible loss patterns");
    }
}

internal static class PublisherSummary
{
    public static void Print(Stopwatch publishSw, int channelGroupCount, long publishedPackets, long publishedBytes)
    {
        double totalSecs = publishSw.Elapsed.TotalSeconds;
        Console.WriteLine($"═══ Publish complete ({publishSw.Elapsed:hh\\:mm\\:ss}) ═══");
        Console.WriteLine($"  Channel groups: {channelGroupCount}");
        Console.WriteLine($"  Packets:        {publishedPackets:N0}  ({(totalSecs > 0 ? (long)(publishedPackets / totalSecs) : 0):N0}/s avg)");
        Console.WriteLine($"  Bytes:          {publishedBytes:N0}");
    }
}

internal static class ReplaySourcesBuilder
{
    private static readonly (string Suffix, ChannelType Channel)[] PrefixedChannels =
    {
        ("Incremental_FeedA", ChannelType.IncrementalA),
        ("Incremental_FeedB", ChannelType.IncrementalB),
        ("InstrumentDefinition", ChannelType.InstrumentDefinition),
        ("SnapshotRecovery", ChannelType.SnapshotRecovery),
    };

    private static readonly ChannelType[] PositionalChannels =
    {
        ChannelType.IncrementalA,
        ChannelType.IncrementalB,
        ChannelType.InstrumentDefinition,
        ChannelType.SnapshotRecovery,
    };

    public static bool TryBuild(
        IReadOnlyList<string> pcapPrefixes,
        IReadOnlyList<string> positionalArgs,
        out List<PcapChannelSource> sources,
        out List<int> replayGroupIds)
    {
        sources = new List<PcapChannelSource>();
        replayGroupIds = new List<int>();

        if (pcapPrefixes.Count > 0)
        {
            for (int g = 0; g < pcapPrefixes.Count; g++)
            {
                var prefix = pcapPrefixes[g];
                Console.WriteLine($"  Channel group {g}: {Path.GetFileName(prefix)}");
                replayGroupIds.Add(g);

                foreach (var (suffix, channelType) in PrefixedChannels)
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
            replayGroupIds.Add(0);
            for (int i = 0; i < positionalArgs.Count && i < PositionalChannels.Length; i++)
            {
                if (!File.Exists(positionalArgs[i]))
                {
                    Console.Error.WriteLine($"File not found: {positionalArgs[i]}");
                    return false;
                }

                sources.Add(new PcapChannelSource(positionalArgs[i], PositionalChannels[i], 0));
                Console.WriteLine($"  {PositionalChannels[i],-25} <- {positionalArgs[i]}");
            }

            return true;
        }

        UsageBanner.Print();
        return false;
    }
}

/// <summary>
/// Wires per-group fanout suppression: while a feed group is in
/// Recovery/CatchUp, the matching <see cref="GroupConflationHandler"/>
/// suppresses per-client fanout. On RealTime entry it schedules a fresh
/// snapshot for every Book subscriber in the group. It additionally
/// attaches a <see cref="PerSymbolFanoutGate"/> that suppresses fanout
/// when too many symbols are Stale.
/// </summary>
internal static class FanoutSuppressionWiring
{
    public static void Wire(
        IReadOnlyList<int> groupIds,
        IReadOnlyList<GroupConflationHandler> groupHandlers,
        IReadOnlyList<BookManager> bookManagers,
        AppSettings settings,
        MultiFeedManager? multiFeed,
        FeedHandler? singleFeed)
    {
        var byGid = new Dictionary<int, (GroupConflationHandler Conflation, BookManager Book)>();
        for (int i = 0; i < groupIds.Count && i < groupHandlers.Count && i < bookManagers.Count; i++)
            byGid[groupIds[i]] = (groupHandlers[i], bookManagers[i]);

        if (multiFeed is not null)
        {
            foreach (var (gid, fh) in multiFeed.Handlers)
                WireOne(byGid, gid, fh, settings);
        }
        else if (singleFeed is not null && groupIds.Count > 0)
        {
            WireOne(byGid, groupIds[0], singleFeed, settings);
        }
    }

    private static void WireOne(
        Dictionary<int, (GroupConflationHandler Conflation, BookManager Book)> byGid,
        int gid,
        FeedHandler fh,
        AppSettings settings)
    {
        if (!byGid.TryGetValue(gid, out var entry)) return;
        var gh = entry.Conflation;
        gh.SetFanoutSuppressed(fh.State != FeedState.Streaming);
        fh.StateChanged += (_, newState) =>
            gh.SetFanoutSuppressed(newState != FeedState.Streaming);

        // Per-symbol fanout gate: additionally engage market-wide fanout
        // suppression when a large fraction of symbols becomes Stale (e.g.
        // ChannelReset_11, mass loss). The channel-state gate above only fires
        // pre-Streaming; post-Streaming the per-symbol layer handles gaps
        // without leaving Streaming, so a separate evaluator is required.
        var gate = new PerSymbolFanoutGate(
            entry.Book.StateRegistry,
            gh,
            settings.PerSymbolFanoutSuppressHighPct,
            settings.PerSymbolFanoutSuppressLowPct);
        if (gate.Enabled)
            gh.PreBatchEvaluator = gate.Evaluate;
    }
}
