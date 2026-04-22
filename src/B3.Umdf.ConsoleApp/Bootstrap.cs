using System.Diagnostics;
using B3.Umdf.Feed;
using B3.Umdf.PcapReplay;
using B3.Umdf.Server;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.ConsoleApp;

internal static class HealthCheckCommand
{
    public static async Task<int?> TryRunAsync(string[] args)
    {
        if (args.Length != 1 || args[0] != "--health-check") return null;
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
/// snapshot for every Book subscriber in the group.
/// </summary>
internal static class FanoutSuppressionWiring
{
    public static void Wire(
        IReadOnlyList<int> groupIds,
        IReadOnlyList<GroupConflationHandler> groupHandlers,
        MultiFeedManager? multiFeed,
        FeedHandler? singleFeed)
    {
        var byGid = new Dictionary<int, GroupConflationHandler>();
        for (int i = 0; i < groupIds.Count && i < groupHandlers.Count; i++)
            byGid[groupIds[i]] = groupHandlers[i];

        if (multiFeed is not null)
        {
            foreach (var (gid, fh) in multiFeed.Handlers)
                WireOne(byGid, gid, fh);
        }
        else if (singleFeed is not null && groupIds.Count > 0)
        {
            WireOne(byGid, groupIds[0], singleFeed);
        }
    }

    private static void WireOne(Dictionary<int, GroupConflationHandler> byGid, int gid, FeedHandler fh)
    {
        if (!byGid.TryGetValue(gid, out var gh)) return;
        gh.SetFanoutSuppressed(fh.State != FeedState.RealTime);
        fh.StateChanged += (_, newState) =>
            gh.SetFanoutSuppressed(newState != FeedState.RealTime);
    }
}
