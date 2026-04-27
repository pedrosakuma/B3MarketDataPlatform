using System.Diagnostics;
using System.Globalization;
using System.Text;
using B3.Umdf.Feed;
using B3.Umdf.PcapReplay;
using B3.Umdf.Transport;

namespace B3.Umdf.Tools.InboundLatencyProbe;

/// <summary>
/// A2 of the per-channel ring investigation: replay one or more recorded PCAPs
/// in-process through MultiFeedManager / FeedHandler with BookManager swapped
/// for a latency-recording handler. Emits per-100ms-bucket latency percentiles
/// + correlated packet rate + cumulative drop counter so we can see whether
/// the dispatcher absorbs the real Inc bursts (≈118 kpps in DRV-072) without
/// queueing pathologies.
///
/// Scope of v1: in-process push (no kernel UDP path, no SO_REUSEPORT). The
/// measured latency is dispatcher + FeedHandler state-machine cost, not
/// network/syscall cost. This isolates the dispatcher question; a future v2
/// could add a loopback mode for end-to-end measurement.
/// </summary>
internal static class Program
{
    private static int Main(string[] args)
    {
        if (args.Length == 0 || HasFlag(args, "--help") || HasFlag(args, "-h"))
        {
            PrintUsage();
            return args.Length == 0 ? 1 : 0;
        }

        var inputs = new List<(ChannelType Channel, string Path)>();
        double speed = 1.0;
        int warmupSec = 10;
        int durationSec = 0;
        int bucketMs = 100;
        int channelGroup = 0;
        string outCsv = "./latency.csv";

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--feed-a":     inputs.Add((ChannelType.IncrementalA, args[++i])); break;
                case "--feed-b":     inputs.Add((ChannelType.IncrementalB, args[++i])); break;
                case "--snap":       inputs.Add((ChannelType.SnapshotRecovery, args[++i])); break;
                case "--instr":      inputs.Add((ChannelType.InstrumentDefinition, args[++i])); break;
                case "--speed":      speed = double.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--warmup-s":   warmupSec = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--duration-s": durationSec = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--bucket-ms":  bucketMs = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--group":      channelGroup = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
                case "--out-csv":    outCsv = args[++i]; break;
                default:
                    Console.Error.WriteLine($"Unknown argument: {args[i]}");
                    PrintUsage();
                    return 1;
            }
        }

        if (inputs.Count == 0)
        {
            Console.Error.WriteLine("At least one --feed-a/--feed-b/--snap/--instr is required.");
            return 1;
        }
        if (speed <= 0)
        {
            Console.Error.WriteLine("--speed must be > 0");
            return 1;
        }

        Console.WriteLine($"Speed={speed}x  warmup={warmupSec}s  duration={(durationSec == 0 ? "unbounded" : $"{durationSec}s")}  bucket={bucketMs}ms  group={channelGroup}");
        Console.WriteLine($"Inputs:");
        foreach (var (ch, p) in inputs)
            Console.WriteLine($"  {ch,-20} {p}");

        // Find the earliest PCAP timestamp across all inputs so producers
        // share a common time origin (preserves cross-channel arrival order).
        long firstPcapMicros = long.MaxValue;
        foreach (var (_, p) in inputs)
        {
            using var r = new MmapPcapReader(p);
            if (r.TryReadNext(out var first))
            {
                if (first.TimestampMicros < firstPcapMicros)
                    firstPcapMicros = first.TimestampMicros;
            }
        }
        if (firstPcapMicros == long.MaxValue)
        {
            Console.Error.WriteLine("Could not read first packet from any input.");
            return 1;
        }

        long ticksPerSec = Stopwatch.Frequency;
        long bucketTicks = ticksPerSec / 1000L * bucketMs;
        long warmupTicks = ticksPerSec * warmupSec;
        long runStartTicks = Stopwatch.GetTimestamp();

        var recorder = new LatencyRecorderHandler(runStartTicks, bucketTicks, warmupTicks);
        var manager = new MultiFeedManager(
            groupIds: new[] { channelGroup },
            eventHandler: recorder);

        using var cts = new CancellationTokenSource();
        if (durationSec > 0)
            cts.CancelAfter(TimeSpan.FromSeconds(warmupSec + durationSec));
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        manager.StartAsync(cts.Token).GetAwaiter().GetResult();

        var producers = new List<(string Label, PcapReplayProducer Producer, Thread Thread)>();
        foreach (var (channel, path) in inputs)
        {
            var producer = new PcapReplayProducer(path, channel, channelGroup, speed, runStartTicks, firstPcapMicros, manager);
            var thread = new Thread(() => producer.Run(cts.Token))
            {
                IsBackground = true,
                Name = $"PcapReplay-{channel}",
            };
            thread.Start();
            producers.Add(($"{channel}", producer, thread));
        }

        // Periodically log progress so long runs don't look frozen.
        var progressTimer = new Timer(_ =>
        {
            long total = 0;
            foreach (var p in producers) total += p.Producer.PacketsPushed;
            long drops = manager.DroppedPacketsTotal;
            double elapsedSec = (Stopwatch.GetTimestamp() - runStartTicks) / (double)ticksPerSec;
            Console.WriteLine($"[t={elapsedSec,8:F1}s]  pushed={total,12:N0}  drops={drops,8:N0}  buckets={recorder.Buckets.Count,7:N0}");
        }, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

        foreach (var (_, _, thread) in producers)
            thread.Join();

        // After producers finish, give the dispatcher a moment to drain.
        Thread.Sleep(500);
        cts.Cancel();
        manager.StopAsync().GetAwaiter().GetResult();
        progressTimer.Dispose();

        // Emit CSV.
        Console.WriteLine();
        Console.WriteLine($"Run complete. Writing latency CSV to {outCsv}");
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(outCsv))!);
        EmitCsv(outCsv, recorder, bucketMs, ticksPerSec, manager.DroppedPacketsTotal);

        long totalPushed = 0;
        foreach (var (label, p, _) in producers)
        {
            Console.WriteLine($"  {label,-20} pushed={p.PacketsPushed,12:N0}");
            totalPushed += p.PacketsPushed;
        }
        Console.WriteLine($"  {"TOTAL",-20} pushed={totalPushed,12:N0}  drops={manager.DroppedPacketsTotal,8:N0}");
        return 0;
    }

    private static void EmitCsv(string path, LatencyRecorderHandler recorder, int bucketMs, long ticksPerSec, long totalDrops)
    {
        using var w = new StreamWriter(path);
        w.WriteLine("bucket_ms_offset,samples,rate_kpps,p50_us,p99_us,p999_us,max_us");
        var keys = new List<long>(recorder.Buckets.Keys);
        keys.Sort();
        foreach (var idx in keys)
        {
            var samples = recorder.Buckets[idx].Snapshot();
            if (samples.Length == 0) continue;
            Array.Sort(samples);
            long p50 = samples[(int)(samples.Length * 0.50)];
            long p99 = samples[Math.Min(samples.Length - 1, (int)(samples.Length * 0.99))];
            long p999 = samples[Math.Min(samples.Length - 1, (int)(samples.Length * 0.999))];
            long max = samples[^1];
            double bucketSec = bucketMs / 1000.0;
            double rateKpps = samples.Length / bucketSec / 1_000.0;
            long bucketOffsetMs = idx * bucketMs;
            w.WriteLine(string.Format(CultureInfo.InvariantCulture,
                "{0},{1},{2:F1},{3:F1},{4:F1},{5:F1},{6:F1}",
                bucketOffsetMs,
                samples.Length,
                rateKpps,
                TicksToMicros(p50, ticksPerSec),
                TicksToMicros(p99, ticksPerSec),
                TicksToMicros(p999, ticksPerSec),
                TicksToMicros(max, ticksPerSec)));
        }
        w.WriteLine($"# total_drops={totalDrops}");
    }

    private static double TicksToMicros(long ticks, long ticksPerSec)
        => ticks * 1_000_000.0 / ticksPerSec;

    private static bool HasFlag(string[] args, string flag)
    {
        foreach (var a in args) if (a == flag) return true;
        return false;
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: inbound-latency-probe [options]");
        Console.WriteLine("  --feed-a <pcap>      PCAP for IncrementalA channel");
        Console.WriteLine("  --feed-b <pcap>      PCAP for IncrementalB channel");
        Console.WriteLine("  --snap <pcap>        PCAP for SnapshotRecovery channel");
        Console.WriteLine("  --instr <pcap>       PCAP for InstrumentDefinition channel");
        Console.WriteLine("  --speed <n>          Replay speed multiplier (default 1.0)");
        Console.WriteLine("  --warmup-s <n>       Warmup seconds discarded from histograms (default 10)");
        Console.WriteLine("  --duration-s <n>     Stop after warmup+N seconds (0 = until PCAPs exhausted)");
        Console.WriteLine("  --bucket-ms <n>      Latency bucket width in milliseconds (default 100)");
        Console.WriteLine("  --group <n>          Channel group id (default 0)");
        Console.WriteLine("  --out-csv <path>     Output CSV path (default ./latency.csv)");
    }
}
