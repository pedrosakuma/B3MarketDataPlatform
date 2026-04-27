using System.Globalization;
using System.Text;
using B3.Umdf.PcapReplay;

namespace B3.Umdf.Tools.PcapTrafficCharacterizer;

/// <summary>
/// Phase 0 of the per-channel ring isolation PoC: extract ground-truth traffic
/// statistics from recorded PCAPs so the dispatcher benchmark workloads are
/// derived from real B3 traffic instead of synthetic guesses.
///
/// Per-bucket CSV (one row per channel × time bucket):
///   session,channel,bucket_unix_ms,pkts,bytes,min_size,max_size
/// Per-channel size histogram CSV:
///   session,channel,bin_le_size,count
/// Console summary table for quick review (mean kpps, peak kpps, peak Mbps).
/// </summary>
internal static class Program
{
    private const int PacketHeaderSize = 16;

    private static readonly int[] SizeHistogramBins = { 80, 128, 192, 256, 384, 512, 768, 1024, 1500 };

    private static int Main(string[] args)
    {
        if (args.Length == 0 || HasFlag(args, "--help") || HasFlag(args, "-h"))
        {
            PrintUsage();
            return args.Length == 0 ? 1 : 0;
        }

        var inputs = new List<(string Channel, string Path)>();
        string session = "session";
        string outDir = "./pcap-stats";
        long bucketMicros = 10_000;
        long maxPackets = long.MaxValue;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--feed-a":  inputs.Add(("IncrementalA", args[++i])); break;
                case "--feed-b":  inputs.Add(("IncrementalB", args[++i])); break;
                case "--snap":    inputs.Add(("SnapshotRecovery", args[++i])); break;
                case "--instr":   inputs.Add(("InstrumentDefinition", args[++i])); break;
                case "--session": session = args[++i]; break;
                case "--out-dir": outDir = args[++i]; break;
                case "--bucket-ms": bucketMicros = long.Parse(args[++i], CultureInfo.InvariantCulture) * 1_000; break;
                case "--max-packets": maxPackets = long.Parse(args[++i], CultureInfo.InvariantCulture); break;
                default:
                    Console.Error.WriteLine($"Unknown argument: {args[i]}");
                    PrintUsage();
                    return 1;
            }
        }

        if (inputs.Count == 0)
        {
            Console.Error.WriteLine("At least one of --feed-a/--feed-b/--snap/--instr is required.");
            return 1;
        }

        Directory.CreateDirectory(outDir);
        string bucketCsv = Path.Combine(outDir, $"{session}-buckets.csv");
        string sizesCsv = Path.Combine(outDir, $"{session}-sizes.csv");

        using var bucketWriter = new StreamWriter(bucketCsv);
        using var sizesWriter = new StreamWriter(sizesCsv);
        bucketWriter.WriteLine("session,channel,bucket_unix_ms,pkts,bytes,min_size,max_size");
        sizesWriter.WriteLine("session,channel,bin_le_size,count");

        var summary = new List<ChannelSummary>();

        foreach (var (channel, path) in inputs)
        {
            Console.WriteLine($"[{channel}] reading {path}");
            var s = Characterize(session, channel, path, bucketMicros, maxPackets, bucketWriter);
            summary.Add(s);

            for (int b = 0; b < SizeHistogramBins.Length; b++)
                sizesWriter.WriteLine($"{Csv(session)},{Csv(channel)},{SizeHistogramBins[b]},{s.SizeHistogram[b]}");
            // Overflow bin (any payload larger than the last bound).
            sizesWriter.WriteLine($"{Csv(session)},{Csv(channel)},-1,{s.SizeHistogram[SizeHistogramBins.Length]}");
        }

        Console.WriteLine();
        Console.WriteLine($"Per-bucket CSV: {bucketCsv}");
        Console.WriteLine($"Sizes CSV:      {sizesCsv}");
        Console.WriteLine();
        PrintSummary(summary);
        return 0;
    }

    private readonly record struct ChannelSummary(
        string Session,
        string Channel,
        long Packets,
        long Bytes,
        long DurationMicros,
        long PeakPktsPer100ms,
        long PeakBytesPer100ms,
        long PeakPktsPer1s,
        long PeakBytesPer1s,
        long[] SizeHistogram);

    private static ChannelSummary Characterize(
        string session, string channel, string path,
        long bucketMicros, long maxPackets, StreamWriter bucketWriter)
    {
        using var reader = new MmapPcapReader(path);
        int udpOffset = -1;
        long pktCount = 0;

        long firstTs = -1;
        long lastTs = -1;
        long bytesTotal = 0;

        // Streaming bucket aggregation: emit a row when we cross to a new bucket.
        long currentBucketStart = -1;
        long bucketPkts = 0;
        long bucketBytes = 0;
        int bucketMin = int.MaxValue;
        int bucketMax = int.MinValue;

        // Coarser windows for "peak" metrics (sliding-window via fixed-bucket roll-up).
        var window100ms = new RollingPeak(100_000 / bucketMicros);
        var window1s = new RollingPeak(1_000_000 / bucketMicros);

        var sizeHist = new long[SizeHistogramBins.Length + 1];

        var sb = new StringBuilder(96);

        while (reader.TryReadNext(out var pkt) && pktCount < maxPackets)
        {
            pktCount++;
            var frame = pkt.Data.Span;
            if (udpOffset < 0)
                udpOffset = UdpExtractor.ComputeUdpPayloadOffset(frame, reader.LinkType);
            if (udpOffset >= frame.Length)
                continue;
            var payload = frame[udpOffset..];
            if (payload.Length < PacketHeaderSize)
                continue;

            int size = payload.Length;
            long ts = pkt.TimestampMicros;
            if (firstTs < 0) firstTs = ts;
            lastTs = ts;
            bytesTotal += size;

            int bin = SizeHistogramBins.Length;
            for (int i = 0; i < SizeHistogramBins.Length; i++)
                if (size <= SizeHistogramBins[i]) { bin = i; break; }
            sizeHist[bin]++;

            long bucketStart = ts - (ts % bucketMicros);
            if (currentBucketStart < 0)
            {
                currentBucketStart = bucketStart;
            }
            else if (bucketStart != currentBucketStart)
            {
                // Flush gap-included buckets so peaks reflect idle time correctly.
                FlushBucket(bucketWriter, sb, session, channel, currentBucketStart,
                    bucketPkts, bucketBytes, bucketMin, bucketMax,
                    window100ms, window1s);
                long step = bucketStart - currentBucketStart;
                long emptyBuckets = (step / bucketMicros) - 1;
                for (long e = 0; e < emptyBuckets; e++)
                {
                    window100ms.Push(0, 0);
                    window1s.Push(0, 0);
                }
                currentBucketStart = bucketStart;
                bucketPkts = 0;
                bucketBytes = 0;
                bucketMin = int.MaxValue;
                bucketMax = int.MinValue;
            }

            bucketPkts++;
            bucketBytes += size;
            if (size < bucketMin) bucketMin = size;
            if (size > bucketMax) bucketMax = size;
        }

        if (currentBucketStart >= 0)
        {
            FlushBucket(bucketWriter, sb, session, channel, currentBucketStart,
                bucketPkts, bucketBytes, bucketMin, bucketMax,
                window100ms, window1s);
        }

        return new ChannelSummary(
            session, channel, pktCount, bytesTotal,
            DurationMicros: firstTs < 0 ? 0 : (lastTs - firstTs),
            PeakPktsPer100ms: window100ms.PeakPkts,
            PeakBytesPer100ms: window100ms.PeakBytes,
            PeakPktsPer1s: window1s.PeakPkts,
            PeakBytesPer1s: window1s.PeakBytes,
            SizeHistogram: sizeHist);
    }

    private static void FlushBucket(
        StreamWriter writer, StringBuilder sb,
        string session, string channel, long bucketStartMicros,
        long pkts, long bytes, int min, int max,
        RollingPeak w100ms, RollingPeak w1s)
    {
        if (min == int.MaxValue) min = 0;
        if (max == int.MinValue) max = 0;
        long bucketUnixMs = bucketStartMicros / 1_000;
        sb.Clear();
        sb.Append(Csv(session)).Append(',')
          .Append(Csv(channel)).Append(',')
          .Append(bucketUnixMs).Append(',')
          .Append(pkts).Append(',')
          .Append(bytes).Append(',')
          .Append(min).Append(',')
          .Append(max);
        writer.WriteLine(sb.ToString());
        w100ms.Push(pkts, bytes);
        w1s.Push(pkts, bytes);
    }

    private static void PrintSummary(List<ChannelSummary> summary)
    {
        Console.WriteLine("Channel              | pkts        | GB     | duration_s | mean_kpps | peak_kpps_100ms | peak_Mbps_100ms | peak_kpps_1s");
        Console.WriteLine("---------------------+-------------+--------+------------+-----------+-----------------+-----------------+-------------");
        foreach (var s in summary)
        {
            double durSec = s.DurationMicros / 1_000_000.0;
            double meanKpps = durSec > 0 ? s.Packets / durSec / 1_000.0 : 0;
            double gb = s.Bytes / 1_073_741_824.0;
            double peakKpps100 = s.PeakPktsPer100ms / 0.1 / 1_000.0;
            double peakMbps100 = s.PeakBytesPer100ms * 8 / 0.1 / 1_000_000.0;
            double peakKpps1s = s.PeakPktsPer1s / 1.0 / 1_000.0;
            Console.WriteLine($"{s.Channel,-20} | {s.Packets,11:N0} | {gb,6:F2} | {durSec,10:F1} | {meanKpps,9:F1} | {peakKpps100,15:F1} | {peakMbps100,15:F1} | {peakKpps1s,11:F1}");
        }
    }

    private static bool HasFlag(string[] args, string flag)
    {
        foreach (var a in args) if (a == flag) return true;
        return false;
    }

    private static string Csv(string s) => s.IndexOfAny(new[] { ',', '"', '\n' }) < 0 ? s : "\"" + s.Replace("\"", "\"\"") + "\"";

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: pcap-traffic-characterizer [options]");
        Console.WriteLine("  --feed-a <path>      PCAP for IncrementalA channel");
        Console.WriteLine("  --feed-b <path>      PCAP for IncrementalB channel");
        Console.WriteLine("  --snap <path>        PCAP for SnapshotRecovery channel");
        Console.WriteLine("  --instr <path>       PCAP for InstrumentDefinition channel");
        Console.WriteLine("  --session <name>     Session label (default: session)");
        Console.WriteLine("  --out-dir <path>     Output directory (default: ./pcap-stats)");
        Console.WriteLine("  --bucket-ms <n>      Bucket size in milliseconds (default: 10)");
        Console.WriteLine("  --max-packets <n>    Stop after N packets per file");
    }
}

/// <summary>
/// Rolling sum over the last <c>WindowSize</c> buckets, tracking peak pkts and bytes.
/// </summary>
internal sealed class RollingPeak
{
    private readonly long[] _pkts;
    private readonly long[] _bytes;
    private int _index;
    private long _sumPkts;
    private long _sumBytes;
    private long _peakPkts;
    private long _peakBytes;
    private int _filled;

    public RollingPeak(long windowSize)
    {
        if (windowSize < 1) windowSize = 1;
        _pkts = new long[windowSize];
        _bytes = new long[windowSize];
    }

    public long PeakPkts => _peakPkts;
    public long PeakBytes => _peakBytes;

    public void Push(long pkts, long bytes)
    {
        _sumPkts -= _pkts[_index];
        _sumBytes -= _bytes[_index];
        _pkts[_index] = pkts;
        _bytes[_index] = bytes;
        _sumPkts += pkts;
        _sumBytes += bytes;
        _index++;
        if (_index >= _pkts.Length) _index = 0;
        if (_filled < _pkts.Length) _filled++;
        if (_sumPkts > _peakPkts) _peakPkts = _sumPkts;
        if (_sumBytes > _peakBytes) _peakBytes = _sumBytes;
    }
}
