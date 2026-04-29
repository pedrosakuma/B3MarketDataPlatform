using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Mbo.Sbe.V16.V6;
using B3.Umdf.Transport;

// Synthetic UMDF publisher: emits a stateless full-bootstrap stream over multicast.
//
// Bootstrap recipe (stateless on producer side):
//   * InstrumentDefinition channel: emits SecurityDefinition_12 per symbol with
//     TotNoRelatedSym=N — populates SymbolRegistry and triggers
//     OnInstrumentDefinitionsComplete so the consumer leaves WaitInstrumentDefinition.
//   * SnapshotRecovery channel: emits SnapshotFullRefresh_Header_30 with
//     OrdersExpected=0 + LastRptSeq=null per symbol — triggers
//     SnapshotApplier.HealFromIlliquidEmptySnapshot (book starts empty, healed).
//   * IncrementalA/B channels: blasts Order_MBO_50 NEW-only with monotonic
//     SecondaryOrderID + per-symbol monotonic RptSeq. Grow-only books (never
//     DELETE / CHANGE) — the consumer can never cross an invariant on missing
//     prior order state, so no producer-side book mirror is needed.
//
// PacketHeader.SequenceVersion is held at 0 across all channels — the consumer's
// snapshot version-gate is bypassed (`if (currentVersion != 0 ...)`), so we don't
// need V15+ snap with LastSequenceVersion set.
//
// SecurityID space is keyed on the *channel group* (not channel index) so the
// SecDef + Snap + Inc threads of the same group share the same symbol pool.

var opts = ParseArgs(args);
if (opts is null) return 1;

var feedConfig = MulticastFeedConfig.Load(opts.MulticastConfigPath);
var publishChannels = feedConfig.ToPublishChannelConfigs();
if (publishChannels.Count == 0)
{
    Console.Error.WriteLine($"No publish channels found in {opts.MulticastConfigPath}");
    return 2;
}

if (opts.ChannelTypeFilter is { } filter)
{
    publishChannels = publishChannels.Where(c => c.Type == filter).ToList();
    if (publishChannels.Count == 0)
    {
        Console.Error.WriteLine($"No channels match --only-channel-type={filter}");
        return 2;
    }
}

Console.WriteLine($"=== Synthetic UMDF Publisher ===");
Console.WriteLine($"  config           : {opts.MulticastConfigPath}");
Console.WriteLine($"  channels         : {publishChannels.Count}");
Console.WriteLine($"  symbols/group    : {opts.SymbolsPerGroup}");
Console.WriteLine($"  msgs/inc-packet  : {opts.MessagesPerIncPacket}");
Console.WriteLine($"  inc target pps   : {(opts.IncTargetPps == 0 ? "max" : opts.IncTargetPps.ToString(CultureInfo.InvariantCulture))} per Inc thread");
Console.WriteLine($"  snap pps         : {opts.SnapPps}");
Console.WriteLine($"  instrdef pps     : {opts.InstrDefPps}");
Console.WriteLine($"  ramp seconds     : {opts.RampSeconds}");
Console.WriteLine($"  duration         : {(opts.DurationSeconds == 0 ? "until ctrl-c" : opts.DurationSeconds + "s")}");
foreach (var ch in publishChannels)
    Console.WriteLine($"  -> grp={ch.ChannelGroup} ch={ch.ChannelId,3} type={ch.Type,-22} {ch.MulticastGroup}:{ch.Port}");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
if (opts.DurationSeconds > 0)
    cts.CancelAfter(TimeSpan.FromSeconds(opts.DurationSeconds));

var workers = new List<Worker>(publishChannels.Count);
foreach (var ch in publishChannels)
{
    var mode = ch.Type switch
    {
        ChannelType.IncrementalA or ChannelType.IncrementalB => WorkerMode.Incremental,
        ChannelType.SnapshotRecovery => WorkerMode.Snapshot,
        ChannelType.InstrumentDefinition => WorkerMode.InstrumentDefinition,
        _ => (WorkerMode?)null
    };
    if (mode is null) continue;
    workers.Add(new Worker(ch, opts, mode.Value));
}

foreach (var w in workers) w.Start(cts.Token);

var stats = new StatsPrinter(workers);
var statsTask = Task.Run(() => stats.RunAsync(cts.Token));

foreach (var w in workers) w.Thread.Join();
try { await statsTask; } catch (OperationCanceledException) { }

stats.PrintFinal();
return 0;

static Options? ParseArgs(string[] args)
{
    var opts = new Options();
    for (int i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--multicast-config" when i + 1 < args.Length:
                opts.MulticastConfigPath = args[++i]; break;
            case "--symbols" when i + 1 < args.Length:
                opts.SymbolsPerGroup = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--messages-per-packet" when i + 1 < args.Length:
                opts.MessagesPerIncPacket = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--target-pps" when i + 1 < args.Length:
                opts.IncTargetPps = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--snap-pps" when i + 1 < args.Length:
                opts.SnapPps = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--instrdef-pps" when i + 1 < args.Length:
                opts.InstrDefPps = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--ramp-seconds" when i + 1 < args.Length:
                opts.RampSeconds = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--duration" when i + 1 < args.Length:
                opts.DurationSeconds = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--only-channel-type" when i + 1 < args.Length:
                opts.ChannelTypeFilter = Enum.Parse<ChannelType>(args[++i], ignoreCase: true); break;
            case "-h" or "--help":
                PrintUsage(); return null;
            default:
                Console.Error.WriteLine($"Unknown arg: {args[i]}");
                PrintUsage(); return null;
        }
    }
    if (string.IsNullOrEmpty(opts.MulticastConfigPath))
    {
        Console.Error.WriteLine("--multicast-config is required");
        PrintUsage(); return null;
    }
    if (opts.MessagesPerIncPacket < 1 || opts.MessagesPerIncPacket > 64)
    {
        Console.Error.WriteLine("--messages-per-packet must be 1..64");
        return null;
    }
    return opts;
}

static void PrintUsage()
{
    Console.WriteLine("Usage:");
    Console.WriteLine("  synthetic-umdf-publisher --multicast-config <path> [options]");
    Console.WriteLine("Options:");
    Console.WriteLine("  --symbols <n>             Synthetic SecurityID pool per channel group (default 100)");
    Console.WriteLine("  --messages-per-packet <n> Order_MBO_50 messages per Inc datagram (default 1)");
    Console.WriteLine("  --target-pps <n>          Per-Inc-thread packets/sec cap; 0 = max (default 0)");
    Console.WriteLine("  --snap-pps <n>            Snap Header_30 packets/sec per Snap channel (default 2000)");
    Console.WriteLine("  --instrdef-pps <n>        SecDef_12 packets/sec per InstrDef channel (default 1000)");
    Console.WriteLine("  --ramp-seconds <n>        Delay (seconds) before Inc threads start blasting (default 3)");
    Console.WriteLine("  --duration <secs>         Run for N seconds; 0 = until ctrl-c (default 0)");
    Console.WriteLine("  --only-channel-type <t>   Restrict to one ChannelType (e.g. IncrementalA)");
}

internal enum WorkerMode { Incremental, Snapshot, InstrumentDefinition }

internal sealed class Options
{
    public string MulticastConfigPath { get; set; } = "";
    public int SymbolsPerGroup { get; set; } = 100;
    public int MessagesPerIncPacket { get; set; } = 1;
    public int IncTargetPps { get; set; } = 0;
    public int SnapPps { get; set; } = 2000;
    public int InstrDefPps { get; set; } = 1000;
    public int RampSeconds { get; set; } = 3;
    public int DurationSeconds { get; set; } = 0;
    public ChannelType? ChannelTypeFilter { get; set; }
}

internal static class WireOffsets
{
    public const int PacketHeaderSize = 16;
    public const int FramingHeaderSize = 4;
    public const int SbeMessageHeaderSize = 8;

    // PacketHeader (Pack=1): byte channel, byte reserved, ushort seqVersion, uint sequenceNumber@4, ulong sendingTime@8
    public const int PacketHeaderSequenceNumberOffset = 4;
    public const int PacketHeaderSendingTimeOffset = 8;

    // Order_MBO_50V6 body offsets (FieldOffsets in generated code)
    public const int OrderBlockLength = 56;
    public const int OrderBodySecurityIdOffset = 0;        // long
    public const int OrderBodyMdInsertTimestampOffset = 36; // ulong nanos
    public const int OrderBodySecondaryOrderIdOffset = 44;  // long
    public const int OrderBodyRptSeqOffset = 52;           // uint

    // SnapshotFullRefresh_Header_30V6 body offsets
    public const int SnapBlockLength = 32;
    public const int SnapBodySecurityIdOffset = 0;          // long
    public const int SnapBodyTotNumReportsOffset = 12;      // uint
    public const int SnapBodyTotNumBidsOffset = 16;         // uint (= OrdersExpected/2 — leave 0)
    public const int SnapBodyTotNumOffersOffset = 20;       // uint
    public const int SnapBodyTotNumStatsOffset = 24;        // ushort
    public const int SnapBodyLastRptSeqOffset = 28;         // uint (0 = NULL → empty illiquid)

    // SecurityDefinition_12V6 body offsets — see SecurityDefinition_12V6.cs
    public const int SecDefBlockLength = 230;
    public const int SecDefSecurityIdOffset = 0;            // long
    public const int SecDefSecurityExchangeOffset = 8;      // SecurityExchange (4 ASCII bytes)
    public const int SecDefSymbolOffset = 16;               // Symbol (20 ASCII bytes)
    public const int SecDefSecurityTypeOffset = 37;         // SecurityType (1 byte enum)
    public const int SecDefTotNoRelatedSymOffset = 40;      // uint
    public const int SecDefSecurityValidityTimestampOffset = 76; // long (UTCTimestampSeconds.time)
    public const int SecDefMaturityDateOffset = 140;        // int (LocalMktDate, 0 = null sentinel)
    public const int SecDefIsinNumberOffset = 164;          // ISINNumber (12 ASCII bytes)

    // We append three empty repeating-group headers (NoUnderlyings, NoLegs,
    // NoInstrAttribs) so any future consumer path that calls ReadGroups stays
    // safe; HandleSecurityDefinition's foreach enumerators don't strictly
    // require them, but the cost is 9 bytes per packet.
    public const int GroupSizeEncodingSize = 3;
    public const int SecDefBodyTotal = SecDefBlockLength + GroupSizeEncodingSize * 3;
}

internal sealed class Worker
{
    private readonly MulticastPublishChannelConfig _config;
    private readonly Options _opts;
    private readonly WorkerMode _mode;
    private byte[] _buffer = null!;
    private int _packetSize;
    private int _perMessageWireLen;
    private int _msgsPerPacket;
    private long _packetsSent;
    private long _bytesSent;

    public Thread Thread { get; private set; } = null!;
    public string Name => $"grp{_config.ChannelGroup}-ch{_config.ChannelId}-{_mode}";
    public WorkerMode Mode => _mode;
    public long PacketsSent => Interlocked.Read(ref _packetsSent);
    public long BytesSent => Interlocked.Read(ref _bytesSent);

    public Worker(MulticastPublishChannelConfig config, Options opts, WorkerMode mode)
    {
        _config = config;
        _opts = opts;
        _mode = mode;
        BuildTemplate();
    }

    private void BuildTemplate()
    {
        switch (_mode)
        {
            case WorkerMode.Incremental:
                _msgsPerPacket = _opts.MessagesPerIncPacket;
                _perMessageWireLen = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + WireOffsets.OrderBlockLength;
                break;
            case WorkerMode.Snapshot:
                _msgsPerPacket = 1;
                _perMessageWireLen = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + WireOffsets.SnapBlockLength;
                break;
            case WorkerMode.InstrumentDefinition:
                _msgsPerPacket = 1;
                _perMessageWireLen = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + WireOffsets.SecDefBodyTotal;
                break;
        }
        _packetSize = WireOffsets.PacketHeaderSize + _perMessageWireLen * _msgsPerPacket;
        _buffer = new byte[_packetSize];

        // PacketHeader — SequenceVersion=0 across the board (snapshot version
        // gate `if (currentVersion != 0 ...)` is bypassed, so a V6 snapshot
        // with absent LastSequenceVersion is accepted).
        ref var hdr = ref MemoryMarshal.AsRef<PacketHeader>(_buffer.AsSpan(0, WireOffsets.PacketHeaderSize));
        hdr.ChannelNumber = (byte)_config.ChannelId;
        hdr.Reserved = 0;
        hdr.SequenceVersion = 0;
        hdr.SequenceNumber = 0;
        hdr.SendingTime = 0;

        for (int m = 0; m < _msgsPerPacket; m++)
        {
            int off = WireOffsets.PacketHeaderSize + _perMessageWireLen * m;
            ref var framing = ref MemoryMarshal.AsRef<FramingHeader>(_buffer.AsSpan(off, WireOffsets.FramingHeaderSize));
            framing.MessageLength = (ushort)_perMessageWireLen;
            framing.EncodingType = 0;
            int mhOff = off + WireOffsets.FramingHeaderSize;
            int bodyOff = mhOff + WireOffsets.SbeMessageHeaderSize;

            switch (_mode)
            {
                case WorkerMode.Incremental:
                    B3.Umdf.Mbo.Sbe.V16.V6.Order_MBO_50Data.WriteHeader(_buffer.AsSpan(mhOff, WireOffsets.SbeMessageHeaderSize));
                    InitOrderBody(_buffer.AsSpan(bodyOff, WireOffsets.OrderBlockLength));
                    break;
                case WorkerMode.Snapshot:
                    B3.Umdf.Mbo.Sbe.V16.V6.SnapshotFullRefresh_Header_30Data.WriteHeader(_buffer.AsSpan(mhOff, WireOffsets.SbeMessageHeaderSize));
                    InitSnapBody(_buffer.AsSpan(bodyOff, WireOffsets.SnapBlockLength));
                    break;
                case WorkerMode.InstrumentDefinition:
                    B3.Umdf.Mbo.Sbe.V16.V6.SecurityDefinition_12Data.WriteHeader(_buffer.AsSpan(mhOff, WireOffsets.SbeMessageHeaderSize));
                    InitSecDefBody(_buffer.AsSpan(bodyOff, WireOffsets.SecDefBodyTotal));
                    break;
            }
        }
    }

    private static void InitOrderBody(Span<byte> body)
    {
        // MDUpdateAction (offset 9) = NEW (0) — already zero, kept for clarity
        body[9] = (byte)MDUpdateAction.NEW;
        // MDEntryType (offset 10) = BID ('0' = 0x30)
        body[10] = (byte)MDEntryType.BID;
        // MDEntryPx.Mantissa (offset 12, 8 bytes)
        long pxMantissa = 1000_0000_0000L;
        MemoryMarshal.Write(body.Slice(12, 8), in pxMantissa);
        // MDEntrySize.Value (offset 20, 8 bytes)
        long size = 100;
        MemoryMarshal.Write(body.Slice(20, 8), in size);
    }

    private void InitSnapBody(Span<byte> body)
    {
        // SecurityExchange "BVMF" at offset 8 (4 ASCII bytes)
        Encoding.ASCII.GetBytes("BVMF", body.Slice(8, 4));
        // TotNumReports/Bids/Offers/Stats stay zero → OrdersExpected = 0 → empty illiquid heal
        // LastRptSeq stays zero → NULL sentinel in V6 layout
        // SecurityID is mutated per-send.
    }

    private void InitSecDefBody(Span<byte> body)
    {
        // SecurityExchange "BVMF" at offset 8
        Encoding.ASCII.GetBytes("BVMF", body.Slice(WireOffsets.SecDefSecurityExchangeOffset, 4));
        // SecurityType byte at offset 37 — use 5 (FUTURE) as a generic non-zero sentinel.
        body[WireOffsets.SecDefSecurityTypeOffset] = 5;
        // TotNoRelatedSym (uint) at offset 40 — must equal the number of distinct
        // SecurityIDs we'll emit on this channel so OnInstrumentDefinitionsComplete fires.
        uint totNoRelated = (uint)_opts.SymbolsPerGroup;
        MemoryMarshal.Write(body.Slice(WireOffsets.SecDefTotNoRelatedSymOffset, 4), in totNoRelated);
        // SecurityValidityTimestamp.time (long) at offset 76 — set to a fixed
        // non-zero epoch so HandleSecurityDefinition's skip-if-unchanged hot path
        // engages on resends. Per-symbol TS varies (mutated per-send below).
        // MaturityDate stays zero → null sentinel.
        // IsinNumber/Symbol/Asset/etc. zero — fine for tracking. Symbol is set per-send.
        // Trailing 9 bytes are 3 zero GroupSizeEncodings (NumInGroup=0, BlockLength=0).
    }

    public void Start(CancellationToken ct)
    {
        Thread = new Thread(() => Run(ct)) { Name = Name, IsBackground = true };
        Thread.Start();
    }

    private void Run(CancellationToken ct)
    {
        // Inc threads delay-start so InstrDef + Snap can bootstrap symbols first.
        if (_mode == WorkerMode.Incremental && _opts.RampSeconds > 0)
        {
            try { Task.Delay(TimeSpan.FromSeconds(_opts.RampSeconds), ct).Wait(ct); }
            catch (OperationCanceledException) { return; }
        }

        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 4);
        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, 4 * 1024 * 1024);
        var endpoint = new IPEndPoint(_config.MulticastGroup, _config.Port);

        long securityIdBase = 900_000_000_000L + (_config.ChannelGroup * 1_000_000L);
        int symbolPool = _opts.SymbolsPerGroup;
        var rptSeqs = _mode == WorkerMode.Incremental ? new uint[symbolPool] : Array.Empty<uint>();

        int targetPps = _mode switch
        {
            WorkerMode.Incremental => _opts.IncTargetPps,
            WorkerMode.Snapshot => _opts.SnapPps,
            WorkerMode.InstrumentDefinition => _opts.InstrDefPps,
            _ => 0
        };

        var sw = Stopwatch.StartNew();
        long ticksPerPacket = targetPps > 0 ? Stopwatch.Frequency / targetPps : 0;
        long nextSendTicks = sw.ElapsedTicks;

        uint pktSeq = 0;
        long secondaryOrderId = 1;
        int symbolCursor = 0;

        var bufferSpan = _buffer.AsSpan();

        while (!ct.IsCancellationRequested)
        {
            if (ticksPerPacket > 0)
            {
                long now = sw.ElapsedTicks;
                long delay = nextSendTicks - now;
                if (delay > 0)
                {
                    long ticksPerMs = Stopwatch.Frequency / 1000;
                    if (delay > ticksPerMs * 2)
                    {
                        try { Thread.Sleep((int)(delay / ticksPerMs) - 1); }
                        catch { }
                    }
                    while (sw.ElapsedTicks < nextSendTicks && !ct.IsCancellationRequested)
                        Thread.SpinWait(20);
                }
                nextSendTicks += ticksPerPacket;
            }

            // PacketHeader.SequenceNumber + SendingTime
            pktSeq++;
            MemoryMarshal.Write(bufferSpan.Slice(WireOffsets.PacketHeaderSequenceNumberOffset, 4), in pktSeq);
            ulong sendingTimeNs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000UL;
            MemoryMarshal.Write(bufferSpan.Slice(WireOffsets.PacketHeaderSendingTimeOffset, 8), in sendingTimeNs);

            for (int m = 0; m < _msgsPerPacket; m++)
            {
                int bodyOff = WireOffsets.PacketHeaderSize + _perMessageWireLen * m
                    + WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize;

                int symIdx = symbolCursor;
                symbolCursor++;
                if (symbolCursor >= symbolPool) symbolCursor = 0;
                long secId = securityIdBase + symIdx;

                switch (_mode)
                {
                    case WorkerMode.Incremental:
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.OrderBodySecurityIdOffset, 8), in secId);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.OrderBodyMdInsertTimestampOffset, 8), in sendingTimeNs);
                        long oid = secondaryOrderId++;
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.OrderBodySecondaryOrderIdOffset, 8), in oid);
                        rptSeqs[symIdx]++;
                        uint rs = rptSeqs[symIdx];
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.OrderBodyRptSeqOffset, 4), in rs);
                        break;
                    case WorkerMode.Snapshot:
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.SnapBodySecurityIdOffset, 8), in secId);
                        // OrdersExpected stays 0; LastRptSeq stays 0 (=null).
                        break;
                    case WorkerMode.InstrumentDefinition:
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.SecDefSecurityIdOffset, 8), in secId);
                        // Symbol "SYNT<grp>_<idx>" padded to 20 bytes
                        Span<byte> symSpan = bufferSpan.Slice(bodyOff + WireOffsets.SecDefSymbolOffset, 20);
                        symSpan.Clear();
                        WriteAsciiSymbol(symSpan, _config.ChannelGroup, symIdx);
                        // Per-symbol fixed SecurityValidityTimestamp (epoch seconds derived
                        // from securityId so resends are dedupe'd by the consumer's
                        // skip-if-unchanged hot path). Using a constant per (group, idx)
                        // is essential — varying TS would force full re-parse + group
                        // walk on every resend.
                        long validityTs = 1_700_000_000L + secId; // arbitrary stable per-symbol epoch
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.SecDefSecurityValidityTimestampOffset, 8), in validityTs);
                        break;
                }
            }

            try
            {
                int sent = socket.SendTo(_buffer, 0, _packetSize, SocketFlags.None, endpoint);
                Interlocked.Increment(ref _packetsSent);
                Interlocked.Add(ref _bytesSent, sent);
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.NetworkUnreachable
                                          || ex.SocketErrorCode == SocketError.HostUnreachable)
            {
                Thread.Sleep(10);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private static void WriteAsciiSymbol(Span<byte> dst, int group, int idx)
    {
        // "SYNTHg<group>_<idx>" — keep within 20 ASCII bytes
        Span<char> chars = stackalloc char[20];
        var success = $"SYN{group:D2}_{idx:D6}".TryCopyTo(chars);
        if (!success)
        {
            // Should not happen with current widths (3+2+1+6 = 12 chars).
            "SYN?".AsSpan().CopyTo(chars);
        }
        int charsWritten = Math.Min(20, $"SYN{group:D2}_{idx:D6}".Length);
        Encoding.ASCII.GetBytes(chars[..charsWritten], dst);
    }
}

internal sealed class StatsPrinter
{
    private readonly IReadOnlyList<Worker> _workers;
    private readonly Stopwatch _sw = Stopwatch.StartNew();
    private long _lastPackets;
    private long _lastBytes;
    private long _lastTicks;

    public StatsPrinter(IReadOnlyList<Worker> workers) { _workers = workers; }

    public async Task RunAsync(CancellationToken ct)
    {
        _lastTicks = _sw.ElapsedTicks;
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(1000, ct); } catch { break; }
            PrintInterval();
        }
    }

    private void PrintInterval()
    {
        long packets = 0, bytes = 0;
        long incPackets = 0, snapPackets = 0, sdPackets = 0;
        foreach (var w in _workers)
        {
            packets += w.PacketsSent;
            bytes += w.BytesSent;
            switch (w.Mode)
            {
                case WorkerMode.Incremental: incPackets += w.PacketsSent; break;
                case WorkerMode.Snapshot: snapPackets += w.PacketsSent; break;
                case WorkerMode.InstrumentDefinition: sdPackets += w.PacketsSent; break;
            }
        }
        long now = _sw.ElapsedTicks;
        double secs = (double)(now - _lastTicks) / Stopwatch.Frequency;
        long dp = packets - _lastPackets;
        long db = bytes - _lastBytes;
        _lastPackets = packets; _lastBytes = bytes; _lastTicks = now;
        double pps = dp / Math.Max(secs, 1e-6);
        double mbps = (db * 8.0) / Math.Max(secs, 1e-6) / 1_000_000.0;
        Console.WriteLine($"[{_sw.Elapsed:hh\\:mm\\:ss}] {pps,10:N0} pps  {mbps,7:N1} Mb/s  inc={incPackets:N0} snap={snapPackets:N0} sd={sdPackets:N0}");
    }

    public void PrintFinal()
    {
        long packets = 0, bytes = 0;
        foreach (var w in _workers) { packets += w.PacketsSent; bytes += w.BytesSent; }
        Console.WriteLine();
        Console.WriteLine($"=== Final ({_sw.Elapsed:hh\\:mm\\:ss}) ===");
        Console.WriteLine($"  total packets: {packets:N0}");
        Console.WriteLine($"  total bytes  : {bytes:N0}");
        Console.WriteLine($"  avg pps      : {packets / Math.Max(_sw.Elapsed.TotalSeconds, 1e-6):N0}");
        Console.WriteLine($"  per-channel:");
        foreach (var w in _workers)
            Console.WriteLine($"    {w.Name,-36} {w.PacketsSent,12:N0} pkts  {w.BytesSent,14:N0} bytes");
    }
}
