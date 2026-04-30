using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Mbo.Sbe.V16.V6;
using B3.Umdf.Transport;
using B3.Umdf.WireEncoder;

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
Console.WriteLine($"  price mode       : {opts.PriceMode}{(opts.PriceMode == PriceMode.Spread ? $" (levels={opts.PriceLevels})" : "")}");
Console.WriteLine($"  rotation window  : {(opts.RotationWindow == 0 ? "off (grow-only NEW)" : $"K={opts.RotationWindow}")}");
if (opts.RotationWindow > 0)
{
    // Coherent snap mode: snap worker derives book state from formula at the
    // current shared rs counter. Re-snaps after first heal take the consumer's
    // Skipped path (Healthy + ahead) → no book mutation → safe to re-emit.
    int snapEntries = opts.RotationWindow;
    int snapPacketSize = 16 + (4 + 8 + 32) + (4 + 8 + 8 + 3 + snapEntries * 42);
    if (snapPacketSize > 1400)
    {
        Console.Error.WriteLine($"--rotation-window={opts.RotationWindow} would produce snap packets of {snapPacketSize} bytes (max active set = K orders × 42B), exceeding 1400-byte safe MTU. Reduce K or implement Orders_71 chunking.");
        return 2;
    }
    Console.WriteLine($"  coherent snap    : Header(LastRptSeq=rs) + Orders_71(K active) per symbol, ~{snapPacketSize}B/pkt");
}
Console.WriteLine($"  snap bootstrap   : {(opts.SnapBootstrapOnly ? "ONE snap per symbol then stop" : "continuous")}");
Console.WriteLine($"  duration         : {(opts.DurationSeconds == 0 ? "until ctrl-c" : opts.DurationSeconds + "s")}");
foreach (var ch in publishChannels)
    Console.WriteLine($"  -> grp={ch.ChannelGroup} ch={ch.ChannelId,3} type={ch.Type,-22} {ch.MulticastGroup}:{ch.Port}");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
if (opts.DurationSeconds > 0)
    cts.CancelAfter(TimeSpan.FromSeconds(opts.DurationSeconds));

var workers = new List<Worker>(publishChannels.Count);
// Per-group shared per-symbol rs counter. Inc workers Interlocked.Increment to
// produce monotonic rs; snap worker reads (Volatile) and emits a coherent snap
// (header LastRptSeq=rs, Orders_71 = active set derived from rotation formula).
// Without this sharing, the snap can't know what state to encode that matches
// what inc has already published.
var sharedRptSeqs = new Dictionary<int, uint[]>();
foreach (var ch in publishChannels)
{
    if (!sharedRptSeqs.ContainsKey(ch.ChannelGroup))
        sharedRptSeqs[ch.ChannelGroup] = new uint[opts.SymbolsPerGroup];
}
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
    workers.Add(new Worker(ch, opts, mode.Value, sharedRptSeqs[ch.ChannelGroup]));
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
            case "--price-mode" when i + 1 < args.Length:
                opts.PriceMode = Enum.Parse<PriceMode>(args[++i], ignoreCase: true); break;
            case "--price-levels" when i + 1 < args.Length:
                opts.PriceLevels = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--rotation-window" when i + 1 < args.Length:
                opts.RotationWindow = int.Parse(args[++i], CultureInfo.InvariantCulture); break;
            case "--snap-bootstrap-only":
                opts.SnapBootstrapOnly = true; break;
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
    Console.WriteLine("  --price-mode <m>          same|top|spread (default same — best case for conflation)");
    Console.WriteLine("  --price-levels <n>        Distinct price buckets in 'spread' mode (default 64)");
    Console.WriteLine("  --rotation-window <K>     0=grow-only NEW (default). K>0: per-symbol FIFO ring of K live");
    Console.WriteLine("                            orders. After K NEW warm-up, alternates DELETE/NEW so the book");
    Console.WriteLine("                            oscillates in [K-1,K]. Action+oid derived from RptSeq, no state.");
    Console.WriteLine("  --snap-bootstrap-only     Snap workers emit one snapshot per symbol then exit. Required");
    Console.WriteLine("                            with --rotation-window>0 (periodic snaps would clear the book).");
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
    public PriceMode PriceMode { get; set; } = PriceMode.Same;
    public int PriceLevels { get; set; } = 64;
    public int RotationWindow { get; set; } = 0;
    public bool SnapBootstrapOnly { get; set; } = false;
}

internal enum PriceMode
{
    /// <summary>All NEW orders share the same price (best case for conflation —
    /// TOB price never moves, only quantity grows; many adds collapse to a single
    /// TOB update per flush window).</summary>
    Same,
    /// <summary>Each NEW order is at a strictly better price than the prior one
    /// (worst case for conflation — every add becomes the new TOB and forces a
    /// fresh frame, no aggregation possible).</summary>
    Top,
    /// <summary>Per-symbol prices spread uniformly across <c>--price-levels</c>
    /// distinct buckets (realistic mid-range case — TOB shifts intermittently
    /// but most adds go to mid-book levels and conflate).</summary>
    Spread,
}

internal sealed class Worker
{
    private readonly MulticastPublishChannelConfig _config;
    private readonly Options _opts;
    private readonly WorkerMode _mode;
    private readonly uint[] _sharedRptSeqs; // per-group per-symbol monotonic counter (shared across inc + snap workers in same group)
    private byte[] _buffer = null!;
    private int _packetSize;
    private int _perMessageWireLen;
    private int _msgsPerPacket;
    // Rotation mode: pre-built per-slot prototype buffers (framing+sbeHdr+body) for
    // each message type. NEW slot uses Order_MBO_50 (68B); DELETE slot uses
    // DeleteOrder_MBO_51 (48B). When rotation is enabled, _buffer is allocated for
    // the worst case (all NEWs) and the hot loop walks slots with a cumulative
    // cursor, copying the right prototype per slot.
    private bool _rotationEnabled;
    private byte[]? _newSlotProto;
    private byte[]? _delSlotProto;
    private byte[]? _ordersSbeHdrProto;
    private int _newSlotLen;
    private int _delSlotLen;
    private long _packetsSent;
    private long _bytesSent;
    private long _newCount;
    private long _delCount;

    public Thread Thread { get; private set; } = null!;
    public string Name => $"grp{_config.ChannelGroup}-ch{_config.ChannelId}-{_mode}";
    public WorkerMode Mode => _mode;
    public long PacketsSent => Interlocked.Read(ref _packetsSent);
    public long BytesSent => Interlocked.Read(ref _bytesSent);
    public long NewCount => Interlocked.Read(ref _newCount);
    public long DelCount => Interlocked.Read(ref _delCount);

    public Worker(MulticastPublishChannelConfig config, Options opts, WorkerMode mode, uint[] sharedRptSeqs)
    {
        _config = config;
        _opts = opts;
        _mode = mode;
        _sharedRptSeqs = sharedRptSeqs;
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
                _perMessageWireLen = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + WireOffsets.SnapHeaderBlockLength;
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
                    InitSnapBody(_buffer.AsSpan(bodyOff, WireOffsets.SnapHeaderBlockLength));
                    break;
                case WorkerMode.InstrumentDefinition:
                    B3.Umdf.Mbo.Sbe.V16.V6.SecurityDefinition_12Data.WriteHeader(_buffer.AsSpan(mhOff, WireOffsets.SbeMessageHeaderSize));
                    InitSecDefBody(_buffer.AsSpan(bodyOff, WireOffsets.SecDefBodyTotal));
                    break;
            }
        }

        // Coherent snap mode (rotation enabled): allocate buffer large enough for
        // PacketHeader + Header_30 frame + Orders_71 frame with K active entries.
        if (_mode == WorkerMode.Snapshot && _opts.RotationWindow > 0)
        {
            int K = _opts.RotationWindow;
            int headerFrame = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + WireOffsets.SnapHeaderBlockLength;
            int ordersFrame = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize
                              + 8 /*SecurityID*/ + 3 /*GroupSizeEncoding*/ + K * 42 /*entries*/;
            int total = WireOffsets.PacketHeaderSize + headerFrame + ordersFrame;
            if (total > _packetSize)
            {
                var bigger = new byte[total];
                _buffer.AsSpan(0, WireOffsets.PacketHeaderSize).CopyTo(bigger);
                _buffer = bigger;
                _packetSize = total;
            }
            // Pre-write the Orders_71 SBE message header into a prototype slot
            // that the hot loop can copy. The body (SecurityID + group + entries)
            // is written per-send.
            _ordersSbeHdrProto = new byte[WireOffsets.SbeMessageHeaderSize];
            B3.Umdf.Mbo.Sbe.V16.SnapshotFullRefresh_Orders_MBO_71Data.WriteHeader(_ordersSbeHdrProto.AsSpan(0, WireOffsets.SbeMessageHeaderSize));
        }

        // Rotation mode: build per-slot prototypes for both NEW (50) and DELETE (51).
        // The hot loop will choose per-slot which prototype to copy into _buffer.
        if (_mode == WorkerMode.Incremental && _opts.RotationWindow > 0)
        {
            _rotationEnabled = true;
            _newSlotLen = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + WireOffsets.OrderBlockLength;
            _delSlotLen = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + WireOffsets.DeleteOrderBlockLength;

            _newSlotProto = new byte[_newSlotLen];
            var newSpan = _newSlotProto.AsSpan();
            ref var newFraming = ref MemoryMarshal.AsRef<FramingHeader>(newSpan.Slice(0, WireOffsets.FramingHeaderSize));
            newFraming.MessageLength = (ushort)_newSlotLen;
            newFraming.EncodingType = 0;
            B3.Umdf.Mbo.Sbe.V16.V6.Order_MBO_50Data.WriteHeader(newSpan.Slice(WireOffsets.FramingHeaderSize, WireOffsets.SbeMessageHeaderSize));
            InitOrderBody(newSpan.Slice(WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize, WireOffsets.OrderBlockLength));

            _delSlotProto = new byte[_delSlotLen];
            var delSpan = _delSlotProto.AsSpan();
            ref var delFraming = ref MemoryMarshal.AsRef<FramingHeader>(delSpan.Slice(0, WireOffsets.FramingHeaderSize));
            delFraming.MessageLength = (ushort)_delSlotLen;
            delFraming.EncodingType = 0;
            B3.Umdf.Mbo.Sbe.V16.DeleteOrder_MBO_51Data.WriteHeader(delSpan.Slice(WireOffsets.FramingHeaderSize, WireOffsets.SbeMessageHeaderSize));
            InitDeleteBody(delSpan.Slice(WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize, WireOffsets.DeleteOrderBlockLength));

            // Worst-case size: every slot is a NEW.
            int worstCase = WireOffsets.PacketHeaderSize + _newSlotLen * _msgsPerPacket;
            if (worstCase > _packetSize)
            {
                var bigger = new byte[worstCase];
                _buffer.AsSpan(0, WireOffsets.PacketHeaderSize).CopyTo(bigger);
                _buffer = bigger;
                _packetSize = worstCase;
            }
        }
    }

    private static void InitDeleteBody(Span<byte> body)
    {
        // MDEntryType (offset 10) = BID. Other fields (securityExchange/matchEvent
        // at 8/9) are not validated by the consumer's HandleDeleteOrder; leave
        // zero. SecurityID, SecondaryOrderID, RptSeq are mutated per-send.
        body[WireOffsets.DeleteOrderBodyMdEntryTypeOffset] = (byte)MDEntryType.BID;
        long size = 100;
        MemoryMarshal.Write(body.Slice(WireOffsets.DeleteOrderBodyMdEntrySizeOffset, 8), in size);
        // V15+ MDEntryPx (PriceOptional @44, 8 bytes mantissa) — write NULL
        // sentinel (long.MinValue). TransactTime @32 stays 0 (non-null but
        // ignored by HandleDeleteOrder).
        long pxNull = long.MinValue;
        MemoryMarshal.Write(body.Slice(WireOffsets.DeleteOrderBodyMdEntryPxOffset, 8), in pxNull);
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
        // Snap bootstrap: wait briefly so the consumer multicast join + InstrDef
        // arrive before snaps fire. Without this, the consumer may drop pre-join
        // snap pkts and never heal (rotation would never start applying).
        if (_mode == WorkerMode.Snapshot && _opts.SnapBootstrapOnly)
        {
            try { Task.Delay(TimeSpan.FromSeconds(Math.Max(1, _opts.RampSeconds / 2)), ct).Wait(ct); }
            catch (OperationCanceledException) { return; }
        }

        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 4);
        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer, 4 * 1024 * 1024);
        var endpoint = new IPEndPoint(_config.MulticastGroup, _config.Port);

        long securityIdBase = 900_000_000_000L + (_config.ChannelGroup * 1_000_000L);
        int symbolPool = _opts.SymbolsPerGroup;
        // Shared per-group per-symbol monotonic rs counter. Inc workers
        // Interlocked.Increment to claim the next rs; Snap worker reads
        // (Volatile) to know what state to encode in coherent snaps.
        var rptSeqs = _sharedRptSeqs;
        // Per-symbol price state (used only in Incremental mode + non-Same modes).
        // Top: counter that increments → progressively better BID price.
        // Spread: deterministic LCG bucket selector seeded per symbol.
        var topOffsets = (_mode == WorkerMode.Incremental && _opts.PriceMode == PriceMode.Top) ? new int[symbolPool] : Array.Empty<int>();
        var spreadStates = (_mode == WorkerMode.Incremental && _opts.PriceMode == PriceMode.Spread) ? new uint[symbolPool] : Array.Empty<uint>();
        if (_mode == WorkerMode.Incremental && _opts.PriceMode == PriceMode.Spread)
        {
            for (int i = 0; i < spreadStates.Length; i++)
                spreadStates[i] = (uint)(0x9E3779B1u ^ (uint)(_config.ChannelGroup * 31 + _config.ChannelId * 17 + i));
        }
        // 0.0001 tick at PriceOptional 8-decimal scale = 10000 mantissa units.
        const long basePxMantissa = 1000_0000_0000L;
        const long tickMantissa = 10_000L;

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
        int symbolCursor = 0;
        int rotationK = _opts.RotationWindow;
        bool rotation = _rotationEnabled;
        // Variable-length packet path: rotation Inc (NEW/DEL slots differ), or
        // coherent snap (per-symbol Header_30 + Orders_71 with K active entries).
        bool variableLength = rotation || (_mode == WorkerMode.Snapshot && rotationK > 0);
        bool snapBootstrapOnly = _mode == WorkerMode.Snapshot && _opts.SnapBootstrapOnly;
        // Per-symbol snap counter: emit each symbol's snap up to N passes (defense
        // against early loss before consumer's multicast subscription stabilizes).
        const int snapBootstrapPasses = 3;
        int[]? snapPassCount = snapBootstrapOnly ? new int[symbolPool] : null;
        int snappedCount = 0;

        var bufferSpan = _buffer.AsSpan();

        while (!ct.IsCancellationRequested)
        {
            if (snapBootstrapOnly && snappedCount >= symbolPool)
                break; // all symbols snapshotted once — stop emitting to avoid re-clearing books
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

            // Cursor used only in rotation mode (variable per-slot size).
            int writeCursor = WireOffsets.PacketHeaderSize;

            for (int m = 0; m < _msgsPerPacket; m++)
            {
                int symIdx = symbolCursor;
                symbolCursor++;
                if (symbolCursor >= symbolPool) symbolCursor = 0;
                if (snapBootstrapOnly)
                {
                    // Advance past symbols that already received their N bootstrap passes.
                    int sentinel = 0;
                    while (snapPassCount![symIdx] >= snapBootstrapPasses && sentinel++ < symbolPool)
                    {
                        symIdx = symbolCursor;
                        symbolCursor++;
                        if (symbolCursor >= symbolPool) symbolCursor = 0;
                    }
                    if (snapPassCount[symIdx] >= snapBootstrapPasses) goto endLoop;
                }
                long secId = securityIdBase + symIdx;

                if (_mode == WorkerMode.Incremental && rotation)
                {
                    // Atomic claim of next rs from the per-group shared counter
                    // (so Snap worker reading the same array sees a coherent
                    // monotonic step count for all symbols).
                    uint rs = Interlocked.Increment(ref rptSeqs[symIdx]);
                    long step = rs - 1;
                    bool isDelete;
                    long localId;
                    if (step < rotationK)
                    {
                        isDelete = false;
                        localId = step;
                    }
                    else
                    {
                        long off = step - rotationK;
                        if ((off & 1) == 0)
                        {
                            isDelete = true;
                            localId = off >> 1;
                        }
                        else
                        {
                            isDelete = false;
                            localId = rotationK + (off >> 1);
                        }
                    }

                    int slotLen = isDelete ? _delSlotLen : _newSlotLen;
                    var proto = isDelete ? _delSlotProto! : _newSlotProto!;
                    proto.AsSpan().CopyTo(bufferSpan.Slice(writeCursor, slotLen));
                    int bodyOff = writeCursor + WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize;
                    writeCursor += slotLen;

                    long symBase = ((long)symIdx + 1) << 40;
                    long oid = symBase | localId;

                    if (isDelete)
                    {
                        Interlocked.Increment(ref _delCount);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.DeleteOrderBodySecurityIdOffset, 8), in secId);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.DeleteOrderBodySecondaryOrderIdOffset, 8), in oid);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.DeleteOrderBodyRptSeqOffset, 4), in rs);
                    }
                    else
                    {
                        Interlocked.Increment(ref _newCount);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.OrderBodySecurityIdOffset, 8), in secId);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.OrderBodyMdInsertTimestampOffset, 8), in sendingTimeNs);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.OrderBodySecondaryOrderIdOffset, 8), in oid);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + WireOffsets.OrderBodyRptSeqOffset, 4), in rs);
                        bufferSpan[bodyOff + 9] = (byte)MDUpdateAction.NEW;

                        long pxRot = ComputePrice(symIdx, topOffsets, spreadStates, basePxMantissa, tickMantissa);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff + 12, 8), in pxRot);
                    }
                    continue;
                }

                int bodyOffFixed = WireOffsets.PacketHeaderSize + _perMessageWireLen * m
                    + WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize;
                int bodyOff_ = bodyOffFixed;

                switch (_mode)
                {
                    case WorkerMode.Incremental:
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff_ + WireOffsets.OrderBodySecurityIdOffset, 8), in secId);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff_ + WireOffsets.OrderBodyMdInsertTimestampOffset, 8), in sendingTimeNs);
                        uint rs2 = Interlocked.Increment(ref rptSeqs[symIdx]);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff_ + WireOffsets.OrderBodyRptSeqOffset, 4), in rs2);

                        long symBase2 = ((long)symIdx + 1) << 40;
                        long oid2 = symBase2 | (long)(rs2 - 1);
                        bufferSpan[bodyOff_ + 9] = (byte)MDUpdateAction.NEW;
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff_ + WireOffsets.OrderBodySecondaryOrderIdOffset, 8), in oid2);

                        long px = ComputePrice(symIdx, topOffsets, spreadStates, basePxMantissa, tickMantissa);
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff_ + 12, 8), in px);
                        break;
                    case WorkerMode.Snapshot:
                        if (_opts.RotationWindow > 0)
                        {
                            // Coherent snap: Header_30(LastRptSeq=rs) + Orders_71(active set
                            // derived from rotation formula at rs). For Healthy syms the
                            // consumer takes the Skipped path → no book mutation. For Stale
                            // syms (initial bootstrap) this populates the book exactly to
                            // match what inc has emitted up to rs.
                            //
                            // KNOWN RACE: V6 SnapshotFullRefresh_Header_30.LastRptSeq has
                            // null sentinel = 0 (LastRptSeqNullValue). When inc has not yet
                            // incremented (snap reads rs=0), the consumer interprets the
                            // empty-set, null-rpt snap as the IlliquidEmpty path. After
                            // bootstrap, sym occasionally goes Stale and the inc/snap race
                            // window can let MinHealRptSeq advance past subsequent snap
                            // LastRptSeq → snap rejected as too old. Test runs show
                            // healed=8/8 bootstrap then steady-state add advancing slowly
                            // before Stale latches. Mitigations to evaluate:
                            //   (a) Keep snap-pps >> per-sym inc-rate so race window is small.
                            //   (b) Use larger --ramp-seconds so snap heals every sym before
                            //       inc starts mutating; inc then arrives strictly Healthy.
                            //   (c) Per-sym lock on rs read+encode so snap captures a
                            //       consistent (rs, active-set) pair atomic w.r.t. inc.
                            uint snapRs = Volatile.Read(ref rptSeqs[symIdx]);
                            int snapPktLen = WriteCoherentSnapPacket(bufferSpan, secId, symIdx, snapRs, rotationK);
                            // Variable-length send: writeCursor controls send length.
                            // Snap path always emits exactly one symbol's snap per packet.
                            writeCursor = snapPktLen;
                            if (snapBootstrapOnly)
                            {
                                snapPassCount![symIdx]++;
                                if (snapPassCount[symIdx] == snapBootstrapPasses)
                                    snappedCount++;
                            }
                        }
                        else
                        {
                            MemoryMarshal.Write(bufferSpan.Slice(bodyOff_ + WireOffsets.SnapHeaderBodySecurityIdOffset, 8), in secId);
                            // OrdersExpected stays 0; LastRptSeq stays 0 (=null).
                            if (snapBootstrapOnly)
                            {
                                snapPassCount![symIdx]++;
                                if (snapPassCount[symIdx] == snapBootstrapPasses)
                                    snappedCount++;
                            }
                        }
                        break;
                    case WorkerMode.InstrumentDefinition:
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff_ + WireOffsets.SecDefSecurityIdOffset, 8), in secId);
                        // Symbol "SYNT<grp>_<idx>" padded to 20 bytes
                        Span<byte> symSpan = bufferSpan.Slice(bodyOff_ + WireOffsets.SecDefSymbolOffset, 20);
                        symSpan.Clear();
                        WriteAsciiSymbol(symSpan, _config.ChannelGroup, symIdx);
                        long validityTs = 1_700_000_000L + secId;
                        MemoryMarshal.Write(bufferSpan.Slice(bodyOff_ + WireOffsets.SecDefSecurityValidityTimestampOffset, 8), in validityTs);
                        break;
                }
            }

            int sendLen = variableLength ? writeCursor : _packetSize;

            // Skip header-only sends (rotation Snap may produce no payload when
            // all symbols in the round are still pre-inc; see WorkerMode.Snapshot).
            if (variableLength && sendLen <= WireOffsets.PacketHeaderSize)
                continue;

            try
            {
                int sent = socket.SendTo(_buffer, 0, sendLen, SocketFlags.None, endpoint);
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
            continue;
            endLoop: break;
        }
    }

    private long ComputePrice(int symIdx, int[] topOffsets, uint[] spreadStates, long basePxMantissa, long tickMantissa)
    {
        switch (_opts.PriceMode)
        {
            case PriceMode.Top:
                topOffsets[symIdx] = (topOffsets[symIdx] + 1) & 0xFFFFFF;
                return basePxMantissa + (long)topOffsets[symIdx] * tickMantissa;
            case PriceMode.Spread:
                uint state = spreadStates[symIdx] * 1103515245u + 12345u;
                spreadStates[symIdx] = state;
                int bucket = (int)((state >> 8) % (uint)Math.Max(1, _opts.PriceLevels));
                return basePxMantissa + (long)bucket * tickMantissa;
            default:
                return basePxMantissa;
        }
    }

    /// <summary>
    /// Writes a complete UDP packet payload containing PacketHeader + Header_30 +
    /// Orders_71 (with the active orderId set derived from the rotation formula
    /// at <paramref name="rs"/>). Returns the total packet length.
    ///
    /// PacketHeader.SequenceNumber and SendingTime are already set by the caller.
    ///
    /// The Orders_71 group entries reference SecondaryOrderID = (symIdx+1)&lt;&lt;40 | localId,
    /// which exactly matches what Inc workers emit. So when the consumer accepts
    /// this snap (Stale → Healthy), the populated book matches what subsequent
    /// Inc messages reference for DELETEs.
    /// </summary>
    private int WriteCoherentSnapPacket(Span<byte> buf, long secId, int symIdx, uint rs, int K)
    {
        int cursor = WireOffsets.PacketHeaderSize;

        // --- Header_30 frame ---
        ref var hdrFraming = ref MemoryMarshal.AsRef<FramingHeader>(buf.Slice(cursor, WireOffsets.FramingHeaderSize));
        int headerWireLen = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + WireOffsets.SnapHeaderBlockLength;
        hdrFraming.MessageLength = (ushort)headerWireLen;
        hdrFraming.EncodingType = 0;
        cursor += WireOffsets.FramingHeaderSize;

        B3.Umdf.Mbo.Sbe.V16.V6.SnapshotFullRefresh_Header_30Data.WriteHeader(buf.Slice(cursor, WireOffsets.SbeMessageHeaderSize));
        cursor += WireOffsets.SbeMessageHeaderSize;

        // Header_30 body: SecurityID + SecurityExchange("BVMF") + counts + LastRptSeq.
        Span<byte> headerBody = buf.Slice(cursor, WireOffsets.SnapHeaderBlockLength);
        headerBody.Clear();
        MemoryMarshal.Write(headerBody.Slice(WireOffsets.SnapHeaderBodySecurityIdOffset, 8), in secId);
        Encoding.ASCII.GetBytes("BVMF", headerBody.Slice(8, 4));

        // Compute active orderIds and split bid/ask counts.
        // For our publisher all orders are BID (matches inc body InitOrderBody).
        // Allocate scratch on the stack — K is small (validated <= 32 at startup).
        Span<long> activeIds = stackalloc long[Math.Max(1, K)];
        int activeCount = ComputeActiveOrders(rs, K, activeIds);

        uint totBids = (uint)activeCount;
        uint totOffers = 0u;
        MemoryMarshal.Write(headerBody.Slice(WireOffsets.SnapHeaderBodyTotNumBidsOffset, 4), in totBids);
        MemoryMarshal.Write(headerBody.Slice(WireOffsets.SnapHeaderBodyTotNumOffersOffset, 4), in totOffers);
        // LastRptSeq = rs (non-null, non-zero → HasRptSeq=true → CompleteNormalSnapshot path on heal).
        MemoryMarshal.Write(headerBody.Slice(WireOffsets.SnapHeaderBodyLastRptSeqOffset, 4), in rs);
        cursor += WireOffsets.SnapHeaderBlockLength;

        // --- Orders_71 frame ---
        // Body: SecurityID(8) + GroupSizeEncoding(3) + N entries × 42.
        // Block_length=8 per generated WriteHeader (only SecurityID is "block").
        // The struct has SecurityExchange @ FieldOffset(8), but since
        // block_length=8, the parser starts groups at offset 8 — so the GSE
        // fully overlaps the SecurityExchange slot. We don't write SecExch.
        int ordersBodyLen = 8 + 3 + activeCount * 42;
        int ordersWireLen = WireOffsets.FramingHeaderSize + WireOffsets.SbeMessageHeaderSize + ordersBodyLen;
        ref var ordersFraming = ref MemoryMarshal.AsRef<FramingHeader>(buf.Slice(cursor, WireOffsets.FramingHeaderSize));
        ordersFraming.MessageLength = (ushort)ordersWireLen;
        ordersFraming.EncodingType = 0;
        cursor += WireOffsets.FramingHeaderSize;

        // SBE message header for template 71 (block_length=8 per generated WriteHeader).
        _ordersSbeHdrProto!.AsSpan().CopyTo(buf.Slice(cursor, WireOffsets.SbeMessageHeaderSize));
        cursor += WireOffsets.SbeMessageHeaderSize;

        MemoryMarshal.Write(buf.Slice(cursor + 0, 8), in secId);
        ushort entryBlockLength = 42;
        byte numInGroup = (byte)activeCount;
        MemoryMarshal.Write(buf.Slice(cursor + 8, 2), in entryBlockLength);
        buf[cursor + 10] = numInGroup;
        int entriesOff = cursor + 11;
        for (int i = 0; i < activeCount; i++)
        {
            Span<byte> e = buf.Slice(entriesOff + i * 42, 42);
            e.Clear();
            // MDEntryPx (PriceOptional, mantissa @ 0, 8 bytes)
            long pxMantissa = 1000_0000_0000L;
            MemoryMarshal.Write(e.Slice(0, 8), in pxMantissa);
            // MDEntrySize @ 8 (long, 8 bytes)
            long size = 100;
            MemoryMarshal.Write(e.Slice(8, 8), in size);
            // EnteringFirm @ 20 stays 0 (NULL sentinel)
            // MDInsertTimestamp @ 24 (8 bytes ulong) — non-zero so consumer doesn't treat as null
            ulong tsNs = 1_700_000_000_000_000_000UL;
            MemoryMarshal.Write(e.Slice(24, 8), in tsNs);
            // SecondaryOrderID @ 32 (8 bytes)
            long oid = (((long)symIdx + 1) << 40) | activeIds[i];
            MemoryMarshal.Write(e.Slice(32, 8), in oid);
            // MDEntryType @ 40 = BID
            e[40] = (byte)MDEntryType.BID;
            // MatchEventIndicator @ 41 stays 0
        }
        cursor += ordersBodyLen;

        return cursor;
    }

    /// <summary>
    /// Enumerates the active orderId set (localIds within a symbol) at the
    /// rotation step indicated by <paramref name="rs"/>. The formula matches the
    /// inc rotation hot loop: warmup [0..K-1] adds, then alternating DEL(off/2)
    /// / NEW(K + (off-1)/2) for off = step-K starting at step=K.
    /// Steady state has exactly K active orders (DEL/NEW pair per rs).
    /// </summary>
    internal static int ComputeActiveOrders(uint rs, int K, Span<long> outLocalIds)
    {
        long step = (long)rs;
        if (step <= 0) return 0;
        if (step <= K)
        {
            for (int i = 0; i < step; i++) outLocalIds[i] = i;
            return (int)step;
        }
        long offMax = step - K - 1; // last completed off (off=0 at step=K+1)
        long delsDone;
        long lastNewLocalId;
        if ((offMax & 1) == 0)
        {
            delsDone = (offMax / 2) + 1;
            lastNewLocalId = K + (offMax / 2) - 1;
        }
        else
        {
            delsDone = ((offMax - 1) / 2) + 1;
            lastNewLocalId = K + ((offMax - 1) / 2);
        }
        int idx = 0;
        // Warmup remaining: localIds [delsDone, K-1] (empty once delsDone >= K)
        for (long lid = delsDone; lid < K; lid++) outLocalIds[idx++] = lid;
        // Phase-B NEWs not yet DEL'd: [max(K, delsDone), lastNewLocalId]
        long firstAlivePhaseB = Math.Max(K, delsDone);
        for (long lid = firstAlivePhaseB; lid <= lastNewLocalId; lid++) outLocalIds[idx++] = lid;
        return idx;
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
        long packets = 0, bytes = 0, news = 0, dels = 0;
        foreach (var w in _workers) { packets += w.PacketsSent; bytes += w.BytesSent; news += w.NewCount; dels += w.DelCount; }
        Console.WriteLine();
        Console.WriteLine($"=== Final ({_sw.Elapsed:hh\\:mm\\:ss}) ===");
        Console.WriteLine($"  total packets: {packets:N0}");
        Console.WriteLine($"  total bytes  : {bytes:N0}");
        Console.WriteLine($"  avg pps      : {packets / Math.Max(_sw.Elapsed.TotalSeconds, 1e-6):N0}");
        Console.WriteLine($"  rotation     : new={news:N0} del={dels:N0}");
        Console.WriteLine($"  per-channel:");
        foreach (var w in _workers)
            Console.WriteLine($"    {w.Name,-36} {w.PacketsSent,12:N0} pkts  {w.BytesSent,14:N0} bytes");
    }
}
