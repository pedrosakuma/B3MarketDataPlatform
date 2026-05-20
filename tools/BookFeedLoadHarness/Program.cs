using System.Diagnostics.Tracing;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using B3.MarketData.WebSocketClient;

namespace B3.MarketData.Tools.BookFeedLoadHarness;

/// <summary>
/// Client-side load harness that drives <see cref="BookFeed"/> against a live
/// server (typically the <c>B3.Umdf.ConsoleApp</c> replaying a PCAP). Designed
/// to be the target process for <c>dotnet-diagnostics-mcp</c> when measuring
/// the SDK's materialized-book layer.
///
/// The harness publishes per-second counters via the <c>B3.BookFeedHarness</c>
/// EventSource so <c>dotnet-counters monitor</c> / EventPipe consumers can see
/// throughput without parsing console output. Connect to the server, discover
/// the top symbols via <c>GET /symbols</c>, subscribe with
/// <see cref="SubscribeFlags.Book"/>, then count every <c>BookFeed.Changed</c>
/// invocation.
/// </summary>
internal static class Program
{
    private static long s_changedCount;
    private static long s_snapshotCount;
    private static long s_uniqueSymbols;
    private static long s_topReadCount;

    public static async Task<int> Main(string[] args)
    {
        var opts = HarnessOptions.Parse(args);
        Console.WriteLine($"[harness] endpoint={opts.Endpoint} http={opts.HttpBase} symbols={(opts.Symbols.Length == 0 ? $"discover top {opts.DiscoverTop}" : string.Join(",", opts.Symbols))} duration={opts.DurationSeconds}s topReadHz={opts.TopReadHz} pid={Environment.ProcessId}");

        var symbols = opts.Symbols;
        if (symbols.Length == 0)
        {
            symbols = await DiscoverSymbolsAsync(opts.HttpBase, opts.DiscoverTop).ConfigureAwait(false);
            Console.WriteLine($"[harness] discovered {symbols.Length} symbols: {string.Join(",", symbols)}");
        }

        if (symbols.Length == 0)
        {
            Console.Error.WriteLine("[harness] no symbols to subscribe — aborting");
            return 2;
        }

        await using var client = new MarketDataClient(new MarketDataClientOptions
        {
            Endpoint = opts.Endpoint,
            EventChannelCapacity = 16_384,
        });

        var bookFeed = client.CreateBookFeed();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        bookFeed.Changed += sym =>
        {
            Interlocked.Increment(ref s_changedCount);
            lock (seen)
            {
                if (seen.Add(sym))
                    Interlocked.Exchange(ref s_uniqueSymbols, seen.Count);
            }
        };
        client.BookSnapshot += _ => Interlocked.Increment(ref s_snapshotCount);

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        await client.ConnectAsync(cts.Token).ConfigureAwait(false);
        Console.WriteLine("[harness] connected");

        foreach (var symbol in symbols)
        {
            try
            {
                await client.SubscribeAsync(symbol, SubscribeFlags.Book, cts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[harness] subscribe {symbol} failed: {ex.Message}");
            }
        }

        Console.WriteLine($"[harness] subscribed {symbols.Length} symbols — warming up {opts.WarmupSeconds}s");
        try { await Task.Delay(TimeSpan.FromSeconds(opts.WarmupSeconds), cts.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) { return 0; }

        var topReadTask = opts.TopReadHz > 0
            ? Task.Run(() => HotPathLoop(bookFeed, symbols, opts.TopReadHz, cts.Token))
            : Task.CompletedTask;

        var reporter = Task.Run(() => ReportLoop(cts.Token));

        try { await Task.Delay(TimeSpan.FromSeconds(opts.DurationSeconds), cts.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) { }

        cts.Cancel();
        try { await topReadTask.ConfigureAwait(false); } catch { }
        try { await reporter.ConfigureAwait(false); } catch { }

        Console.WriteLine($"[harness] done — changed={s_changedCount} snapshots={s_snapshotCount} uniqueSymbols={s_uniqueSymbols} topReads={s_topReadCount}");
        return 0;
    }

    private static async Task HotPathLoop(BookFeed feed, string[] symbols, int hz, CancellationToken ct)
    {
        var delayMs = Math.Max(1, 1000 / hz);
        var i = 0;
        while (!ct.IsCancellationRequested)
        {
            var symbol = symbols[i++ % symbols.Length];
            if (feed.TryGetTop(symbol, out _))
                Interlocked.Increment(ref s_topReadCount);
            try { await Task.Delay(delayMs, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }
    }

    private static async Task ReportLoop(CancellationToken ct)
    {
        long prevChanged = 0;
        long prevSnapshots = 0;
        long prevTopReads = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(1000, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }

            var changed = Interlocked.Read(ref s_changedCount);
            var snaps = Interlocked.Read(ref s_snapshotCount);
            var tops = Interlocked.Read(ref s_topReadCount);
            var uniq = Interlocked.Read(ref s_uniqueSymbols);

            HarnessEventSource.Log.Tick(changed - prevChanged, snaps - prevSnapshots, tops - prevTopReads, uniq);

            Console.WriteLine($"[harness {sw.Elapsed:mm\\:ss}] changed/s={changed - prevChanged} snaps/s={snaps - prevSnapshots} topReads/s={tops - prevTopReads} uniqSymbols={uniq}");
            prevChanged = changed;
            prevSnapshots = snaps;
            prevTopReads = tops;
        }
    }

    private static async Task<string[]> DiscoverSymbolsAsync(Uri httpBase, int top)
    {
        using var http = new HttpClient { BaseAddress = httpBase, Timeout = TimeSpan.FromSeconds(5) };
        for (var attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                var resp = await http.GetFromJsonAsync($"/symbols?limit={top}", SymbolsContext.Default.SymbolsResponse).ConfigureAwait(false);
                if (resp?.Symbols is { Length: > 0 } sy) return sy;
            }
            catch { /* server still warming */ }
            await Task.Delay(500).ConfigureAwait(false);
        }
        return Array.Empty<string>();
    }
}

internal sealed record HarnessOptions(
    Uri Endpoint,
    Uri HttpBase,
    string[] Symbols,
    int DiscoverTop,
    int DurationSeconds,
    int WarmupSeconds,
    int TopReadHz)
{
    public static HarnessOptions Parse(string[] args)
    {
        var endpoint = new Uri("ws://localhost:8080/ws");
        var httpBase = new Uri("http://localhost:8080");
        string[] symbols = Array.Empty<string>();
        var top = 25;
        var duration = 30;
        var warmup = 3;
        var topHz = 0;

        for (var i = 0; i < args.Length; i++)
        {
            var a = args[i];
            string Next() => ++i < args.Length ? args[i] : throw new ArgumentException($"missing value for {a}");
            switch (a)
            {
                case "--endpoint": endpoint = new Uri(Next()); break;
                case "--http": httpBase = new Uri(Next()); break;
                case "--symbols": symbols = Next().Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries); break;
                case "--top": top = int.Parse(Next()); break;
                case "--duration": duration = int.Parse(Next()); break;
                case "--warmup": warmup = int.Parse(Next()); break;
                case "--top-read-hz": topHz = int.Parse(Next()); break;
                case "-h" or "--help":
                    Console.WriteLine("BookFeedLoadHarness — drives BookFeed against a live B3 server\n" +
                        "  --endpoint <ws-uri>     default ws://localhost:8080/ws\n" +
                        "  --http <http-uri>       default http://localhost:8080 (for /symbols discovery)\n" +
                        "  --symbols A,B,C         explicit list; if omitted, discover via GET /symbols\n" +
                        "  --top N                 discovery limit (default 25)\n" +
                        "  --duration N            measurement window in seconds (default 30)\n" +
                        "  --warmup N              warmup before counters start (default 3)\n" +
                        "  --top-read-hz N         optional hot-path TryGetTop reads per second (default 0)\n");
                    Environment.Exit(0); break;
                default: throw new ArgumentException($"unknown arg {a}");
            }
        }
        return new HarnessOptions(endpoint, httpBase, symbols, top, duration, warmup, topHz);
    }
}

internal sealed class SymbolsResponse
{
    public int Count { get; set; }
    public int Matched { get; set; }
    public string[] Symbols { get; set; } = Array.Empty<string>();
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(SymbolsResponse))]
internal sealed partial class SymbolsContext : JsonSerializerContext { }

[EventSource(Name = "B3.BookFeedHarness")]
internal sealed class HarnessEventSource : EventSource
{
    public static readonly HarnessEventSource Log = new();

    [Event(1, Level = EventLevel.Informational)]
    public void Tick(long changedPerSec, long snapshotsPerSec, long topReadsPerSec, long uniqueSymbols)
        => WriteEvent(1, changedPerSec, snapshotsPerSec, topReadsPerSec, uniqueSymbols);
}
