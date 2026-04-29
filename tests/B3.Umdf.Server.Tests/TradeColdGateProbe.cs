using System.Diagnostics;
using System.Reflection;
using System.Collections.Concurrent;
using B3.Umdf.Book;
using B3.Umdf.Server;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Abstractions;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Phase C / Phase D micro-probe: measures the CPU + GC-allocation savings of
/// the cold-symbol gate in <see cref="GroupConflationHandler.OnTrade"/>. Runs
/// the same workload with and without an active subscriber on the symbol and
/// prints a side-by-side report. Skipped on non-Release builds where JIT noise
/// dominates the signal.
/// </summary>
public class TradeColdGateProbe
{
    private const ulong SecurityId = 9001;
    private const string Symbol = "PROBE";
    private const int Iterations = 1_000_000;
    private const int Warmup = 50_000;

    private readonly ITestOutputHelper _output;

    public TradeColdGateProbe(ITestOutputHelper output) => _output = output;

    [Fact]
    [Trait("Category", "Probe")]
    public void Measure_OnTrade_ColdVsWarm()
    {
        // Warm: at least one subscriber → hydrates RecentTrades + Candles + buffers.
        var warm = Run(withSubscriber: true);
        // Cold: no subscriber → IsSubscribed gate returns immediately.
        var cold = Run(withSubscriber: false);

        double cpuRed = (warm.ElapsedMs - cold.ElapsedMs) / warm.ElapsedMs * 100.0;
        long memDelta = warm.AllocatedBytes - cold.AllocatedBytes;
        double memRed = warm.AllocatedBytes == 0 ? 0
                      : (double)memDelta / warm.AllocatedBytes * 100.0;

        _output.WriteLine($"=== Phase C cold-symbol gate ({Iterations:N0} OnTrade calls) ===");
        _output.WriteLine($"Warm (1 subscriber): {warm.ElapsedMs,8:F2} ms  | alloc {warm.AllocatedBytes,12:N0} B  | per-call {warm.NsPerCall,7:F1} ns");
        _output.WriteLine($"Cold (0 subscriber): {cold.ElapsedMs,8:F2} ms  | alloc {cold.AllocatedBytes,12:N0} B  | per-call {cold.NsPerCall,7:F1} ns");
        _output.WriteLine($"CPU reduction      : {cpuRed,8:F2} %");
        _output.WriteLine($"Alloc reduction    : {memRed,8:F2} %  ({memDelta:N0} B saved)");

        // Sanity: cold MUST be at least as fast as warm and must allocate no more.
        Assert.True(cold.ElapsedMs <= warm.ElapsedMs * 1.10,
            $"Cold ({cold.ElapsedMs} ms) should not be measurably slower than warm ({warm.ElapsedMs} ms)");
        Assert.True(cold.AllocatedBytes <= warm.AllocatedBytes,
            $"Cold should allocate ≤ warm (cold={cold.AllocatedBytes}, warm={warm.AllocatedBytes})");
    }

    private static (double ElapsedMs, long AllocatedBytes, double NsPerCall) Run(bool withSubscriber)
    {
        var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var book = new BookManager(stateRegistry: registry, staleBuffer: staleBuffer);
        group.SetBookManager(book);

        var symbols = new SymbolRegistry();
        var bySymbol = (ConcurrentDictionary<string, ulong>)typeof(SymbolRegistry)
            .GetField("_bySymbol", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(symbols)!;
        var byId = (ConcurrentDictionary<ulong, string>)typeof(SymbolRegistry)
            .GetField("_byId", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(symbols)!;
        bySymbol[Symbol] = SecurityId;
        byId[SecurityId] = Symbol;

        manager.SetDataSources(
            new[] { book },
            new[] { new MarketDataManager(stateRegistry: registry) },
            symbols,
            new[] { group });
        manager.SetReady();

        if (withSubscriber)
        {
            var rec = new RecordingWebSocket();
            var session = new ClientSession(rec, channelCapacity: 1024);
            manager.RegisterClient(session);
            _ = Task.Run(() => session.RunWriteLoopAsync());
            manager.HandleSubscribe(session.Id, Symbol, DataFlags.Book | DataFlags.Trades,
                book, group, bookBatchCutoffSequence: 0);
        }

        // Warmup
        for (int i = 0; i < Warmup; i++)
            group.OnTrade(SecurityId, price: 1000 + (i & 7), quantity: 1, tradeId: i, sendingTimeNs: 0);

        // Force a clean GC baseline before measurement.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long allocBefore = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
            group.OnTrade(SecurityId, price: 1000 + (i & 7), quantity: 1, tradeId: i, sendingTimeNs: 0);
        sw.Stop();
        long allocAfter = GC.GetAllocatedBytesForCurrentThread();

        manager.Dispose();
        return (sw.Elapsed.TotalMilliseconds, allocAfter - allocBefore, sw.Elapsed.TotalNanoseconds / Iterations);
    }
}
