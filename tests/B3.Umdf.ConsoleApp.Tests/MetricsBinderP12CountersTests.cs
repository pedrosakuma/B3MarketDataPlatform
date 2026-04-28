using System.Diagnostics.Metrics;
using B3.Umdf.Book;
using B3.Umdf.ConsoleApp;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.ConsoleApp.Tests;

/// <summary>
/// Pins that newly-added P11/P12 counters are registered as observable
/// instruments under the <c>B3.Umdf.Consumer</c> meter and that they
/// emit the underlying counter value verbatim. Uses MeterListener to
/// capture a single record-cycle.
/// </summary>
public class MetricsBinderP12CountersTests
{
    [Fact]
    public void NewCounters_AreRegistered_AndEmitUnderlyingValues()
    {
        // Arrange: minimal MarketDataManager + BookManager whose counters
        // we can mutate via reflection (or by exercising the public API).
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var stats = new Stats();
        var symbolReg = new SymbolRegistry();
        var staleBuffer = new StaleMboBuffer(NullLogger.Instance);
        var mdm = new MarketDataManager(stateRegistry: registry);
        var bm = new BookManager(stats, stateRegistry: registry, staleBuffer: staleBuffer);

        // Drive the new counters via reflection on the private fields —
        // public API would require synthesising SBE traffic which is
        // already covered by the unit tests in B3.Umdf.Book.Tests.
        SetField(mdm, "_secDefTimestampRegressionCount", 7L);
        SetField(mdm, "_instrumentIdentityChangedCount", 3L);
        SetField(bm, "_instrumentsReplaced", 5L);

        // Register all metrics. We deliberately scope the meter so the
        // test does not pick up any process-wide pre-existing instrument.
        MetricsBinder.Register(
            stats,
            new[] { bm },
            new[] { mdm },
            new[] { 1 },
            multiFeed: null,
            singleFeed: null,
            multicastMerger: null,
            subscriptionManager: null,
            groupHandlers: null,
            symbolRegistry: symbolReg);

        // Act: collect one snapshot via MeterListener.
        var captured = new Dictionary<string, long>(StringComparer.Ordinal);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "B3.Umdf.Consumer")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            // last write wins on duplicate names — fine for this single-group test.
            captured[instrument.Name] = value;
        });
        listener.Start();
        listener.RecordObservableInstruments();

        // Assert: each new instrument is present with the expected value.
        Assert.True(captured.TryGetValue("b3.umdf.instruments.security_definitions_timestamp_regressed", out var tsReg));
        Assert.Equal(7L, tsReg);
        Assert.True(captured.TryGetValue("b3.umdf.instruments.identity_changed", out var idChg));
        Assert.Equal(3L, idChg);
        Assert.True(captured.TryGetValue("b3.umdf.book.instruments_replaced", out var replaced));
        Assert.Equal(5L, replaced);

        // Sanity: the snapshot pipeline counters added in this phase are
        // also wired (value can be 0 — we assert presence only).
        Assert.True(captured.ContainsKey("b3.umdf.persymbol.snapshots_rejected_stale_version"));
        Assert.True(captured.ContainsKey("b3.umdf.persymbol.snapshots_abandoned"));
        Assert.True(captured.ContainsKey("b3.umdf.persymbol.snapshots_aborted_by_epoch"));
    }

    private static void SetField(object target, string name, long value)
    {
        var f = target.GetType().GetField(name,
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field {name} not found on {target.GetType().Name}");
        f.SetValue(target, value);
    }
}
