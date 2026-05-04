using System;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Fuzz.Tests;

/// <summary>
/// Property-based exploration of <see cref="SymbolStateRegistry"/>.
///
/// We generate random sequences of well-typed operations
/// (<see cref="Op"/>) drawn from a small alphabet — Observe, HealFromSnapshot,
/// HealFromIlliquidEmptySnapshot, BumpMinHeal, ResetEpoch, ResetSymbolEpoch,
/// MarkAllStale, EnsureRegistered, TickClock — apply them one by one and
/// re-check the registry's invariants after every step.
///
/// Invariants checked:
///   I1. No exception escapes (the registry is supposed to be total).
///   I2. KnownSymbolCount &gt;= StaleSymbolCount &gt;= 0 — gauges are
///       non-negative and consistent.
///   I3. Counters (LaggingSnapshotCount, StaleAuthoritativeResetCount, ...)
///       are monotonically non-decreasing.
///   I4. For every (secId, kind) ever Observe'd: <c>GetState</c> returns a
///       defined enum value (no garbage cast).
///   I5. <c>IsAnyStale(secId)</c> is consistent with at least one kind being
///       Stale among the Mbo bucket (the only bucket that can flip
///       <c>StaleKindMask</c> via gap detection in the current model).
///   I6. The aggregate snapshot's <c>TotalStaleSymbols</c> equals
///       <c>StaleSymbolCount</c>.
///
/// On failure we dump the seed + the full command trace, so the failure
/// reproduces by replaying the trace deterministically.
/// </summary>
public class SymbolStateRegistryPropertyTests
{
    private const int Iterations = 200;
    private const int MaxCommandsPerRun = 60;
    private const int SymbolDomain = 4;   // small domain → high collision density
    private const int RptSeqDomain = 32;  // small domain → exercise gap/duplicate paths

    private sealed class FakeClock : IClock
    {
        private long _ticks = 1_000_000;
        public long NowTicks => _ticks;
        public void Advance(long ms) => _ticks += ms;
    }

    private sealed class Model
    {
        public SymbolStateRegistry Registry = null!;
        public FakeClock Clock = null!;
        public long PrevLagging;
        public long PrevAuthReset;
        public int PeakKnown;
    }

    private abstract record Op
    {
        public sealed record Observe(ulong SecId, SymbolGapKind Kind, uint RptSeq) : Op
        {
            public override string ToString() => $"Observe({SecId},{Kind},{RptSeq})";
        }
        public sealed record HealSnap(ulong SecId, SymbolGapKind Kind, uint RptSeq) : Op
        {
            public override string ToString() => $"HealSnap({SecId},{Kind},{RptSeq})";
        }
        public sealed record HealEmpty(ulong SecId, SymbolGapKind Kind) : Op
        {
            public override string ToString() => $"HealEmpty({SecId},{Kind})";
        }
        public sealed record BumpMin(ulong SecId, SymbolGapKind Kind, uint NewMin) : Op
        {
            public override string ToString() => $"BumpMin({SecId},{Kind},{NewMin})";
        }
        public sealed record ResetEpoch_(string Reason) : Op
        {
            public override string ToString() => $"ResetEpoch({Reason})";
        }
        public sealed record ResetSymbol(ulong SecId, SymbolGapKind Kind) : Op
        {
            public override string ToString() => $"ResetSymbol({SecId},{Kind})";
        }
        public sealed record MarkStale(string Reason) : Op
        {
            public override string ToString() => $"MarkStale({Reason})";
        }
        public sealed record Register(ulong SecId) : Op
        {
            public override string ToString() => $"Register({SecId})";
        }
        public sealed record Tick(int Ms) : Op
        {
            public override string ToString() => $"Tick({Ms}ms)";
        }
    }

    private static readonly SymbolGapKind[] AllKinds = (SymbolGapKind[])Enum.GetValues(typeof(SymbolGapKind));

    private static Op MakeRandomCommand(Random rng)
    {
        var sec = (ulong)rng.Next(1, SymbolDomain + 1);
        var kind = AllKinds[rng.Next(AllKinds.Length)];
        var rpt = (uint)rng.Next(0, RptSeqDomain + 1);
        return rng.Next(0, 100) switch
        {
            < 50 => new Op.Observe(sec, kind, rpt),               // dominant: Observe
            < 70 => new Op.HealSnap(sec, kind, rpt),
            < 75 => new Op.HealEmpty(sec, kind),
            < 80 => new Op.BumpMin(sec, kind, rpt),
            < 85 => new Op.Tick(rng.Next(0, 5_000)),
            < 90 => new Op.Register(sec),
            < 95 => new Op.ResetSymbol(sec, kind),
            < 98 => new Op.MarkStale("fuzz"),
            _    => new Op.ResetEpoch_("fuzz"),
        };
    }

    [Fact]
    public void Registry_RandomCommandSequences_PreserveInvariants()
    {
        PropertyRunner.ForAllCommandSequences<Model, Op>(
            iterations: Iterations,
            maxCommandsPerRun: MaxCommandsPerRun,
            modelFactory: () =>
            {
                var clock = new FakeClock();
                var reg = new SymbolStateRegistry(NullLogger.Instance, clock)
                {
                    StaleEscapeTimeoutMs = 1_000,
                };
                return new Model { Registry = reg, Clock = clock };
            },
            commandFactory: MakeRandomCommand,
            apply: ApplyCommand,
            invariant: CheckInvariants);
    }

    private static void ApplyCommand(Model m, Op op)
    {
        switch (op)
        {
            case Op.Observe o:
                _ = m.Registry.Observe(o.SecId, o.Kind, o.RptSeq);
                break;
            case Op.HealSnap o:
                _ = m.Registry.HealFromSnapshot(o.SecId, o.Kind, o.RptSeq);
                break;
            case Op.HealEmpty o:
                _ = m.Registry.HealFromIlliquidEmptySnapshot(o.SecId, o.Kind);
                break;
            case Op.BumpMin o:
                _ = m.Registry.BumpMinHeal(o.SecId, o.Kind, o.NewMin);
                break;
            case Op.ResetEpoch_ o:
                m.Registry.ResetEpoch(o.Reason);
                break;
            case Op.ResetSymbol o:
                m.Registry.ResetSymbolEpoch(o.SecId, o.Kind);
                break;
            case Op.MarkStale o:
                m.Registry.MarkAllStale(o.Reason);
                break;
            case Op.Register o:
                m.Registry.EnsureRegistered(o.SecId);
                break;
            case Op.Tick o:
                m.Clock.Advance(o.Ms);
                break;
            default:
                throw new InvalidOperationException($"Unknown op: {op.GetType().Name}");
        }
    }

    private static void CheckInvariants(Model m)
    {
        var reg = m.Registry;

        // I2: gauges non-negative and Stale ⊆ Known.
        Assert.True(reg.KnownSymbolCount >= 0, $"KnownSymbolCount went negative: {reg.KnownSymbolCount}");
        Assert.True(reg.StaleSymbolCount >= 0, $"StaleSymbolCount went negative: {reg.StaleSymbolCount}");
        Assert.True(reg.StaleSymbolCount <= reg.KnownSymbolCount,
            $"StaleSymbolCount ({reg.StaleSymbolCount}) > KnownSymbolCount ({reg.KnownSymbolCount})");

        // KnownSymbolCount must never decrease during a run (registry never
        // un-registers symbols; ResetEpoch clears state but keeps entries).
        Assert.True(reg.KnownSymbolCount >= m.PeakKnown,
            $"KnownSymbolCount decreased: was peak {m.PeakKnown}, now {reg.KnownSymbolCount}");
        m.PeakKnown = reg.KnownSymbolCount;

        // I3: monotonically non-decreasing counters.
        Assert.True(reg.LaggingSnapshotCount >= m.PrevLagging,
            $"LaggingSnapshotCount decreased: {m.PrevLagging} → {reg.LaggingSnapshotCount}");
        Assert.True(reg.StaleAuthoritativeResetCount >= m.PrevAuthReset,
            $"StaleAuthoritativeResetCount decreased: {m.PrevAuthReset} → {reg.StaleAuthoritativeResetCount}");
        m.PrevLagging = reg.LaggingSnapshotCount;
        m.PrevAuthReset = reg.StaleAuthoritativeResetCount;

        // I4: GetState returns a defined enum value for every (sec, kind) in
        // our exploration domain.
        for (ulong sec = 1; sec <= SymbolDomain; sec++)
        {
            foreach (var kind in AllKinds)
            {
                var state = reg.GetState(sec, kind);
                Assert.True(Enum.IsDefined(typeof(SymbolState), state),
                    $"GetState({sec},{kind}) returned undefined enum value: {(byte)state}");
            }

            // I5: IsAnyStale must be true iff at least one kind is Stale for
            // that symbol (using GetState as the model).
            bool anyStaleByModel = false;
            foreach (var kind in AllKinds)
            {
                if (reg.GetState(sec, kind) == SymbolState.Stale)
                {
                    anyStaleByModel = true;
                    break;
                }
            }
            Assert.Equal(anyStaleByModel, reg.IsAnyStale(sec));
        }

        // I6: aggregate snapshot agrees with the StaleSymbolCount gauge.
        var agg = reg.GetAggregateSnapshot();
        Assert.Equal(reg.StaleSymbolCount, agg.TotalStaleSymbols);
        Assert.Equal(reg.KnownSymbolCount, agg.TotalSymbols);
        Assert.NotNull(agg.StaleByKind);
        Assert.Equal(AllKinds.Length, agg.StaleByKind.Length);
        foreach (var k in AllKinds)
        {
            Assert.True(agg.StaleOf(k) >= 0, $"StaleOf({k}) went negative");
            Assert.True(agg.StaleOf(k) <= agg.TotalStaleSymbols,
                $"StaleOf({k})={agg.StaleOf(k)} exceeds total {agg.TotalStaleSymbols}");
        }
    }
}
