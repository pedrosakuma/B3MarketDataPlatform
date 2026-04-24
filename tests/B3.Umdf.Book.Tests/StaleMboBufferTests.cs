using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class StaleMboBufferTests
{
    private static StaleMboBuffer NewBuffer(int perSymbolCap = 1024, long globalByteCap = 64L * 1024 * 1024, int hotPerSymbolCap = 65536)
        => new(NullLogger.Instance, perSymbolCap, globalByteCap, hotPerSymbolCap);

    [Fact]
    public void Enqueue_StoresMessageBody()
    {
        var buf = NewBuffer();
        var body = new byte[] { 1, 2, 3, 4 };
        Assert.True(buf.Enqueue(securityId: 100, templateId: 50, rptSeq: 5, sendingTimeNs: 1234, body));
        Assert.Equal(1, buf.EnqueuedCount);
        Assert.Equal(4, buf.TotalBytes);
        Assert.Equal(1, buf.DepthOf(100));
    }

    [Fact]
    public void Drain_AppliesInRptSeqOrder_RegardlessOfArrivalOrder()
    {
        var buf = NewBuffer();
        // Arrival order shuffled (simulating A/B reorder).
        buf.Enqueue(1, 50, rptSeq: 12, 0, new byte[] { 12 });
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 });
        buf.Enqueue(1, 50, rptSeq: 11, 0, new byte[] { 11 });

        var applied = new List<uint>();
        var n = buf.Drain(1, drainFrom: 10, drainTo: 12, m => applied.Add(m.RptSeq));

        Assert.Equal(3, n);
        Assert.Equal(new uint[] { 10, 11, 12 }, applied);
    }

    [Fact]
    public void Drain_DropsBelowWindow_KeepsAboveWindow()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, rptSeq: 5, 0, new byte[] { 5 });   // below
        buf.Enqueue(1, 50, rptSeq: 10, 0, new byte[] { 10 }); // in
        buf.Enqueue(1, 50, rptSeq: 15, 0, new byte[] { 15 }); // above

        var applied = new List<uint>();
        var n = buf.Drain(1, drainFrom: 10, drainTo: 12, m => applied.Add(m.RptSeq));

        Assert.Equal(1, n);
        Assert.Equal(new uint[] { 10 }, applied);
        Assert.Equal(1, buf.DepthOf(1)); // 15 retained for future drain
    }

    [Fact]
    public void Drain_EmptyWindow_NoOp()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 10, 0, new byte[] { 10 });

        // drainTo < drainFrom signals nothing to drain (registry returns this when no buffered messages).
        var n = buf.Drain(1, drainFrom: 10, drainTo: 9, _ => Assert.Fail("should not apply"));
        Assert.Equal(0, n);
    }

    [Fact]
    public void Drain_FuturePreservedAcrossCalls()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 20, 0, new byte[] { 20 });
        buf.Drain(1, 10, 15, _ => { });
        // Now heal again with broader window.
        var applied = new List<uint>();
        buf.Drain(1, 16, 25, m => applied.Add(m.RptSeq));
        Assert.Equal(new uint[] { 20 }, applied);
    }

    [Fact]
    public void FirstOverflow_PromotesToHotCap_NoEviction()
    {
        // Two-tier buffer: the first overflow at the normal cap must NOT evict —
        // the symbol is promoted to the hot cap and the in-flight message is
        // simply enqueued. This protects hot symbols (mini-index futures, etc.)
        // whose throughput outpaces the snapshot rotation; their buffer needs to
        // span the rotation latency without losing operations.
        var buf = NewBuffer(perSymbolCap: 2, hotPerSymbolCap: 4);
        uint? evicted = null;
        Assert.True(buf.Enqueue(1, 50, 10, 0, new byte[] { 1 }));
        Assert.True(buf.Enqueue(1, 50, 11, 0, new byte[] { 2 }));
        // Third message hits normal cap → promote to hot cap, no eviction.
        Assert.True(buf.Enqueue(1, 50, 12, 0, new byte[] { 3 }, e => evicted = e));
        Assert.Null(evicted);
        Assert.Equal(0, buf.EvictedPerSymbolCapCount);
        Assert.Equal(1, buf.HotPromotionCount);
        Assert.Equal(3, buf.DepthOf(1));

        // Fill up to hot cap (4) — still no eviction.
        Assert.True(buf.Enqueue(1, 50, 13, 0, new byte[] { 4 }, e => evicted = e));
        Assert.Null(evicted);
        Assert.Equal(4, buf.DepthOf(1));

        // Drain confirms all four retained in arrival order.
        var seen = new List<uint>();
        buf.Drain(1, 10, 13, m => seen.Add(m.RptSeq));
        Assert.Equal(new uint[] { 10, 11, 12, 13 }, seen);
    }

    [Fact]
    public void HotCapOverflow_EvictsOldest_RetainsNewest()
    {
        // After promotion to the hot tier, subsequent overflows fall back to
        // drop-oldest with onEvictedOldest invoked so the caller can advance
        // its MinHeal baseline (rejecting future snapshots that can't bridge).
        var buf = NewBuffer(perSymbolCap: 1, hotPerSymbolCap: 2);
        uint? evicted = null;
        Assert.True(buf.Enqueue(1, 50, 10, 0, new byte[] { 1 }));
        // First overflow → promote to hot cap (2), no eviction.
        Assert.True(buf.Enqueue(1, 50, 11, 0, new byte[] { 2 }, e => evicted = e));
        Assert.Null(evicted);
        Assert.Equal(1, buf.HotPromotionCount);
        // Now at hot cap; next enqueue evicts oldest (rptSeq=10).
        Assert.True(buf.Enqueue(1, 50, 12, 0, new byte[] { 3 }, e => evicted = e));
        Assert.Equal((uint)10, evicted);
        Assert.Equal(1, buf.EvictedPerSymbolCapCount);
        Assert.Equal(2, buf.DepthOf(1));

        var seen = new List<uint>();
        buf.Drain(1, 11, 12, m => seen.Add(m.RptSeq));
        Assert.Equal(new uint[] { 11, 12 }, seen);
    }

    [Fact]
    public void GlobalByteCap_DropsNewest()
    {
        var buf = NewBuffer(perSymbolCap: 1000, globalByteCap: 10);
        Assert.True(buf.Enqueue(1, 50, 1, 0, new byte[6]));
        Assert.False(buf.Enqueue(2, 50, 1, 0, new byte[8])); // 6+8 > 10
        Assert.Equal(1, buf.DroppedGlobalCapCount);
    }

    [Fact]
    public void Clear_DiscardsAndReleasesBytes()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 10, 0, new byte[100]);
        buf.Enqueue(1, 50, 11, 0, new byte[100]);
        Assert.Equal(200, buf.TotalBytes);

        Assert.Equal(2, buf.Clear(1));
        Assert.Equal(0, buf.TotalBytes);
        Assert.Equal(0, buf.DepthOf(1));
    }

    [Fact]
    public void ClearAll_ClearsEverySymbol()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 1, 0, new byte[10]);
        buf.Enqueue(2, 50, 1, 0, new byte[10]);
        buf.Enqueue(3, 50, 1, 0, new byte[10]);
        Assert.Equal(3, buf.ClearAll());
        Assert.Equal(0, buf.TotalBytes);
    }

    [Fact]
    public void Drain_ReleasesByteAccountingForDroppedAndApplied()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 5, 0, new byte[20]);   // below → dropped
        buf.Enqueue(1, 50, 10, 0, new byte[30]);  // applied
        buf.Enqueue(1, 50, 20, 0, new byte[40]);  // above → kept

        Assert.Equal(90, buf.TotalBytes);
        buf.Drain(1, drainFrom: 10, drainTo: 15, _ => { });
        Assert.Equal(40, buf.TotalBytes); // only the kept future entry remains
    }

    [Fact]
    public void DepthOf_ReturnsZeroForUnknownSymbol()
    {
        var buf = NewBuffer();
        Assert.Equal(0, buf.DepthOf(999));
    }

    // ─── Floor pin ──────────────────────────────────────────────────────

    [Fact]
    public void FloorPin_EvictionBelowFloor_DoesNotInvokeCallback()
    {
        // Floor set: snapshot in-flight covers msgs with rptSeq ≤ 10. Hot-cap eviction
        // of msgs at rptSeq 11 (below floor=12) must be SAFE — no MinHeal bump.
        var buf = NewBuffer(perSymbolCap: 1, hotPerSymbolCap: 2);
        buf.Enqueue(1, 50, 11, 0, new byte[] { 11 });
        buf.Enqueue(1, 50, 12, 0, new byte[] { 12 }); // promote to hot cap
        buf.SetProtectedFloor(1, floor: 13); // snapshot covers ≤12
        uint? evicted = null;
        // Enqueue rptSeq=14 → at cap, evict oldest (11). 11 < 13 → safe.
        buf.Enqueue(1, 50, 14, 0, new byte[] { 14 }, e => evicted = e);
        Assert.Null(evicted);
        Assert.Equal(0, buf.EvictedPerSymbolCapCount);
        Assert.Equal(1, buf.SafeEvictedBelowFloorCount);
    }

    [Fact]
    public void FloorPin_EvictionAtOrAboveFloor_BumpsCallbackAsBefore()
    {
        // Floor set, but the evicted msg is above the floor → must signal eviction
        // (snapshot does NOT cover it, so caller must bump MinHeal, leading to
        // the snapshot being rejected at CompleteSnapshot — fail-safe path).
        var buf = NewBuffer(perSymbolCap: 1, hotPerSymbolCap: 2);
        buf.Enqueue(1, 50, 20, 0, new byte[] { 20 });
        buf.Enqueue(1, 50, 21, 0, new byte[] { 21 }); // promote
        buf.SetProtectedFloor(1, floor: 15); // snapshot covers ≤14, our buffer is all > 14
        uint? evicted = null;
        buf.Enqueue(1, 50, 22, 0, new byte[] { 22 }, e => evicted = e);
        Assert.Equal((uint)20, evicted);
        Assert.Equal(1, buf.EvictedPerSymbolCapCount);
        Assert.Equal(0, buf.SafeEvictedBelowFloorCount);
    }

    [Fact]
    public void FloorPin_NoFloor_BehavesAsLegacy()
    {
        // Without a floor, every hot-cap eviction signals (legacy behavior).
        var buf = NewBuffer(perSymbolCap: 1, hotPerSymbolCap: 2);
        buf.Enqueue(1, 50, 5, 0, new byte[] { 5 });
        buf.Enqueue(1, 50, 6, 0, new byte[] { 6 }); // promote
        uint? evicted = null;
        buf.Enqueue(1, 50, 7, 0, new byte[] { 7 }, e => evicted = e);
        Assert.Equal((uint)5, evicted);
        Assert.Equal(1, buf.EvictedPerSymbolCapCount);
        Assert.Equal(0, buf.SafeEvictedBelowFloorCount);
    }

    [Fact]
    public void FloorPin_SetIsMonotonic_LowerValueIgnored()
    {
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 10, 0, new byte[] { 1 }); // lazy-create queue
        buf.SetProtectedFloor(1, 100);
        buf.SetProtectedFloor(1, 50); // ignored (lower)
        Assert.Equal(100u, buf.ProtectedFloorOf(1));
        buf.SetProtectedFloor(1, 200); // raises
        Assert.Equal(200u, buf.ProtectedFloorOf(1));
    }

    [Fact]
    public void FloorPin_SetCreatesQueue_IfMissing()
    {
        // Snapshot Begin can race ahead of the first buffered msg for a symbol.
        // Setting floor must lazily create the queue without crashing.
        var buf = NewBuffer();
        buf.SetProtectedFloor(99, 42);
        Assert.Equal(42u, buf.ProtectedFloorOf(99));
        Assert.Equal(0, buf.DepthOf(99));
    }

    [Fact]
    public void FloorPin_Clear_RemovesFloor()
    {
        var buf = NewBuffer();
        buf.SetProtectedFloor(1, 10);
        buf.ClearProtectedFloor(1);
        Assert.Equal(0u, buf.ProtectedFloorOf(1));
    }

    [Fact]
    public void FloorPin_BufferClear_AlsoClearsFloor()
    {
        // Clear() (epoch reset path / CompleteSnapshot drop-all) must reset the floor;
        // a stale floor lingering across snapshot lifecycles would skew accounting.
        var buf = NewBuffer();
        buf.Enqueue(1, 50, 10, 0, new byte[] { 1 });
        buf.SetProtectedFloor(1, 99);
        buf.Clear(1);
        Assert.Equal(0u, buf.ProtectedFloorOf(1));
    }

    [Fact]
    public void FloorPin_AllProtectedAndOverflowing_FallsBackToBumping()
    {
        // Pathological case: buffer is at hot cap and EVERY message is above the
        // floor (snapshot covers nothing in our buffer). Eviction MUST still
        // happen (otherwise we'd exceed the cap) and MUST bump MinHeal —
        // otherwise we'd silently leave a hole and the snapshot wouldn't reject.
        var buf = NewBuffer(perSymbolCap: 1, hotPerSymbolCap: 2);
        buf.Enqueue(1, 50, 100, 0, new byte[] { 1 });
        buf.Enqueue(1, 50, 101, 0, new byte[] { 2 }); // promote to hot cap
        buf.SetProtectedFloor(1, 50); // covers nothing in our buffer
        uint? evicted = null;
        buf.Enqueue(1, 50, 102, 0, new byte[] { 3 }, e => evicted = e);
        Assert.Equal((uint)100, evicted); // bumped — snapshot will be rejected
        Assert.Equal(1, buf.EvictedPerSymbolCapCount);
    }

    // ── Multi-tier dynamic-grow ladder ───────────────────────────────────────

    [Fact]
    public void MultiTier_PromotesThroughEachLevel()
    {
        var buf = new StaleMboBuffer(NullLogger.Instance, capLevels: new[] { 2, 4, 8 });
        // Fill base tier (2). Next enqueue triggers promotion to level 1 (cap 4).
        for (int i = 0; i < 3; i++)
            buf.Enqueue(1, 50, (uint)(100 + i), 0, new byte[] { (byte)i });
        Assert.Equal(1, buf.HotPromotionCount);             // legacy counter (0→1)
        Assert.Equal(0, buf.EvictedPerSymbolCapCount);
        Assert.Equal(3, buf.DepthOf(1));

        // Fill to level-1 cap (4). Next enqueue promotes to level 2 (cap 8).
        for (int i = 3; i < 5; i++)
            buf.Enqueue(1, 50, (uint)(100 + i), 0, new byte[] { (byte)i });
        var byLevel = buf.GetPromotionsByLevel();
        Assert.Equal(0, byLevel[0]);
        Assert.Equal(1, byLevel[1]);
        Assert.Equal(1, byLevel[2]);
        Assert.Equal(1, buf.HotPromotionCount);             // level 0→1 only
        Assert.Equal(0, buf.EvictedPerSymbolCapCount);
        Assert.Equal(5, buf.DepthOf(1));
    }

    [Fact]
    public void MultiTier_AtTopTierEvictsOldest()
    {
        var buf = new StaleMboBuffer(NullLogger.Instance, capLevels: new[] { 1, 2, 3 });
        // Fill all tiers and beyond.
        for (int i = 0; i < 4; i++)
            buf.Enqueue(1, 50, (uint)(100 + i), 0, new byte[] { (byte)i });
        Assert.Equal(3, buf.DepthOf(1));                    // top tier cap=3
        Assert.Equal(1, buf.EvictedPerSymbolCapCount);      // one unsafe eviction

        var promByLevel = buf.GetPromotionsByLevel();
        Assert.Equal(1, promByLevel[1]);
        Assert.Equal(1, promByLevel[2]);
    }

    [Fact]
    public void MultiTier_UpperTierGatedWhenGlobalBudgetTight()
    {
        // ladder=[1, 2, 4], globalCap=20 → upper-tier gate at 14 bytes (70%).
        // Strategy: symbol 1 fills 15 bytes (above gate). Symbol 2 then promotes
        // 0→1 (always allowed) but 1→2 must be refused.
        var buf = new StaleMboBuffer(NullLogger.Instance,
            capLevels: new[] { 1, 2, 4 },
            globalByteCap: 20);

        buf.Enqueue(1, 50, 100, 0, new byte[15]);            // bytes=15, > gate=14
        buf.Enqueue(2, 50, 200, 0, new byte[] { 1 });        // sym2 depth=1, level 0
        buf.Enqueue(2, 50, 201, 0, new byte[] { 2 });        // overflow → promote 0→1 (always allowed)
        Assert.Equal(2, buf.DepthOf(2));
        Assert.Equal(1, buf.HotPromotionCount);

        // Next overflow: nextLevel=2 (upper tier). bytes=17 > gate=14 → REFUSED → drop oldest.
        buf.Enqueue(2, 50, 202, 0, new byte[] { 3 });
        Assert.Equal(1, buf.PromotionsRefusedGlobalCapCount);
        Assert.Equal(2, buf.DepthOf(2));                    // stayed at level-1 cap=2
        Assert.Equal(1, buf.EvictedPerSymbolCapCount);
        var promByLevel = buf.GetPromotionsByLevel();
        Assert.Equal(0, promByLevel[2]);                    // never reached level 2
    }

    [Fact]
    public void MultiTier_Level1AlwaysAllowedEvenWhenGlobalTight()
    {
        // Global budget mostly full, but level 1 promotion (legacy hot tier)
        // must STILL be allowed — preserves pre-multi-tier guarantee.
        var buf = new StaleMboBuffer(NullLogger.Instance,
            capLevels: new[] { 1, 4 },
            globalByteCap: 4);

        // Symbol A fills global budget to 75% via single allocs.
        buf.Enqueue(1, 50, 100, 0, new byte[] { 1 });
        // Symbol B's first overflow → promotion to level 1 should succeed.
        buf.Enqueue(2, 50, 200, 0, new byte[] { 2 });
        buf.Enqueue(2, 50, 201, 0, new byte[] { 3 }); // at base cap, overflow

        Assert.Equal(1, buf.HotPromotionCount);             // level 1 promoted
        Assert.Equal(0, buf.PromotionsRefusedGlobalCapCount);
        Assert.Equal(0, buf.EvictedPerSymbolCapCount);      // no eviction
    }

    [Fact]
    public void MultiTier_RejectsInvalidCapLevels()
    {
        Assert.Throws<ArgumentException>(() =>
            new StaleMboBuffer(NullLogger.Instance, capLevels: Array.Empty<int>()));
        Assert.Throws<ArgumentException>(() =>
            new StaleMboBuffer(NullLogger.Instance, capLevels: new[] { 0, 1 }));
        Assert.Throws<ArgumentException>(() =>
            new StaleMboBuffer(NullLogger.Instance, capLevels: new[] { 4, 4 })); // not strictly increasing
        Assert.Throws<ArgumentException>(() =>
            new StaleMboBuffer(NullLogger.Instance, capLevels: new[] { 4, 2 })); // decreasing
    }

    [Fact]
    public void LegacyConstructor_BehavesAs2TierLadder()
    {
        // Old ctor: must produce exactly [perSymbolCap, hotPerSymbolCap].
        var buf = new StaleMboBuffer(NullLogger.Instance, perSymbolCap: 2, hotPerSymbolCap: 4);
        Assert.Equal(2, buf.CapLevelCount);
        var levels = buf.GetCapLevels();
        Assert.Equal(2, levels[0]);
        Assert.Equal(4, levels[1]);
    }
}
