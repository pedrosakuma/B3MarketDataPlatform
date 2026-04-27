using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins <see cref="StaleMboBuffer.SafeEvictedBelowFloorCount"/> behavior
/// (recovery improvement #10): evictions of msgs below the snapshot's
/// protected floor must be counted SEPARATELY from poisoning evictions —
/// they are safe (the snapshot already covers them) and MUST NOT bump
/// MinHealRptSeq via the onEvictedOldest callback.
/// </summary>
public class StaleMboBufferProtectedFloorEvictionTests
{
    [Fact]
    public void EvictionUnderFloor_IncrementsSafeCounter_AndSkipsCallback()
    {
        // Use a tiny single-tier cap so eviction triggers immediately on the
        // 3rd enqueue (no promotion to a hot tier exists at length 1).
        var buf = new StaleMboBuffer(NullLogger.Instance, capLevels: new[] { 2 });
        const ulong sec = 42;
        // Snapshot covers everything up to rptSeq < 1_000_000.
        buf.SetProtectedFloor(sec, 1_000_000);

        int evictionCallbackCalls = 0;
        var body = new byte[8];
        // Cap=2 → enqueue 5 → 3 evictions, all UNDER the floor.
        for (uint r = 1; r <= 5; r++)
        {
            buf.Enqueue(sec, templateId: 50, rptSeq: r, sendingTimeNs: 0, body,
                        onEvictedOldest: _ => evictionCallbackCalls++);
        }

        Assert.Equal(3, buf.SafeEvictedBelowFloorCount);
        Assert.Equal(0, buf.EvictedPerSymbolCapCount);
        Assert.Equal(0, evictionCallbackCalls);
    }

    [Fact]
    public void EvictionAboveFloor_BumpsMinHealCallback_NotSafe()
    {
        var buf = new StaleMboBuffer(NullLogger.Instance, capLevels: new[] { 2 });
        const ulong sec = 42;
        // No protected floor → all evictions are poisoning.
        int callbackCount = 0;
        uint lastEvictedRpt = 0;
        var body = new byte[8];

        for (uint r = 1; r <= 5; r++)
        {
            buf.Enqueue(sec, templateId: 50, rptSeq: r, sendingTimeNs: 0, body,
                        onEvictedOldest: rpt => { callbackCount++; lastEvictedRpt = rpt; });
        }

        Assert.Equal(0, buf.SafeEvictedBelowFloorCount);
        Assert.Equal(3, buf.EvictedPerSymbolCapCount);
        Assert.Equal(3, callbackCount);
        Assert.Equal(3u, lastEvictedRpt);
    }
}
