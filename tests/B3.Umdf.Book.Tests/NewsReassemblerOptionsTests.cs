using System;
using System.Collections.Generic;
using B3.Umdf.Book;
using Xunit;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Stress / option-coverage tests for <see cref="NewsReassembler"/> driven through
/// <see cref="NewsReassemblerOptions"/>. Validates that the configurable TTL,
/// inflight-byte cap, per-part byte cap, and per-assembly part-count cap behave
/// as documented and that legacy default behavior is preserved.
/// </summary>
public class NewsReassemblerOptionsTests
{
    private sealed record CapturedNews(ulong NewsId, byte[] Headline, byte[] Text, byte[] Url);

    private static (NewsReassembler, List<CapturedNews>, Action<long>) CreateWithClock(
        NewsReassemblerOptions options, long startTicks = 0)
    {
        long clock = startTicks;
        var captured = new List<CapturedNews>();
        var reasm = new NewsReassembler(
            (sec, id, src, lang, ts, h, t, u) =>
                captured.Add(new CapturedNews(id, h.ToArray(), t.ToArray(), u.ToArray())),
            options,
            monotonicTicks: () => clock);
        return (reasm, captured, delta => clock += delta);
    }

    private static byte[] B(int n, byte fill = (byte)'x')
    {
        var a = new byte[n];
        Array.Fill(a, fill);
        return a;
    }

    [Fact]
    public void Defaults_MatchLegacyConstants()
    {
        var opts = NewsReassemblerOptions.Default;
        Assert.Equal(TimeSpan.FromSeconds(5), opts.Ttl);
        Assert.Equal(NewsReassembler.MaxInflightBytes, opts.MaxInflightBytes);
        Assert.Equal(NewsReassembler.MaxInflightBytes, opts.MaxPartBytes);
        Assert.Equal(NewsReassembler.MaxPartCount, opts.MaxParts);
    }

    [Fact]
    public void PartExactlyAtMaxPartBytes_IsAccepted()
    {
        // Per-part cap of 1024 bytes; submit a single multi-part part whose total
        // payload (headline + text + url) is exactly 1024.
        var opts = new NewsReassemblerOptions { MaxPartBytes = 1024 };
        var (r, captured, _) = CreateWithClock(opts);

        // 2-part assembly: part 1 sized at exactly the cap (300 + 700 + 24 = 1024).
        r.Submit(100, 7, 0, 0, partCount: 2, partNumber: 1, origTimeNanos: 1, totalTextLength: 0,
                 B(300, (byte)'h'), B(700, (byte)'t'), B(24, (byte)'u'));
        Assert.Equal(1, r.Inflight);
        Assert.Equal(0, r.DroppedInvalidPart);

        // Part 2 also at cap (1 + 1023 + 0 = 1024).
        r.Submit(100, 7, 0, 0, 2, 2, 1, 0, B(1, (byte)'H'), B(1023, (byte)'T'), Array.Empty<byte>());
        Assert.Single(captured);
        Assert.Equal(0, r.Inflight);
        Assert.Equal(301, captured[0].Headline.Length);
        Assert.Equal(1723, captured[0].Text.Length);
        Assert.Equal(24, captured[0].Url.Length);
    }

    [Fact]
    public void PartOneByteOverMaxPartBytes_DropsAssembly()
    {
        var opts = new NewsReassemblerOptions { MaxPartBytes = 1024 };
        var (r, _, _) = CreateWithClock(opts);

        // Build an in-flight assembly (well-sized part 1).
        r.Submit(100, 7, 0, 0, 2, 1, 0, 0, B(10), B(10), B(10));
        Assert.Equal(1, r.Inflight);

        // Part 2 is one byte over the cap (1024 + 1).
        r.Submit(100, 7, 0, 0, 2, 2, 0, 0, B(513), B(512), Array.Empty<byte>());

        Assert.Equal(1, r.DroppedInvalidPart);
        Assert.Equal(0, r.Inflight); // existing assembly was also evicted
    }

    [Fact]
    public void OversizedTotalTextLength_DropsBeforeBuffering()
    {
        var opts = new NewsReassemblerOptions { MaxInflightBytes = 4096 };
        var (r, _, _) = CreateWithClock(opts);

        // totalTextLength claims 1 MiB — way over the 4 KiB inflight cap.
        r.Submit(100, 7, 0, 0, partCount: 2, partNumber: 1, origTimeNanos: 0,
                 totalTextLength: 1_000_000u,
                 B(8), B(8), Array.Empty<byte>());

        Assert.Equal(0, r.Inflight);
        Assert.Equal(0, r.InflightBytes);
        Assert.Equal(1, r.DroppedInvalidPart);
    }

    [Fact]
    public void EvictionUnderConcurrentExpiryAndCapPressure()
    {
        // Tiny inflight byte cap forces aggressive LRU eviction; short TTL forces
        // expiry on the older assemblies during the same sweep cycle.
        var opts = new NewsReassemblerOptions
        {
            MaxInflightBytes = 4096,
            MaxPartBytes = 4096,
            Ttl = TimeSpan.FromTicks(1000),
        };
        var (r, captured, advance) = CreateWithClock(opts, startTicks: 1_000);

        // Stage four assemblies that together exceed MaxInflightBytes.
        // Each part-1 holds 1500 bytes (headline+text+url = 500 each); after the
        // 3rd insert in-flight is 4500 > 4096, so the 4th insert triggers eviction.
        for (ulong id = 1; id <= 4; id++)
        {
            r.Submit(100, id, 0, 0, partCount: 2, partNumber: 1, origTimeNanos: 0, totalTextLength: 0,
                     B(500, (byte)'h'), B(500, (byte)'t'), B(500, (byte)'u'));
        }

        // The 4th insert's EvictIfNeeded sweep must have evicted at least one LRU entry.
        Assert.True(r.DroppedCap >= 1, $"Expected at least one cap eviction, got {r.DroppedCap}");
        Assert.True(r.InflightBytes <= 4500);

        // Now advance past TTL and submit another assembly to trigger SweepExpired.
        advance(2_000);
        r.Submit(100, 99, 0, 0, partCount: 2, partNumber: 1, origTimeNanos: 0, totalTextLength: 0,
                 B(100), B(100), B(100));

        // Anything older than now-1000 ticks should have been swept.
        Assert.True(r.DroppedTtl >= 1, $"Expected at least one TTL eviction after clock advance, got {r.DroppedTtl}");
        Assert.Empty(captured); // none completed
    }

    [Fact]
    public void Ttl_CustomValue_RespectsConfiguredWindow()
    {
        // Custom 10-second TTL — half-second advance should not expire.
        var opts = new NewsReassemblerOptions { Ttl = TimeSpan.FromSeconds(10) };
        var (r, _, advance) = CreateWithClock(opts, startTicks: 1_000_000);

        r.Submit(100, 7, 0, 0, 2, 1, 0, 0, B(8), B(8), Array.Empty<byte>());
        Assert.Equal(1, r.Inflight);

        advance(TimeSpan.FromSeconds(5).Ticks);
        r.Submit(100, 8, 0, 0, 2, 1, 0, 0, B(8), B(8), Array.Empty<byte>());

        Assert.Equal(0, r.DroppedTtl); // 5s < 10s TTL
        Assert.Equal(2, r.Inflight);

        advance(TimeSpan.FromSeconds(10).Ticks + 1);
        r.Submit(100, 9, 0, 0, 2, 1, 0, 0, B(8), B(8), Array.Empty<byte>());
        Assert.True(r.DroppedTtl >= 1);
    }

    [Fact]
    public void MaxParts_BelowDefault_RejectsLargeAssemblies()
    {
        var opts = new NewsReassemblerOptions { MaxParts = 4 };
        var (r, _, _) = CreateWithClock(opts);

        r.Submit(100, 7, 0, 0, partCount: 5, partNumber: 1, 0, 0, B(2), B(2), Array.Empty<byte>());
        Assert.Equal(1, r.DroppedInvalidPart);
        Assert.Equal(0, r.Inflight);

        // partCount == MaxParts is still accepted.
        r.Submit(100, 8, 0, 0, partCount: 4, partNumber: 1, 0, 0, B(2), B(2), Array.Empty<byte>());
        Assert.Equal(1, r.Inflight);
    }

    [Fact]
    public void Options_Validate_RejectsNonPositiveValues()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new NewsReassembler(
            (a, b, c, d, e, f, g, h) => { }, new NewsReassemblerOptions { Ttl = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NewsReassembler(
            (a, b, c, d, e, f, g, h) => { }, new NewsReassemblerOptions { MaxInflightBytes = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NewsReassembler(
            (a, b, c, d, e, f, g, h) => { }, new NewsReassemblerOptions { MaxPartBytes = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NewsReassembler(
            (a, b, c, d, e, f, g, h) => { }, new NewsReassemblerOptions { MaxParts = 0 }));
    }
}
