using System;
using System.Collections.Generic;
using B3.Umdf.Book;
using Xunit;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// P13-2 test matrix for <see cref="NewsReassembler"/>. Covers single-part fast
/// path, multi-part reassembly (in/out-of-order), header-mismatch invariants,
/// part-number bounds, NewsID=0 multipart drop, duplicate ignore, TTL eviction
/// (manual clock injection) and inflight-count cap eviction.
/// </summary>
public class NewsReassemblerTests
{
    private sealed record CapturedNews(
        ulong SecurityId, ulong NewsId, byte Source, ushort Language, long OrigTime,
        byte[] Headline, byte[] Text, byte[] Url);

    private static (NewsReassembler, List<CapturedNews>, Action<long>) CreateWithClock(long startTicks = 0)
    {
        long clock = startTicks;
        var captured = new List<CapturedNews>();
        var reasm = new NewsReassembler(
            (sec, id, src, lang, ts, h, t, u) =>
                captured.Add(new CapturedNews(sec, id, src, lang, ts, h.ToArray(), t.ToArray(), u.ToArray())),
            monotonicTicks: () => clock);
        return (reasm, captured, delta => clock += delta);
    }

    private static byte[] B(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Fact]
    public void SinglePart_NewsIdNonZero_EmitsImmediately()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 7, 1, 0, partCount: 1, partNumber: 1,
            origTimeNanos: 1_000, totalTextLength: 5,
            B("HEAD"), B("TEXT"), B("http://x"));

        Assert.Single(captured);
        Assert.Equal(7UL, captured[0].NewsId);
        Assert.Equal(1L, r.Reassembled);
        Assert.Equal(0, r.Inflight);
    }

    [Fact]
    public void SinglePart_NewsIdZero_StillEmits()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(0, 0, 0, 0, partCount: 1, partNumber: 1, 0, 0,
            B("h"), B("t"), B("u"));
        Assert.Single(captured);
    }

    [Fact]
    public void MultiPart_InOrder_EmitsOnLastPart()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 9, 0, 0, partCount: 3, partNumber: 1, 1_000, 9, B("AAA"), B("111"), B("u1"));
        Assert.Empty(captured);
        r.Submit(100, 9, 0, 0, partCount: 3, partNumber: 2, 2_000, 9, B("BBB"), B("222"), B("u2"));
        Assert.Empty(captured);
        r.Submit(100, 9, 0, 0, partCount: 3, partNumber: 3, 3_000, 9, B("CCC"), B("333"), B("u3"));

        Assert.Single(captured);
        Assert.Equal("AAABBBCCC", System.Text.Encoding.UTF8.GetString(captured[0].Headline));
        Assert.Equal("111222333", System.Text.Encoding.UTF8.GetString(captured[0].Text));
        Assert.Equal("u1u2u3", System.Text.Encoding.UTF8.GetString(captured[0].Url));
        Assert.Equal(0, r.Inflight);
    }

    [Fact]
    public void MultiPart_OutOfOrder_ReassembledByPartNumber()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 7, 0, 0, 3, 3, 0, 0, B("CC"), B("33"), B(""));
        r.Submit(100, 7, 0, 0, 3, 1, 0, 0, B("AA"), B("11"), B(""));
        r.Submit(100, 7, 0, 0, 3, 2, 0, 0, B("BB"), B("22"), B(""));

        Assert.Single(captured);
        Assert.Equal("AABBCC", System.Text.Encoding.UTF8.GetString(captured[0].Headline));
        Assert.Equal("112233", System.Text.Encoding.UTF8.GetString(captured[0].Text));
    }

    [Fact]
    public void MultiPart_NewsIdZero_DroppedWithCounter()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 0, 0, 0, partCount: 2, partNumber: 1, 0, 0, B("a"), B("b"), B(""));
        r.Submit(100, 0, 0, 0, partCount: 2, partNumber: 2, 0, 0, B("c"), B("d"), B(""));
        Assert.Empty(captured);
        Assert.Equal(2L, r.DroppedNoId);
    }

    [Fact]
    public void DuplicatePart_IgnoredSilently()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 7, 0, 0, 2, 1, 0, 0, B("aa"), B("bb"), B(""));
        r.Submit(100, 7, 0, 0, 2, 1, 0, 0, B("XX"), B("YY"), B("")); // duplicate
        r.Submit(100, 7, 0, 0, 2, 2, 0, 0, B("cc"), B("dd"), B(""));

        Assert.Single(captured);
        Assert.Equal("aacc", System.Text.Encoding.UTF8.GetString(captured[0].Headline));
    }

    [Fact]
    public void PartCountZero_Dropped()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 7, 0, 0, partCount: 0, partNumber: 1, 0, 0, B(""), B(""), B(""));
        Assert.Empty(captured);
        Assert.Equal(1L, r.DroppedInvalidPart);
    }

    [Fact]
    public void PartCountAboveCap_Dropped()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 7, 0, 0, partCount: (ushort)(NewsReassembler.MaxPartCount + 1),
            partNumber: 1, 0, 0, B(""), B(""), B(""));
        Assert.Empty(captured);
        Assert.Equal(1L, r.DroppedInvalidPart);
    }

    [Fact]
    public void PartNumberOutOfRange_Dropped()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 7, 0, 0, partCount: 3, partNumber: 0, 0, 0, B(""), B(""), B(""));
        r.Submit(100, 7, 0, 0, partCount: 3, partNumber: 4, 0, 0, B(""), B(""), B(""));
        Assert.Empty(captured);
        Assert.Equal(2L, r.DroppedInvalidPart);
    }

    [Fact]
    public void HeaderMismatch_DropsEntireAssembly()
    {
        var (r, captured, _) = CreateWithClock();
        r.Submit(100, 7, source: 1, language: 0, 3, 1, 0, totalTextLength: 9,
            B("aa"), B("bb"), B(""));
        // Mismatched source byte -> drop assembly + counter.
        r.Submit(100, 7, source: 2, language: 0, 3, 2, 0, totalTextLength: 9,
            B("cc"), B("dd"), B(""));

        Assert.Empty(captured);
        Assert.Equal(1L, r.DroppedInconsistent);
        Assert.Equal(0, r.Inflight);

        // After drop, a fresh part 1 should start a new assembly.
        r.Submit(100, 7, 1, 0, 1, 1, 0, 0, B("solo"), B("body"), B(""));
        Assert.Single(captured);
    }

    [Fact]
    public void TtlExpiry_DropsStaleAssemblyOnNextSubmit()
    {
        var (r, captured, advance) = CreateWithClock(startTicks: 1_000);
        r.Submit(100, 7, 0, 0, 2, 1, 0, 0, B("aa"), B("bb"), B(""));
        Assert.Equal(1, r.Inflight);

        // Advance past TTL.
        advance(NewsReassembler.TtlTicks + 1);

        // Submit a multi-part (any non-single-part path) to trigger SweepExpired.
        r.Submit(101, 8, 0, 0, 2, 1, 0, 0, B("x"), B("y"), B(""));

        Assert.Empty(captured);
        Assert.True(r.DroppedTtl >= 1);
        // The new in-flight assembly remains; the old one was swept.
        Assert.Equal(1, r.Inflight);
    }

    [Fact]
    public void InflightCap_EvictsLeastRecentlyUsed()
    {
        var (r, captured, _) = CreateWithClock();
        // Fill to cap.
        for (int i = 0; i < NewsReassembler.MaxInflight; i++)
        {
            r.Submit(100, (ulong)(i + 1), 0, 0, 2, 1, 0, 0, B("a"), B("b"), B(""));
        }
        Assert.Equal(NewsReassembler.MaxInflight, r.Inflight);

        // One more new newsId pushes the LRU out.
        r.Submit(100, 999_999, 0, 0, 2, 1, 0, 0, B("x"), B("y"), B(""));
        Assert.True(r.DroppedCap >= 1);
        Assert.Equal(NewsReassembler.MaxInflight, r.Inflight);
    }
}
