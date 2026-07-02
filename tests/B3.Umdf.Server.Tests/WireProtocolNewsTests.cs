using System;
using System.Buffers.Binary;
using B3.Umdf.Server;
using Xunit;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// P13-1 wire protocol coverage for News fragmented frames (NewsBegin, NewsChunk,
/// NewsEnd). Confirms layout, version byte, length validation and round-trip.
/// </summary>
public class WireProtocolNewsTests
{
    [Fact]
    public void NewsBegin_RoundTrip_PreservesAllFields()
    {
        Span<byte> buf = stackalloc byte[WireProtocol.NewsBeginTotalSize];
        int written = WireProtocol.WriteNewsBegin(
            buf,
            securityIdOrZero: 12345,
            newsId: 0xDEADBEEFCAFEFEEDUL,
            source: 7,
            language: 0x4250, // "BP"
            origTimeNanos: 1_700_000_000_000_000_000L,
            totalHeadlineLen: 64,
            totalTextLen: 4096,
            totalUrlLen: 128);

        Assert.Equal(WireProtocol.NewsBeginTotalSize, written);

        // Framing header sanity.
        ushort frameLen = BinaryPrimitives.ReadUInt16LittleEndian(buf[..2]);
        Assert.Equal((ushort)WireProtocol.NewsBeginTotalSize, frameLen);

        var payload = buf[WireProtocol.FramingHeaderSize..];
        bool ok = WireProtocol.TryReadNewsBegin(
            payload,
            out var version,
            out var secId,
            out var newsId,
            out var source,
            out var language,
            out var origTime,
            out var hLen,
            out var tLen,
            out var uLen);

        Assert.True(ok);
        Assert.Equal(WireProtocol.NewsFrameVersion, version);
        Assert.Equal(12345UL, secId);
        Assert.Equal(0xDEADBEEFCAFEFEEDUL, newsId);
        Assert.Equal((byte)7, source);
        Assert.Equal((ushort)0x4250, language);
        Assert.Equal(1_700_000_000_000_000_000L, origTime);
        Assert.Equal(64u, hLen);
        Assert.Equal(4096u, tLen);
        Assert.Equal(128u, uLen);
    }

    [Fact]
    public void NewsBegin_WithZeroSecurityId_AndZeroNewsId_RoundTrips()
    {
        Span<byte> buf = stackalloc byte[WireProtocol.NewsBeginTotalSize];
        WireProtocol.WriteNewsBegin(buf, 0, 0, 0, 0, 0, 0, 0, 0);
        bool ok = WireProtocol.TryReadNewsBegin(
            buf[WireProtocol.FramingHeaderSize..],
            out _, out var sec, out var nid, out var src, out var lang, out var ts,
            out var h, out var t, out var u);
        Assert.True(ok);
        Assert.Equal(0UL, sec);
        Assert.Equal(0UL, nid);
        Assert.Equal((byte)0, src);
        Assert.Equal((ushort)0, lang);
        Assert.Equal(0L, ts);
        Assert.Equal(0u, h);
        Assert.Equal(0u, t);
        Assert.Equal(0u, u);
    }

    [Fact]
    public void NewsChunk_EmptyFragment_WritesHeaderOnly()
    {
        Span<byte> buf = stackalloc byte[WireProtocol.NewsChunkTotalSize(0)];
        int written = WireProtocol.WriteNewsChunk(buf, newsId: 42, WireProtocol.NewsField.Headline,
            ReadOnlySpan<byte>.Empty, isFinal: false);
        Assert.Equal(WireProtocol.NewsChunkTotalSize(0), written);

        // Read MessageType from framing header (offset 2 = type, 1 byte after version byte);
        // exact offset: see WriteFramingHeader. Confirm via length byte instead.
        ushort frameLen = BinaryPrimitives.ReadUInt16LittleEndian(buf[..2]);
        Assert.Equal((ushort)WireProtocol.NewsChunkTotalSize(0), frameLen);
    }

    [Fact]
    public void NewsChunk_FinalFlag_EmitsNewsEndType()
    {
        Span<byte> chunk = stackalloc byte[WireProtocol.NewsChunkTotalSize(8)];
        Span<byte> end = stackalloc byte[WireProtocol.NewsChunkTotalSize(8)];
        ReadOnlySpan<byte> data = stackalloc byte[8] { 1, 2, 3, 4, 5, 6, 7, 8 };

        WireProtocol.WriteNewsChunk(chunk, 1, WireProtocol.NewsField.Text, data, isFinal: false);
        WireProtocol.WriteNewsChunk(end, 1, WireProtocol.NewsField.Text, data, isFinal: true);

        // Type field (u16) sits inside the framing header at offset 4 — they must differ.
        Assert.NotEqual(chunk[4], end[4]);
    }

    [Fact]
    public void NewsChunk_OversizedFragment_Throws()
    {
        var oversized = new byte[WireProtocol.NewsChunkMaxFragment + 1];
        var buf = new byte[WireProtocol.NewsChunkTotalSize(oversized.Length)];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            WireProtocol.WriteNewsChunk(buf, 1, WireProtocol.NewsField.Url, oversized, isFinal: true));
    }

    [Fact]
    public void NewsBegin_TryRead_RejectsShortPayload()
    {
        Span<byte> shortBuf = stackalloc byte[WireProtocol.NewsBeginPayloadSize - 1];
        bool ok = WireProtocol.TryReadNewsBegin(shortBuf,
            out _, out _, out _, out _, out _, out _, out _, out _, out _);
        Assert.False(ok);
    }

    [Fact]
    public void DataFlags_Everything_IncludesNewsBit()
    {
        Assert.True((DataFlags.AllKnown & DataFlags.News) == DataFlags.News);
        Assert.True((DataFlags.AllKnown & DataFlags.Book) == DataFlags.Book);
        Assert.True((DataFlags.AllKnown & DataFlags.Info) == DataFlags.Info);
    }

    [Fact]
    public void DataFlags_All_DoesNotIncludeNews_BackCompat()
    {
        // News must be opt-in; legacy "All" remained Book|Info to preserve the
        // wire contract for existing clients.
        Assert.True((DataFlags.All & DataFlags.News) == 0);
    }
}
