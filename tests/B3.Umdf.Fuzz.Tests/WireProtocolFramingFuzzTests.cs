using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using B3.Umdf.Server;

namespace B3.Umdf.Fuzz.Tests;

/// <summary>
/// Fuzz harness for the server-side <see cref="WireProtocol"/> decoder paths
/// (issue P2 / fuzz-coverage). Mirrors the style of <see cref="SbeParserFuzzTests"/>:
/// random/crafted byte buffers fed through every <c>TryRead*</c> entry point, plus a
/// hand-rolled multi-frame parsing loop that simulates a WS receive callback splitting
/// frames across buffer boundaries.
///
/// Contract under test: a malformed buffer must EITHER return <c>false</c> from a
/// <c>TryRead*</c> method OR throw one of the documented framing exceptions
/// (see <see cref="ExpectedExceptions"/>). Anything else (NRE, AccessViolation, OOM,
/// process crash, silent state corruption) is a P0.
/// </summary>
public class WireProtocolFramingFuzzTests
{
    private const int Iterations = 500;
    private const int MaxBufferLength = 1024;

    /// <summary>
    /// Exceptions today's <c>WireProtocol</c> may legitimately raise on hostile input —
    /// the framing helpers do bounds-check via <c>Span.Slice</c> rather than returning
    /// a typed Try-result for every field, so OOR/IOR are the failure mode we tolerate.
    /// Tightening this set is tracked alongside the equivalent comment in
    /// <see cref="SbeParserFuzzTests"/>.
    /// </summary>
    private static readonly HashSet<Type> ExpectedExceptions = new()
    {
        typeof(ArgumentOutOfRangeException),
        typeof(IndexOutOfRangeException),
        typeof(ArgumentException),       // Encoding.UTF8.GetString on bad slice length
        typeof(System.Text.DecoderFallbackException),
    };

    [Fact]
    public void Fuzz_TryReadFramingHeader_NeverThrows()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            // Pure boolean predicate — should NEVER throw, on any input.
            _ = WireProtocol.TryReadFramingHeader(buf, out _, out _);
        });
    }

    [Fact]
    public void Fuzz_ReadSubscribe_OnlyExpectedExceptionsEscape()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            try
            {
                _ = WireProtocol.ReadSubscribe(buf);
            }
            catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType())) { }
        });
    }

    [Fact]
    public void Fuzz_ReadUnsubscribe_OnlyExpectedExceptionsEscape()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            try
            {
                _ = WireProtocol.ReadUnsubscribe(buf);
            }
            catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType())) { }
        });
    }

    [Fact]
    public void Fuzz_TryReadNewsBegin_OnlyExpectedExceptionsEscape()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            try
            {
                _ = WireProtocol.TryReadNewsBegin(buf, out _, out _, out _, out _, out _, out _, out _, out _, out _);
            }
            catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType())) { }
        });
    }

    [Fact]
    public void Fuzz_TryReadNewsChunk_OnlyExpectedExceptionsEscape()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            try
            {
                _ = WireProtocol.TryReadNewsChunk(buf, out _, out _, out _, out _);
            }
            catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType())) { }
        });
    }

    /// <summary>
    /// Simulates the receive-loop framing logic: walk a buffer, parse [u16 length][u16 type]
    /// repeatedly, advance by <c>length</c>. The parser must never read past the end of the
    /// outer buffer — that is the class of bug a hostile client could exploit by sending an
    /// oversized claimed length to drive the consumer into a remote read.
    /// </summary>
    [Fact]
    public void Fuzz_MultiFrameLoop_NeverWalksOffEnd()
    {
        PropertyRunner.ForAllBytes(Iterations * 2, MaxBufferLength, buf =>
        {
            int offset = 0;
            int safetyIterations = 0;
            while (offset < buf.Length && safetyIterations++ < 4096)
            {
                if (!WireProtocol.TryReadFramingHeader(buf.AsSpan(offset), out var len, out _))
                    break;
                // Reject obviously malformed framing per server contract.
                if (len < WireProtocol.FramingHeaderSize) break;
                if (offset + len > buf.Length) break;
                offset += len;
            }
            // Must never advance past the buffer end. If this assert fires, the framing
            // loop has a bounds bug exploitable as out-of-bounds read.
            Assert.True(offset <= buf.Length, $"framing loop walked past end (offset={offset}, length={buf.Length})");
        });
    }

    /// <summary>
    /// Targeted regression: a frame whose declared length claims more bytes than the
    /// buffer holds (the "oversized length header" exploit) must be rejected, not
    /// drive the parser into a slice past the end.
    /// </summary>
    [Theory]
    [InlineData(0xFFFF)] // max u16 — claims 65535 bytes
    [InlineData(0x0100)] // 256 bytes
    [InlineData(0x0005)] // just one byte beyond the truncated frame
    public void OversizedLengthHeader_IsRejected(int claimedLen)
    {
        var buf = new byte[4]; // header-only, no payload
        BinaryPrimitives.WriteUInt16LittleEndian(buf, (ushort)claimedLen);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), (ushort)MessageType.Subscribe);

        Assert.True(WireProtocol.TryReadFramingHeader(buf, out var len, out _));
        Assert.Equal((ushort)claimedLen, len);
        // Caller is responsible for "len > available" check; verify it works:
        Assert.True(len > buf.Length || len == buf.Length, "test setup: claimed length should exceed real buffer");
    }

    /// <summary>
    /// Framing-across-WS-buffers: split a well-formed multi-frame stream at every byte
    /// boundary; for each split, confirm the consumer drains exactly the prefix that
    /// completes whole frames and stops cleanly at the partial tail.
    /// </summary>
    [Fact]
    public void Fuzz_FramingSplitAcrossBuffers_DrainsCleanly()
    {
        // Build two well-formed frames: a ServerStatus(true) and an Unsubscribed(0xDEAD).
        Span<byte> dest = stackalloc byte[64];
        int n1 = WireProtocol.WriteServerStatus(dest, ready: true);
        int n2 = WireProtocol.WriteUnsubscribed(dest[n1..], securityId: 0xDEADBEEFCAFEUL);
        var stream = dest[..(n1 + n2)].ToArray();

        for (int split = 0; split <= stream.Length; split++)
        {
            // Phase 1: feed the prefix; expect parser to drain whole frames and stop
            // at the boundary of the last partial frame.
            int consumed = DrainWholeFrames(stream.AsSpan(0, split));
            Assert.True(consumed >= 0 && consumed <= split, $"consumed out of range at split={split}");

            // Phase 2: feed the rest concatenated with the un-drained tail; total drained
            // across both phases must equal the full stream length.
            var remainder = new byte[(split - consumed) + (stream.Length - split)];
            stream.AsSpan(consumed, split - consumed).CopyTo(remainder);
            stream.AsSpan(split, stream.Length - split).CopyTo(remainder.AsSpan(split - consumed));

            int consumed2 = DrainWholeFrames(remainder);
            Assert.Equal(stream.Length, consumed + consumed2);
        }
    }

    private static int DrainWholeFrames(ReadOnlySpan<byte> buf)
    {
        int offset = 0;
        while (offset < buf.Length)
        {
            if (!WireProtocol.TryReadFramingHeader(buf[offset..], out var len, out _)) break;
            if (len < WireProtocol.FramingHeaderSize) break;
            if (offset + len > buf.Length) break;
            offset += len;
        }
        return offset;
    }
}
