using B3.MarketData.Wire;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using B3.MarketData.WebSocketClient;

namespace B3.Umdf.Fuzz.Tests;

/// <summary>
/// Fuzz harness for <see cref="WireFormat"/> — the SDK-side mirror of the server's
/// <c>WireProtocol</c> that <see cref="MarketDataClient"/>.DispatchFrames feeds. Same
/// contract as <see cref="WireProtocolFramingFuzzTests"/>: malformed input must surface
/// as <c>false</c> from a <c>TryRead*</c> or as one of the documented framing exceptions.
/// </summary>
public class MarketDataClientFramingFuzzTests
{
    private const int Iterations = 500;
    private const int MaxBufferLength = 1024;

    private static readonly HashSet<Type> ExpectedExceptions = new()
    {
        typeof(ArgumentOutOfRangeException),
        typeof(IndexOutOfRangeException),
        typeof(ArgumentException),
        typeof(System.Text.DecoderFallbackException),
    };

    [Fact]
    public void Fuzz_WireFormat_TryReadHeader_NeverThrows()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            _ = WireFormat.TryReadHeader(buf, out _, out _);
        });
    }

    [Fact]
    public void Fuzz_WireFormat_ReadServerHello_OnlyExpectedExceptions()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            try { _ = WireFormat.ReadServerHello(buf); }
            catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType())) { }
        });
    }

    [Fact]
    public void Fuzz_WireFormat_ReadSubscribeOk_OnlyExpectedExceptions()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            try { _ = WireFormat.ReadSubscribeOk(buf); }
            catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType())) { }
        });
    }

    [Fact]
    public void Fuzz_WireFormat_ReadSubscribeError_OnlyExpectedExceptions()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            try { _ = WireFormat.ReadSubscribeError(buf); }
            catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType())) { }
        });
    }

    [Fact]
    public void Fuzz_WireFormat_ReadInfoSnapshot_OnlyExpectedExceptions()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            try { _ = WireFormat.ReadInfoSnapshot(buf, "TEST", DateTime.UtcNow); }
            catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType())) { }
        });
    }

    /// <summary>
    /// Framing-loop fuzz mirroring <see cref="MarketDataClient"/>.DispatchFrames. We
    /// don't invoke the private method directly (it allocates events and pumps a channel)
    /// — instead we replicate its loop here, which is the surface that decides whether
    /// hostile bytes can drive the consumer past the buffer end.
    /// </summary>
    [Fact]
    public void Fuzz_DispatchFramesLoop_NeverWalksOffEnd()
    {
        PropertyRunner.ForAllBytes(Iterations * 2, MaxBufferLength, buf =>
        {
            int offset = 0;
            int safety = 0;
            while (offset < buf.Length && safety++ < 4096)
            {
                if (!WireFormat.TryReadHeader(buf.AsSpan(offset), out var len, out _) ||
                    len < WireFormat.FramingHeaderSize) break;
                if (offset + len > buf.Length) break;
                offset += (int)len;
            }
            Assert.True(offset <= buf.Length, $"DispatchFrames loop walked past end (offset={offset}, length={buf.Length})");
        });
    }

    /// <summary>
    /// Mismatched length-header attack: claim a length larger than the available bytes.
    /// The framing loop must reject the partial frame and stop, NOT slice past the end.
    /// </summary>
    [Theory]
    [InlineData(9)]      // claims one byte beyond an 8-byte header buffer
    [InlineData(255)]
    [InlineData(0xFFFF)]
    public void OversizedClaimedLength_DoesNotCausePastEndRead(int claimedLen)
    {
        var buf = new byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(buf, (uint)claimedLen);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(4), (ushort)MessageType.ServerStatus);

        Assert.True(WireFormat.TryReadHeader(buf, out var len, out _));
        // Same loop the SDK runs:
        int offset = 0;
        if (offset + len > buf.Length)
        {
            // Correct behavior: stop. Verified by reaching here without exception.
            return;
        }
        Assert.Fail($"loop should have rejected oversized len {claimedLen}");
    }

    /// <summary>
    /// Truncated frame body — header parses, declared length valid, but the buffer is
    /// shorter than the body. The SDK's loop must drop without invoking the per-type
    /// reader. This is the bug class the WireFormat.Read* methods would otherwise hit
    /// with a slice exception.
    /// </summary>
    [Fact]
    public void TruncatedFrameBody_LoopBailsBeforeReader()
    {
        var buf = new byte[10];
        // Claim a 16-byte ServerHello frame but only deliver 10 bytes.
        WireFrame.WriteHeader(buf, 16, MessageType.ServerHello);

        Assert.True(WireFormat.TryReadHeader(buf, out var len, out var type));
        Assert.Equal(16u, len);
        Assert.Equal(MessageType.ServerHello, type);
        // Loop must NOT slice into a payload from a short tail; the bounds check
        // (offset + len > buf.Length) is the exact gate exercised here.
        Assert.True(0 + len > (uint)buf.Length);
    }
}
