using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Direct tests for <see cref="SbeFrameWalker"/>, the shared SBE-frame walker
/// extracted from <see cref="MessageDispatcher"/> and <c>FeedHandler</c>.
///
/// The walker contract: malformed / truncated frames silently terminate the
/// walk (callers must not catch). These tests pin the corner cases that were
/// previously implicit in two diverging copies of the framing logic.
/// </summary>
public class SbeFrameWalkerTests
{
    private const int FramingSize = FramingHeader.MESSAGE_SIZE; // 4
    private const int SbeHeaderSize = SbeFrameWalker.SbeHeaderSize; // 8

    [Fact]
    public void MinimumLengthFrame_IsWalkedAndAdvancesOffset()
    {
        // Single frame with FramingHeader covering exactly framing + SBE header
        // (no body). This is the smallest valid SBE frame on the UMDF wire.
        const ushort templateId = 30;
        var buf = new byte[FramingSize + SbeHeaderSize];
        WriteFrame(buf, 0, totalLen: (ushort)(FramingSize + SbeHeaderSize), templateId);

        int offset = 0;
        bool ok = SbeFrameWalker.TryReadNext(buf, ref offset, out var sbeSlice, out var tid);

        Assert.True(ok);
        Assert.Equal(templateId, tid);
        Assert.Equal(FramingSize + SbeHeaderSize, offset);
        Assert.True(sbeSlice.Length >= SbeHeaderSize);

        // Second call past the only frame returns false and does not advance.
        bool again = SbeFrameWalker.TryReadNext(buf, ref offset, out _, out _);
        Assert.False(again);
        Assert.Equal(FramingSize + SbeHeaderSize, offset);
    }

    [Fact]
    public void OversizedClaimedLength_TerminatesWalkWithoutAdvancing()
    {
        // FramingHeader claims more bytes than the buffer actually contains.
        // The walker MUST refuse to dispatch (no partial / out-of-bounds read)
        // and leave offset unchanged so callers know the walk ended cleanly.
        var buf = new byte[FramingSize + SbeHeaderSize];
        // Claim 4 extra bytes that don't exist in the buffer.
        WriteFrame(buf, 0, totalLen: (ushort)(FramingSize + SbeHeaderSize + 4), templateId: 30);

        int offset = 0;
        bool ok = SbeFrameWalker.TryReadNext(buf, ref offset, out _, out _);

        Assert.False(ok);
        Assert.Equal(0, offset);
    }

    [Fact]
    public void UnknownTemplateId_StillAdvancesPastFrame()
    {
        // The walker is template-agnostic: an unrecognized template id is
        // surfaced to the caller (which may ignore it) and the offset advances
        // so subsequent valid frames in the same packet remain reachable.
        const ushort unknownTid = 0xBEEF;
        const ushort knownTid = 30;
        var buf = new byte[2 * (FramingSize + SbeHeaderSize)];
        WriteFrame(buf, 0, totalLen: (ushort)(FramingSize + SbeHeaderSize), unknownTid);
        WriteFrame(buf, FramingSize + SbeHeaderSize,
            totalLen: (ushort)(FramingSize + SbeHeaderSize), knownTid);

        int offset = 0;
        Assert.True(SbeFrameWalker.TryReadNext(buf, ref offset, out _, out var tid1));
        Assert.Equal(unknownTid, tid1);
        Assert.Equal(FramingSize + SbeHeaderSize, offset);

        Assert.True(SbeFrameWalker.TryReadNext(buf, ref offset, out _, out var tid2));
        Assert.Equal(knownTid, tid2);
        Assert.Equal(2 * (FramingSize + SbeHeaderSize), offset);

        Assert.False(SbeFrameWalker.TryReadNext(buf, ref offset, out _, out _));
    }

    [Fact]
    public void FrameLengthBelowMinimum_TerminatesWalk()
    {
        // FramingHeader.MessageLength must cover at least framing + SBE header.
        // A claimed length smaller than that is malformed; the walker stops
        // rather than dispatching a sub-header SBE slice.
        var buf = new byte[FramingSize + SbeHeaderSize];
        WriteFrame(buf, 0, totalLen: (ushort)(FramingSize + SbeHeaderSize - 1), templateId: 30);

        int offset = 0;
        Assert.False(SbeFrameWalker.TryReadNext(buf, ref offset, out _, out _));
        Assert.Equal(0, offset);
    }

    private static void WriteFrame(Span<byte> buf, int offset, ushort totalLen, ushort templateId)
    {
        // FramingHeader: u16 messageLength (LE) + u16 encodingType
        BinaryPrimitives.WriteUInt16LittleEndian(buf.Slice(offset), totalLen);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.Slice(offset + 2), 0);
        // SBE MessageHeader layout (B3 UMDF): blockLength(u16), templateId(u16) at +2
        // Match the layout used by MessageDispatcherSequenceResetTests.
        BinaryPrimitives.WriteUInt16LittleEndian(buf.Slice(offset + FramingSize + 2), templateId);
    }
}
