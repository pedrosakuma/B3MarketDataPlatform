using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

/// <summary>
/// Single source of truth for walking the SBE-framed body of a UMDF packet.
///
/// A UMDF packet body (after the 16-byte <see cref="PacketHeader"/>) is a
/// concatenation of <c>[FramingHeader 4B][SBE MessageHeader 8B][SBE body]</c>
/// triples where <c>FramingHeader.MessageLength</c> covers the framing header
/// itself. This helper centralizes the per-frame validation that used to be
/// duplicated between <see cref="MessageDispatcher"/> (live incremental path)
/// and <c>FeedHandler</c> (snapshot / instrument-definition paths):
///
///   * minimum-length check (framing + SBE header must fit)
///   * <c>FramingHeader.TryParse</c>
///   * claimed-length sanity (must cover at least framing + SBE header)
///   * bounds check against the outer span (truncated trailing frame ⇒ stop)
///   * extraction of the SBE slice and template id
///
/// The contract is deliberately "best-effort, never throw": malformed or
/// truncated frames silently terminate the walk so callers do not need
/// try/catch on the hot path. Path-specific behavior (sequence-reset
/// fan-out, snapshot lifecycle hooks, instr-def tracking) stays at the call
/// site.
///
/// Implemented as a static method over <see cref="ReadOnlySpan{T}"/> with an
/// <c>out</c> slice + <c>ref int</c> offset to preserve the existing zero-
/// allocation, no-closure dispatch style.
/// </summary>
internal static class SbeFrameWalker
{
    /// <summary>Size in bytes of the SBE <see cref="MessageHeader"/>.</summary>
    public const int SbeHeaderSize = MessageHeader.MESSAGE_SIZE;

    /// <summary>
    /// Advance <paramref name="offset"/> past one framed SBE message in
    /// <paramref name="packetSpan"/>, returning the SBE slice (header + body)
    /// and the decoded template id. Returns <c>false</c> at end-of-packet or
    /// on any malformed/truncated frame.
    /// </summary>
    public static bool TryReadNext(
        ReadOnlySpan<byte> packetSpan,
        ref int offset,
        out ReadOnlySpan<byte> sbeSlice,
        out ushort templateId)
    {
        sbeSlice = default;
        templateId = 0;

        if (offset + FramingHeader.MESSAGE_SIZE + SbeHeaderSize > packetSpan.Length)
            return false;

        var framingSlice = packetSpan[offset..];
        if (!FramingHeader.TryParse(framingSlice, out var framing, out _))
            return false;

        if (framing.MessageLength < FramingHeader.MESSAGE_SIZE + SbeHeaderSize)
            return false;

        if (offset + framing.MessageLength > packetSpan.Length)
            return false;

        sbeSlice = packetSpan[(offset + FramingHeader.MESSAGE_SIZE)..];
        if (!MessageHeader.TryReadTemplateId(sbeSlice, out templateId))
            return false;

        offset += framing.MessageLength;
        return true;
    }
}
