using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public static class MessageDispatcher
{
    public const int SbeHeaderSize = SbeFrameWalker.SbeHeaderSize;

    public static void Dispatch(in UmdfPacket packet, IFeedEventHandler handler)
    {
        Dispatch(in packet, packet.Data.Span, handler);
    }

    /// <summary>
    /// Dispatches SBE messages from a pre-computed span, avoiding redundant .Data.Span calls.
    /// Additionally fires <see cref="IFeedEventHandler.OnSequenceReset(int, SequenceResetReason)"/>
    /// when the decoded template is one of the UMDF reset templates
    /// (<c>SequenceReset_1</c>, <c>ChannelReset_11</c>) so downstream consumers
    /// can react to mid-session resets without re-decoding the SBE body.
    ///
    /// Per-frame walking (framing parse, length validation, SBE slice/template
    /// extraction) is delegated to <see cref="SbeFrameWalker"/>; this method
    /// owns only the dispatcher-specific fan-out.
    /// </summary>
    public static void Dispatch(in UmdfPacket packet, ReadOnlySpan<byte> span, IFeedEventHandler handler)
    {
        if (span.Length < UmdfPacketHeader.Size)
            return;

        int offset = UmdfPacketHeader.Size;

        while (SbeFrameWalker.TryReadNext(span, ref offset, out var sbeSlice, out var templateId))
        {
            handler.OnPacket(in packet, sbeSlice, templateId);

            // SequenceReset fan-out: SBE template ids 1 and 11 are mid-session
            // reset signals on the B3 UMDF wire. Surface them via the typed
            // OnSequenceReset overload so downstream policy (epoch resets,
            // pending-snapshot drops, dashboards) can run without the consumer
            // having to re-decode SBE bodies.
            if (templateId == SequenceReset_1Data.MESSAGE_ID)
                handler.OnSequenceReset(packet.ChannelGroup, SequenceResetReason.SequenceReset);
            else if (templateId == ChannelReset_11Data.MESSAGE_ID)
                handler.OnSequenceReset(packet.ChannelGroup, SequenceResetReason.ChannelReset);
        }
    }
}
