using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public static class MessageDispatcher
{
    public const int SbeHeaderSize = MessageHeader.MESSAGE_SIZE;

    public static void Dispatch(in UmdfPacket packet, IFeedEventHandler handler)
    {
        Dispatch(in packet, packet.Data.Span, handler);
    }

    /// <summary>
    /// Dispatches SBE messages from a pre-computed span, avoiding redundant .Data.Span calls.
    /// </summary>
    public static void Dispatch(in UmdfPacket packet, ReadOnlySpan<byte> span, IFeedEventHandler handler)
    {
        if (span.Length < UmdfPacketHeader.Size)
            return;

        int offset = UmdfPacketHeader.Size;

        while (offset + FramingHeader.MESSAGE_SIZE + SbeHeaderSize <= span.Length)
        {
            var framingSlice = span[offset..];
            if (!FramingHeader.TryParse(framingSlice, out var framing, out _))
                break;

            if (framing.MessageLength < FramingHeader.MESSAGE_SIZE + SbeHeaderSize)
                break;

            if (offset + framing.MessageLength > span.Length)
                break;

            // SBE message starts right after the FramingHeader
            var sbeSlice = span[(offset + FramingHeader.MESSAGE_SIZE)..];
            if (!MessageHeader.TryReadTemplateId(sbeSlice, out var templateId))
                break;

            handler.OnPacket(in packet, sbeSlice, templateId);

            offset += framing.MessageLength;
        }
    }
}
