using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public static class MessageDispatcher
{
    public const int SbeHeaderSize = 8; // SBE MessageHeader: blockLength(2) + templateId(2) + schemaId(2) + version(2)

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
            ushort templateId = System.Runtime.InteropServices.MemoryMarshal.Read<ushort>(sbeSlice[2..]);

            handler.OnPacket(in packet, sbeSlice, templateId);

            offset += framing.MessageLength;
        }
    }
}
