using System.Runtime.InteropServices;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public static class MessageDispatcher
{
    public const int SbeHeaderSize = 8;

    public static void Dispatch(in UmdfPacket packet, IFeedEventHandler handler)
    {
        var span = packet.Data.Span;
        if (span.Length < UmdfPacketHeader.Size)
            return;

        ref readonly var header = ref UmdfPacketHeader.Read(span);
        int offset = UmdfPacketHeader.Size;

        for (int i = 0; i < header.MessageCount; i++)
        {
            if (offset + SbeHeaderSize > span.Length)
                break;

            ushort blockLength = MemoryMarshal.Read<ushort>(span[offset..]);
            ushort templateId = MemoryMarshal.Read<ushort>(span[(offset + 2)..]);

            var messageSpan = span[offset..];
            handler.OnPacket(in packet, messageSpan, templateId);

            // Advance past SBE header + block
            offset += SbeHeaderSize + blockLength;
        }
    }
}
