using System.Runtime.InteropServices;

namespace B3.Umdf.Transport;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct UmdfPacketHeader
{
    public readonly uint SequenceNumber;
    public readonly ulong SendingTime;
    public readonly ushort MessageCount;

    public const int Size = 14;

    public static ref readonly UmdfPacketHeader Read(ReadOnlySpan<byte> buffer)
    {
        return ref MemoryMarshal.AsRef<UmdfPacketHeader>(buffer[..Size]);
    }
}
