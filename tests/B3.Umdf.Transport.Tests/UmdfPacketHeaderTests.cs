using System.Runtime.InteropServices;
using B3.Umdf.Transport;

namespace B3.Umdf.Transport.Tests;

public class UmdfPacketHeaderTests
{
    [Fact]
    public void Size_Is_14_Bytes()
    {
        Assert.Equal(14, UmdfPacketHeader.Size);
        Assert.Equal(14, Marshal.SizeOf<UmdfPacketHeader>());
    }

    [Fact]
    public void Read_ParsesFieldsCorrectly()
    {
        var buffer = new byte[14];
        // SequenceNumber = 42 (uint32 LE)
        BitConverter.TryWriteBytes(buffer.AsSpan(0), (uint)42);
        // SendingTime = 1234567890 (uint64 LE)
        BitConverter.TryWriteBytes(buffer.AsSpan(4), (ulong)1234567890);
        // MessageCount = 3 (uint16 LE)
        BitConverter.TryWriteBytes(buffer.AsSpan(12), (ushort)3);

        ref readonly var header = ref UmdfPacketHeader.Read(buffer);

        Assert.Equal(42u, header.SequenceNumber);
        Assert.Equal(1234567890ul, header.SendingTime);
        Assert.Equal(3, header.MessageCount);
    }
}
