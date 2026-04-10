using System.Runtime.InteropServices;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Transport.Tests;

public class UmdfPacketHeaderTests
{
    [Fact]
    public void PacketHeader_Size_Is_16_Bytes()
    {
        Assert.Equal(16, PacketHeader.MESSAGE_SIZE);
        Assert.Equal(16, UmdfPacketHeader.Size);
        Assert.Equal(16, Marshal.SizeOf<PacketHeader>());
    }

    [Fact]
    public void FramingHeader_Size_Is_4_Bytes()
    {
        Assert.Equal(4, FramingHeader.MESSAGE_SIZE);
        Assert.Equal(4, Marshal.SizeOf<FramingHeader>());
    }

    [Fact]
    public void TryRead_ParsesFieldsCorrectly()
    {
        var buffer = new byte[16];
        // ChannelNumber = 52 (uint8)
        buffer[0] = 52;
        // Reserved = 0
        buffer[1] = 0;
        // SequenceVersion = 3582 (uint16 LE)
        BitConverter.TryWriteBytes(buffer.AsSpan(2), (ushort)3582);
        // SequenceNumber = 42 (uint32 LE)
        BitConverter.TryWriteBytes(buffer.AsSpan(4), (uint)42);
        // SendingTime = 1234567890 (uint64 LE)
        BitConverter.TryWriteBytes(buffer.AsSpan(8), (ulong)1234567890);

        Assert.True(UmdfPacketHeader.TryRead(buffer, out var header));
        Assert.Equal((byte)52, header.ChannelNumber);
        Assert.Equal((ushort)3582, header.SequenceVersion);
        Assert.Equal(42u, header.SequenceNumber);
        Assert.Equal(1234567890ul, header.SendingTime);
    }
}
