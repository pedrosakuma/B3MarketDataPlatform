using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Transport.Tests;

/// <summary>
/// Tests for the oversized-datagram detection path in <see cref="MulticastPacketSource"/>.
/// Covers the in-process truncation-flag helper directly, plus a loopback-multicast
/// integration test that exercises the kernel MSG_TRUNC path end-to-end.
/// </summary>
public class MulticastPacketSourceTruncationTests
{
    [Fact]
    public void IsKernelTruncated_DirectMsgTruncFlag_ReturnsTrue()
    {
        // Direct flag: regardless of length vs cap, the kernel told us the datagram was truncated.
        Assert.True(MulticastPacketSource.IsKernelTruncated(LinuxNative.MSG_TRUNC, receivedLen: 100, bufferCap: 9216));
    }

    [Fact]
    public void IsKernelTruncated_NoFlagAndShortDatagram_ReturnsFalse()
    {
        Assert.False(MulticastPacketSource.IsKernelTruncated(msgFlags: 0, receivedLen: 1500, bufferCap: 9216));
    }

    [Fact]
    public void IsKernelTruncated_NoFlagButReceivedAtCap_HeuristicReturnsTrue()
    {
        // Heuristic fallback for kernels that don't propagate per-message MSG_TRUNC through recvmmsg.
        Assert.True(MulticastPacketSource.IsKernelTruncated(msgFlags: 0, receivedLen: 9216, bufferCap: 9216));
    }

    [Fact]
    public void IsKernelTruncated_OtherFlagsDoNotTrigger()
    {
        // Flags unrelated to truncation (e.g. MSG_WAITFORONE) must not be misread.
        Assert.False(MulticastPacketSource.IsKernelTruncated(LinuxNative.MSG_WAITFORONE, receivedLen: 100, bufferCap: 9216));
    }

    [Fact]
    public void IsKernelTruncated_MaximumUdpPayloadAtCap_IsValid()
    {
        Assert.False(MulticastPacketSource.IsKernelTruncated(
            msgFlags: 0,
            receivedLen: MulticastPacketSource.MaximumUdpPayloadBytes,
            bufferCap: MulticastPacketSource.MaximumUdpPayloadBytes));
    }

    [Fact]
    public void ReceiveBatch_SnapshotDatagramAboveLegacyCap_IsDeliveredIntact()
    {
        if (!OperatingSystem.IsLinux()) return;

        using var src = NewSource(ChannelType.SnapshotRecovery);
        const int payloadLength = 32 * 1024;
        var expected = new byte[payloadLength];
        for (int i = 0; i < expected.Length; i++)
            expected[i] = (byte)(i % 251);

        src._recvmmsgInvoker = (int fd, IntPtr msgvec, uint vlen, int flags, out int errno) =>
        {
            errno = 0;
            WriteDatagram(msgvec, expected, SocketFlags.None);
            return 1;
        };

        var batch = new UmdfPacket[1];
        Assert.Equal(1, src.ReceiveBatch(batch));
        try
        {
            Assert.Equal(payloadLength, batch[0].Data.Length);
            Assert.Equal(expected, batch[0].Data.ToArray());
            Assert.Equal(0, src.TruncatedDatagramCount);
        }
        finally
        {
            batch[0].Release();
        }
    }

    [Fact]
    public void ReceiveBatch_TruncatedDatagramThenValidTraffic_RecoversWithoutRestart()
    {
        if (!OperatingSystem.IsLinux()) return;

        using var src = NewSource(ChannelType.IncrementalA, maxDatagramBytes: 1024);
        int invocation = 0;
        byte[] valid = [0x11, 0x22, 0x33, 0x44];
        src._recvmmsgInvoker = (int fd, IntPtr msgvec, uint vlen, int flags, out int errno) =>
        {
            errno = 0;
            invocation++;
            if (invocation == 1)
            {
                WriteDatagram(msgvec, new byte[1024], (SocketFlags)LinuxNative.MSG_TRUNC);
                return 1;
            }

            WriteDatagram(msgvec, valid, SocketFlags.None);
            return 1;
        };

        var batch = new UmdfPacket[1];
        Assert.Equal(0, src.ReceiveBatch(batch));
        Assert.Equal(1, src.TruncatedDatagramCount);

        Assert.Equal(1, src.ReceiveBatch(batch));
        try
        {
            Assert.Equal(valid, batch[0].Data.ToArray());
            Assert.Equal(1, src.TruncatedDatagramCount);
        }
        finally
        {
            batch[0].Release();
        }
    }

    private static MulticastPacketSource NewSource(ChannelType type, int maxDatagramBytes = 0)
    {
        var config = new ChannelConfig(
            ChannelId: 99,
            Type: type,
            MulticastGroup: IPAddress.Any,
            Port: 0,
            ReceiveBufferBytes: 1 * 1024 * 1024,
            Transport: TransportKind.Unicast,
            MaxDatagramBytes: maxDatagramBytes);
        return new MulticastPacketSource(config, NullLogger<MulticastPacketSource>.Instance);
    }

    private static void WriteDatagram(IntPtr msgvec, byte[] payload, SocketFlags flags)
    {
        var header = Marshal.PtrToStructure<LinuxNative.Mmsghdr>(msgvec);
        var iovec = Marshal.PtrToStructure<LinuxNative.Iovec>(header.msg_hdr.msg_iov);
        Assert.True((nuint)payload.Length <= iovec.iov_len);
        Marshal.Copy(payload, 0, iovec.iov_base, payload.Length);
        header.msg_len = (uint)payload.Length;
        header.msg_hdr.msg_flags = (int)flags;
        Marshal.StructureToPtr(header, msgvec, fDeleteOld: false);
    }
}
