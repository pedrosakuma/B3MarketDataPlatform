using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Transport.Tests;

/// <summary>
/// Tests for the oversized-datagram detection path in <see cref="MulticastPacketSource"/>.
/// Covers the policy helper directly plus real loopback UDP delivery at and above
/// a configured cap.
/// </summary>
public class MulticastPacketSourceTruncationTests
{
    [Fact]
    public void IsOversizedOrTruncated_DirectMsgTruncFlag_ReturnsTrue()
    {
        // Direct flag: regardless of length vs cap, the kernel told us the datagram was truncated.
        Assert.True(MulticastPacketSource.IsOversizedOrTruncated(
            LinuxNative.MSG_TRUNC, receivedLen: 100, maxDatagramBytes: 9216));
    }

    [Fact]
    public void IsOversizedOrTruncated_NoFlagAndShortDatagram_ReturnsFalse()
    {
        Assert.False(MulticastPacketSource.IsOversizedOrTruncated(
            msgFlags: 0, receivedLen: 1500, maxDatagramBytes: 9216));
    }

    [Fact]
    public void IsOversizedOrTruncated_NoFlagAndExactlyAtCap_ReturnsFalse()
    {
        Assert.False(MulticastPacketSource.IsOversizedOrTruncated(
            msgFlags: 0, receivedLen: 9216, maxDatagramBytes: 9216));
    }

    [Fact]
    public void IsOversizedOrTruncated_NoFlagAndAboveCap_ReturnsTrue()
    {
        Assert.True(MulticastPacketSource.IsOversizedOrTruncated(
            msgFlags: 0, receivedLen: 9217, maxDatagramBytes: 9216));
    }

    [Fact]
    public void IsOversizedOrTruncated_OtherFlagsDoNotTrigger()
    {
        // Flags unrelated to truncation (e.g. MSG_WAITFORONE) must not be misread.
        Assert.False(MulticastPacketSource.IsOversizedOrTruncated(
            LinuxNative.MSG_WAITFORONE, receivedLen: 100, maxDatagramBytes: 9216));
    }

    [Fact]
    public void IsOversizedOrTruncated_MaximumUdpPayloadAtCap_IsValid()
    {
        Assert.False(MulticastPacketSource.IsOversizedOrTruncated(
            msgFlags: 0,
            receivedLen: MulticastPacketSource.MaximumUdpPayloadBytes,
            maxDatagramBytes: MulticastPacketSource.MaximumUdpPayloadBytes));
    }

    [Fact]
    public void ReceiveBatch_ExactConfiguredCap_RealSocketDeliversIntact()
    {
        if (!OperatingSystem.IsLinux()) return;

        const int cap = 1024;
        int port = GetEphemeralPort();
        using var src = NewSource(ChannelType.IncrementalA, cap, port);
        using var sender = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        var expected = Enumerable.Range(0, cap).Select(i => (byte)(i % 251)).ToArray();
        sender.SendTo(expected, new IPEndPoint(IPAddress.Loopback, port));

        var batch = new UmdfPacket[1];
        Assert.Equal(1, src.ReceiveBatch(batch));
        try
        {
            Assert.Equal(expected, batch[0].Data.ToArray());
            Assert.Equal(0, src.TruncatedDatagramCount);
        }
        finally
        {
            batch[0].Release();
        }
    }

    [Fact]
    public void ReceiveBatch_CapPlusOne_RealSocketDropsThenAcceptsValidTraffic()
    {
        if (!OperatingSystem.IsLinux()) return;

        const int cap = 1024;
        int port = GetEphemeralPort();
        using var src = NewSource(ChannelType.IncrementalA, cap, port);
        using var sender = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        var endpoint = new IPEndPoint(IPAddress.Loopback, port);
        sender.SendTo(new byte[cap + 1], endpoint);

        var batch = new UmdfPacket[1];
        Assert.Equal(0, src.ReceiveBatch(batch));
        Assert.Equal(1, src.TruncatedDatagramCount);

        byte[] valid = [0x11, 0x22, 0x33];
        sender.SendTo(valid, endpoint);
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

    private static MulticastPacketSource NewSource(
        ChannelType type,
        int maxDatagramBytes = 0,
        int port = 0)
    {
        var config = new ChannelConfig(
            ChannelId: 99,
            Type: type,
            MulticastGroup: IPAddress.Loopback,
            Port: port,
            ReceiveBufferBytes: 1 * 1024 * 1024,
            Transport: TransportKind.Unicast,
            MaxDatagramBytes: maxDatagramBytes);
        return new MulticastPacketSource(config, NullLogger<MulticastPacketSource>.Instance);
    }

    private static int GetEphemeralPort()
    {
        using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)probe.LocalEndPoint!).Port;
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
