using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Transport.Tests;

public class MulticastPacketSourceBatchTests
{
    [Fact]
    public void LinuxNative_StructsHaveExpectedSizes()
    {
        if (!OperatingSystem.IsLinux()) return;

        // x86_64 Linux glibc/musl: iovec=16, msghdr=56, mmsghdr=64.
        Assert.Equal(16, System.Runtime.InteropServices.Marshal.SizeOf<LinuxNative.Iovec>());
        Assert.Equal(56, System.Runtime.InteropServices.Marshal.SizeOf<LinuxNative.Msghdr>());
        Assert.Equal(64, System.Runtime.InteropServices.Marshal.SizeOf<LinuxNative.Mmsghdr>());
    }

    [Fact]
    public void ReceiveBatch_DeliversMultipleDatagramsInOrder()
    {
        if (!OperatingSystem.IsLinux()) return;

        // Loopback multicast on a fixed group/port. WSL2/Docker host loopback supports multicast.
        var group = IPAddress.Parse("239.99.99.99");
        int port = GetEphemeralPort();
        var loopback = IPAddress.Loopback;

        var config = new ChannelConfig(
            ChannelId: 99,
            Type: ChannelType.IncrementalA,
            MulticastGroup: group,
            Port: port,
            SourceAddress: null,
            LocalAddress: loopback,
            ReceiveBufferBytes: 1 * 1024 * 1024,
            ChannelGroup: 0,
            ReceiveSocketCount: 1);

        using var src = new MulticastPacketSource(config, NullLogger<MulticastPacketSource>.Instance);

        // Sender bound to the loopback interface so the multicast goes out on lo.
        using var sender = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        sender.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastInterface, loopback.GetAddressBytes());
        sender.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 1);
        var dest = new IPEndPoint(group, port);

        const int N = 10;
        // Send before the receiver starts blocking — datagrams accumulate in the kernel buffer.
        for (int i = 0; i < N; i++)
        {
            var payload = new byte[] { (byte)i, 0xAA, 0xBB, 0xCC };
            sender.SendTo(payload, dest);
        }

        // Give kernel a brief moment.
        Thread.Sleep(100);

        var batch = new UmdfPacket[MulticastPacketSource.MaxBatchSize];
        int total = 0;
        var values = new List<byte>();
        while (total < N)
        {
            int n = src.ReceiveBatch(batch);
            for (int i = 0; i < n; i++)
            {
                Assert.Equal(4, batch[i].Data.Length);
                values.Add(batch[i].Data.Span[0]);
                batch[i].Lease?.Release();
            }
            total += n;
        }

        Assert.Equal(Enumerable.Range(0, N).Select(i => (byte)i).ToArray(), values.Take(N).ToArray());
    }

    [Fact]
    public void Dispose_ReleasesBatchStateReferences()
    {
        if (!OperatingSystem.IsLinux()) return;

        var group = IPAddress.Parse("239.99.99.98");
        int port = GetEphemeralPort();
        var loopback = IPAddress.Loopback;
        var config = new ChannelConfig(
            ChannelId: 99,
            Type: ChannelType.IncrementalA,
            MulticastGroup: group,
            Port: port,
            SourceAddress: null,
            LocalAddress: loopback,
            ReceiveBufferBytes: 1 * 1024 * 1024,
            ChannelGroup: 0,
            ReceiveSocketCount: 1);

        var src = new MulticastPacketSource(config, NullLogger<MulticastPacketSource>.Instance);

        // Trigger EnsureBatchStateInitialized via a non-blocking ReceiveBatch.
        // Empty kernel queue -> recvmmsg with MSG_WAITFORONE would block, so
        // we send 1 packet first to ensure the call returns immediately.
        using (var sender = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp))
        {
            sender.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastInterface, loopback.GetAddressBytes());
            sender.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 1);
            sender.SendTo(new byte[] { 0x01 }, new IPEndPoint(group, port));
        }
        Thread.Sleep(50);
        var batch = new UmdfPacket[MulticastPacketSource.MaxBatchSize];
        int n = src.ReceiveBatch(batch);
        for (int i = 0; i < n; i++) batch[i].Lease?.Release();

        Assert.True(src.BatchStateAllocatedForTests,
            "ReceiveBatch must have initialized the POH buffer pool / pinned iovec arrays");

        src.Dispose();

        Assert.False(src.BatchStateAllocatedForTests,
            "Dispose must drop references to the POH buffer pool so GC can reclaim ~576KB of pinned buffers promptly, " +
            "rather than waiting for the source object itself to become unreferenced");
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        if (!OperatingSystem.IsLinux()) return;

        var config = new ChannelConfig(
            ChannelId: 99,
            Type: ChannelType.IncrementalA,
            MulticastGroup: IPAddress.Parse("239.99.99.97"),
            Port: GetEphemeralPort(),
            SourceAddress: null,
            LocalAddress: IPAddress.Loopback,
            ReceiveBufferBytes: 1 * 1024 * 1024,
            ChannelGroup: 0,
            ReceiveSocketCount: 1);

        var src = new MulticastPacketSource(config, NullLogger<MulticastPacketSource>.Instance);
        src.Dispose();
        // Second Dispose must be safe (no double-Free of GCHandles, no NRE on
        // null batch state).
        src.Dispose();
    }

    private static int GetEphemeralPort()
    {
        using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)probe.LocalEndPoint!).Port;
    }
}
