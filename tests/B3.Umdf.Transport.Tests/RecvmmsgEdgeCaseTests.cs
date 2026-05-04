using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Transport.Tests;

/// <summary>
/// Edge-case coverage for the recvmmsg(2) path in <see cref="MulticastPacketSource.ReceiveBatch"/>:
///   - syscall returns 0 (no datagrams)
///   - syscall returns N &lt; batch capacity (partial batch)
///   - syscall returns -1 / EINTR (interrupted, must retry without spurious accounting)
///
/// We use Option A (delegate seam) because EINTR cannot be reliably provoked from a real
/// loopback test, and partial-batch / 0-return require deterministic recvmmsg behaviour.
/// </summary>
public class RecvmmsgEdgeCaseTests
{
    [Fact]
    public void ReceiveBatch_RecvmmsgReturnsZero_NoDeadlockNoDispatchNoCounterIncrement()
    {
        if (!OperatingSystem.IsLinux()) return;

        using var src = NewSource(out _);
        src._recvmmsgInvoker = (int fd, IntPtr msgvec, uint vlen, int flags, out int errno) =>
        {
            errno = 0;
            return 0; // no datagrams ready
        };

        long syscallsBefore = src.BatchedSyscalls;
        long datagramsBefore = src.BatchedDatagrams;

        var batch = new UmdfPacket[MulticastPacketSource.MaxBatchSize];
        int delivered = src.ReceiveBatch(batch);

        Assert.Equal(0, delivered);
        // No spurious counter bumps when nothing was received.
        Assert.Equal(syscallsBefore, src.BatchedSyscalls);
        Assert.Equal(datagramsBefore, src.BatchedDatagrams);
        // No leases handed out — every slot in the destination must be its default value.
        for (int i = 0; i < batch.Length; i++)
            Assert.Null(batch[i].Lease);
    }

    [Fact]
    public void ReceiveBatch_PartialBatch_OnlyDispatchesNAndDoesNotDoubleReleaseUnusedSlots()
    {
        if (!OperatingSystem.IsLinux()) return;

        using var src = NewSource(out _);

        const int partialN = 3;
        const int payloadLen = 17;

        src._recvmmsgInvoker = (int fd, IntPtr msgvec, uint vlen, int flags, out int errno) =>
        {
            errno = 0;
            // Mark only the first partialN slots as having received `payloadLen` bytes.
            // msg_len is the field in mmsghdr at offset 56 (after the embedded msghdr).
            // Use Marshal helpers so we don't duplicate the layout knowledge.
            int mmsghdrSize = Marshal.SizeOf<LinuxNative.Mmsghdr>();
            for (int i = 0; i < partialN; i++)
            {
                IntPtr slot = msgvec + i * mmsghdrSize;
                var hdr = Marshal.PtrToStructure<LinuxNative.Mmsghdr>(slot);
                hdr.msg_len = (uint)payloadLen;
                hdr.msg_hdr.msg_flags = 0;
                Marshal.StructureToPtr(hdr, slot, fDeleteOld: false);
            }
            return partialN;
        };

        var batch = new UmdfPacket[MulticastPacketSource.MaxBatchSize];
        int delivered = src.ReceiveBatch(batch);

        Assert.Equal(partialN, delivered);
        Assert.Equal(1, src.BatchedSyscalls);
        Assert.Equal(partialN, src.BatchedDatagrams);

        // Delivered slots must have a lease and the advertised payload length.
        for (int i = 0; i < partialN; i++)
        {
            Assert.NotNull(batch[i].Lease);
            Assert.Equal(payloadLen, batch[i].Data.Length);
        }
        // Unused slots must NOT have been surfaced as packets — destination[partialN..] is
        // the caller's territory and the implementation must not have written leases there.
        for (int i = partialN; i < batch.Length; i++)
            Assert.Null(batch[i].Lease);

        // Releasing the delivered leases must succeed exactly once each. A double-release
        // of an unused slot's pre-rented buffer would manifest as PinnedPoolPacketLease
        // throwing on the second Release; we exercise the released-once contract here.
        for (int i = 0; i < partialN; i++)
            batch[i].Lease!.Release();

        // A subsequent ReceiveBatch with the same fake (returning 0) must not throw —
        // confirms the pool / iovec state for the unused slots was not corrupted.
        src._recvmmsgInvoker = (int fd, IntPtr msgvec, uint vlen, int flags, out int errno) =>
        {
            errno = 0;
            return 0;
        };
        Assert.Equal(0, src.ReceiveBatch(batch));
    }

    [Fact]
    public void ReceiveBatch_EintrThenSuccess_RetriesCleanlyWithoutSpuriousAccounting()
    {
        if (!OperatingSystem.IsLinux()) return;

        using var src = NewSource(out _);

        int callCount = 0;
        const int payloadLen = 42;
        const int returnedN = 2;

        src._recvmmsgInvoker = (int fd, IntPtr msgvec, uint vlen, int flags, out int errno) =>
        {
            callCount++;
            if (callCount == 1)
            {
                // Simulate a kernel-interrupted syscall: -1 with EINTR.
                errno = LinuxNative.EINTR;
                return -1;
            }
            errno = 0;
            int mmsghdrSize = Marshal.SizeOf<LinuxNative.Mmsghdr>();
            for (int i = 0; i < returnedN; i++)
            {
                IntPtr slot = msgvec + i * mmsghdrSize;
                var hdr = Marshal.PtrToStructure<LinuxNative.Mmsghdr>(slot);
                hdr.msg_len = (uint)payloadLen;
                hdr.msg_hdr.msg_flags = 0;
                Marshal.StructureToPtr(hdr, slot, fDeleteOld: false);
            }
            return returnedN;
        };

        long truncatedBefore = src.TruncatedDatagramCount;

        var batch = new UmdfPacket[MulticastPacketSource.MaxBatchSize];
        int delivered = src.ReceiveBatch(batch);

        // Retried exactly once, then succeeded.
        Assert.Equal(2, callCount);
        Assert.Equal(returnedN, delivered);
        // EINTR must NOT inflate the truncated/drop counter — it is not a drop.
        Assert.Equal(truncatedBefore, src.TruncatedDatagramCount);
        // Syscall counter bumps once per *successful* batch (n>0), not per attempt.
        Assert.Equal(1, src.BatchedSyscalls);
        Assert.Equal(returnedN, src.BatchedDatagrams);

        for (int i = 0; i < delivered; i++)
        {
            Assert.NotNull(batch[i].Lease);
            Assert.Equal(payloadLen, batch[i].Data.Length);
            batch[i].Lease!.Release();
        }
    }

    private static MulticastPacketSource NewSource(out int port)
    {
        port = GetEphemeralPort();
        var config = new ChannelConfig(
            ChannelId: 99,
            Type: ChannelType.IncrementalA,
            MulticastGroup: IPAddress.Parse("239.99.88.77"),
            Port: port,
            SourceAddress: null,
            LocalAddress: IPAddress.Loopback,
            ReceiveBufferBytes: 1 * 1024 * 1024,
            ChannelGroup: 0,
            ReceiveSocketCount: 1);
        return new MulticastPacketSource(config, NullLogger<MulticastPacketSource>.Instance);
    }

    private static int GetEphemeralPort()
    {
        using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)probe.LocalEndPoint!).Port;
    }
}
