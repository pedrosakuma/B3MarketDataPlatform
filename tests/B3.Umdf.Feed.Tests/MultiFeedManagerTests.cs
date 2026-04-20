using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

public class MultiFeedManagerTests
{
    [Fact]
    public async Task InlineDispatch_ProcessesAllPackets_AndReleasesLeases()
    {
        // With inline dispatch (Option A) there is no internal queue. The dispatch thread
        // drains the source synchronously by calling FeedHandler.FeedPacket under the
        // per-group lock; every packet must be processed exactly once and its lease
        // released exactly once.
        var leases = Enumerable.Range(1, 5)
            .Select(_ => new CountingPacketLease())
            .ToArray();
        var packets = leases
            .Select((lease, index) => MakePacket((uint)(index + 1), lease))
            .ToArray();

        using var source = new TestSyncPacketSource(packets);
        var handler = new NoopFeedEventHandler();
        var manager = new MultiFeedManager(source, new[] { 0 }, handler);
        try
        {
            await manager.StartAsync();
            await manager.WaitForCompletionAsync();
        }
        finally
        {
            await manager.DisposeAsync();
        }

        Assert.Equal(packets.Length, manager.TotalPacketCount);
        Assert.All(leases, lease => Assert.Equal(1, lease.ReleaseCount));
        Assert.DoesNotContain(manager.GetChannelStats(), s => s.Depth != 0 || s.DroppedPackets != 0);
    }

    [Fact]
    public void PushPacket_UnknownGroup_ReleasesLease()
    {
        // PushPacket must be defensive about packets routed to an unknown channel group:
        // the lease has to be released so we don't leak rented buffers.
        var lease = new CountingPacketLease();
        var packet = MakePacket(seqNum: 1, lease, channelGroup: 99);

        var handler = new NoopFeedEventHandler();
        using var manager = new MultiFeedManager(new[] { 0 }, handler);

        manager.PushPacket(in packet);

        Assert.Equal(1, lease.ReleaseCount);
        Assert.Equal(0, manager.TotalPacketCount);
    }

    private static UmdfPacket MakePacket(uint seqNum, CountingPacketLease lease, byte channelGroup = 0)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return UmdfPacket.CreateOwned(
            buf,
            ChannelType.IncrementalA,
            channelGroup: channelGroup,
            receivedTimestampTicks: 1,
            lease);
    }

    private sealed class TestSyncPacketSource : IPacketSource, ISyncPacketSource
    {
        private readonly Queue<UmdfPacket> _packets;

        public TestSyncPacketSource(IEnumerable<UmdfPacket> packets)
        {
            _packets = new Queue<UmdfPacket>(packets);
        }

        public bool TryReceive(out UmdfPacket packet)
        {
            lock (_packets)
            {
                if (_packets.Count > 0)
                {
                    packet = _packets.Dequeue();
                    return true;
                }
            }

            packet = default;
            return false;
        }

        public ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
        {
            if (TryReceive(out var packet))
                return ValueTask.FromResult(packet);

            return ValueTask.FromException<UmdfPacket>(new InvalidOperationException("No packets remaining."));
        }

        public void Dispose() { }
    }

    private sealed class NoopFeedEventHandler : IFeedEventHandler
    {
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnGapDetected(uint expected, uint received) { }
        public void OnSequenceReset() { }
        public void OnSnapshotStart() { }
        public void OnSnapshotComplete(uint lastRptSeq) { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnPacketProcessed() { }
    }

    private sealed class CountingPacketLease : UmdfPacketLease
    {
        private int _refCount = 1;
        public int ReleaseCount { get; private set; }

        public override void Retain() => Interlocked.Increment(ref _refCount);

        public override void Release()
        {
            if (Interlocked.Decrement(ref _refCount) == 0)
                ReleaseCount++;
        }
    }
}
