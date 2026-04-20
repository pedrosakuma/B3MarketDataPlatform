using System.Buffers.Binary;
using System.Threading.Channels;
using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Transport.Tests;

public class MulticastChannelMergerTests
{
    [Fact]
    public async Task QueueOverflow_DropsOldestAndCountsDrops()
    {
        var source = new TestPacketSource();
        using var merger = new MulticastChannelMerger([source], capacity: 2);

        var firstLease = new CountingPacketLease();
        var secondLease = new CountingPacketLease();
        var thirdLease = new CountingPacketLease();

        await source.WriteAsync(MakePacket(1, firstLease));
        await source.WriteAsync(MakePacket(2, secondLease));
        await source.WriteAsync(MakePacket(3, thirdLease));

        await WaitForAsync(() => merger.DroppedPackets == 1 && merger.QueueDepth == 2);
        Assert.Equal(1, firstLease.ReleaseCount);
        Assert.Equal(0, secondLease.ReleaseCount);
        Assert.Equal(0, thirdLease.ReleaseCount);

        var first = await merger.ReceiveAsync();
        var second = await merger.ReceiveAsync();

        first.Release();
        second.Release();

        Assert.Equal(2u, ReadSequence(first));
        Assert.Equal(3u, ReadSequence(second));
        Assert.Equal(1, merger.DroppedPackets);
        Assert.Equal(0, merger.QueueDepth);
        Assert.Equal(1, secondLease.ReleaseCount);
        Assert.Equal(1, thirdLease.ReleaseCount);
    }

    private static async Task WaitForAsync(Func<bool> predicate)
    {
        for (int i = 0; i < 50; i++)
        {
            if (predicate())
                return;

            await Task.Delay(10);
        }

        Assert.True(predicate());
    }

    private static UmdfPacket MakePacket(uint seqNum, UmdfPacketLease lease)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return UmdfPacket.CreateOwned(
            buf,
            ChannelType.IncrementalA,
            channelGroup: 0,
            receivedTimestampTicks: 1,
            lease);
    }

    private static uint ReadSequence(UmdfPacket packet)
    {
        Assert.True(PacketHeader.TryParse(packet.Data.Span, out var header, out _));
        return header.SequenceNumber;
    }

    private sealed class TestPacketSource : IPacketSource
    {
        private readonly Channel<UmdfPacket> _channel = Channel.CreateUnbounded<UmdfPacket>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        public ValueTask WriteAsync(UmdfPacket packet) => _channel.Writer.WriteAsync(packet);

        public async ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default) =>
            await _channel.Reader.ReadAsync(ct);

        public void Dispose() => _channel.Writer.TryComplete();
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
