namespace B3.Umdf.Transport.Tests;

public class UmdfPacketLeaseTests
{
    [Fact]
    public void RetainAndRelease_ReferenceCountedLease_ReleasesOnce()
    {
        var lease = new CountingPacketLease();
        var packet = UmdfPacket.CreateOwned(
            new byte[16],
            ChannelType.IncrementalA,
            channelGroup: 0,
            receivedTimestampTicks: 1,
            lease);

        packet.Retain();
        packet.Release();
        Assert.Equal(0, lease.ReleaseCount);

        packet.Release();
        Assert.Equal(1, lease.ReleaseCount);
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
