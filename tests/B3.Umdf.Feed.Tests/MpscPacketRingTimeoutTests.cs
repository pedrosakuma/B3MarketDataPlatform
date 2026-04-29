using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

public class MpscPacketRingTimeoutTests
{
    [Fact]
    public void WaitForItems_Timeout_ReturnsFalse_WhenNoItemArrives()
    {
        var ring = new MpscPacketRing(8);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        bool got = ring.WaitForItems(50, CancellationToken.None);
        sw.Stop();

        Assert.False(got);
        // Allow generous slack for CI scheduler jitter; we just want to ensure
        // we did wait roughly the timeout, not the legacy Infinite.
        Assert.True(sw.ElapsedMilliseconds >= 30, $"Returned too early: {sw.ElapsedMilliseconds}ms");
        Assert.True(sw.ElapsedMilliseconds < 5000, $"Did not respect timeout: {sw.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void WaitForItems_Timeout_ReturnsTrue_WhenItemAlreadyEnqueuedBeforeWait()
    {
        var ring = new MpscPacketRing(8);
        var lease = new TestPacketLease();
        ring.TryEnqueue(MakePacket(1, lease));

        bool got = ring.WaitForItems(1000, CancellationToken.None);

        Assert.True(got, "Expected synchronous return when item already present");
        Assert.True(ring.TryDequeue(out var packet));
        packet.Release();
    }

    [Fact]
    public async Task WaitForItems_Timeout_WakesOnAsyncEnqueue()
    {
        var ring = new MpscPacketRing(8);
        var lease = new TestPacketLease();

        var waitTask = Task.Run(() => ring.WaitForItems(2000, CancellationToken.None));

        // Give the consumer time to enter the wait, then enqueue.
        await Task.Delay(50);
        ring.TryEnqueue(MakePacket(1, lease));

        bool got = await waitTask.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.True(got);
        Assert.True(ring.TryDequeue(out var packet));
        packet.Release();
    }

    [Fact]
    public void WaitForItems_Timeout_Cancellation_Throws()
    {
        var ring = new MpscPacketRing(8);
        using var cts = new CancellationTokenSource(50);
        Assert.Throws<OperationCanceledException>(() => ring.WaitForItems(5000, cts.Token));
    }

    private static UmdfPacket MakePacket(uint seqNum, TestPacketLease lease)
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

    private sealed class TestPacketLease : UmdfPacketLease
    {
        public override void Retain() { }
        public override void Release() { }
    }
}
