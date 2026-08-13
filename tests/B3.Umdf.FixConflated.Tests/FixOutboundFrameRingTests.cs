namespace B3.Umdf.FixConflated.Tests;

public sealed class FixOutboundFrameRingTests
{
    [Fact]
    public void TryEnqueue_ReturnsFalse_WhenRingIsFull()
    {
        using var ring = new FixOutboundFrameRing(2);

        Assert.True(ring.TryEnqueue([0x01]));
        Assert.True(ring.TryEnqueue([0x02]));
        Assert.False(ring.TryEnqueue([0x03]));
    }

    [Fact]
    public void TryEnqueue_And_Dequeue_WrapAround_PreservesOrder()
    {
        byte[] first = [0x01];
        byte[] second = [0x02];
        byte[] third = [0x03];
        byte[] fourth = [0x04];

        using var ring = new FixOutboundFrameRing(2);
        Assert.True(ring.TryEnqueue(first));
        Assert.True(ring.TryEnqueue(second));

        Assert.True(ring.TryDequeue(out byte[]? dequeued));
        Assert.Same(first, dequeued);

        Assert.True(ring.TryEnqueue(third));

        Assert.True(ring.TryDequeue(out dequeued));
        Assert.Same(second, dequeued);

        Assert.True(ring.TryEnqueue(fourth));

        Assert.True(ring.TryDequeue(out dequeued));
        Assert.Same(third, dequeued);
        Assert.True(ring.TryDequeue(out dequeued));
        Assert.Same(fourth, dequeued);
        Assert.False(ring.TryDequeue(out _));
    }

    [Fact]
    public async Task WaitForItems_Unblocks_WhenProducerEnqueues()
    {
        using var ring = new FixOutboundFrameRing(4);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var waiterStarted = new ManualResetEventSlim(false);

        Task waitTask = Task.Run(() =>
        {
            waiterStarted.Set();
            ring.WaitForItems(cts.Token);
        }, cts.Token);

        waiterStarted.Wait(cts.Token);
        await Task.Delay(50, cts.Token);
        Assert.False(waitTask.IsCompleted);

        byte[] payload = [0x2A];
        Assert.True(ring.TryEnqueue(payload));

        await waitTask.WaitAsync(cts.Token);
        Assert.True(ring.TryDequeue(out byte[]? dequeued));
        Assert.Same(payload, dequeued);
    }
}
