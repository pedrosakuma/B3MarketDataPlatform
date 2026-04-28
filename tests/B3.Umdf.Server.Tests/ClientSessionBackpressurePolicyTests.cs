using System.Buffers;
using System.Threading;
using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// P12-10 — pins slow-client backpressure invariants on the batch
/// enqueue path. Existing tests cover the per-message bytes/depth
/// budgets and the outlier sweep; these tests close the
/// <see cref="ClientSession.TryEnqueueBatch"/> gap (used by
/// <c>GroupConflationHandler</c> for coalesced flushes) and the
/// post-disconnect contract (subsequent enqueues must be silent
/// no-ops returning false).
/// </summary>
public class ClientSessionBackpressurePolicyTests
{
    [Fact]
    public void TryEnqueueBatch_BytesBudgetExceeded_DisconnectsAndReturnsPool()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(
            ws,
            channelCapacity: 1024,
            maxPendingBytes: 16);

        // Pre-fill the budget to within a hair of the cap.
        Assert.True(session.TryEnqueue(new byte[10]));
        Assert.Equal(10, session.PendingBytes);

        // Coalesced batch with a pooled buffer pushes the budget over.
        // Contract: returns false, trips the slow-consumer disconnect, and
        // returns the pooled array (no leak).
        var pool = ArrayPool<byte>.Shared.Rent(64);
        bool accepted = session.TryEnqueueBatch(
            new ReadOnlyMemory<byte>(pool, 0, 32),
            logicalMessageCount: 4,
            pooledArray: pool);

        Assert.False(accepted);
        Assert.True(session.CancellationToken.IsCancellationRequested);
        // Bytes counter is unchanged when the new payload is rejected.
        Assert.Equal(10, session.PendingBytes);
    }

    [Fact]
    public void TryEnqueueBatch_OnAlreadyCancelledSession_ReturnsFalseAndPool()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws, channelCapacity: 1024, maxPendingBytes: 0);
        session.Cancel();

        var pool = ArrayPool<byte>.Shared.Rent(32);
        bool accepted = session.TryEnqueueBatch(
            new ReadOnlyMemory<byte>(pool, 0, 16),
            logicalMessageCount: 2,
            pooledArray: pool);

        Assert.False(accepted);
        // Did not advance queue depth.
        Assert.Equal(0, session.QueueDepth);
    }

    [Fact]
    public void TryEnqueueBatch_EmptyBatch_NoOpReturnsTrueAndReleasesPool()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws, channelCapacity: 1024, maxPendingBytes: 0);
        var pool = ArrayPool<byte>.Shared.Rent(32);

        // Empty batch (Memory.IsEmpty) must short-circuit and still return
        // the pooled array — caller should not have to special-case zero
        // logical count after a flush race.
        bool ok = session.TryEnqueueBatch(
            ReadOnlyMemory<byte>.Empty,
            logicalMessageCount: 0,
            pooledArray: pool);

        Assert.True(ok);
        Assert.Equal(0, session.QueueDepth);
        Assert.False(session.CancellationToken.IsCancellationRequested);
    }

    [Fact]
    public void Disconnect_SubsequentEnqueuesAreSilentNoOps()
    {
        var ws = new FakeWebSocket();
        var session = new ClientSession(ws, channelCapacity: 1024, maxPendingBytes: 32);

        // Trigger disconnect via budget overflow.
        Assert.True(session.TryEnqueue(new byte[20]));
        Assert.False(session.TryEnqueue(new byte[20])); // 20+20 > 32
        Assert.True(session.CancellationToken.IsCancellationRequested);

        int depthAtDisconnect = session.QueueDepth;
        long bytesAtDisconnect = session.PendingBytes;

        // After disconnect, every producer call must return false without
        // mutating queue depth / bytes counter (cancellation observed at
        // the head of TryEnqueueCore). Pin this contract: the caller may
        // continue producing for a few cycles before noticing the cancel
        // token; we cannot let those late writes inflate the queue or
        // double-account bytes.
        Assert.False(session.TryEnqueue(new byte[1]));
        Assert.False(session.TryEnqueue(new byte[1]));
        Assert.False(session.NotifyInfoAvailable());
        var pool = ArrayPool<byte>.Shared.Rent(8);
        Assert.False(session.TryEnqueueBatch(new ReadOnlyMemory<byte>(pool, 0, 4), 1, pool));

        Assert.Equal(depthAtDisconnect, session.QueueDepth);
        Assert.Equal(bytesAtDisconnect, session.PendingBytes);
    }

    [Fact]
    public void ConcurrentBudgetEnforcement_DoesNotDoubleAccountOnFailure()
    {
        // Race: many producers attempting to enqueue past the cap concurrently.
        // The bytes counter must never exceed the budget, and after the
        // disconnect trips the counter must reflect only successfully-enqueued
        // payloads (no double-decrement on the failure release path).
        var ws = new FakeWebSocket();
        var session = new ClientSession(
            ws,
            channelCapacity: 1024,
            maxPendingBytes: 1024);

        const int producers = 8;
        const int perProducer = 200;
        const int payloadSize = 16; // 8 * 200 * 16 = 25_600 bytes attempted vs 1024 cap

        var threads = new Thread[producers];
        for (int p = 0; p < producers; p++)
        {
            threads[p] = new Thread(() =>
            {
                var msg = new byte[payloadSize];
                for (int i = 0; i < perProducer; i++)
                    session.TryEnqueue(msg);
            });
            threads[p].IsBackground = true;
        }
        foreach (var t in threads) t.Start();
        foreach (var t in threads) t.Join();

        // After the storm, session is disconnected and pendingBytes is
        // bounded by the cap (whatever was successfully enqueued before
        // the trip). Must not be negative or beyond the budget.
        Assert.True(session.CancellationToken.IsCancellationRequested);
        Assert.InRange(session.PendingBytes, 0L, 1024L);
    }
}
