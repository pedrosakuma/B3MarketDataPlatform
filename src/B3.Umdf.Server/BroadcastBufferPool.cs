using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;

namespace B3.Umdf.Server;

/// <summary>
/// Size-segregated buffer pool tuned for the broadcast → write-loop ownership
/// transfer: a single producer thread (<see cref="GroupConflationHandler"/>'s
/// broadcaster, plus the snapshot path) <see cref="Rent"/>s buffers and hands
/// them, via <c>pooledArray:</c> on <see cref="ClientSession.TryEnqueueBatch"/>,
/// to N per-client write loops that <see cref="Return"/> them after
/// <c>WebSocket.SendAsync</c> completes — typically on a different ThreadPool
/// thread than the one that rented.
///
/// <para>Replaces <see cref="System.Buffers.ArrayPool{T}.Shared"/> on this path
/// only. <c>SharedArrayPool</c> uses a per-thread cache plus a per-bucket
/// <c>Monitor</c>; under our access pattern (1 renter, ~N returners on the
/// ThreadPool) returns continually miss the per-thread cache and contend on the
/// per-bucket lock. Profiling on the 100-client / 5x replay scenario attributed
/// ~47% of all <c>Monitor.Enter_Slowpath</c> samples (~8.6% of total CPU) to
/// these returns from <c>ClientSession.RunWriteLoopAsync</c>.</para>
///
/// <para>Internals: per-bucket <see cref="ConcurrentQueue{T}"/> (lock-free,
/// non-blocking). An approximate per-bucket count guards retained memory.</para>
///
/// <para><b>Ownership invariant</b>: only buffers obtained from this pool may be
/// returned to it. Buffers passed via <c>pooledArray:</c> on the WS outbound
/// path must originate from <see cref="Shared"/>.</para>
/// </summary>
internal sealed class BroadcastBufferPool
{
    // Bucket sizes: 1KiB up to 256KiB. Covers the steady-state batch
    // accumulator range (broadcaster grows geometrically up to a 256KB cap)
    // and most snapshot frames. Rents above the largest bucket allocate raw
    // and are dropped on Return (acceptable for rare deep-book snapshots).
    private static readonly int[] BucketSizes =
        { 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144 };

    // Per-bucket retained-buffer caps tuned to favor smaller buffers (which
    // dominate steady-state) while keeping worst-case retained memory bounded
    // (~58 MiB if every bucket fills to the cap).
    private static readonly int[] MaxBuffersByBucket =
        { 2048, 2048, 1024, 1024, 512, 256, 128, 64, 32 };

    public static readonly BroadcastBufferPool Shared = new();

    private readonly ConcurrentQueue<byte[]>[] _buckets;
    // Approximate per-bucket retained count. Intentionally not linearizable
    // with the queue: the increment-then-Enqueue (decrement-then-Dequeue)
    // ordering can briefly overstate count under producer/consumer races,
    // causing a small number of extra drops/misses, which is safe.
    private readonly int[] _counts;

    public BroadcastBufferPool()
    {
        _buckets = new ConcurrentQueue<byte[]>[BucketSizes.Length];
        for (int i = 0; i < BucketSizes.Length; i++)
            _buckets[i] = new ConcurrentQueue<byte[]>();
        _counts = new int[BucketSizes.Length];
    }

    /// <summary>
    /// Rent a buffer of at least <paramref name="minimumSize"/> bytes. The
    /// returned array's <c>Length</c> is the bucket size (so <see cref="Return"/>
    /// can identify the bucket by exact length). For requests larger than the
    /// largest bucket, allocates a raw <c>byte[minimumSize]</c> that will be
    /// dropped to the GC on <see cref="Return"/>.
    /// </summary>
    public byte[] Rent(int minimumSize)
    {
        if (minimumSize < 0)
            throw new ArgumentOutOfRangeException(nameof(minimumSize));

        int idx = BucketIndex(minimumSize);
        if (idx < 0)
        {
            MetricsRegistry.BroadcastBufferOversizeRents.Add(1);
            return new byte[minimumSize == 0 ? 1 : minimumSize];
        }

        if (_buckets[idx].TryDequeue(out var buf))
        {
            Interlocked.Decrement(ref _counts[idx]);
            MetricsRegistry.BroadcastBufferRentHits.Add(1);
            return buf;
        }

        MetricsRegistry.BroadcastBufferRentMisses.Add(1);
        return new byte[BucketSizes[idx]];
    }

    /// <summary>
    /// Return a buffer previously obtained from <see cref="Rent"/>. Buffers
    /// whose length does not match a bucket size (oversize raw allocations
    /// or arrays from elsewhere) are dropped to the GC.
    /// </summary>
    public void Return(byte[] buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);

        int idx = BucketIndexExact(buffer.Length);
        if (idx < 0)
        {
            MetricsRegistry.BroadcastBufferReturnDrops.Add(1);
            return;
        }

        // Reserve a slot before enqueuing so two concurrent returners can't
        // both believe they fit when only one slot remains.
        int after = Interlocked.Increment(ref _counts[idx]);
        if (after > MaxBuffersByBucket[idx])
        {
            Interlocked.Decrement(ref _counts[idx]);
            MetricsRegistry.BroadcastBufferReturnDrops.Add(1);
            return;
        }

        _buckets[idx].Enqueue(buffer);
    }

    /// <summary>Approximate retained buffer count in the given bucket index.</summary>
    public int ApproximateCount(int bucketIndex) => Volatile.Read(ref _counts[bucketIndex]);

    /// <summary>Bucket sizes exposed for diagnostics/tests.</summary>
    public static ReadOnlySpan<int> Buckets => BucketSizes;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int BucketIndex(int minimumSize)
    {
        // Linear scan over 9 entries — branch-predictor friendly and faster
        // than log/clz tricks at this size.
        for (int i = 0; i < BucketSizes.Length; i++)
            if (BucketSizes[i] >= minimumSize) return i;
        return -1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int BucketIndexExact(int length)
    {
        for (int i = 0; i < BucketSizes.Length; i++)
            if (BucketSizes[i] == length) return i;
        return -1;
    }
}
