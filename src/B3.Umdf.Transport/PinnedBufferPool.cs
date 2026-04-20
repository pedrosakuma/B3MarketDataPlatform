using System.Collections.Concurrent;

namespace B3.Umdf.Transport;

/// <summary>
/// Pool of POH (Pinned Object Heap) byte arrays. POH-allocated arrays are auto-pinned and stable
/// for the entire object lifetime, so they can be passed to native syscalls (recvmmsg) without
/// GCHandle.Alloc overhead per call. Reuse via Rent/Return.
/// The pool is bounded: returns above <see cref="MaxRetained"/> drop the buffer and let the GC
/// reclaim it, so a transient burst that pushes the in-flight set high doesn't create a
/// permanent high-water-mark leak.
/// </summary>
internal sealed class PinnedBufferPool
{
    /// <summary>
    /// Default per-pool retention cap. 4096 × 1500 bytes ≈ 6 MB of pinned memory per pool —
    /// well above any sustained in-flight set under normal operation, and bounded enough that
    /// 8 sources × cap stay comfortably under the container memory limit.
    /// </summary>
    public const int DefaultMaxRetained = 4096;

    private readonly int _bufferSize;
    private readonly int _maxRetained;
    private readonly ConcurrentQueue<byte[]> _free = new();
    private int _retained;

    public PinnedBufferPool(int bufferSize, int maxRetained = DefaultMaxRetained)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRetained, 1);
        _bufferSize = bufferSize;
        _maxRetained = maxRetained;
    }

    public int MaxRetained => _maxRetained;
    public int Retained => Volatile.Read(ref _retained);

    public byte[] Rent()
    {
        if (_free.TryDequeue(out var buf))
        {
            Interlocked.Decrement(ref _retained);
            return buf;
        }
        return GC.AllocateUninitializedArray<byte>(_bufferSize, pinned: true);
    }

    public void Return(byte[] buffer)
    {
        if (buffer.Length != _bufferSize) return;
        // Bound the cache so high-water bursts don't pin memory forever.
        if (Interlocked.Increment(ref _retained) > _maxRetained)
        {
            Interlocked.Decrement(ref _retained);
            return; // drop — GC will reclaim the POH array
        }
        _free.Enqueue(buffer);
    }
}

internal sealed class PinnedPoolPacketLease : UmdfPacketLease
{
    private byte[]? _buffer;
    private readonly PinnedBufferPool _pool;
    private int _refCount = 1;

    public PinnedPoolPacketLease(byte[] buffer, PinnedBufferPool pool)
    {
        _buffer = buffer;
        _pool = pool;
    }

    public override void Retain()
    {
        while (true)
        {
            int current = Volatile.Read(ref _refCount);
            if (current <= 0)
                throw new InvalidOperationException("Cannot retain a released packet lease.");
            if (Interlocked.CompareExchange(ref _refCount, current + 1, current) == current)
                return;
        }
    }

    public override void Release()
    {
        int remaining = Interlocked.Decrement(ref _refCount);
        if (remaining > 0) return;
        if (remaining < 0) throw new InvalidOperationException("Packet lease released more than once.");
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is not null) _pool.Return(buffer);
    }
}
