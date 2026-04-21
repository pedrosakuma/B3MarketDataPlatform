using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace B3.Umdf.Server;

/// <summary>
/// Single-producer, single-consumer bounded ring for <see cref="BroadcastWorkBatch"/>
/// references. Decouples the feed/dispatch thread (producer) from the per-group
/// broadcaster thread (consumer), so client fan-out latency cannot back-pressure UDP
/// packet processing.
///
/// Same Vyukov per-slot-sequence scheme used by <c>MpscPacketRing</c>, trimmed down
/// for SPSC: the producer cursor does not need a CAS loop (single producer), and
/// <c>Monitor.Enter</c> on <c>ManualResetEventSlim.Set()</c> is gated on a
/// <c>_consumerWaiting</c> flag so the steady-state case never touches the kernel
/// event. Enqueue returns false when full so the caller can enact the drop +
/// resnapshot policy.
/// </summary>
internal sealed class BroadcastRing
{
    private readonly BroadcastWorkBatch?[] _slots;
    private readonly long[] _seqs;
    private readonly int _mask;

    private long _producerSeq;
    private long _consumerSeq;
    private long _droppedBatches;

    private int _consumerWaiting;
    private readonly ManualResetEventSlim _itemsAvailable = new(initialState: false, spinCount: 0);

    public int Capacity => _slots.Length;
    public long DroppedBatches => Volatile.Read(ref _droppedBatches);

    public int ApproximateDepth
    {
        get
        {
            long depth = Volatile.Read(ref _producerSeq) - Volatile.Read(ref _consumerSeq);
            if (depth < 0) return 0;
            if (depth > _slots.Length) return _slots.Length;
            return (int)depth;
        }
    }

    public BroadcastRing(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 2);
        capacity = NextPow2(capacity);
        _slots = new BroadcastWorkBatch?[capacity];
        _seqs = new long[capacity];
        for (int i = 0; i < capacity; i++) _seqs[i] = i;
        _mask = capacity - 1;
    }

    /// <summary>
    /// SPSC enqueue. Returns false when the ring is full; caller is responsible for
    /// executing drop-with-resync policy and returning the batch to the pool.
    /// </summary>
    public bool TryEnqueue(BroadcastWorkBatch batch)
    {
        long pos = _producerSeq;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);
        if (seq != pos)
        {
            // Slot not free yet (consumer hasn't drained this slot around the ring).
            Interlocked.Increment(ref _droppedBatches);
            return false;
        }

        _slots[idx] = batch;
        Volatile.Write(ref _seqs[idx], pos + 1);
        Volatile.Write(ref _producerSeq, pos + 1);

        // Wake consumer only if it has signaled it is parked. Mirrors MpscPacketRing.
        if (Interlocked.CompareExchange(ref _consumerWaiting, 0, 1) == 1)
            _itemsAvailable.Set();
        return true;
    }

    /// <summary>
    /// SPSC dequeue. Must only be called from the broadcaster thread.
    /// </summary>
    public bool TryDequeue([MaybeNullWhen(false)] out BroadcastWorkBatch batch)
    {
        long pos = _consumerSeq;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);
        if (seq != pos + 1)
        {
            batch = null;
            return false;
        }

        batch = _slots[idx];
        _slots[idx] = null;
        Volatile.Write(ref _seqs[idx], pos + _slots.Length);
        Volatile.Write(ref _consumerSeq, pos + 1);
        return batch is not null;
    }

    public void WaitForItems(CancellationToken ct)
    {
        _itemsAvailable.Reset();
        Interlocked.Exchange(ref _consumerWaiting, 1);
        if (TryDequeueAvailable())
        {
            Volatile.Write(ref _consumerWaiting, 0);
            return;
        }
        try { _itemsAvailable.Wait(ct); }
        finally { Volatile.Write(ref _consumerWaiting, 0); }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryDequeueAvailable()
    {
        long pos = _consumerSeq;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);
        return seq - (pos + 1) == 0;
    }

    public void SignalShutdown() => _itemsAvailable.Set();
    public void Dispose() => _itemsAvailable.Dispose();

    private static int NextPow2(int v)
    {
        if (v <= 0) return 1;
        if ((v & (v - 1)) == 0) return v;
        int n = 1;
        while (n < v) n <<= 1;
        return n;
    }
}
