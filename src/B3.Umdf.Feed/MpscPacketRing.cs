using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

/// <summary>
/// Multi-producer, single-consumer bounded ring buffer for <see cref="UmdfPacket"/>.
/// Implements the Vyukov MPMC scheme (used here in the MPSC role): each slot carries a
/// sequence number that orders writes and reads without locks.
///
/// Memory ordering follows the C# memory model: <see cref="Volatile.Read{T}(ref T)"/> /
/// <see cref="Volatile.Write{T}(ref T, T)"/> establish acquire/release semantics on the
/// per-slot sequence words; <see cref="Interlocked.CompareExchange(ref long, long, long)"/>
/// publishes the producer cursor.
///
/// Only one consumer thread may call <see cref="TryDequeue(out UmdfPacket)"/> /
/// <see cref="DrainAvailableSlot(out UmdfPacket)"/> at a time. Producers may call
/// <see cref="TryEnqueue(in UmdfPacket)"/> from any number of threads.
/// </summary>
internal sealed class MpscPacketRing
{
    private readonly UmdfPacket[] _slots;
    private readonly long[] _seqs;
    private readonly int _mask;

    // Cache-line-padded cursors to avoid false sharing between producers and the consumer.
    private PaddedLong _producerSeq;
    private PaddedLong _consumerSeq;
    private long _droppedPackets;

    // Per-channel drop attribution (index = (int)ChannelType). Drop attribution
    // matters because IncrementalA/B (~118 kpps peak) are 100x the rate of
    // SnapshotRecovery / InstrumentDefinition; without per-channel breakdown a
    // single ring.dropped counter cannot tell whether overflow came from the
    // hot Inc path or from a misbehaving Snap producer. Updated atomically on
    // every overflow inside TryEnqueue; the aggregate _droppedPackets is the
    // sum of these, kept for backward compatibility.
    private readonly long[] _droppedByChannel = new long[ChannelTypeCount];

    // Hard-coded to match the ChannelType enum (4 values: IncrementalA,
    // IncrementalB, InstrumentDefinition, SnapshotRecovery). Keep in sync if
    // the enum gains new members.
    internal const int ChannelTypeCount = 4;

    // 0 = consumer is busy draining; 1 = consumer has parked (or is about to park) and
    // needs an explicit wake. Producers gate the cost of ManualResetEventSlim.Set()
    // (which takes an internal Monitor lock when a waiter exists) on this flag, so the
    // common steady-state case — consumer keeping up — never touches the event.
    private int _consumerWaiting;

    private readonly ManualResetEventSlim _itemsAvailable = new(initialState: false, spinCount: 0);

    public int Capacity => _slots.Length;

    public long DroppedPackets => Volatile.Read(ref _droppedPackets);

    /// <summary>
    /// Per-channel drop count snapshot (indexed by <see cref="ChannelType"/>).
    /// Sum across the returned array equals <see cref="DroppedPackets"/>.
    /// Each element is read atomically; the array as a whole is not a coherent
    /// snapshot under concurrent producers, but each channel value is at least
    /// monotonic and consistent with a real point in time.
    /// </summary>
    public long DroppedFor(ChannelType channel) =>
        Volatile.Read(ref _droppedByChannel[(int)channel]);

    /// <summary>Approximate current depth (producerSeq - consumerSeq). May briefly read negative under racy reads; clamped to 0.</summary>
    public int ApproximateDepth
    {
        get
        {
            long depth = Volatile.Read(ref _producerSeq.Value) - Volatile.Read(ref _consumerSeq.Value);
            if (depth < 0) return 0;
            if (depth > _slots.Length) return _slots.Length;
            return (int)depth;
        }
    }

    public MpscPacketRing(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 2);
        capacity = NextPow2(capacity);
        _slots = new UmdfPacket[capacity];
        _seqs = new long[capacity];
        for (int i = 0; i < capacity; i++)
            _seqs[i] = i;
        _mask = capacity - 1;
    }

    /// <summary>
    /// Lock-free enqueue. Returns false when the ring is full; caller is responsible for
    /// releasing the packet's lease in that case.
    /// </summary>
    public bool TryEnqueue(in UmdfPacket packet)
    {
        long pos = Volatile.Read(ref _producerSeq.Value);
        while (true)
        {
            int idx = (int)(pos & _mask);
            long seq = Volatile.Read(ref _seqs[idx]);
            long diff = seq - pos;

            if (diff == 0)
            {
                // Slot is free for this position; try to claim it.
                if (Interlocked.CompareExchange(ref _producerSeq.Value, pos + 1, pos) == pos)
                {
                    _slots[idx] = packet;
                    Volatile.Write(ref _seqs[idx], pos + 1);
                    // Wake the consumer only if it has signaled it is parked. The
                    // CompareExchange below is a full barrier, so our slot publish
                    // (Volatile.Write above) happens-before this read of the flag;
                    // and the consumer's Interlocked.Exchange(_consumerWaiting, 1)
                    // happens-before its re-check of the slot. Either the consumer
                    // observes our publish on the re-check, or we observe its waiting
                    // flag and Set the event — never both miss.
                    if (Interlocked.CompareExchange(ref _consumerWaiting, 0, 1) == 1)
                        _itemsAvailable.Set();
                    return true;
                }
                // Lost the race; reload.
                pos = Volatile.Read(ref _producerSeq.Value);
            }
            else if (diff < 0)
            {
                // Ring is full at this position.
                Interlocked.Increment(ref _droppedPackets);
                int chIdx = (int)packet.Channel;
                if ((uint)chIdx < (uint)_droppedByChannel.Length)
                    Interlocked.Increment(ref _droppedByChannel[chIdx]);
                return false;
            }
            else
            {
                // Another producer advanced past us; reload.
                pos = Volatile.Read(ref _producerSeq.Value);
            }
        }
    }

    /// <summary>
    /// Single-consumer dequeue. Must only be called from the dispatch thread that owns
    /// this ring. Returns false when no packet is currently available (possibly because
    /// a producer has reserved the slot but not yet published it).
    /// </summary>
    public bool TryDequeue([MaybeNullWhen(false)] out UmdfPacket packet)
    {
        long pos = _consumerSeq.Value;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);
        long diff = seq - (pos + 1);

        if (diff == 0)
        {
            packet = _slots[idx];
            _slots[idx] = default; // help GC and prevent stale lease references
            Volatile.Write(ref _seqs[idx], pos + _slots.Length);
            _consumerSeq.Value = pos + 1;
            return true;
        }

        packet = default;
        return false;
    }

    /// <summary>
    /// Blocks the consumer until at least one item is observed in the ring or the
    /// cancellation token fires. Should be called only when <see cref="TryDequeue"/>
    /// returned false.
    /// </summary>
    public void WaitForItems(CancellationToken ct)
    {
        // Reset before publishing the waiting flag so a concurrent producer that observes
        // the flag will Set the event into a known state. Then publish the flag with a
        // full barrier (Interlocked.Exchange) and re-check the ring before blocking, to
        // avoid sleeping on packets that were enqueued during the gap between the
        // failed TryDequeue (in the caller) and this point.
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

    /// <summary>
    /// Bounded variant of <see cref="WaitForItems(CancellationToken)"/>. Returns true
    /// if signaled within <paramref name="timeoutMs"/> (items likely available), false
    /// on timeout. Replicates the exact reset/publish-flag/recheck pattern to preserve
    /// the wakeup invariant — a producer enqueuing during the race window will not be
    /// missed.
    /// </summary>
    public bool WaitForItems(int timeoutMs, CancellationToken ct)
    {
        _itemsAvailable.Reset();
        Interlocked.Exchange(ref _consumerWaiting, 1);
        if (TryDequeueAvailable())
        {
            Volatile.Write(ref _consumerWaiting, 0);
            return true;
        }
        try { return _itemsAvailable.Wait(timeoutMs, ct); }
        finally { Volatile.Write(ref _consumerWaiting, 0); }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryDequeueAvailable()
    {
        long pos = _consumerSeq.Value;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);
        return seq - (pos + 1) == 0;
    }

    /// <summary>Wakes the consumer (used at shutdown to break a Wait).</summary>
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

    // 64-byte cache line padding around a long to keep producer / consumer cursors apart.
    [System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Explicit, Size = 128)]
    private struct PaddedLong
    {
        [System.Runtime.InteropServices.FieldOffset(64)]
        public long Value;
    }
}
