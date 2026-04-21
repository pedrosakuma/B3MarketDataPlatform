using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace B3.Umdf.Server;

/// <summary>
/// Carries one queued outbound payload from a producer (group flush thread, snapshot
/// path, or info-wake) to the per-client write loop. Top-level so the lock-free
/// <see cref="MpscOutboundRing"/> can store instances directly.
/// </summary>
internal readonly struct OutboundMessage
{
    public static readonly OutboundMessage InfoWake = new(ReadOnlyMemory<byte>.Empty, isInfoWake: true, logicalCount: 0, pooledArray: null);

    public OutboundMessage(ReadOnlyMemory<byte> payload, bool isInfoWake = false, int logicalCount = 1, byte[]? pooledArray = null)
    {
        Payload = payload;
        IsInfoWake = isInfoWake;
        LogicalCount = logicalCount;
        PooledArray = pooledArray;
    }

    public ReadOnlyMemory<byte> Payload { get; }
    public bool IsInfoWake { get; }

    /// <summary>
    /// Number of logical wire messages contained in this payload. Coalesced batches
    /// from upstream conflation carry N pre-serialized messages back-to-back so the
    /// stat counters reflect the true wire-event volume rather than the queue-write
    /// count.
    /// </summary>
    public int LogicalCount { get; }

    /// <summary>
    /// When non-null, the backing array was rented from
    /// <see cref="System.Buffers.ArrayPool{T}.Shared"/> by the producer; the write
    /// loop must return it after the WS frame is sent (or the ring drain on shutdown
    /// must return it). <see cref="Payload"/> is a slice of this array.
    /// </summary>
    public byte[]? PooledArray { get; }
}

/// <summary>
/// Multi-producer / single-consumer bounded ring buffer for <see cref="OutboundMessage"/>.
/// Lock-free Vyukov sequence-numbered scheme; producers <see cref="TryEnqueue(in OutboundMessage)"/>
/// from any thread, the per-client write loop is the sole consumer.
///
/// Designed to replace <see cref="System.Threading.Channels.Channel{T}"/> on the WS
/// outbound path: <c>Channel{T}.TryWrite</c> takes an internal Monitor on every call,
/// which dominated the broadcast-side CPU cost in trace 014 even after per-flush
/// coalescing. The wake/park path uses <see cref="SemaphoreSlim"/> so the consumer
/// can <c>await</c> without blocking a thread-pool worker, gated by the
/// <c>_consumerWaiting</c> flag so the steady-state hot path never touches the
/// semaphore.
/// </summary>
internal sealed class MpscOutboundRing : IDisposable
{
    private readonly OutboundMessage[] _slots;
    private readonly long[] _seqs;
    private readonly int _mask;

    private PaddedLong _producerSeq;
    private PaddedLong _consumerSeq;

    // 0 = consumer is busy draining; 1 = consumer has parked (or is about to park) on
    // the semaphore. Producers gate the cost of SemaphoreSlim.Release (which acquires
    // an internal Monitor lock) on this flag, so the steady-state case — consumer
    // keeping up — never wakes the semaphore.
    private int _consumerWaiting;

    private readonly SemaphoreSlim _itemsAvailable = new(initialCount: 0);

    public int Capacity => _slots.Length;

    /// <summary>Approximate current depth (producerSeq - consumerSeq), clamped.</summary>
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

    public MpscOutboundRing(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        capacity = NextPow2(capacity);
        _slots = new OutboundMessage[capacity];
        _seqs = new long[capacity];
        for (int i = 0; i < capacity; i++)
            _seqs[i] = i;
        _mask = capacity - 1;
    }

    /// <summary>
    /// Lock-free enqueue. Returns false when the ring is full; caller is responsible
    /// for any cleanup (e.g. returning a pooled array) of the rejected message.
    /// </summary>
    public bool TryEnqueue(in OutboundMessage msg)
    {
        long pos = Volatile.Read(ref _producerSeq.Value);
        while (true)
        {
            int idx = (int)(pos & _mask);
            long seq = Volatile.Read(ref _seqs[idx]);
            long diff = seq - pos;

            if (diff == 0)
            {
                if (Interlocked.CompareExchange(ref _producerSeq.Value, pos + 1, pos) == pos)
                {
                    _slots[idx] = msg;
                    Volatile.Write(ref _seqs[idx], pos + 1);
                    // Wake the consumer only if it has signaled it is parked.
                    // CompareExchange is a full barrier, so our slot publish above
                    // happens-before this read; and the consumer's Exchange of
                    // _consumerWaiting=1 happens-before its re-check of the slot.
                    // Either it observes our publish on the re-check, or we observe
                    // its waiting flag and Release the semaphore — never both miss.
                    if (Interlocked.CompareExchange(ref _consumerWaiting, 0, 1) == 1)
                        _itemsAvailable.Release();
                    return true;
                }
                pos = Volatile.Read(ref _producerSeq.Value);
            }
            else if (diff < 0)
            {
                return false;
            }
            else
            {
                pos = Volatile.Read(ref _producerSeq.Value);
            }
        }
    }

    /// <summary>
    /// Single-consumer dequeue. Must only be called from the dedicated write loop
    /// task. Returns false when no message is currently available.
    /// </summary>
    public bool TryDequeue([MaybeNullWhen(false)] out OutboundMessage msg)
    {
        long pos = _consumerSeq.Value;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);

        if (seq - (pos + 1) == 0)
        {
            msg = _slots[idx];
            _slots[idx] = default;
            Volatile.Write(ref _seqs[idx], pos + _slots.Length);
            _consumerSeq.Value = pos + 1;
            return true;
        }

        msg = default;
        return false;
    }

    /// <summary>
    /// Asynchronously park the consumer until at least one message is observable in
    /// the ring or <paramref name="ct"/> fires. Caller must have already drained
    /// everything available via <see cref="TryDequeue"/>.
    /// </summary>
    public ValueTask WaitForItemsAsync(CancellationToken ct)
    {
        // Publish "waiting" with a full barrier, then re-check the ring to close the
        // window where a producer enqueued between the caller's last failed
        // TryDequeue and this point.
        Interlocked.Exchange(ref _consumerWaiting, 1);
        if (TryPeekAvailable())
        {
            Volatile.Write(ref _consumerWaiting, 0);
            return ValueTask.CompletedTask;
        }
        return new ValueTask(WaitSlowAsync(ct));
    }

    private async Task WaitSlowAsync(CancellationToken ct)
    {
        try { await _itemsAvailable.WaitAsync(ct).ConfigureAwait(false); }
        finally { Volatile.Write(ref _consumerWaiting, 0); }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryPeekAvailable()
    {
        long pos = _consumerSeq.Value;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);
        return seq - (pos + 1) == 0;
    }

    /// <summary>Wakes the consumer (used at shutdown/cancel to break a Wait).</summary>
    public void SignalShutdown()
    {
        // Best-effort: if nobody is waiting, this just raises the count and the next
        // WaitForItemsAsync returns immediately. The fast-path consumer would then
        // re-enter, find an empty ring, and re-park — harmless one-shot extra cycle.
        try { _itemsAvailable.Release(); } catch (ObjectDisposedException) { }
    }

    public void Dispose() => _itemsAvailable.Dispose();

    private static int NextPow2(int v)
    {
        if (v <= 0) return 1;
        if ((v & (v - 1)) == 0) return v;
        int n = 1;
        while (n < v) n <<= 1;
        return n;
    }

    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct PaddedLong
    {
        [FieldOffset(64)]
        public long Value;
    }
}
