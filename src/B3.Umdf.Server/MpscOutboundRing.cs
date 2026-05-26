using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks.Sources;

namespace B3.Umdf.Server;

/// <summary>
/// Discriminator for entries pushed through the outbound ring. Most entries carry a
/// pre-serialized <c>Payload</c>; the admin kinds (<see cref="AddInfoSub"/>,
/// <see cref="RemoveInfoSub"/>) carry a <c>SecurityId</c> instead so that the per-
/// client write loop can mutate its own (single-threaded) info-version map without
/// needing a <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey, TValue}"/>.
/// </summary>
internal enum OutboundKind : byte
{
    Payload = 0,
    InfoWake = 1,
    AddInfoSub = 2,
    RemoveInfoSub = 3,
    /// <summary>Wake the write loop to flush any <c>SecurityDefinition</c> deltas
    /// that arrived since the last cycle. Mirrors <see cref="InfoWake"/> but for
    /// the SecurityDefinition channel — kept independent so consumers that
    /// subscribe only to one channel don't wake on the other's traffic.</summary>
    SecurityDefinitionWake = 4,
    AddSecurityDefinitionSub = 5,
    RemoveSecurityDefinitionSub = 6,
}

/// <summary>
/// Carries one queued outbound payload from a producer (group flush thread, snapshot
/// path, or info-wake) to the per-client write loop. Top-level so the lock-free
/// <see cref="MpscOutboundRing"/> can store instances directly.
/// </summary>
internal readonly struct OutboundMessage
{
    public static readonly OutboundMessage InfoWake = new(OutboundKind.InfoWake, ReadOnlyMemory<byte>.Empty, securityId: 0, logicalCount: 0, pooledArray: null);

    public OutboundMessage(ReadOnlyMemory<byte> payload, int logicalCount = 1, byte[]? pooledArray = null)
        : this(OutboundKind.Payload, payload, securityId: 0, logicalCount, pooledArray)
    {
    }

    private OutboundMessage(OutboundKind kind, ReadOnlyMemory<byte> payload, ulong securityId, int logicalCount, byte[]? pooledArray)
    {
        Kind = kind;
        Payload = payload;
        SecurityId = securityId;
        LogicalCount = logicalCount;
        PooledArray = pooledArray;
    }

    public static OutboundMessage AddInfoSub(ulong securityId) =>
        new(OutboundKind.AddInfoSub, ReadOnlyMemory<byte>.Empty, securityId, logicalCount: 0, pooledArray: null);

    public static OutboundMessage RemoveInfoSub(ulong securityId) =>
        new(OutboundKind.RemoveInfoSub, ReadOnlyMemory<byte>.Empty, securityId, logicalCount: 0, pooledArray: null);

    public static readonly OutboundMessage SecurityDefinitionWake =
        new(OutboundKind.SecurityDefinitionWake, ReadOnlyMemory<byte>.Empty, securityId: 0, logicalCount: 0, pooledArray: null);

    public static OutboundMessage AddSecurityDefinitionSub(ulong securityId) =>
        new(OutboundKind.AddSecurityDefinitionSub, ReadOnlyMemory<byte>.Empty, securityId, logicalCount: 0, pooledArray: null);

    public static OutboundMessage RemoveSecurityDefinitionSub(ulong securityId) =>
        new(OutboundKind.RemoveSecurityDefinitionSub, ReadOnlyMemory<byte>.Empty, securityId, logicalCount: 0, pooledArray: null);

    public OutboundKind Kind { get; }
    public ReadOnlyMemory<byte> Payload { get; }
    public ulong SecurityId { get; }
    public bool IsInfoWake => Kind == OutboundKind.InfoWake;

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
/// coalescing. The wake/park path is a custom allocation-free
/// <see cref="IValueTaskSource{TResult}"/> (built on
/// <see cref="ManualResetValueTaskSourceCore{TResult}"/>) so that the write loop can
/// <c>await</c> without boxing the async state machine or allocating a completion
/// source on every park. Producers gate the wake on the <c>_consumerWaiting</c> flag
/// so the steady-state hot path touches nothing shared beyond the ring slots.
/// </summary>
internal sealed class MpscOutboundRing : IValueTaskSource, IDisposable
{
    private readonly OutboundMessage[] _slots;
    private readonly long[] _seqs;
    private readonly int _mask;

    private PaddedLong _producerSeq;
    private PaddedLong _consumerSeq;

    // 0 = consumer is busy draining; 1 = consumer has parked (or is about to park)
    // on the IValueTaskSource. At most one actor (producer, cancellation callback,
    // or consumer fast-path rescue) wins the 1→0 CAS and is therefore the sole
    // completer of the current wait cycle, preventing double-SetResult.
    private int _consumerWaiting;

    // RunContinuationsAsynchronously=true offloads the write loop to the ThreadPool
    // when a producer (feed thread) wakes the consumer. Otherwise SetResult runs the
    // entire writer chain inline — drain → coalesce → SendAsync → Kestrel pipe write
    // — on the feed thread, serializing all per-client writes through that single
    // thread and amplifying Kestrel pipe-lock contention. Trace 020 attributed ~85%
    // of Monitor.Enter_Slowpath samples to that inline chain. The cost of this flag
    // is one ThreadPool dispatch per wake (a few per second per client), which is
    // negligible compared to the parallelism it unlocks.
    private ManualResetValueTaskSourceCore<bool> _vts = new() { RunContinuationsAsynchronously = true };
    private CancellationTokenRegistration _ctr;
    private CancellationToken _activeCt;
    private volatile bool _disposed;

    private static readonly Action<object?, CancellationToken> s_cancelCallback = static (state, ct) =>
    {
        var ring = (MpscOutboundRing)state!;
        // Only the winner of the CAS completes the wait; if the producer already won,
        // we leave the wait completed with success and let the consumer loop observe
        // cancellation on its next pass through RunWriteLoopAsync.
        if (Interlocked.CompareExchange(ref ring._consumerWaiting, 0, 1) == 1)
            ring._vts.SetException(new OperationCanceledException(ct));
    };

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
                    // its waiting flag and complete the value-task source — never both miss.
                    if (Interlocked.CompareExchange(ref _consumerWaiting, 0, 1) == 1)
                        _vts.SetResult(true);
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
    /// everything available via <see cref="TryDequeue"/> and must not invoke this
    /// concurrently — there is exactly one consumer by design.
    /// </summary>
    public ValueTask WaitForItemsAsync(CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
            return ValueTask.FromCanceled(ct);
        if (_disposed)
            return ValueTask.CompletedTask;

        // Reset the source for a new wait cycle. Only the single consumer ever
        // touches _vts.Reset, so this is safe without synchronization.
        _vts.Reset();
        _activeCt = ct;

        // Publish "waiting" with a full barrier, then re-check the ring to close the
        // window where a producer enqueued between the caller's last failed
        // TryDequeue and this point.
        Interlocked.Exchange(ref _consumerWaiting, 1);
        if (TryPeekAvailable())
        {
            // If we still own the flag, rescue it ourselves so the producer that
            // enqueued the racing item does not later observe 1 and call SetResult
            // on a source we never awaited. If the producer already flipped the flag
            // it has also called SetResult(true), so we must go through the VTS.
            if (Interlocked.CompareExchange(ref _consumerWaiting, 0, 1) == 1)
                return ValueTask.CompletedTask;
        }

        if (ct.CanBeCanceled)
            _ctr = ct.UnsafeRegister(s_cancelCallback, this);

        return new ValueTask(this, _vts.Version);
    }

    // --- IValueTaskSource ---
    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _vts.GetStatus(token);

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _vts.OnCompleted(continuation, state, token, flags);

    void IValueTaskSource.GetResult(short token)
    {
        // Dispose the registration first so we never leak a pending callback; this
        // is cheap (no-op if default) and also synchronously waits for any in-flight
        // callback to complete so it's safe to Reset the source on the next cycle.
        _ctr.Dispose();
        _ctr = default;
        try
        {
            _vts.GetResult(token);
        }
        finally
        {
            _activeCt = default;
        }
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
        _disposed = true;
        // If the consumer is parked, rescue it so it exits the Wait and the write
        // loop can observe cancellation via its CancellationToken. If it isn't
        // parked, the next call to WaitForItemsAsync returns CompletedTask because
        // _disposed is set.
        if (Interlocked.CompareExchange(ref _consumerWaiting, 0, 1) == 1)
        {
            try { _vts.SetResult(true); } catch (InvalidOperationException) { /* already completed */ }
        }
    }

    public void Dispose()
    {
        _disposed = true;
        _ctr.Dispose();
        _ctr = default;
    }

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
