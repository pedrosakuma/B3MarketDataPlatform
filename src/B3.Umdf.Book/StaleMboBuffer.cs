using System.Buffers;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Book;

/// <summary>
/// Per-symbol payload buffer for MBO/Trade messages received while
/// <see cref="SymbolStateRegistry"/> reports the symbol as
/// <see cref="SymbolState.Stale"/> or <see cref="SymbolState.Unknown"/>.
/// On heal, the caller drains the entries in the window returned by
/// <see cref="SymbolStateRegistry.HealFromSnapshot"/>.
/// </summary>
/// <remarks>
/// <para><b>Storage.</b> Each entry copies the SBE message body bytes
/// (rented from <see cref="ArrayPool{T}.Shared"/>) plus the discriminating
/// template id, the parsed rptSeq (for drain filtering), and the packet's
/// SendingTime. Copies are unavoidable because the source span is owned by
/// the receive batch and recycled immediately after dispatch.</para>
///
/// <para><b>Caps.</b> Per-symbol queue depth is capped (default 8192) so
/// one pathological symbol cannot exhaust memory. Global byte cap (default
/// 256 MiB) protects against many symbols filling buffers simultaneously.
/// When either cap trips, the newest message is dropped and the overflow
/// counter increments; subsequent drain may produce a gap mid-window that
/// re-flips the symbol Stale, which is the correct behavior — we wait for
/// the next snapshot cycle to heal cleanly.</para>
///
/// <para><b>Concurrency.</b> The feed thread is the sole writer per group;
/// outer dictionary is concurrent so multiple groups can share one buffer
/// instance if ever needed. Per-symbol queue mutations are guarded by an
/// entry-local lock.</para>
/// </remarks>
public sealed class StaleMboBuffer
{
    private readonly ConcurrentDictionary<ulong, PerSymbolQueue> _queues = new();
    private readonly ILogger _logger;
    private readonly int _perSymbolCap;
    private readonly long _globalByteCap;

    private long _totalBytes;
    private long _enqueued;
    private long _drained;
    private long _droppedPerSymbolCap;
    private long _droppedGlobalCap;

    public StaleMboBuffer(ILogger logger, int perSymbolCap = 8192, long globalByteCap = 256L * 1024 * 1024)
    {
        _logger = logger;
        _perSymbolCap = perSymbolCap;
        _globalByteCap = globalByteCap;
    }

    public long TotalBytes => Volatile.Read(ref _totalBytes);
    public long EnqueuedCount => Volatile.Read(ref _enqueued);
    public long DrainedCount => Volatile.Read(ref _drained);
    public long DroppedPerSymbolCapCount => Volatile.Read(ref _droppedPerSymbolCap);
    public long DroppedGlobalCapCount => Volatile.Read(ref _droppedGlobalCap);

    /// <summary>
    /// One stored message. <see cref="Body"/> is rented from
    /// <see cref="ArrayPool{T}.Shared"/>; <see cref="BodyLength"/> is the
    /// valid prefix. Caller must <see cref="Release"/> after replay.
    /// </summary>
    public readonly record struct DeferredMboMsg(
        ushort TemplateId,
        uint RptSeq,
        ulong SendingTimeNs,
        byte[] Body,
        int BodyLength)
    {
        public ReadOnlySpan<byte> Span => Body.AsSpan(0, BodyLength);
        public void Release() => ArrayPool<byte>.Shared.Return(Body);
    }

    private sealed class PerSymbolQueue
    {
        internal readonly Lock Sync = new();
        internal readonly Queue<DeferredMboMsg> Items = new();
    }

    /// <summary>
    /// Buffer a message body. Copies <paramref name="body"/> into a pooled
    /// array. Returns true on success, false if a cap tripped (message dropped).
    /// </summary>
    public bool Enqueue(ulong securityId, ushort templateId, uint rptSeq, ulong sendingTimeNs, ReadOnlySpan<byte> body)
    {
        // Global byte cap: check before allocating.
        long postEnqueueBytes = Interlocked.Read(ref _totalBytes) + body.Length;
        if (postEnqueueBytes > _globalByteCap)
        {
            Interlocked.Increment(ref _droppedGlobalCap);
            return false;
        }

        var queue = _queues.GetOrAdd(securityId, static _ => new PerSymbolQueue());
        lock (queue.Sync)
        {
            if (queue.Items.Count >= _perSymbolCap)
            {
                Interlocked.Increment(ref _droppedPerSymbolCap);
                return false;
            }
            var rented = ArrayPool<byte>.Shared.Rent(body.Length);
            body.CopyTo(rented);
            queue.Items.Enqueue(new DeferredMboMsg(templateId, rptSeq, sendingTimeNs, rented, body.Length));
        }
        Interlocked.Add(ref _totalBytes, body.Length);
        Interlocked.Increment(ref _enqueued);
        return true;
    }

    /// <summary>
    /// Drain messages with <c>RptSeq ∈ [drainFrom, drainTo]</c> in queue
    /// order, invoking <paramref name="apply"/> for each. Messages with
    /// rptSeq below the window are dropped (already covered by snapshot);
    /// messages above the window are kept (future). The caller must
    /// <see cref="DeferredMboMsg.Release"/> each message.
    /// </summary>
    public int Drain(ulong securityId, uint drainFrom, uint drainTo, Action<DeferredMboMsg> apply)
    {
        if (!_queues.TryGetValue(securityId, out var queue)) return 0;
        if (drainTo < drainFrom) return 0; // empty window

        int applied = 0;
        long releasedBytes = 0;
        lock (queue.Sync)
        {
            // Single linear pass: items in queue are in arrival order, not rptSeq
            // order (A/B reorder may have shuffled them). We collect+sort applies
            // to preserve causal apply order, then re-enqueue out-of-window items.
            var matches = new List<DeferredMboMsg>();
            var future = new List<DeferredMboMsg>();
            while (queue.Items.TryDequeue(out var item))
            {
                if (item.RptSeq < drainFrom)
                {
                    // Below snapshot baseline → covered, drop.
                    releasedBytes += item.BodyLength;
                    item.Release();
                    continue;
                }
                if (item.RptSeq > drainTo)
                {
                    future.Add(item);
                    continue;
                }
                matches.Add(item);
            }

            matches.Sort(static (a, b) => a.RptSeq.CompareTo(b.RptSeq));
            foreach (var m in matches)
            {
                apply(m);
                releasedBytes += m.BodyLength;
                m.Release();
                applied++;
            }
            // Restore future items for the next drain.
            foreach (var f in future) queue.Items.Enqueue(f);
        }
        Interlocked.Add(ref _totalBytes, -releasedBytes);
        Interlocked.Add(ref _drained, applied);
        return applied;
    }

    /// <summary>
    /// Discard all buffered entries for a symbol (e.g. on epoch reset).
    /// Returns the count discarded.
    /// </summary>
    public int Clear(ulong securityId)
    {
        if (!_queues.TryGetValue(securityId, out var queue)) return 0;
        int count = 0;
        long releasedBytes = 0;
        lock (queue.Sync)
        {
            while (queue.Items.TryDequeue(out var item))
            {
                releasedBytes += item.BodyLength;
                item.Release();
                count++;
            }
        }
        Interlocked.Add(ref _totalBytes, -releasedBytes);
        return count;
    }

    /// <summary>Discard all buffers across every symbol (catastrophic reset path).</summary>
    public int ClearAll()
    {
        int total = 0;
        foreach (var kv in _queues)
            total += Clear(kv.Key);
        return total;
    }

    /// <summary>Current depth of a symbol's queue (for tests/metrics).</summary>
    public int DepthOf(ulong securityId) =>
        _queues.TryGetValue(securityId, out var q) ? q.Items.Count : 0;
}
