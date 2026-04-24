using System.Buffers;
using System.Collections.Generic;
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
/// <para><b>Two-tier per-symbol cap.</b> Each symbol starts with the normal
/// cap (default 8192 messages). The first time a symbol's buffer is about
/// to overflow, it is permanently promoted to the hot cap (default 65536).
/// This sizes the buffer for the snapshot-rotation latency of high-rate
/// symbols (mini-index futures ~10k msg/s × ~6s rotation ≈ 60k msgs)
/// without overcommitting memory for the long tail of inactive symbols.
/// Once a symbol is at the hot cap, further overflows fall back to
/// drop-oldest (preserving the freshest window for the next snapshot heal).</para>
///
/// <para><b>Caps.</b> Global byte cap (default 256 MiB) protects against
/// many symbols filling buffers simultaneously. When the global cap trips,
/// the newest message is dropped and the overflow counter increments;
/// subsequent drain may produce a gap mid-window that re-flips the symbol
/// Stale, which is the correct behavior — we wait for the next snapshot
/// cycle to heal cleanly.</para>
///
/// <para><b>Concurrency.</b> Single-writer, owned by the per-group feed
/// thread (each group has its own buffer instance — see
/// <c>Program.cs</c>). All mutating methods (<see cref="Enqueue"/>,
/// <see cref="Drain"/>, <see cref="Clear"/>, <see cref="ClearAll"/>) must
/// be called from that single thread. Cross-thread observers are limited
/// to the atomic counter properties (<see cref="TotalBytes"/>,
/// <see cref="EnqueuedCount"/>, etc.); they MUST NOT enumerate the inner
/// per-symbol queues.</para>
/// </remarks>
public sealed class StaleMboBuffer
{
    private readonly Dictionary<ulong, PerSymbolQueue> _queues = new();
    private readonly ILogger _logger;
    private readonly int _perSymbolCap;
    private readonly int _hotPerSymbolCap;
    private readonly long _globalByteCap;

    private long _totalBytes;
    private long _enqueued;
    private long _drained;
#pragma warning disable CS0649 // retained for ABI/metric compatibility; drop-oldest policy now evicts via _evictedPerSymbolCap
    private long _droppedPerSymbolCap;
#pragma warning restore CS0649
    private long _droppedGlobalCap;

    private long _evictedPerSymbolCap;
    private long _safeEvictedBelowFloor;
    private long _hotPromotions;

    public StaleMboBuffer(ILogger logger, int perSymbolCap = 8192, long globalByteCap = 256L * 1024 * 1024, int hotPerSymbolCap = 65536)
    {
        _logger = logger;
        _perSymbolCap = perSymbolCap;
        _hotPerSymbolCap = Math.Max(perSymbolCap, hotPerSymbolCap);
        _globalByteCap = globalByteCap;
    }

    public long TotalBytes => Volatile.Read(ref _totalBytes);
    public long EnqueuedCount => Volatile.Read(ref _enqueued);
    public long DrainedCount => Volatile.Read(ref _drained);
    public long DroppedPerSymbolCapCount => Volatile.Read(ref _droppedPerSymbolCap);
    public long DroppedGlobalCapCount => Volatile.Read(ref _droppedGlobalCap);
    /// <summary>Count of oldest messages evicted that bumped the symbol's MinHeal baseline
    /// (drop-oldest at hot cap, evicted msg's rptSeq was >= the protected floor — i.e., a
    /// message we still needed for drain). Each one of these effectively makes the next
    /// snapshot heal harder for that symbol.</summary>
    public long EvictedPerSymbolCapCount => Volatile.Read(ref _evictedPerSymbolCap);
    /// <summary>Count of oldest messages evicted that fell BELOW the protected floor (i.e.,
    /// snapshot in flight already covers them — safe to drop without bumping MinHeal).
    /// High values vs <see cref="EvictedPerSymbolCapCount"/> indicate the floor pin is
    /// successfully absorbing snapshot-delivery latency.</summary>
    public long SafeEvictedBelowFloorCount => Volatile.Read(ref _safeEvictedBelowFloor);
    /// <summary>Count of symbols that were promoted from the normal cap to the hot cap on first overflow.</summary>
    public long HotPromotionCount => Volatile.Read(ref _hotPromotions);

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
        internal readonly Queue<DeferredMboMsg> Items = new();
        // Per-symbol cap. Starts at the normal tier; promoted to the hot tier
        // on first overflow. One-way ratchet — never demoted.
        internal int Cap;
        // Protected-floor pin: while a snapshot is in flight for this symbol,
        // the BookManager publishes the snapshot's rptSeq+1 as the floor.
        // Messages with rptSeq < ProtectedFloor are NOT needed for the eventual
        // drain (snapshot already covers them). At hot-cap eviction time:
        //   - If oldest.RptSeq < ProtectedFloor → safe drop, MinHeal NOT bumped.
        //   - Else (or ProtectedFloor == 0) → unsafe drop, MinHeal bumped (current behavior).
        // This protects the post-snapshot drain window from being lost while the
        // snapshot is still being delivered chunk-by-chunk.
        internal uint ProtectedFloor;
    }

    /// <summary>
    /// Buffer a message body. Copies <paramref name="body"/> into a pooled
    /// array. Returns true on success, false if the global cap tripped
    /// (message dropped). Two-tier behavior:
    /// <list type="number">
    ///   <item>Normal tier: when the buffer reaches the normal cap, the symbol
    ///   is permanently promoted to the hot cap; no message is evicted.</item>
    ///   <item>Hot tier: when the buffer is already at the hot cap, the OLDEST
    ///   buffered message is evicted to make room — the buffer always retains
    ///   the most-recent <c>hotPerSymbolCap</c> messages so the next snapshot
    ///   heal reflects the freshest possible state. If the evicted message's
    ///   rptSeq is BELOW the symbol's protected floor (set by the caller when
    ///   a snapshot is in flight), the eviction is "safe" — the snapshot
    ///   already covers that message — and <paramref name="onEvictedOldest"/>
    ///   is NOT invoked. Otherwise (no floor set, or evicted msg ≥ floor),
    ///   the callback fires so the caller can advance its
    ///   <see cref="SymbolStateRegistry.BumpMinHeal"/> baseline (rejecting
    ///   future snapshots that would leave a hole between the snapshot and
    ///   the new buffer earliest).</item>
    /// </list>
    /// </summary>
    public bool Enqueue(ulong securityId, ushort templateId, uint rptSeq, ulong sendingTimeNs, ReadOnlySpan<byte> body, Action<uint>? onEvictedOldest = null)
    {
        // Global byte cap: check before allocating.
        long postEnqueueBytes = Volatile.Read(ref _totalBytes) + body.Length;
        if (postEnqueueBytes > _globalByteCap)
        {
            _droppedGlobalCap++;
            return false;
        }

        if (!_queues.TryGetValue(securityId, out var queue))
        {
            queue = new PerSymbolQueue { Cap = _perSymbolCap };
            _queues[securityId] = queue;
        }
        long evictedBytes = 0;
        uint evictedRptSeq = 0;
        bool evicted = false;
        bool safeEviction = false;
        bool promoted = false;
        if (queue.Items.Count >= queue.Cap)
        {
            if (queue.Cap < _hotPerSymbolCap)
            {
                // First overflow: promote to hot cap, no eviction.
                queue.Cap = _hotPerSymbolCap;
                promoted = true;
            }
            else if (queue.Items.TryDequeue(out var old))
            {
                evictedBytes = old.BodyLength;
                evictedRptSeq = old.RptSeq;
                evicted = true;
                // Floor pin: if the evicted message is below the snapshot floor,
                // the in-flight snapshot already covers it — safe to drop without
                // pushing MinHeal forward. This is what allows a high-rate symbol
                // to absorb msgs during the snapshot's Begin→End delivery window
                // without poisoning its own heal.
                safeEviction = queue.ProtectedFloor != 0 && old.RptSeq < queue.ProtectedFloor;
                old.Release();
                if (safeEviction) _safeEvictedBelowFloor++;
                else _evictedPerSymbolCap++;
            }
        }
        var rented = ArrayPool<byte>.Shared.Rent(body.Length);
        body.CopyTo(rented);
        queue.Items.Enqueue(new DeferredMboMsg(templateId, rptSeq, sendingTimeNs, rented, body.Length));
        Volatile.Write(ref _totalBytes, Volatile.Read(ref _totalBytes) + body.Length - evictedBytes);
        _enqueued++;
        if (promoted)
        {
            _hotPromotions++;
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "StaleMboBuffer: symbol {SecurityId} promoted to hot cap ({HotCap}); normal cap {NormalCap} exceeded",
                    securityId, _hotPerSymbolCap, _perSymbolCap);
        }
        if (evicted && !safeEviction) onEvictedOldest?.Invoke(evictedRptSeq);
        return true;
    }

    /// <summary>
    /// Mark the symbol as having an in-flight snapshot whose baseline rptSeq is
    /// <paramref name="floor"/> − 1. While the floor is set, hot-cap eviction
    /// of messages with <c>rptSeq &lt; floor</c> is "safe" (snapshot covers
    /// them) and does NOT advance <see cref="SymbolStateRegistry.BumpMinHeal"/>.
    /// Eviction of messages with <c>rptSeq &gt;= floor</c> still advances
    /// MinHeal as a last-resort safety guard. The floor is monotonic per snapshot
    /// — calling again with a smaller value is a no-op (a newer snapshot only
    /// raises the floor; a stale set would weaken the protection). Caller MUST
    /// invoke <see cref="ClearProtectedFloor"/> on snapshot completion or rejection.
    /// </summary>
    public void SetProtectedFloor(ulong securityId, uint floor)
    {
        if (floor == 0) return;
        if (!_queues.TryGetValue(securityId, out var queue))
        {
            queue = new PerSymbolQueue { Cap = _perSymbolCap };
            _queues[securityId] = queue;
        }
        if (floor > queue.ProtectedFloor) queue.ProtectedFloor = floor;
    }

    /// <summary>
    /// Remove the protected-floor pin for a symbol (snapshot completed,
    /// rejected, or superseded). Subsequent evictions revert to bumping
    /// MinHeal as before. No-op if no floor was set.
    /// </summary>
    public void ClearProtectedFloor(ulong securityId)
    {
        if (_queues.TryGetValue(securityId, out var queue))
            queue.ProtectedFloor = 0;
    }

    /// <summary>Current protected floor for a symbol (0 if none). Test/diagnostic helper.</summary>
    public uint ProtectedFloorOf(ulong securityId) =>
        _queues.TryGetValue(securityId, out var q) ? q.ProtectedFloor : 0u;

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
        Volatile.Write(ref _totalBytes, Volatile.Read(ref _totalBytes) - releasedBytes);
        _drained += applied;
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
        while (queue.Items.TryDequeue(out var item))
        {
            releasedBytes += item.BodyLength;
            item.Release();
            count++;
        }
        // Clear floor too — it is only meaningful while pendingSnapshot is alive,
        // and Clear is called by the symbol-level reset paths (epoch reset, drain
        // window-empty CompleteSnapshot success).
        queue.ProtectedFloor = 0;
        Volatile.Write(ref _totalBytes, Volatile.Read(ref _totalBytes) - releasedBytes);
        return count;
    }

    /// <summary>Discard all buffers across every symbol (catastrophic reset path).</summary>
    public int ClearAll()
    {
        int total = 0;
        // Snapshot keys first to avoid mutating-while-enumerating risk.
        ulong[] keys = new ulong[_queues.Count];
        int i = 0;
        foreach (var kv in _queues) keys[i++] = kv.Key;
        foreach (var k in keys) total += Clear(k);
        return total;
    }

    /// <summary>Current depth of a symbol's queue (for tests/metrics).</summary>
    public int DepthOf(ulong securityId) =>
        _queues.TryGetValue(securityId, out var q) ? q.Items.Count : 0;
}
