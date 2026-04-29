using System.Collections.Concurrent;

namespace B3.Umdf.Server;

/// <summary>
/// Per-packet work unit handed from the feed/dispatch thread to the group's broadcaster
/// thread. Holds the concatenated wire-bytes of every event that was produced while
/// handling a single UDP packet, plus per-event records pointing back into the buffer.
///
/// The batch is rented from <see cref="Pool"/> by the dispatch thread, populated via
/// <see cref="Append"/>, published to the broadcast ring on <c>OnBatchComplete</c>, and
/// released back to the pool by the broadcaster thread after fan-out. Because exactly
/// one dispatch thread and one broadcaster thread touch each batch at a time, no
/// internal synchronization is needed.
/// </summary>
internal sealed class BroadcastWorkBatch
{
    /// <summary>
    /// Discriminator for routing events on the broadcaster thread.
    /// </summary>
    public enum EventKind : byte
    {
        /// <summary>Default: route to subscribers of <see cref="EventRecord.SecId"/>
        /// that have <c>DataFlags.Book</c>. Existing book/trade/info path.</summary>
        BookSubscribers = 0,
        /// <summary>News scoped to <see cref="EventRecord.SecId"/>: route to
        /// subscribers of that securityId that have <c>DataFlags.News</c>.</summary>
        NewsForSecurity = 1,
        /// <summary>Global news (<see cref="EventRecord.SecId"/> = 0): route to
        /// every connected client with <c>DataFlags.News</c>, regardless of
        /// per-symbol subscriptions.</summary>
        NewsGlobal = 2,
        /// <summary>MBP (price-level) frames: route to subscribers of
        /// <see cref="EventRecord.SecId"/> that have <c>DataFlags.Mbp</c>.
        /// Honors the same broadcast-sequence cutoff as Book.</summary>
        MbpSubscribers = 3,
        /// <summary>Shared book-context frames (BookCleared, StaleStatus,
        /// MarketTier, CandleUpdate): route to subscribers of
        /// <see cref="EventRecord.SecId"/> that have either <c>DataFlags.Book</c>
        /// or <c>DataFlags.Mbp</c>. Both views need these signals; only the
        /// per-order vs per-level deltas are stream-specific.</summary>
        BookOrMbpSubscribers = 4,
        /// <summary>Trade prints and corrections (<see cref="MessageType.Trade"/>,
        /// <see cref="MessageType.TradeBust"/>): route only to subscribers of
        /// <see cref="EventRecord.SecId"/> that have <c>DataFlags.Trades</c>.
        /// Honors the same broadcast-sequence cutoff as Book/Mbp.</summary>
        TradeSubscribers = 5,
    }

    public struct EventRecord
    {
        public ulong SecId;
        public int Offset;
        public int Len;
        public int LogicalCount;
        public EventKind Kind;
    }

    public byte[] Buffer = new byte[4096];
    public int BufferLen;
    public long Sequence;

    public EventRecord[] Events = new EventRecord[64];
    public int EventCount;

    public void Reset()
    {
        BufferLen = 0;
        EventCount = 0;
        Sequence = 0;
    }

    public void Append(ulong secId, ReadOnlySpan<byte> bytes, int logicalCount)
        => Append(secId, bytes, logicalCount, EventKind.BookSubscribers);

    public void Append(ulong secId, ReadOnlySpan<byte> bytes, int logicalCount, EventKind kind)
    {
        if (bytes.Length == 0) return;
        int newLen = BufferLen + bytes.Length;
        if (newLen > Buffer.Length)
        {
            int cap = Buffer.Length;
            while (cap < newLen) cap *= 2;
            Array.Resize(ref Buffer, cap);
        }
        if (EventCount == Events.Length)
        {
            Array.Resize(ref Events, Events.Length * 2);
        }
        bytes.CopyTo(Buffer.AsSpan(BufferLen));
        Events[EventCount++] = new EventRecord
        {
            SecId = secId,
            Offset = BufferLen,
            Len = bytes.Length,
            LogicalCount = logicalCount,
            Kind = kind,
        };
        BufferLen = newLen;
    }

    public ReadOnlySpan<byte> GetEventBytes(int index)
    {
        ref var e = ref Events[index];
        return Buffer.AsSpan(e.Offset, e.Len);
    }

    // ── Pool ─────────────────────────────────────────────────────────────────────
    //
    // Lock-free pool. Dispatch thread is the sole renter; broadcaster thread is the
    // sole returner. A ConcurrentStack keeps allocations low without ever blocking
    // the dispatch thread. The pool is unbounded in theory but bounded in practice
    // by the broadcast ring capacity (dispatch cannot outrun the ring + pool by more
    // than ring.Capacity batches in flight).

    private static readonly ConcurrentStack<BroadcastWorkBatch> Pool = new();

    public static BroadcastWorkBatch Rent()
    {
        if (Pool.TryPop(out var batch))
        {
            batch.Reset();
            return batch;
        }
        return new BroadcastWorkBatch();
    }

    public static void Return(BroadcastWorkBatch batch)
    {
        batch.Reset();
        // Cap pool size to avoid unbounded growth if a pathological spike inflated many batches.
        if (Pool.Count < 256) Pool.Push(batch);
    }
}
