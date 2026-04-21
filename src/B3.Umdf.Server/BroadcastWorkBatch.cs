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
    public struct EventRecord
    {
        public ulong SecId;
        public int Offset;
        public int Len;
        public int LogicalCount;
    }

    public byte[] Buffer = new byte[4096];
    public int BufferLen;

    public EventRecord[] Events = new EventRecord[64];
    public int EventCount;

    public void Reset()
    {
        BufferLen = 0;
        EventCount = 0;
    }

    public void Append(ulong secId, ReadOnlySpan<byte> bytes, int logicalCount)
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
