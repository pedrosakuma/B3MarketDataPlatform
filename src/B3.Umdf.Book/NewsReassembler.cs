using System.Buffers;
using System.Diagnostics;
using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Book;

/// <summary>
/// Reassembles multi-part <c>News_5</c> messages and invokes a callback once a
/// complete delivery is available. All access is from the feed thread
/// (single-writer); no internal synchronization.
///
/// <para>
/// Caps and policies (P13-2, derived from rubber-duck review):
/// </para>
/// <list type="bullet">
///   <item><description><see cref="MaxPartCount"/> = 64. Larger PartCount values
///   are dropped (defensive cap against malformed or hostile inputs).</description></item>
///   <item><description><see cref="MaxInflight"/> = 1024 simultaneous in-flight
///   news IDs. LRU eviction beyond that.</description></item>
///   <item><description><see cref="MaxInflightBytes"/> = 16 MiB aggregate buffer
///   budget. LRU eviction when exceeded.</description></item>
///   <item><description><see cref="TtlTicks"/> ≈ 5 seconds (monotonic). Expired
///   assemblies dropped on the next insert.</description></item>
///   <item><description>NewsID == 0 with PartCount > 1 is dropped (no safe
///   reassembly key — see B3 critique). PartCount == 1 is emitted directly
///   regardless of NewsID, no state retained.</description></item>
///   <item><description>Per-part header invariants (SecurityID, NewsSource,
///   LanguageCode, PartCount, TotalTextLength) must match the first-seen header
///   of an in-flight assembly; mismatch drops the entire assembly.</description></item>
/// </list>
/// </summary>
internal sealed class NewsReassembler
{
    public const int MaxPartCount = 64;
    public const int MaxInflight = 1024;
    public const long MaxInflightBytes = 16L * 1024 * 1024;
    public static readonly long TtlTicks = TimeSpan.FromSeconds(5).Ticks;

    /// <summary>Receiver of completed news. Spans are valid only during the call.</summary>
    public delegate void NewsCompletedCallback(
        ulong securityIdOrZero,
        ulong newsId,
        byte source,
        ushort language,
        long origTimeNanos,
        ReadOnlySpan<byte> headline,
        ReadOnlySpan<byte> text,
        ReadOnlySpan<byte> url);

    private readonly NewsCompletedCallback _onComplete;
    private readonly Func<long> _monotonicTicks;
    private readonly Dictionary<ulong, Assembly> _inflight = new(MaxInflight);
    // LinkedList tracks LRU order — most recently used at the tail.
    private readonly LinkedList<ulong> _lru = new();
    private long _inflightBytes;

    // Counters (read by tests / metrics binder)
    public long PartsReceived { get; private set; }
    public long Reassembled { get; private set; }
    public long DroppedCap { get; private set; }
    public long DroppedTtl { get; private set; }
    public long DroppedNoId { get; private set; }
    public long DroppedInconsistent { get; private set; }
    public long DroppedInvalidPart { get; private set; }
    public int Inflight => _inflight.Count;
    public long InflightBytes => _inflightBytes;

    public NewsReassembler(NewsCompletedCallback onComplete, Func<long>? monotonicTicks = null)
    {
        _onComplete = onComplete ?? throw new ArgumentNullException(nameof(onComplete));
        _monotonicTicks = monotonicTicks ?? (() => Stopwatch.GetTimestamp());
    }

    /// <summary>Submit one News_5 part. Spans are consumed synchronously; safe
    /// to back by any short-lived buffer (e.g. SBE message buffer).</summary>
    public void Submit(
        ulong securityIdOrZero,
        ulong newsId,
        byte source,
        ushort language,
        ushort partCount,
        ushort partNumber,
        long origTimeNanos,
        uint totalTextLength,
        ReadOnlySpan<byte> headline,
        ReadOnlySpan<byte> text,
        ReadOnlySpan<byte> url)
    {
        PartsReceived++;

        // Validate part bookkeeping.
        if (partCount == 0 || partCount > MaxPartCount)
        {
            DroppedInvalidPart++;
            return;
        }
        if (partNumber == 0 || partNumber > partCount)
        {
            DroppedInvalidPart++;
            return;
        }

        // Single-part fast path — emit immediately, no state.
        if (partCount == 1)
        {
            Reassembled++;
            _onComplete(securityIdOrZero, newsId, source, language, origTimeNanos, headline, text, url);
            return;
        }

        // Multi-part with no NewsID — no safe key; drop.
        if (newsId == 0)
        {
            DroppedNoId++;
            return;
        }

        SweepExpired();

        if (!_inflight.TryGetValue(newsId, out var asm))
        {
            EvictIfNeeded();
            asm = new Assembly(securityIdOrZero, newsId, source, language, partCount, totalTextLength, _monotonicTicks());
            _inflight[newsId] = asm;
            asm.LruNode = _lru.AddLast(newsId);
        }
        else
        {
            // Validate per-part invariants.
            if (asm.SecurityIdOrZero != securityIdOrZero
                || asm.Source != source
                || asm.Language != language
                || asm.PartCount != partCount
                || asm.TotalTextLength != totalTextLength)
            {
                DroppedInconsistent++;
                Remove(newsId, asm);
                return;
            }
            // Touch LRU.
            _lru.Remove(asm.LruNode!);
            asm.LruNode = _lru.AddLast(newsId);
        }

        // Duplicate part: silent ignore.
        if (asm.HasPart(partNumber)) return;

        // Append fragments (per-field). All three buffers grow together by part,
        // each part contributing its own headline/text/url chunk (concatenated
        // in PartNumber order on emit).
        asm.AddPart(partNumber, headline, text, url, ref _inflightBytes);

        // First-seen origTime wins — clients see the time of the first arriving fragment.
        if (asm.PartsReceived == 1) asm.OrigTimeNanos = origTimeNanos;

        if (asm.PartsReceived == asm.PartCount)
        {
            EmitAndRelease(asm);
        }
    }

    private void EmitAndRelease(Assembly asm)
    {
        // Concatenate per-part fragments into a single contiguous span per field.
        // Use ArrayPool buffers for the temporary contiguous representation.
        byte[] headlineBuf = ArrayPool<byte>.Shared.Rent(asm.HeadlineTotalBytes);
        byte[] textBuf = ArrayPool<byte>.Shared.Rent(asm.TextTotalBytes);
        byte[] urlBuf = ArrayPool<byte>.Shared.Rent(asm.UrlTotalBytes);
        try
        {
            int hOff = 0, tOff = 0, uOff = 0;
            for (int p = 1; p <= asm.PartCount; p++)
            {
                int idx = p - 1;
                var h = asm.HeadlinePart(idx);
                var t = asm.TextPart(idx);
                var u = asm.UrlPart(idx);
                h.CopyTo(headlineBuf.AsSpan(hOff)); hOff += h.Length;
                t.CopyTo(textBuf.AsSpan(tOff)); tOff += t.Length;
                u.CopyTo(urlBuf.AsSpan(uOff)); uOff += u.Length;
            }
            Reassembled++;
            _onComplete(
                asm.SecurityIdOrZero, asm.NewsId, asm.Source, asm.Language, asm.OrigTimeNanos,
                headlineBuf.AsSpan(0, hOff), textBuf.AsSpan(0, tOff), urlBuf.AsSpan(0, uOff));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(headlineBuf);
            ArrayPool<byte>.Shared.Return(textBuf);
            ArrayPool<byte>.Shared.Return(urlBuf);
            Remove(asm.NewsId, asm);
        }
    }

    private void Remove(ulong newsId, Assembly asm)
    {
        _inflight.Remove(newsId);
        if (asm.LruNode != null) _lru.Remove(asm.LruNode);
        _inflightBytes -= asm.BytesHeld;
        asm.Release();
    }

    private void SweepExpired()
    {
        long now = _monotonicTicks();
        // Walk LRU from oldest; stop at first non-expired (LRU is ordered by
        // last-touch which is monotonic; entries near head are oldest).
        var node = _lru.First;
        while (node != null)
        {
            var next = node.Next;
            if (!_inflight.TryGetValue(node.Value, out var asm))
            {
                _lru.Remove(node);
                node = next;
                continue;
            }
            if (now - asm.CreatedTicks <= TtlTicks) break;
            DroppedTtl++;
            Remove(node.Value, asm);
            node = next;
        }
    }

    private void EvictIfNeeded()
    {
        while (_inflight.Count >= MaxInflight || _inflightBytes >= MaxInflightBytes)
        {
            var oldest = _lru.First;
            if (oldest == null) break;
            if (!_inflight.TryGetValue(oldest.Value, out var asm))
            {
                _lru.RemoveFirst();
                continue;
            }
            DroppedCap++;
            Remove(oldest.Value, asm);
        }
    }

    private sealed class Assembly
    {
        public readonly ulong SecurityIdOrZero;
        public readonly ulong NewsId;
        public readonly byte Source;
        public readonly ushort Language;
        public readonly ushort PartCount;
        public readonly uint TotalTextLength;
        public readonly long CreatedTicks;
        public long OrigTimeNanos;
        public LinkedListNode<ulong>? LruNode;

        public int PartsReceived;
        public int HeadlineTotalBytes;
        public int TextTotalBytes;
        public int UrlTotalBytes;
        public int BytesHeld;

        // Per-part rented buffers. Indexed [0..PartCount-1], each slot may be null until that part arrives.
        private readonly byte[]?[] _headlineParts;
        private readonly byte[]?[] _textParts;
        private readonly byte[]?[] _urlParts;
        // Actual lengths used inside each rented buffer.
        private readonly int[] _headlineLens;
        private readonly int[] _textLens;
        private readonly int[] _urlLens;
        private readonly bool[] _received;

        public Assembly(ulong securityIdOrZero, ulong newsId, byte source, ushort language,
                        ushort partCount, uint totalTextLength, long createdTicks)
        {
            SecurityIdOrZero = securityIdOrZero;
            NewsId = newsId;
            Source = source;
            Language = language;
            PartCount = partCount;
            TotalTextLength = totalTextLength;
            CreatedTicks = createdTicks;
            _headlineParts = new byte[partCount][];
            _textParts = new byte[partCount][];
            _urlParts = new byte[partCount][];
            _headlineLens = new int[partCount];
            _textLens = new int[partCount];
            _urlLens = new int[partCount];
            _received = new bool[partCount];
        }

        public bool HasPart(ushort partNumber) => _received[partNumber - 1];

        public void AddPart(ushort partNumber,
                            ReadOnlySpan<byte> headline,
                            ReadOnlySpan<byte> text,
                            ReadOnlySpan<byte> url,
                            ref long inflightBytesCounter)
        {
            int idx = partNumber - 1;
            _received[idx] = true;
            PartsReceived++;

            if (headline.Length > 0)
            {
                var buf = ArrayPool<byte>.Shared.Rent(headline.Length);
                headline.CopyTo(buf);
                _headlineParts[idx] = buf;
                _headlineLens[idx] = headline.Length;
                HeadlineTotalBytes += headline.Length;
                BytesHeld += headline.Length;
                inflightBytesCounter += headline.Length;
            }
            if (text.Length > 0)
            {
                var buf = ArrayPool<byte>.Shared.Rent(text.Length);
                text.CopyTo(buf);
                _textParts[idx] = buf;
                _textLens[idx] = text.Length;
                TextTotalBytes += text.Length;
                BytesHeld += text.Length;
                inflightBytesCounter += text.Length;
            }
            if (url.Length > 0)
            {
                var buf = ArrayPool<byte>.Shared.Rent(url.Length);
                url.CopyTo(buf);
                _urlParts[idx] = buf;
                _urlLens[idx] = url.Length;
                UrlTotalBytes += url.Length;
                BytesHeld += url.Length;
                inflightBytesCounter += url.Length;
            }
        }

        public ReadOnlySpan<byte> HeadlinePart(int idx) => _headlineParts[idx] is { } b ? b.AsSpan(0, _headlineLens[idx]) : ReadOnlySpan<byte>.Empty;
        public ReadOnlySpan<byte> TextPart(int idx) => _textParts[idx] is { } b ? b.AsSpan(0, _textLens[idx]) : ReadOnlySpan<byte>.Empty;
        public ReadOnlySpan<byte> UrlPart(int idx) => _urlParts[idx] is { } b ? b.AsSpan(0, _urlLens[idx]) : ReadOnlySpan<byte>.Empty;

        public void Release()
        {
            for (int i = 0; i < PartCount; i++)
            {
                if (_headlineParts[i] is { } h) { ArrayPool<byte>.Shared.Return(h); _headlineParts[i] = null; }
                if (_textParts[i] is { } t) { ArrayPool<byte>.Shared.Return(t); _textParts[i] = null; }
                if (_urlParts[i] is { } u) { ArrayPool<byte>.Shared.Return(u); _urlParts[i] = null; }
            }
        }
    }
}
