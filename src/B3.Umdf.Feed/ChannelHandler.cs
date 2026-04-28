using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

/// <summary>
/// Per-channel-group incremental handler with A/B feed arbitration via a
/// small reorder buffer.
///
/// A and B carry identical sequenced content over independent multicast paths.
/// In a perfect world a packet missing from A is still delivered by B (and
/// vice-versa). The challenge is that the two paths arrive with slightly
/// different timing — if A delivers seq N+1 before B delivers seq N, a naive
/// gap detector will declare a gap and trigger snapshot recovery, even though
/// B is microseconds away from filling the hole.
///
/// This handler delays the gap declaration: when a future packet arrives we
/// stash it in <see cref="_reorderBuffer"/> (keyed by SeqNum) and keep
/// processing. As soon as the missing packet is delivered (typically by the
/// other feed) we drain the buffer in order. Only when the gap exceeds
/// <see cref="MaxReorderDistance"/> packets do we declare a real gap and let
/// the FeedHandler fall into snapshot recovery.
///
/// The buffer holds retained <see cref="UmdfPacket"/> instances; the handler
/// drains and releases them on <see cref="Dispose"/>.
/// </summary>
public sealed class ChannelHandler : IDisposable
{
    /// <summary>
    /// Default reorder window (instance value can override via constructor).
    /// 256 packets ≈ 384 KB pinned per group worst case. At 30k pkt/s that
    /// is ≈ 8 ms of look-ahead. We saw bursts of exactly 129 packets in
    /// loopback at REPLAY=0 (one over the previous 128-window) tripping
    /// recovery; doubling the window absorbs them with negligible memory cost.
    /// </summary>
    public const int MaxReorderDistance = 256;

    private readonly IFeedEventHandler _eventHandler;
    private readonly int _maxReorderDistance;
    private readonly Dictionary<uint, UmdfPacket> _reorderBuffer;

    private uint _expectedSeqNum = 1;

    /// <summary>
    /// Last observed SequenceVersion from the incremental stream PacketHeader.
    /// 0 means "no packet observed yet" (pre-bootstrap). When a packet
    /// arrives with a different non-zero version, the channel resets
    /// (B3 spec §6.5.5.1 — weekly rollover or failover).
    /// </summary>
    private ushort _currentSequenceVersion;

    private long _packetsProcessed;
    private long _duplicatesSkipped;
    private long _gapsDetected;
    private long _reorderHits;
    private long _sequenceVersionResets;
    private int _maxObservedReorderDepth;
    private volatile uint _lastGapExpected;
    private volatile uint _lastGapReceived;

    public uint ExpectedSequenceNumber => _expectedSeqNum;
    /// <summary>Current SequenceVersion observed on this channel; 0 before first packet.</summary>
    public ushort CurrentSequenceVersion => _currentSequenceVersion;
    public long PacketsProcessed => Volatile.Read(ref _packetsProcessed);
    public long DuplicatesSkipped => Volatile.Read(ref _duplicatesSkipped);
    public long GapsDetected => Volatile.Read(ref _gapsDetected);
    /// <summary>
    /// Count of channel-wide resets triggered by a SequenceVersion change
    /// in the PacketHeader (B3 spec §6.5.5.1).
    /// </summary>
    public long SequenceVersionResets => Volatile.Read(ref _sequenceVersionResets);
    /// <summary>
    /// Count of packets that arrived out-of-order and were later drained from the
    /// reorder buffer (i.e. saved a recovery cycle thanks to A/B arbitration).
    /// </summary>
    public long ReorderHits => Volatile.Read(ref _reorderHits);
    /// <summary>Current depth of the reorder buffer.</summary>
    public int ReorderBufferDepth => _reorderBuffer.Count;
    /// <summary>
    /// High-watermark of <see cref="ReorderBufferDepth"/> ever observed since
    /// process start. Useful to size <see cref="MaxReorderDistanceConfigured"/>
    /// — if this approaches the configured limit, real gaps are being declared
    /// just below the threshold and bumping the window may avoid recoveries.
    /// </summary>
    public int MaxObservedReorderBufferDepth => Volatile.Read(ref _maxObservedReorderDepth);
    /// <summary>Effective per-instance reorder window (constructor-injected; defaults to <see cref="MaxReorderDistance"/>).</summary>
    public int MaxReorderDistanceConfigured => _maxReorderDistance;
    public uint LastGapExpected => _lastGapExpected;
    public uint LastGapReceived => _lastGapReceived;

    public ChannelHandler(IFeedEventHandler eventHandler)
        : this(eventHandler, MaxReorderDistance) { }

    public ChannelHandler(IFeedEventHandler eventHandler, int maxReorderDistance)
    {
        if (maxReorderDistance < 1)
            throw new ArgumentOutOfRangeException(nameof(maxReorderDistance), "Must be ≥ 1.");
        _eventHandler = eventHandler;
        _maxReorderDistance = maxReorderDistance;
        _reorderBuffer = new Dictionary<uint, UmdfPacket>(maxReorderDistance);
    }

    public GapResult HandlePacket(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (!PacketHeader.TryParse(span, out var header, out _))
            return GapResult.InSequence;

        uint seq = header.SequenceNumber;

        // Spec §6.5.5.1: SequenceVersion increments on weekly rollover or
        // failover; SequenceNumber resets to 1 in the new version. Detect
        // the version change before any seq comparison so we don't treat
        // post-rollover seq=1 as a "duplicate" of pre-rollover seq=N.
        ushort version = header.SequenceVersion;
        if (_currentSequenceVersion == 0)
        {
            _currentSequenceVersion = version;
        }
        else if (version != _currentSequenceVersion)
        {
            // Spec §6.5.5.1: SequenceVersion is monotonically increasing per
            // session. A packet whose version is OLDER than the established
            // current is a stale reorder-arrival from before the rollover —
            // upstream (BookManager / SymbolStateRegistry) has already reset
            // its V1 state. Replaying it would corrupt the V2 epoch. Drop
            // silently as a duplicate (the other feed delivered the V2
            // version first).
            //
            // ushort comparison would wrap if version actually rolls past
            // 65535 (impossible in practice — weekly bumps for ~1259 years).
            // We treat that hypothetical wrap conservatively: only accept
            // strict-greater within a 16-bit window, which keeps the gate
            // robust against any single misordered packet.
            if (version < _currentSequenceVersion)
            {
                _duplicatesSkipped++;
                return GapResult.Duplicate;
            }
            HandleSequenceVersionChange(version, seq);
            // Fall through with the post-reset _expectedSeqNum so this
            // packet (the first of the new version) is processed normally.
        }

        // Cold-start: if the very first packet is far above the initial
        // _expectedSeqNum=1 (live multicast joined mid-stream), seed the
        // baseline from this seq rather than treating it as a giant gap.
        // The per-symbol layer heals via snapshot, so the channel just needs
        // a sane baseline. Bounded by MaxReorderDistance so legitimate small
        // start-from-1 scenarios (tests, replay) keep the existing behavior.
        if (_packetsProcessed == 0 && _reorderBuffer.Count == 0
            && seq > _expectedSeqNum
            && seq - _expectedSeqNum > _maxReorderDistance)
        {
            _expectedSeqNum = seq;
        }

        // Already past expected — duplicate from the other feed (or already
        // filled by reorder drain).
        if (seq < _expectedSeqNum)
        {
            _duplicatesSkipped++;
            return GapResult.Duplicate;
        }

        // In sequence: process and drain any consecutive packets that the
        // reorder buffer was holding for this exact slot.
        if (seq == _expectedSeqNum)
        {
            ProcessAndAdvance(in packet, span);
            DrainReorderedConsecutives();
            return GapResult.InSequence;
        }

        // Future packet. If within the reorder window, stash and wait for the
        // hole to fill. Otherwise declare a real gap.
        uint distance = seq - _expectedSeqNum;
        if (distance > _maxReorderDistance)
        {
            _gapsDetected++;
            _lastGapExpected = _expectedSeqNum;
            _lastGapReceived = seq;
            return GapResult.Gap;
        }

        // Already buffered (other feed delivered it first) → duplicate.
        if (_reorderBuffer.ContainsKey(seq))
        {
            _duplicatesSkipped++;
            return GapResult.Duplicate;
        }

        packet.Retain();
        _reorderBuffer[seq] = packet;
        int depth = _reorderBuffer.Count;
        if (depth > Volatile.Read(ref _maxObservedReorderDepth))
            Volatile.Write(ref _maxObservedReorderDepth, depth);
        return GapResult.InSequence;
    }

    /// <summary>
    /// Helper for the per-symbol heal path: when a real gap is declared
    /// (distance &gt; <see cref="MaxReorderDistance"/>), dispatch the gapped
    /// packet, advance the expected SeqNum past it, and dispatch any
    /// reorder-buffered packets ahead in SeqNum order. Per-symbol routing
    /// handles healing on a per-instrument basis — there is no channel-level
    /// Recovery to enter.
    /// </summary>
    public void AcceptGapAndAdvance(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (!PacketHeader.TryParse(span, out var header, out _))
            return;
        uint seq = header.SequenceNumber;

        _packetsProcessed++;
        MessageDispatcher.Dispatch(in packet, span, _eventHandler);
        _eventHandler.OnPacketProcessed();
        _expectedSeqNum = seq + 1;

        if (_reorderBuffer.Count > 0)
        {
            var ordered = _reorderBuffer.Values.OrderBy(static p =>
            {
                PacketHeader.TryParse(p.Data.Span, out var h, out _);
                return h.SequenceNumber;
            }).ToList();
            _reorderBuffer.Clear();

            foreach (var p in ordered)
            {
                try
                {
                    if (!PacketHeader.TryParse(p.Data.Span, out var ph, out _))
                        continue;
                    if (ph.SequenceNumber < _expectedSeqNum)
                        continue; // covered by current packet (shouldn't happen, defensive)
                    _packetsProcessed++;
                    MessageDispatcher.Dispatch(in p, p.Data.Span, _eventHandler);
                    _eventHandler.OnPacketProcessed();
                    _expectedSeqNum = ph.SequenceNumber + 1;
                }
                finally
                {
                    p.Release();
                }
            }
        }
    }

    public void Dispose()
    {
        DiscardReorderBuffer();
    }

    /// <summary>
    /// Reset to follow a new SequenceVersion. Discards any reorder-buffered
    /// packets (they belonged to the previous version's seq space) and
    /// re-baselines <see cref="_expectedSeqNum"/> to the first observed seq
    /// of the new version. Notifies the event handler so per-symbol /
    /// per-instrument state can be reset by upstream managers.
    /// </summary>
    private void HandleSequenceVersionChange(ushort newVersion, uint firstSeqOfNewVersion)
    {
        DiscardReorderBuffer();
        _currentSequenceVersion = newVersion;
        _expectedSeqNum = firstSeqOfNewVersion;
        Interlocked.Increment(ref _sequenceVersionResets);
        try { _eventHandler.OnSequenceVersionChanged(newVersion); }
        catch { /* event handler exceptions must not destabilize the channel; counters still reflect the reset */ }
    }

    private void ProcessAndAdvance(in UmdfPacket packet, ReadOnlySpan<byte> span)
    {
        _packetsProcessed++;
        _expectedSeqNum++;
        MessageDispatcher.Dispatch(in packet, span, _eventHandler);
        _eventHandler.OnPacketProcessed();
    }

    private void DrainReorderedConsecutives()
    {
        while (_reorderBuffer.Remove(_expectedSeqNum, out var queued))
        {
            try
            {
                _reorderHits++;
                _packetsProcessed++;
                _expectedSeqNum++;
                MessageDispatcher.Dispatch(in queued, queued.Data.Span, _eventHandler);
                _eventHandler.OnPacketProcessed();
            }
            finally
            {
                queued.Release();
            }
        }
    }

    private void DiscardReorderBuffer()
    {
        if (_reorderBuffer.Count == 0)
            return;
        foreach (var kvp in _reorderBuffer)
            kvp.Value.Release();
        _reorderBuffer.Clear();
    }
}
