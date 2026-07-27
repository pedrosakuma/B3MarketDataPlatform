using B3.Umdf.Mbo.Sbe.V17;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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

    /// <summary>
    /// Wall-clock stall escape valve (issue #71): if <see cref="_expectedSeqNum"/>
    /// has not advanced for this many milliseconds (measured via the
    /// packet-supplied <see cref="UmdfPacket.ReceivedTimestampTicks"/>, same
    /// clock source as <c>FeedHandler.InstrDefStuckTimeoutMs</c>) while a
    /// future packet is pending, the channel force-declares a real gap
    /// regardless of how small the SeqNum distance is. Bounds how long a
    /// low-volume channel (few packets/sec) can be wedged waiting for a
    /// backlog that will never grow past <see cref="MaxReorderDistance"/>.
    /// Well above realistic A/B feed jitter (single-digit ms) so legitimate
    /// short-lived reordering is unaffected.
    /// </summary>
    public const long ReorderStallTimeoutMs = 2_000;

    private readonly IFeedEventHandler _eventHandler;
    private readonly int _maxReorderDistance;
    private readonly Dictionary<uint, UmdfPacket> _reorderBuffer;
    private readonly ChannelEpochCoordinator _epochCoordinator;

    private uint _expectedSeqNum = 1;
    private long _expectedSeqNumAdvancedAtTicks;
    private bool _stallClockStarted;
    private bool _awaitingFirstPacket;
    private ushort _awaitingFirstPacketVersion;

    private long _packetsProcessed;
    private long _duplicatesSkipped;
    private long _gapsDetected;
    private long _reorderHits;
    private int _maxObservedReorderDepth;
    private volatile uint _lastGapExpected;
    private volatile uint _lastGapReceived;

    public uint ExpectedSequenceNumber => _expectedSeqNum;
    /// <summary>Current SequenceVersion observed on this channel; 0 before first packet.</summary>
    public ushort CurrentSequenceVersion => _epochCoordinator.CurrentVersion;
    public long PacketsProcessed => Volatile.Read(ref _packetsProcessed);
    public long DuplicatesSkipped => Volatile.Read(ref _duplicatesSkipped);
    public long GapsDetected => Volatile.Read(ref _gapsDetected);
    /// <summary>
    /// Count of channel-wide epoch advances observed from either the
    /// incremental packet header or snapshot LastSequenceVersion.
    /// </summary>
    public long SequenceVersionResets => _epochCoordinator.EpochAdvances;
    /// <summary>
    /// Number of exceptions thrown by the downstream <see cref="IFeedEventHandler.OnSequenceVersionChanged"/>
    /// callback during a SequenceVersion change. Previously these were silently
    /// swallowed; now we surface a counter and log so a misbehaving handler can
    /// be diagnosed without destabilising the channel.
    /// </summary>
    public long SequenceResetHandlerExceptionCount => _epochCoordinator.HandlerExceptionCount;
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
        : this(eventHandler, MaxReorderDistance, NullLogger.Instance) { }

    public ChannelHandler(IFeedEventHandler eventHandler, int maxReorderDistance)
        : this(eventHandler, maxReorderDistance, NullLogger.Instance) { }

    public ChannelHandler(IFeedEventHandler eventHandler, int maxReorderDistance, ILogger logger)
        : this(eventHandler, maxReorderDistance, logger, new ChannelEpochCoordinator(logger))
    {
        _epochCoordinator.RegisterEventHandler(eventHandler);
    }

    public ChannelHandler(
        IFeedEventHandler eventHandler,
        int maxReorderDistance,
        ILogger logger,
        ChannelEpochCoordinator epochCoordinator)
    {
        if (maxReorderDistance < 1)
            throw new ArgumentOutOfRangeException(nameof(maxReorderDistance), "Must be ≥ 1.");
        _eventHandler = eventHandler;
        _maxReorderDistance = maxReorderDistance;
        _reorderBuffer = new Dictionary<uint, UmdfPacket>(maxReorderDistance);
        _epochCoordinator = epochCoordinator ?? throw new ArgumentNullException(nameof(epochCoordinator));
        _epochCoordinator.RegisterChannelStateReset(OnEpochAdvanced);
        if (_epochCoordinator.CurrentVersion != 0)
        {
            _awaitingFirstPacket = true;
            _awaitingFirstPacketVersion = _epochCoordinator.CurrentVersion;
        }
    }

    public GapResult HandlePacket(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (!PacketHeader.TryParse(span, out var header, out _))
            return GapResult.InSequence;

        uint seq = header.SequenceNumber;

        // Start the stall-escape clock on the very first packet this
        // instance ever sees (any channel type/path), so the timeout is
        // measured relative to real elapsed time rather than an arbitrary
        // zero baseline (which could otherwise be far in the past relative
        // to Environment.TickCount64-based timestamps and fire spuriously).
        if (!_stallClockStarted)
        {
            _expectedSeqNumAdvancedAtTicks = packet.ReceivedTimestampTicks;
            _stallClockStarted = true;
        }

        ushort version = header.SequenceVersion;
        var epochObservation = _epochCoordinator.Observe(version, ChannelEpochSource.Incremental);
        if (epochObservation == ChannelEpochObservation.Stale)
        {
            _duplicatesSkipped++;
            return GapResult.Duplicate;
        }
        if (epochObservation == ChannelEpochObservation.Invalid
            && _epochCoordinator.CurrentVersion != 0)
        {
            _duplicatesSkipped++;
            return GapResult.Duplicate;
        }

        // A snapshot may establish the new epoch before its first incremental
        // packet arrives. Rebase to that first packet so missing the one-shot
        // reset packet cannot wedge the reorder buffer.
        if (_awaitingFirstPacket && _awaitingFirstPacketVersion == version)
        {
            _expectedSeqNum = seq;
            _expectedSeqNumAdvancedAtTicks = packet.ReceivedTimestampTicks;
            _awaitingFirstPacket = false;
            _awaitingFirstPacketVersion = 0;
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
            _expectedSeqNumAdvancedAtTicks = packet.ReceivedTimestampTicks;
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
        // hole to fill. Otherwise declare a real gap — either because the
        // distance exceeds the window, or because (issue #71) the backlog
        // has been stalled at the same _expectedSeqNum for longer than
        // ReorderStallTimeoutMs: at low packet volume the distance alone may
        // never grow past MaxReorderDistance even though the missing SeqNums
        // are permanently gone (joined mid-stream, or lost on both A and B).
        uint distance = seq - _expectedSeqNum;
        bool stalled = packet.ReceivedTimestampTicks - _expectedSeqNumAdvancedAtTicks >= ReorderStallTimeoutMs;
        if (distance > _maxReorderDistance || stalled)
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
        _expectedSeqNumAdvancedAtTicks = packet.ReceivedTimestampTicks;

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
                    _expectedSeqNumAdvancedAtTicks = p.ReceivedTimestampTicks;
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

    private void OnEpochAdvanced(ushort newVersion)
    {
        DiscardReorderBuffer();
        _expectedSeqNum = 1;
        _awaitingFirstPacket = true;
        _awaitingFirstPacketVersion = newVersion;
    }

    private void ProcessAndAdvance(in UmdfPacket packet, ReadOnlySpan<byte> span)
    {
        _packetsProcessed++;
        _expectedSeqNum++;
        _expectedSeqNumAdvancedAtTicks = packet.ReceivedTimestampTicks;
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
                _expectedSeqNumAdvancedAtTicks = queued.ReceivedTimestampTicks;
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
