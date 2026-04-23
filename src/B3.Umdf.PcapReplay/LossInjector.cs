using B3.Umdf.Transport;

namespace B3.Umdf.PcapReplay;

/// <summary>
/// Configurable packet drop policy for the replayer. Used to simulate
/// UDP loss patterns (random / bursty, A-only / B-only / correlated) so
/// the consumer's gap-detection and recovery paths can be exercised
/// deterministically in tests and resilience drills.
/// </summary>
internal sealed class LossInjector
{
    private readonly LossPolicy _policy;
    private readonly Random _rng;
    // Correlated mode: when an A packet is dropped, remember its SeqNum so
    // the matching B packet (same SeqNum) is also dropped. Bounded — entries
    // are removed when the matching packet arrives or when we've seen enough
    // packets past the SeqNum that the B side has clearly missed it.
    private readonly HashSet<uint> _correlatedDropSeqs = new();
    private int _burstRemaining;

    public LossInjector(LossPolicy policy)
    {
        _policy = policy;
        _rng = policy.Seed is { } s ? new Random(s) : new Random();
    }

    public bool ShouldDrop(ChannelType channel, ReadOnlySpan<byte> payload)
    {
        var target = MapChannel(channel);
        if ((_policy.Targets & target) == 0) return false;

        // Correlated mode for A/B incrementals: the SAME SeqNum gets dropped
        // on whichever feed sees it. This is the worst case for A/B
        // arbitration — the consumer cannot mask the loss via the other feed.
        if (_policy.Correlated
            && (target == LossTargets.IncrementalA || target == LossTargets.IncrementalB)
            && UmdfPacketHeader.TryRead(payload, out var hdr))
        {
            uint seq = hdr.SequenceNumber;
            if (_correlatedDropSeqs.Remove(seq)) return true; // matched the partner drop
            if (RollDrop())
            {
                // Mark this SeqNum so the partner feed drops it too.
                _correlatedDropSeqs.Add(seq);
                // Bound the set: cap at 1024 entries — far beyond the A/B reorder window.
                if (_correlatedDropSeqs.Count > 1024)
                {
                    // Evict an arbitrary entry. Older drops that were never matched
                    // mean the partner already passed without the SeqNum (file
                    // ordering quirks). Drop them silently.
                    foreach (var e in _correlatedDropSeqs) { _correlatedDropSeqs.Remove(e); break; }
                }
                return true;
            }
            return false;
        }

        return RollDrop();
    }

    private bool RollDrop()
    {
        if (_policy.Mode == LossMode.Burst)
        {
            if (_burstRemaining > 0) { _burstRemaining--; return true; }
            if (_rng.NextDouble() < _policy.Rate)
            {
                _burstRemaining = Math.Max(0, _policy.BurstSize - 1);
                return true;
            }
            return false;
        }
        return _rng.NextDouble() < _policy.Rate;
    }

    private static LossTargets MapChannel(ChannelType ct) => ct switch
    {
        ChannelType.IncrementalA => LossTargets.IncrementalA,
        ChannelType.IncrementalB => LossTargets.IncrementalB,
        ChannelType.SnapshotRecovery => LossTargets.SnapshotRecovery,
        ChannelType.InstrumentDefinition => LossTargets.InstrumentDef,
        _ => LossTargets.None,
    };
}
