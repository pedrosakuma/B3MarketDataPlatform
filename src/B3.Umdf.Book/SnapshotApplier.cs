using System.Runtime.InteropServices;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.Book;

/// <summary>
/// Owns the in-flight snapshot lifecycle for the MBO feed: caches the per-symbol
/// <see cref="PendingSnapshot"/> staging buffers between Header_30 and the final
/// Orders_71 chunk, then atomically swaps staged content into the live
/// <see cref="OrderBook"/> when the heal is accepted by
/// <see cref="SymbolStateRegistry.HealFromSnapshot"/>.
///
/// All methods are called from the single feed thread of the owning
/// <see cref="BookManager"/>; no internal locking is required.
///
/// Public counters surface via the parent <see cref="BookManager"/>.
/// </summary>
internal sealed class SnapshotApplier
{
    /// <summary>
    /// Explicit per-assembly state. The terminal events
    /// (Complete / Replaced / Orphaned / Aborted) are NOT states an in-flight
    /// <see cref="PendingSnapshot"/> dwells in — they are transitions out of
    /// <see cref="ReceivingChunks"/> (or <see cref="Skipped"/>) accounted via
    /// the matching counters. Only <see cref="ReceivingChunks"/> is eligible
    /// for the atomic swap into the live book.
    /// </summary>
    internal enum SnapshotAssemblyState : byte
    {
        /// <summary>No header observed for this SecurityID — chunks would be orphaned.</summary>
        AwaitingHeader = 0,
        /// <summary>Header observed; staging buffers accumulating chunk payloads.</summary>
        ReceivingChunks = 1,
        /// <summary>
        /// Header observed but the snapshot is being silently absorbed
        /// (symbol Healthy ahead, or stale-version per spec §7.2). Chunks
        /// tick OrdersReceived but never reach the live book.
        /// </summary>
        Skipped = 2,
    }

    private sealed class PendingSnapshot
    {
        public SnapshotAssemblyState State;
        public uint LastRptSeq;        // 0 if no usable rptSeq baseline
        public uint OrdersExpected;    // TotNumBids + TotNumOffers from Header_30
        public uint OrdersReceived;    // accumulated across Orders_71 chunks
        public uint BidsExpected;
        public uint OffersExpected;
        public uint BidsReceived;
        public uint OffersReceived;
        public bool HasRptSeq;         // whether LastRptSeq is usable
        public bool Skipped;           // Snapshot is being absorbed without mutating the live book
        public bool ValidateSideCounts;
        public bool SemanticRepair;
        public readonly List<OrderBookEntry> StagedBids = new();
        public readonly List<OrderBookEntry> StagedAsks = new();
        public readonly List<MarketOrder> StagedMarketBids = new();
        public readonly List<MarketOrder> StagedMarketAsks = new();
    }

    private readonly BookStore _bookStore;
    private readonly SymbolStateRegistry _stateRegistry;
    private readonly StaleMboBuffer _staleBuffer;
    private readonly IBookEventHandler? _eventHandler;
    private readonly ILogger _logger;
    private readonly Func<ulong, uint, uint, int> _replayDeferredMbo;
    private readonly ChannelEpochCoordinator _epochCoordinator;

    private readonly Dictionary<ulong, PendingSnapshot> _pendingSnapshots = new();

    private long _snapshotsHealed;
    private long _snapshotsMissingRptSeq;
    private long _snapshotChunksOrphaned;
    private long _snapshotsRejectedTooOld;
    private long _snapshotsRejectedReplayGap;
    private long _snapshotsSkippedHealthyAhead;
    private long _snapshotsRejectedStaleVersion;
    private long _snapshotMarketOrderAdds;
    private long _snapshotsAbandoned;
    private long _snapshotsAbortedByEpoch;
    private long _snapshotsCompleted;
    private long _snapshotsHeaderOnly;
    private long _snapshotsZeroOrder;
    private long _snapshotsOrphanChunk;
    private long _snapshotsReplacedHeader;
    private long _snapshotsAborted;
    private long _snapshotsSemanticMismatch;
    private long _snapshotsSemanticRepair;
    private long _snapshotsRejectedSideCountMismatch;

    public long SnapshotsHealed => Volatile.Read(ref _snapshotsHealed);
    public long SnapshotsMissingRptSeq => Volatile.Read(ref _snapshotsMissingRptSeq);
    public long SnapshotChunksOrphaned => Volatile.Read(ref _snapshotChunksOrphaned);
    public long SnapshotsRejectedTooOld => Volatile.Read(ref _snapshotsRejectedTooOld);
    public long SnapshotsRejectedReplayGap => Volatile.Read(ref _snapshotsRejectedReplayGap);
    public long SnapshotsSkippedHealthyAhead => Volatile.Read(ref _snapshotsSkippedHealthyAhead);
    public long SnapshotMarketOrderAdds => Volatile.Read(ref _snapshotMarketOrderAdds);
    /// <summary>
    /// Pending snapshots that were overwritten by a new <c>Header_30</c> for the
    /// same instrument before completion. Counts the "incomplete-then-replaced"
    /// pattern that <see cref="SnapshotChunksOrphaned"/> cannot capture
    /// (orphan tracks chunks-without-header; this tracks header-without-completion).
    /// Sustained growth indicates persistent loss of snapshot tail chunks for
    /// specific instruments.
    /// </summary>
    public long SnapshotsAbandoned => Volatile.Read(ref _snapshotsAbandoned);
    /// <summary>
    /// Pending snapshots dropped because of an epoch reset
    /// (<see cref="SnapshotClearReason.SequenceVersionChanged"/> /
    /// <see cref="SnapshotClearReason.ChannelReset"/> /
    /// <see cref="SnapshotClearReason.SequenceReset"/>). Distinct from
    /// <see cref="SnapshotsAbandoned"/> (replacement by another header for the
    /// SAME instrument) — epoch resets discard ALL pending snapshots
    /// indiscriminately because the wire seq space is invalidated.
    /// Sustained growth often indicates frequent failovers or weekly rollover
    /// happening more often than expected.
    /// </summary>
    public long SnapshotsAbortedByEpoch => Volatile.Read(ref _snapshotsAbortedByEpoch);
    /// <summary>
    /// Snapshots skipped because their <c>LastSequenceVersion</c> was older
    /// than the channel epoch, or missing after an epoch was established.
    /// Chunks are absorbed without polluting the orphan counter.
    /// </summary>
    public long SnapshotsRejectedStaleVersion => Volatile.Read(ref _snapshotsRejectedStaleVersion);

    /// <summary>
    /// Successful snapshot atomic-swap operations: a header was observed,
    /// staging completed, the heal was accepted, and the live
    /// <see cref="OrderBook"/> was atomically replaced. Sum of the three
    /// success-case sub-counters (<see cref="SnapshotsHeaderOnly"/>,
    /// <see cref="SnapshotsZeroOrder"/>, and the implicit
    /// "non-empty completion" remainder).
    /// </summary>
    public long SnapshotsCompleted => Volatile.Read(ref _snapshotsCompleted);

    /// <summary>
    /// Subset of <see cref="SnapshotsCompleted"/>: header observed for an
    /// illiquid instrument (no <c>LastRptSeq</c>) declaring zero orders, and
    /// the completion happened immediately at <c>Header_30</c> time with no
    /// <c>Orders_71</c> chunks following. Authoritative "instrument is empty"
    /// signal per spec §7.4.
    /// </summary>
    public long SnapshotsHeaderOnly => Volatile.Read(ref _snapshotsHeaderOnly);

    /// <summary>
    /// Subset of <see cref="SnapshotsCompleted"/>: header observed with a
    /// concrete <c>LastRptSeq</c> declaring zero orders. Empty book swap with
    /// the rptSeq baseline acting as the heal anchor.
    /// </summary>
    public long SnapshotsZeroOrder => Volatile.Read(ref _snapshotsZeroOrder);

    /// <summary>
    /// Anomaly counter — <c>Orders_71</c> chunks observed without a preceding
    /// <c>Header_30</c> for the same SecurityID. Increments alongside the
    /// legacy <see cref="SnapshotChunksOrphaned"/> counter; both reflect the
    /// same event under explicit-state-machine accounting.
    /// </summary>
    public long SnapshotsOrphanChunk => Volatile.Read(ref _snapshotsOrphanChunk);

    /// <summary>
    /// Anomaly counter — a new <c>Header_30</c> arrived for an in-flight
    /// (non-Skipped, non-completed) assembly, discarding the old staging.
    /// The new header path still proceeds normally (success counter
    /// increments on its later completion). Increments alongside the legacy
    /// <see cref="SnapshotsAbandoned"/> counter.
    /// </summary>
    public long SnapshotsReplacedHeader => Volatile.Read(ref _snapshotsReplacedHeader);

    /// <summary>
    /// Mid-assembly aborts — staging was discarded because chunk processing
    /// failed (e.g., a parser/staging exception). The live
    /// <see cref="OrderBook"/> was NOT mutated; the symbol stays in its
    /// pre-snapshot state until the next snapshot rotation.
    /// </summary>
    public long SnapshotsAborted => Volatile.Read(ref _snapshotsAborted);
    public long SnapshotsSemanticMismatch => Volatile.Read(ref _snapshotsSemanticMismatch);
    public long SnapshotsSemanticRepair => Volatile.Read(ref _snapshotsSemanticRepair);
    public long SnapshotsRejectedSideCountMismatch => Volatile.Read(ref _snapshotsRejectedSideCountMismatch);

    public SnapshotApplier(
        BookStore bookStore,
        SymbolStateRegistry stateRegistry,
        StaleMboBuffer staleBuffer,
        IBookEventHandler? eventHandler,
        ILogger logger,
        Func<ulong, uint, uint, int> replayDeferredMbo,
        ChannelEpochCoordinator epochCoordinator)
    {
        _bookStore = bookStore;
        _stateRegistry = stateRegistry;
        _staleBuffer = staleBuffer;
        _eventHandler = eventHandler;
        _logger = logger;
        _replayDeferredMbo = replayDeferredMbo;
        _epochCoordinator = epochCoordinator;
    }

    /// <summary>
    /// Wire-driven entry: parse a Header_30 then begin the snapshot lifecycle.
    /// </summary>
    public void OnHeader(in SnapshotFullRefresh_Header_30DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong secId = (ulong)msg.SecurityID;
        uint expectedBids = msg.TotNumBids;
        uint expectedOffers = msg.TotNumOffers;
        uint expected = expectedBids + expectedOffers;

        // Spec §7.2: "If a client encounters a snapshot whose
        // lastSequenceVersion is less than the sequence version coming from
        // the incremental feed, it is recommended to not process that
        // snapshot and wait for the updated version to avoid incurring
        // inconsistent state of the internal book." We absorb the chunks
        // silently (Skipped path) so the orphan counter is not polluted.
        if (!ObserveSnapshotVersion(msg.LastSequenceVersion))
        {
            ReplacePendingSnapshot(secId, new PendingSnapshot
            {
                State = SnapshotAssemblyState.Skipped,
                OrdersExpected = expected,
                OrdersReceived = 0,
                BidsExpected = expectedBids,
                OffersExpected = expectedOffers,
                ValidateSideCounts = true,
                Skipped = true,
            });
            Interlocked.Increment(ref _snapshotsRejectedStaleVersion);
            return;
        }

        bool hasRpt = msg.LastRptSeq is { } v && v > 0;
        uint lastRpt = hasRpt ? msg.LastRptSeq!.Value : 0u;

        BeginHeader(secId, lastRpt, hasRpt, expectedBids, expectedOffers, validateSideCounts: true);
    }

    /// <summary>
    /// Begin the in-flight snapshot for <paramref name="secId"/>. Shared between
    /// the wire-decode path and tests.
    /// </summary>
    public void BeginHeader(ulong secId, uint lastRptSeq, bool hasRptSeq, uint ordersExpected)
        => BeginHeader(secId, lastRptSeq, hasRptSeq, ordersExpected, 0, validateSideCounts: false);

    private void BeginHeader(
        ulong secId,
        uint lastRptSeq,
        bool hasRptSeq,
        uint expectedBids,
        uint expectedOffers,
        bool validateSideCounts)
    {
        var book = _bookStore.GetOrCreate(secId);
        uint ordersExpected = expectedBids + expectedOffers;
        bool semanticRepair = _stateRegistry.TryGetSemanticReconciliationWatermark(
            secId,
            out uint semanticObservedRptSeq);

        var state = _stateRegistry.GetState(secId, SymbolGapKind.Mbo);
        if (state != SymbolState.Healthy
            && semanticRepair
            && (!hasRptSeq || lastRptSeq > semanticObservedRptSeq))
        {
            AbsorbSnapshot(
                secId,
                lastRptSeq,
                hasRptSeq,
                expectedBids,
                expectedOffers,
                validateSideCounts);
            if (!hasRptSeq)
                Interlocked.Increment(ref _snapshotsMissingRptSeq);
            return;
        }

        if (state == SymbolState.Healthy)
        {
            int liveBids = book.Bids.OrderCount + book.MarketOrderCount(BookSideType.Bid);
            int liveOffers = book.Asks.OrderCount + book.MarketOrderCount(BookSideType.Ask);
            bool countsMatch = !validateSideCounts
                || ((uint)liveBids == expectedBids && (uint)liveOffers == expectedOffers);

            // Preserve the cheap fast path only when sequence health and the
            // authoritative per-side counts agree.
            if (countsMatch)
            {
                AbsorbSnapshot(
                    secId,
                    lastRptSeq,
                    hasRptSeq,
                    expectedBids,
                    expectedOffers,
                    validateSideCounts);
                Interlocked.Increment(ref _snapshotsSkippedHealthyAhead);
                return;
            }

            // Sequence continuity does not prove semantic completeness. Count
            // every authoritative disagreement, even when its watermark cannot
            // safely repair the current book.
            Interlocked.Increment(ref _snapshotsSemanticMismatch);

            if (!hasRptSeq
                || lastRptSeq < book.LastRptSeq
                || !_stateRegistry.BeginSnapshotReconciliation(secId, lastRptSeq))
            {
                AbsorbSnapshot(
                    secId,
                    lastRptSeq,
                    hasRptSeq,
                    expectedBids,
                    expectedOffers,
                    validateSideCounts);
                if (!hasRptSeq)
                    Interlocked.Increment(ref _snapshotsMissingRptSeq);
                return;
            }
            semanticRepair = true;
        }

        // Begin a fresh snapshot for this instrument: stage in a parallel buffer.
        // The live `book` is NOT mutated here — it stays at its prior state until
        // CompleteSnapshot decides Accept (swap) or Reject (discard staging).
        ReplacePendingSnapshot(secId, new PendingSnapshot
        {
            State = SnapshotAssemblyState.ReceivingChunks,
            LastRptSeq = lastRptSeq,
            OrdersExpected = ordersExpected,
            OrdersReceived = 0,
            BidsExpected = expectedBids,
            OffersExpected = expectedOffers,
            HasRptSeq = hasRptSeq,
            ValidateSideCounts = validateSideCounts,
            SemanticRepair = semanticRepair,
        });

        // Floor pin: while this snapshot is in flight (Begin → Orders_71 chunks → End),
        // tell the per-symbol stale buffer that messages with rptSeq ≤ lastRptSeq are
        // already covered by the snapshot — so eviction of those at hot cap is "safe"
        // and must NOT advance MinHeal. Without this pin, a high-rate symbol can:
        //   (a) overflow the hot cap during the snapshot's own delivery window,
        //   (b) bump MinHeal past the snapshot's lastRptSeq,
        //   (c) and have the same snapshot rejected at CompleteSnapshot for being
        //       "too old" — leaving the symbol stuck Stale until a much later
        //       rotation (potentially never, if the race repeats).
        if (hasRptSeq)
            _staleBuffer.SetProtectedFloor(secId, lastRptSeq + 1);

        // Empty book snapshot (no Orders_71 chunks will follow): heal immediately.
        if (ordersExpected == 0)
            CompleteSnapshot(secId, book);
    }

    private void AbsorbSnapshot(
        ulong securityId,
        uint lastRptSeq,
        bool hasRptSeq,
        uint expectedBids,
        uint expectedOffers,
        bool validateSideCounts)
    {
        uint ordersExpected = expectedBids + expectedOffers;
        ReplacePendingSnapshot(securityId, new PendingSnapshot
        {
            State = SnapshotAssemblyState.Skipped,
            LastRptSeq = lastRptSeq,
            OrdersExpected = ordersExpected,
            BidsExpected = expectedBids,
            OffersExpected = expectedOffers,
            HasRptSeq = hasRptSeq,
            Skipped = true,
            ValidateSideCounts = validateSideCounts,
        });
        if (ordersExpected == 0)
            _pendingSnapshots.Remove(securityId);
    }

    /// <summary>
    /// Wire-driven entry: parse and apply an Orders_71 chunk for an in-flight snapshot.
    /// </summary>
    public void OnOrdersChunk(in SnapshotFullRefresh_Orders_MBO_71DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        // An Orders_71 chunk must be preceded by a Header_30 for the same instrument.
        if (!_pendingSnapshots.TryGetValue(securityId, out var pending))
        {
            Interlocked.Increment(ref _snapshotChunksOrphaned);
            Interlocked.Increment(ref _snapshotsOrphanChunk);
            return;
        }

        // Skipped snapshot (Header_30 saw the symbol Healthy + ahead): silently drop chunks
        // and tick OrdersReceived so CompleteSnapshot fires once all expected entries arrive
        // (no orphan-counter increment, no book mutation).
        if (pending.Skipped)
        {
            uint skippedAdded = 0;
            foreach (ref readonly var _ in reader.NoMDEntries)
                skippedAdded++;
            pending.OrdersReceived += skippedAdded;
            if (pending.OrdersReceived >= pending.OrdersExpected)
                _pendingSnapshots.Remove(securityId);
            return;
        }

        uint added = 0;
        var stagedBids = pending.StagedBids;
        var stagedAsks = pending.StagedAsks;
        foreach (ref readonly var entry in reader.NoMDEntries)
        {
            added++;
            long? rawPrice = entry.MDEntryPx.Mantissa;
            var side = entry.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
            if (side == BookSideType.Bid)
                pending.BidsReceived++;
            else
                pending.OffersReceived++;
            long quantity = (long)entry.MDEntrySize;
            ulong orderId = (ulong)entry.SecondaryOrderID;
            uint enteringFirm = entry.EnteringFirm.Value ?? 0;

            if (rawPrice is null)
            {
                var marketOrder = new MarketOrder
                {
                    OrderId = orderId,
                    Side = side,
                    Quantity = quantity,
                    EnteringFirm = enteringFirm,
                    SecurityId = securityId
                };
                if (side == BookSideType.Bid)
                    pending.StagedMarketBids.Add(marketOrder);
                else
                    pending.StagedMarketAsks.Add(marketOrder);
                continue;
            }

            var bookEntry = new OrderBookEntry
            {
                OrderId = orderId,
                Price = rawPrice.Value,
                Quantity = quantity,
                EnteringFirm = enteringFirm,
                SecurityId = securityId,
                Side = side
            };

            if (side == BookSideType.Bid)
                stagedBids.Add(bookEntry);
            else
                stagedAsks.Add(bookEntry);
        }

        pending.OrdersReceived += added;

        if (HasSideCountOverflow(pending))
        {
            RejectSideCountMismatch(securityId);
            return;
        }

        if (IsSnapshotComplete(pending))
        {
            var book = _bookStore.GetOrCreate(securityId);
            CompleteSnapshot(securityId, book);
        }
    }

    /// <summary>
    /// Atomically replaces an in-flight pending snapshot, accounting any
    /// abandoned predecessor and clearing its protected-floor pin so the
    /// stale-buffer cap eviction reverts to the conservative MinHeal-bumping
    /// behavior. Centralized so every call site (normal Begin, Healthy-skip,
    /// stale-version skip, test helpers) gets the bookkeeping for free.
    /// </summary>
    private void ReplacePendingSnapshot(ulong secId, PendingSnapshot next)
    {
        if (_pendingSnapshots.TryGetValue(secId, out var existing))
        {
            // Abandon = a real (non-skipped) header that never reached
            // OrdersReceived >= OrdersExpected. Includes "header arrived, zero
            // chunks delivered" — total loss is at least as important as partial.
            if (!existing.Skipped && existing.OrdersReceived < existing.OrdersExpected)
            {
                Interlocked.Increment(ref _snapshotsAbandoned);
                Interlocked.Increment(ref _snapshotsReplacedHeader);
                if (existing.HasRptSeq)
                    _stateRegistry.BumpMinHeal(secId, SymbolGapKind.Mbo, existing.LastRptSeq);
            }
            // Always release any protected floor that the predecessor pinned —
            // skipped replacements never call CompleteSnapshot, so without this
            // the floor would leak indefinitely.
            _staleBuffer.ClearProtectedFloor(secId);
        }
        _pendingSnapshots[secId] = next;
    }

    /// <summary>
    /// Applies the Binary UMDF §7.2 version rule. Older snapshots are rejected;
    /// a newer snapshot advances the shared channel epoch before its contents
    /// are staged, allowing recovery even when the one-shot reset packet was
    /// lost. Missing versions remain accepted only during initial bootstrap.
    /// </summary>
    private bool ObserveSnapshotVersion(ushort? lastSequenceVersion)
    {
        ushort currentVersion = _epochCoordinator.CurrentVersion;
        if (lastSequenceVersion is not { } version || version == 0)
            return currentVersion == 0;

        return _epochCoordinator.Observe(version, ChannelEpochSource.Snapshot)
            is not ChannelEpochObservation.Stale
            and not ChannelEpochObservation.Invalid;
    }

    /// <summary>
    /// Reset all in-flight snapshot state. Called on epoch reset
    /// (ChannelReset_11 / SequenceReset / SequenceVersion change). Counts
    /// non-Skipped non-completed pendings as <see cref="SnapshotsAbortedByEpoch"/>
    /// for operational visibility, and clears the per-symbol stale-buffer
    /// protected floor that those pendings had pinned.
    /// </summary>
    public void Clear(SnapshotClearReason reason)
    {
        if (_pendingSnapshots.Count == 0) return;
        long aborted = 0;
        foreach (var (secId, pending) in _pendingSnapshots)
        {
            if (!pending.Skipped && pending.OrdersReceived < pending.OrdersExpected)
                aborted++;
            _staleBuffer.ClearProtectedFloor(secId);
        }
        if (aborted > 0)
            Interlocked.Add(ref _snapshotsAbortedByEpoch, aborted);
        _pendingSnapshots.Clear();
    }

    /// <summary>Convenience overload — unspecified reset reason.</summary>
    public void Clear() => Clear(SnapshotClearReason.Unspecified);

    private void CompleteSnapshot(ulong securityId, OrderBook book)
    {
        if (!_pendingSnapshots.Remove(securityId, out var pending))
        {
            // No header recorded — cannot transition to Healthy without a baseline.
            Interlocked.Increment(ref _snapshotsMissingRptSeq);
            return;
        }

        if (pending.ValidateSideCounts && !HasExactSideCounts(pending))
        {
            RejectSideCountMismatch(securityId, pending);
            return;
        }

        if (!pending.HasRptSeq)
            CompleteIlliquidSnapshot(securityId, book, pending);
        else
            CompleteNormalSnapshot(securityId, book, pending);
    }

    /// <summary>
    /// Illiquid instrument case (B3 spec §7.4): LastRptSeq is omitted from the
    /// snapshot header when the instrument has not received any incremental
    /// updates yet. Two sub-cases:
    /// <list type="bullet">
    ///   <item><b>Empty illiquid (OrdersExpected == 0)</b>: authoritative
    ///   "instrument is empty" signal. Heal Mbo regardless of any cross-kind
    ///   global gap that may have flipped it Stale (otherwise the symbol stays
    ///   Stale forever — the wire never produces a non-zero-rpt snapshot for
    ///   an illiquid instrument). Baseline is anchored to the highest rptSeq
    ///   we have ever seen for the symbol so late pre-snapshot MBO packets are
    ///   dropped, not applied to the empty book.</item>
    ///   <item><b>Non-empty illiquid (defensive)</b>: malformed per spec —
    ///   absent rpt with concrete orders is contradictory. Reject (count as
    ///   missing-rpt) to avoid healing without a safe baseline.</item>
    /// </list>
    /// </summary>
    private void CompleteIlliquidSnapshot(ulong securityId, OrderBook book, PendingSnapshot pending)
    {
        if (pending.OrdersExpected != 0)
        {
            // Malformed: snapshot is now rejected — release the floor pin
            // (no Clear() will fire on this path to do it for us).
            _staleBuffer.ClearProtectedFloor(securityId);
            Interlocked.Increment(ref _snapshotsMissingRptSeq);
            return;
        }

        var heal = _stateRegistry.HealFromIlliquidEmptySnapshot(securityId, SymbolGapKind.Mbo);
        ApplySnapshotStaging(book, pending);
        book.LastRptSeq = heal.SnapshotRptSeq;
        if (heal.TransitionedToHealthy)
            Interlocked.Increment(ref _snapshotsHealed);
        Interlocked.Increment(ref _snapshotsCompleted);
        // OrdersExpected == 0 + HasRptSeq == false ⇒ "header-only" success path.
        Interlocked.Increment(ref _snapshotsHeaderOnly);
        // No drain window — illiquid asserts the book is empty as of the
        // snapshot moment, so any Stale-buffered tail is meaningless.
        _staleBuffer.Clear(securityId);
    }

    /// <summary>
    /// Normal heal path: snapshot carries a concrete LastRptSeq that bridges
    /// the per-symbol gap. Either accepted (drain buffer for the post-snapshot
    /// window) or rejected as too-old (live book untouched; symbol stays Stale).
    /// </summary>
    private void CompleteNormalSnapshot(ulong securityId, OrderBook book, PendingSnapshot pending)
    {
        uint snapshotRptSeq = pending.LastRptSeq;
        uint replayFrom = snapshotRptSeq == uint.MaxValue
            ? uint.MaxValue
            : snapshotRptSeq + 1;
        uint[] bufferedRptSeqs = _staleBuffer.SnapshotRptSeqs(
            securityId,
            replayFrom,
            uint.MaxValue);
        if (!_stateRegistry.ValidateMboReplayCoverage(
                securityId,
                snapshotRptSeq,
                bufferedRptSeqs,
                out uint missingRptSeq))
        {
            _stateRegistry.BumpMinHeal(
                securityId,
                SymbolGapKind.Mbo,
                missingRptSeq);
            _staleBuffer.ClearProtectedFloor(securityId);
            Interlocked.Increment(ref _snapshotsRejectedReplayGap);
            _logger.LogWarning(
                "PerSymbol snapshot rejected: sparse MBO replay would bridge missing rptSeq. secId={SecurityId} snapshotRptSeq={SnapshotRptSeq} missingRptSeq={MissingRptSeq}",
                securityId,
                snapshotRptSeq,
                missingRptSeq);
            return;
        }

        var heal = _stateRegistry.HealFromSnapshot(securityId, SymbolGapKind.Mbo, snapshotRptSeq);

        if (!heal.Accepted)
        {
            // Snapshot rejected as too-old — release the floor pin (no Clear()
            // fires on the rejected path; live book is left untouched).
            _staleBuffer.ClearProtectedFloor(securityId);
            Interlocked.Increment(ref _snapshotsRejectedTooOld);
            return;
        }

        ApplySnapshotStaging(book, pending);
        book.LastRptSeq = snapshotRptSeq;
        if (heal.TransitionedToHealthy)
            Interlocked.Increment(ref _snapshotsHealed);
        Interlocked.Increment(ref _snapshotsCompleted);
        if (pending.SemanticRepair)
            Interlocked.Increment(ref _snapshotsSemanticRepair);
        if (pending.OrdersExpected == 0)
            Interlocked.Increment(ref _snapshotsZeroOrder);

        if (heal.DrainTo >= heal.DrainFrom)
        {
            int replayed = _replayDeferredMbo(securityId, heal.DrainFrom, heal.DrainTo);
            if (replayed > 0 && _logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(
                    "PerSymbol heal SecID={SecId}: snapshotRpt={Snap} drain=[{From},{To}] replayed={Replayed}",
                    securityId, snapshotRptSeq, heal.DrainFrom, heal.DrainTo, replayed);
        }
        else
        {
            // No drain window — every buffered message is at-or-below the snapshot
            // baseline OR the authoritative-reset escape fired. Drop them.
            _staleBuffer.Clear(securityId);
        }
    }

    /// <summary>
    /// Atomic swap: clears the live book, repopulates from the staged snapshot
    /// entries, and emits exactly one <see cref="IBookEventHandler.OnBookCleared"/>
    /// event covering both sides. Called only on heal.Accepted.
    /// </summary>
    private void ApplySnapshotStaging(OrderBook book, PendingSnapshot pending)
    {
        book.Clear();
        var bidSide = book.Bids;
        foreach (ref var entry in CollectionsMarshal.AsSpan(pending.StagedBids))
            bidSide.Add(in entry);
        var askSide = book.Asks;
        foreach (ref var entry in CollectionsMarshal.AsSpan(pending.StagedAsks))
            askSide.Add(in entry);
        foreach (ref var order in CollectionsMarshal.AsSpan(pending.StagedMarketBids))
            book.UpsertMarketOrder(order.OrderId, BookSideType.Bid, order.Quantity, order.EnteringFirm);
        foreach (ref var order in CollectionsMarshal.AsSpan(pending.StagedMarketAsks))
            book.UpsertMarketOrder(order.OrderId, BookSideType.Ask, order.Quantity, order.EnteringFirm);
        Interlocked.Add(ref _snapshotMarketOrderAdds, pending.StagedMarketBids.Count + pending.StagedMarketAsks.Count);
        _eventHandler?.OnBookCleared(book.SecurityId, BookClearSide.Both);
        EmitMarketTierChanged(book, BookSideType.Bid);
        EmitMarketTierChanged(book, BookSideType.Ask);
    }

    private void EmitMarketTierChanged(OrderBook book, BookSideType side)
    {
        _eventHandler?.OnMarketTierChanged(
            book,
            side,
            book.MarketOrderQuantity(side),
            book.MarketOrderCount(side));
    }

    private static bool HasExactSideCounts(PendingSnapshot pending)
        => pending.BidsReceived == pending.BidsExpected
            && pending.OffersReceived == pending.OffersExpected;

    private static bool HasSideCountOverflow(PendingSnapshot pending)
        => pending.ValidateSideCounts
            && (pending.BidsReceived > pending.BidsExpected
                || pending.OffersReceived > pending.OffersExpected);

    private static bool IsSnapshotComplete(PendingSnapshot pending)
        => pending.ValidateSideCounts
            ? HasExactSideCounts(pending)
            : pending.OrdersReceived >= pending.OrdersExpected;

    private void RejectSideCountMismatch(ulong securityId)
    {
        if (!_pendingSnapshots.Remove(securityId, out var pending))
            return;
        RejectSideCountMismatch(securityId, pending);
    }

    private void RejectSideCountMismatch(ulong securityId, PendingSnapshot pending)
    {
        // Messages at-or-below LastRptSeq may have been evicted as covered by
        // the protected snapshot floor. Once the assembly is rejected, preserve
        // that coverage requirement in MinHeal before releasing the floor.
        if (pending.HasRptSeq)
            _stateRegistry.BumpMinHeal(securityId, SymbolGapKind.Mbo, pending.LastRptSeq);
        _staleBuffer.ClearProtectedFloor(securityId);
        Interlocked.Increment(ref _snapshotsRejectedSideCountMismatch);
        Interlocked.Increment(ref _snapshotsAborted);
    }

    // ── Test helpers ─────────────────────────────────────────────────────────

    /// <summary>
    /// Test helper: mirrors the wire <see cref="OnHeader"/> path, including
    /// the spec §7.2 version gate. Null/0 is accepted only before the channel
    /// has established a non-zero epoch.
    /// </summary>
    internal void OnHeaderForTest(ulong secId, uint lastRptSeq, uint ordersExpected, ushort? lastSequenceVersion)
    {
        if (!ObserveSnapshotVersion(lastSequenceVersion))
        {
            ReplacePendingSnapshot(secId, new PendingSnapshot
            {
                State = SnapshotAssemblyState.Skipped,
                OrdersExpected = ordersExpected,
                OrdersReceived = 0,
                Skipped = true,
            });
            Interlocked.Increment(ref _snapshotsRejectedStaleVersion);
            return;
        }
        BeginHeader(secId, lastRptSeq, lastRptSeq > 0, ordersExpected);
    }

    internal void OnHeaderForTest(
        ulong secId,
        uint lastRptSeq,
        uint expectedBids,
        uint expectedOffers,
        ushort? lastSequenceVersion)
    {
        if (!ObserveSnapshotVersion(lastSequenceVersion))
        {
            ReplacePendingSnapshot(secId, new PendingSnapshot
            {
                State = SnapshotAssemblyState.Skipped,
                OrdersExpected = expectedBids + expectedOffers,
                BidsExpected = expectedBids,
                OffersExpected = expectedOffers,
                ValidateSideCounts = true,
                Skipped = true,
            });
            Interlocked.Increment(ref _snapshotsRejectedStaleVersion);
            return;
        }

        BeginHeader(
            secId,
            lastRptSeq,
            lastRptSeq > 0,
            expectedBids,
            expectedOffers,
            validateSideCounts: true);
    }

    /// <summary>
    /// Test helper: simulate a Header_30 + N Orders_71 chunked snapshot.
    /// </summary>
    internal void BeginChunkedSnapshotForTest(ulong securityId, uint lastRptSeq, uint ordersExpected)
    {
        var book = _bookStore.GetOrCreate(securityId);
        ReplacePendingSnapshot(securityId, new PendingSnapshot
        {
            State = SnapshotAssemblyState.ReceivingChunks,
            LastRptSeq = lastRptSeq,
            OrdersExpected = ordersExpected,
            OrdersReceived = 0,
            HasRptSeq = lastRptSeq > 0,
        });
        if (lastRptSeq > 0)
            _staleBuffer.SetProtectedFloor(securityId, lastRptSeq + 1);
        if (ordersExpected == 0 && lastRptSeq > 0)
            CompleteSnapshot(securityId, book);
    }

    internal void RecordSnapshotChunkForTest(ulong securityId, uint ordersInChunk)
    {
        if (!_pendingSnapshots.TryGetValue(securityId, out var pending))
        {
            Interlocked.Increment(ref _snapshotChunksOrphaned);
            Interlocked.Increment(ref _snapshotsOrphanChunk);
            return;
        }
        var book = _bookStore.GetOrCreate(securityId);
        pending.OrdersReceived += ordersInChunk;
        if (pending.OrdersReceived >= pending.OrdersExpected)
            CompleteSnapshot(securityId, book);
    }

    internal void StageSnapshotEntryForTest(ulong securityId, BookSideType side, ulong orderId, long price, long quantity)
    {
        if (!_pendingSnapshots.TryGetValue(securityId, out var pending))
            throw new InvalidOperationException($"No pending snapshot for {securityId}");
        var entry = new OrderBookEntry
        {
            OrderId = orderId,
            Price = price,
            Quantity = quantity,
            EnteringFirm = 0,
            SecurityId = securityId,
            Side = side,
        };
        if (side == BookSideType.Bid)
        {
            pending.StagedBids.Add(entry);
            if (pending.ValidateSideCounts) pending.BidsReceived++;
        }
        else
        {
            pending.StagedAsks.Add(entry);
            if (pending.ValidateSideCounts) pending.OffersReceived++;
        }
        pending.OrdersReceived++;
        if (HasSideCountOverflow(pending))
        {
            RejectSideCountMismatch(securityId);
            return;
        }
        if (IsSnapshotComplete(pending))
        {
            var book = _bookStore.GetOrCreate(securityId);
            CompleteSnapshot(securityId, book);
        }
    }

    internal void StageSnapshotMarketOrderForTest(ulong securityId, BookSideType side, ulong orderId, long quantity, uint enteringFirm = 0)
    {
        if (!_pendingSnapshots.TryGetValue(securityId, out var pending))
            throw new InvalidOperationException($"No pending snapshot for {securityId}");
        var order = new MarketOrder
        {
            OrderId = orderId,
            Side = side,
            Quantity = quantity,
            EnteringFirm = enteringFirm,
            SecurityId = securityId,
        };
        if (side == BookSideType.Bid)
        {
            pending.StagedMarketBids.Add(order);
            if (pending.ValidateSideCounts) pending.BidsReceived++;
        }
        else
        {
            pending.StagedMarketAsks.Add(order);
            if (pending.ValidateSideCounts) pending.OffersReceived++;
        }
        pending.OrdersReceived++;
        if (HasSideCountOverflow(pending))
        {
            RejectSideCountMismatch(securityId);
            return;
        }
        if (IsSnapshotComplete(pending))
        {
            var book = _bookStore.GetOrCreate(securityId);
            CompleteSnapshot(securityId, book);
        }
    }

    internal void RecordSnapshotHeader(ulong securityId, uint? lastRptSeq)
    {
        bool hasRpt = lastRptSeq is { } v && v > 0;
        ReplacePendingSnapshot(securityId, new PendingSnapshot
        {
            State = SnapshotAssemblyState.ReceivingChunks,
            LastRptSeq = hasRpt ? lastRptSeq!.Value : 0u,
            OrdersExpected = 0,
            OrdersReceived = 0,
            HasRptSeq = hasRpt,
        });
    }

    /// <summary>
    /// Test helper: simulates a mid-assembly failure (e.g., an exception
    /// thrown while applying a chunk). Discards staging cleanly without
    /// touching the live book and increments <see cref="SnapshotsAborted"/>.
    /// Returns <c>true</c> if a pending assembly was actually aborted.
    /// </summary>
    internal bool AbortPendingSnapshotForTest(ulong securityId)
    {
        if (!_pendingSnapshots.Remove(securityId, out var pending))
            return false;
        _staleBuffer.ClearProtectedFloor(securityId);
        if (!pending.Skipped)
            Interlocked.Increment(ref _snapshotsAborted);
        return true;
    }

    internal void HealAfterSnapshotForTest(ulong securityId)
    {
        var book = _bookStore.GetOrCreate(securityId);
        CompleteSnapshot(securityId, book);
    }
}
