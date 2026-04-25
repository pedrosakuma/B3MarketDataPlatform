using System.Runtime.InteropServices;
using B3.Umdf.Mbo.Sbe.V16;
using Microsoft.Extensions.Logging;
using SnapshotFullRefresh_Header_30Data = B3.Umdf.Mbo.Sbe.V16.V15.SnapshotFullRefresh_Header_30Data;

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
    private sealed class PendingSnapshot
    {
        public uint LastRptSeq;        // 0 if no usable rptSeq baseline
        public uint OrdersExpected;    // TotNumBids + TotNumOffers from Header_30
        public uint OrdersReceived;    // accumulated across Orders_71 chunks
        public bool HasRptSeq;         // whether LastRptSeq is usable
        public bool Skipped;           // Header_30 saw the symbol Healthy + ahead of snap; chunks must be dropped silently
        public readonly List<OrderBookEntry> StagedBids = new();
        public readonly List<OrderBookEntry> StagedAsks = new();
    }

    private readonly BookStore _bookStore;
    private readonly SymbolStateRegistry _stateRegistry;
    private readonly StaleMboBuffer _staleBuffer;
    private readonly IBookEventHandler? _eventHandler;
    private readonly ILogger _logger;
    private readonly Func<ulong, uint, uint, int> _replayDeferredMbo;
    private readonly Func<ushort> _currentSequenceVersion;

    private readonly Dictionary<ulong, PendingSnapshot> _pendingSnapshots = new();

    private long _snapshotsHealed;
    private long _snapshotsMissingRptSeq;
    private long _snapshotChunksOrphaned;
    private long _snapshotsRejectedTooOld;
    private long _snapshotsSkippedHealthyAhead;
    private long _snapshotsRejectedStaleVersion;

    public long SnapshotsHealed => Volatile.Read(ref _snapshotsHealed);
    public long SnapshotsMissingRptSeq => Volatile.Read(ref _snapshotsMissingRptSeq);
    public long SnapshotChunksOrphaned => Volatile.Read(ref _snapshotChunksOrphaned);
    public long SnapshotsRejectedTooOld => Volatile.Read(ref _snapshotsRejectedTooOld);
    public long SnapshotsSkippedHealthyAhead => Volatile.Read(ref _snapshotsSkippedHealthyAhead);
    /// <summary>
    /// Snapshots silently skipped because their <c>LastSequenceVersion</c>
    /// was older than the channel's current SequenceVersion (B3 spec §7.2).
    /// Chunks for skipped snapshots are absorbed without polluting the
    /// orphan counter.
    /// </summary>
    public long SnapshotsRejectedStaleVersion => Volatile.Read(ref _snapshotsRejectedStaleVersion);

    public SnapshotApplier(
        BookStore bookStore,
        SymbolStateRegistry stateRegistry,
        StaleMboBuffer staleBuffer,
        IBookEventHandler? eventHandler,
        ILogger logger,
        Func<ulong, uint, uint, int> replayDeferredMbo,
        Func<ushort>? currentSequenceVersion = null)
    {
        _bookStore = bookStore;
        _stateRegistry = stateRegistry;
        _staleBuffer = staleBuffer;
        _eventHandler = eventHandler;
        _logger = logger;
        _replayDeferredMbo = replayDeferredMbo;
        _currentSequenceVersion = currentSequenceVersion ?? (static () => (ushort)0);
    }

    /// <summary>
    /// Wire-driven entry: parse a Header_30 then begin the snapshot lifecycle.
    /// </summary>
    public void OnHeader(ReadOnlySpan<byte> body)
    {
        if (!SnapshotFullRefresh_Header_30Data.TryParse(body, out var reader)) return;

        ref readonly var msg = ref reader.Data;
        ulong secId = (ulong)msg.SecurityID;
        uint expected = msg.TotNumBids + msg.TotNumOffers;

        // Spec §7.2: "If a client encounters a snapshot whose
        // lastSequenceVersion is less than the sequence version coming from
        // the incremental feed, it is recommended to not process that
        // snapshot and wait for the updated version to avoid incurring
        // inconsistent state of the internal book." We absorb the chunks
        // silently (Skipped path) so the orphan counter is not polluted.
        ushort currentVersion = _currentSequenceVersion();
        if (currentVersion != 0
            && msg.LastSequenceVersion is { } snapVer
            && snapVer != 0
            && snapVer < currentVersion)
        {
            _pendingSnapshots[secId] = new PendingSnapshot
            {
                OrdersExpected = expected,
                OrdersReceived = 0,
                Skipped = true,
            };
            Interlocked.Increment(ref _snapshotsRejectedStaleVersion);
            return;
        }

        bool hasRpt = msg.LastRptSeq is { } v && v > 0;
        uint lastRpt = hasRpt ? msg.LastRptSeq!.Value : 0u;

        BeginHeader(secId, lastRpt, hasRpt, expected);
    }

    /// <summary>
    /// Begin the in-flight snapshot for <paramref name="secId"/>. Shared between
    /// the wire-decode path and tests.
    /// </summary>
    public void BeginHeader(ulong secId, uint lastRptSeq, bool hasRptSeq, uint ordersExpected)
    {
        var book = _bookStore.GetOrCreate(secId);

        // GUARD: never apply a snapshot to an already-Healthy symbol. The B3
        // always-on snapshot stream rotates through every instrument
        // periodically and does not target our consumer's specific state — its
        // payload reflects state-as-of some snapshot moment T, which may be
        // either behind or ahead of where we are. Applying would either
        // clobber in-flight live messages or leave a hole between our last
        // applied rptSeq and the snapshot baseline.
        var state = _stateRegistry.GetState(secId, SymbolGapKind.Mbo);
        if (state == SymbolState.Healthy)
        {
            _pendingSnapshots[secId] = new PendingSnapshot
            {
                LastRptSeq = lastRptSeq,
                OrdersExpected = ordersExpected,
                OrdersReceived = 0,
                HasRptSeq = hasRptSeq,
                Skipped = true,
            };
            Interlocked.Increment(ref _snapshotsSkippedHealthyAhead);
            return;
        }

        // Begin a fresh snapshot for this instrument: stage in a parallel buffer.
        // The live `book` is NOT mutated here — it stays at its prior state until
        // CompleteSnapshot decides Accept (swap) or Reject (discard staging).
        _pendingSnapshots[secId] = new PendingSnapshot
        {
            LastRptSeq = lastRptSeq,
            OrdersExpected = ordersExpected,
            OrdersReceived = 0,
            HasRptSeq = hasRptSeq,
        };

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

    /// <summary>
    /// Wire-driven entry: parse and apply an Orders_71 chunk for an in-flight snapshot.
    /// </summary>
    public void OnOrdersChunk(ReadOnlySpan<byte> body)
    {
        if (!SnapshotFullRefresh_Orders_MBO_71Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;

        // An Orders_71 chunk must be preceded by a Header_30 for the same instrument.
        if (!_pendingSnapshots.TryGetValue(securityId, out var pending))
        {
            Interlocked.Increment(ref _snapshotChunksOrphaned);
            return;
        }

        // Skipped snapshot (Header_30 saw the symbol Healthy + ahead): silently drop chunks
        // and tick OrdersReceived so CompleteSnapshot fires once all expected entries arrive
        // (no orphan-counter increment, no book mutation).
        if (pending.Skipped)
        {
            uint skippedAdded = 0;
            reader.ReadGroups((in SnapshotFullRefresh_Orders_MBO_71Data.NoMDEntriesData _) =>
            {
                skippedAdded++;
            });
            pending.OrdersReceived += skippedAdded;
            if (pending.OrdersReceived >= pending.OrdersExpected)
                _pendingSnapshots.Remove(securityId);
            return;
        }

        uint added = 0;
        var stagedBids = pending.StagedBids;
        var stagedAsks = pending.StagedAsks;
        reader.ReadGroups((in SnapshotFullRefresh_Orders_MBO_71Data.NoMDEntriesData entry) =>
        {
            added++;
            long? rawPrice = entry.MDEntryPx.Mantissa;
            if (rawPrice is null)
                return; // Market orders have no price — counted toward expected but not staged

            var side = entry.MDEntryType == MDEntryType.BID ? BookSideType.Bid : BookSideType.Ask;
            long price = rawPrice.Value;
            long quantity = (long)entry.MDEntrySize;
            ulong orderId = (ulong)entry.SecondaryOrderID;
            uint enteringFirm = entry.EnteringFirm.Value ?? 0;

            var bookEntry = new OrderBookEntry
            {
                OrderId = orderId,
                Price = price,
                Quantity = quantity,
                EnteringFirm = enteringFirm,
                SecurityId = securityId,
                Side = side
            };

            if (side == BookSideType.Bid)
                stagedBids.Add(bookEntry);
            else
                stagedAsks.Add(bookEntry);
        });

        pending.OrdersReceived += added;

        if (pending.OrdersReceived >= pending.OrdersExpected)
        {
            var book = _bookStore.GetOrCreate(securityId);
            CompleteSnapshot(securityId, book);
        }
    }

    /// <summary>
    /// Reset all in-flight snapshot state. Called on epoch reset
    /// (ChannelReset_11 / SequenceReset).
    /// </summary>
    public void Clear() => _pendingSnapshots.Clear();

    private void CompleteSnapshot(ulong securityId, OrderBook book)
    {
        if (!_pendingSnapshots.Remove(securityId, out var pending))
        {
            // No header recorded — cannot transition to Healthy without a baseline.
            Interlocked.Increment(ref _snapshotsMissingRptSeq);
            return;
        }

        // Snapshot lifecycle is ending — release the per-symbol stale-buffer floor pin.
        _staleBuffer.ClearProtectedFloor(securityId);

        if (!pending.HasRptSeq)
            CompleteIlliquidSnapshot(securityId, book, pending);
        else
            CompleteNormalSnapshot(securityId, book, pending);
    }

    /// <summary>
    /// Illiquid instrument case (B3 spec §7.4): LastRptSeq is omitted from the
    /// snapshot header when the instrument has not received any incremental
    /// updates yet. Treat as "anchor at rptSeq=0" so the first incremental
    /// (rptSeq=1) is contiguous.
    /// </summary>
    private void CompleteIlliquidSnapshot(ulong securityId, OrderBook book, PendingSnapshot pending)
    {
        var heal = _stateRegistry.HealFromSnapshot(securityId, SymbolGapKind.Mbo, 0);
        if (!heal.Accepted)
        {
            Interlocked.Increment(ref _snapshotsMissingRptSeq);
            return;
        }

        ApplySnapshotStaging(book, pending);
        book.LastRptSeq = 0;
        if (heal.TransitionedToHealthy)
            Interlocked.Increment(ref _snapshotsHealed);
    }

    /// <summary>
    /// Normal heal path: snapshot carries a concrete LastRptSeq that bridges
    /// the per-symbol gap. Either accepted (drain buffer for the post-snapshot
    /// window) or rejected as too-old (live book untouched; symbol stays Stale).
    /// </summary>
    private void CompleteNormalSnapshot(ulong securityId, OrderBook book, PendingSnapshot pending)
    {
        uint snapshotRptSeq = pending.LastRptSeq;
        var heal = _stateRegistry.HealFromSnapshot(securityId, SymbolGapKind.Mbo, snapshotRptSeq);

        if (!heal.Accepted)
        {
            Interlocked.Increment(ref _snapshotsRejectedTooOld);
            return;
        }

        ApplySnapshotStaging(book, pending);
        book.LastRptSeq = snapshotRptSeq;
        if (heal.TransitionedToHealthy)
            Interlocked.Increment(ref _snapshotsHealed);

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
        _eventHandler?.OnBookCleared(book.SecurityId, BookClearSide.Both);
    }

    // ── Test helpers ─────────────────────────────────────────────────────────

    /// <summary>
    /// Test helper: mirrors the wire <see cref="OnHeader"/> path, including
    /// the spec §7.2 stale-version gate. Set <paramref name="lastSequenceVersion"/>
    /// to null/0 to bypass the version check.
    /// </summary>
    internal void OnHeaderForTest(ulong secId, uint lastRptSeq, uint ordersExpected, ushort? lastSequenceVersion)
    {
        ushort currentVersion = _currentSequenceVersion();
        if (currentVersion != 0
            && lastSequenceVersion is { } snapVer
            && snapVer != 0
            && snapVer < currentVersion)
        {
            _pendingSnapshots[secId] = new PendingSnapshot
            {
                OrdersExpected = ordersExpected,
                OrdersReceived = 0,
                Skipped = true,
            };
            Interlocked.Increment(ref _snapshotsRejectedStaleVersion);
            return;
        }
        BeginHeader(secId, lastRptSeq, lastRptSeq > 0, ordersExpected);
    }

    /// <summary>
    /// Test helper: simulate a Header_30 + N Orders_71 chunked snapshot.
    /// </summary>
    internal void BeginChunkedSnapshotForTest(ulong securityId, uint lastRptSeq, uint ordersExpected)
    {
        var book = _bookStore.GetOrCreate(securityId);
        _pendingSnapshots[securityId] = new PendingSnapshot
        {
            LastRptSeq = lastRptSeq,
            OrdersExpected = ordersExpected,
            OrdersReceived = 0,
            HasRptSeq = lastRptSeq > 0,
        };
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
            pending.StagedBids.Add(entry);
        else
            pending.StagedAsks.Add(entry);
        pending.OrdersReceived++;
        if (pending.OrdersReceived >= pending.OrdersExpected)
        {
            var book = _bookStore.GetOrCreate(securityId);
            CompleteSnapshot(securityId, book);
        }
    }

    internal void RecordSnapshotHeader(ulong securityId, uint? lastRptSeq)
    {
        bool hasRpt = lastRptSeq is { } v && v > 0;
        if (hasRpt)
        {
            _pendingSnapshots[securityId] = new PendingSnapshot
            {
                LastRptSeq = lastRptSeq!.Value,
                OrdersExpected = 0,
                OrdersReceived = 0,
                HasRptSeq = true,
            };
        }
        else
        {
            _pendingSnapshots[securityId] = new PendingSnapshot
            {
                LastRptSeq = 0,
                OrdersExpected = 0,
                OrdersReceived = 0,
                HasRptSeq = false,
            };
        }
    }

    internal void HealAfterSnapshotForTest(ulong securityId)
    {
        var book = _bookStore.GetOrCreate(securityId);
        CompleteSnapshot(securityId, book);
    }
}
