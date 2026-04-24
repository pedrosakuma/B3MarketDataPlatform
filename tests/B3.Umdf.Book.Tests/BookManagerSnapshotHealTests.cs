using B3.Umdf.Feed;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class BookManagerSnapshotHealTests
{
    private static (BookManager bm, SymbolStateRegistry reg, StaleMboBuffer buf) CreatePerSymbol()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);
        return (bm, reg, buf);
    }

    [Fact]
    public void Header_Then_Heal_TransitionsRegistryToHealthy()
    {
        var (bm, reg, _) = CreatePerSymbol();

        // Cold-start: send Observe directly to bump high-water (without buffering bodies).
        for (uint r = 10; r <= 15; r++)
            reg.Observe(securityId: 42, SymbolGapKind.Mbo, r);
        // Cold-start MBO is Unknown (not yet Stale; that requires Healthy→gap).

        bm.RecordSnapshotHeader(42, lastRptSeq: 12);
        bm.HealAfterSnapshotForTest(42);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);
        Assert.Equal(12u, bm.Books[42].LastRptSeq); // book reflects snapshot baseline
    }

    [Fact]
    public void Heal_WithoutHeader_IncrementsMissingCounter()
    {
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 10; r <= 12; r++)
            reg.Observe(securityId: 77, SymbolGapKind.Mbo, r);

        bm.HealAfterSnapshotForTest(77);

        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(1, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void Header_RptSeqZero_IlliquidAutoPromote()
    {
        // B3 spec §7.4: snapshot with no LastRptSeq (null/0) means an illiquid
        // instrument that hasn't yet received any incremental updates. The
        // client is explicitly allowed to process incoming incrementals without
        // discarding them. We auto-promote Unknown symbols to Healthy at
        // baseline=0 so the first live message at rptSeq=1 is accepted as
        // contiguous.
        var (bm, reg, _) = CreatePerSymbol();

        bm.RecordSnapshotHeader(50, lastRptSeq: 0);
        bm.HealAfterSnapshotForTest(50);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);
        Assert.Equal(SymbolState.Healthy, reg.GetState(50, SymbolGapKind.Mbo));
        Assert.Equal(0u, bm.Books[50].LastRptSeq);

        // First live incremental at rptSeq=1 must be accepted (Healthy.Apply).
        var observe = reg.Observe(50, SymbolGapKind.Mbo, 1);
        Assert.Equal(B3.Umdf.Book.SymbolStateRegistry.ObserveAction.Apply, observe.Action);
    }
    [Fact]
    public void Header_RptSeqZero_HealthyAlready_NoRegression()
    {
        // Defensive: if symbol is already Healthy at some baseline, an illiquid-style
        // snapshot (LastRptSeq=null) MUST NOT regress it. Defensive Healthy-ahead
        // guard in HealFromSnapshot rejects snap=0 <= priorHighWater.
        var (bm, reg, _) = CreatePerSymbol();
        reg.Observe(99, SymbolGapKind.Mbo, 100);

        bm.RecordSnapshotHeader(99, lastRptSeq: null);
        bm.HealAfterSnapshotForTest(99);

        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(1, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void Heal_PendingEntry_IsConsumed_NotReused()
    {
        var (bm, _, _) = CreatePerSymbol();

        bm.RecordSnapshotHeader(11, lastRptSeq: 50);
        bm.HealAfterSnapshotForTest(11);
        Assert.Equal(1, bm.SnapshotsHealed);

        // Second snapshot for same symbol with no fresh header → must NOT reuse stale 50
        bm.HealAfterSnapshotForTest(11);
        Assert.Equal(1, bm.SnapshotsHealed); // unchanged
        Assert.Equal(1, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void ChunkedSnapshot_HealsOnlyAfterAllChunks()
    {
        // Regression for production bug: a single instrument's MBO snapshot is delivered as
        // 1× Header_30 + N× Orders_71 chunks (sum entries == TotNumBids+TotNumOffers).
        // Previously each chunk consumed the cached LastRptSeq and re-cleared the book, so:
        //   - chunk 1 healed prematurely (book half-built)
        //   - chunks 2..N incremented snapshots_missing_rptseq and re-Cleared the book
        // Now: heal must only fire when received >= expected.
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 10; r <= 15; r++)
            reg.Observe(securityId: 100, SymbolGapKind.Mbo, r);

        bm.BeginChunkedSnapshotForTest(100, lastRptSeq: 12, ordersExpected: 30);
        bm.RecordSnapshotChunkForTest(100, ordersInChunk: 10);
        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);

        bm.RecordSnapshotChunkForTest(100, ordersInChunk: 10);
        Assert.Equal(0, bm.SnapshotsHealed);

        bm.RecordSnapshotChunkForTest(100, ordersInChunk: 10);
        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);
        Assert.Equal(12u, bm.Books[100].LastRptSeq);
    }

    [Fact]
    public void OrphanChunk_WithoutHeader_IncrementsCounter()
    {
        var (bm, _, _) = CreatePerSymbol();
        bm.RecordSnapshotChunkForTest(200, ordersInChunk: 5);
        Assert.Equal(1, bm.SnapshotChunksOrphaned);
        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsMissingRptSeq);
    }

    [Fact]
    public void EmptyBookSnapshot_HealsImmediatelyOnHeader()
    {
        var (bm, reg, _) = CreatePerSymbol();
        reg.Observe(securityId: 300, SymbolGapKind.Mbo, 50);

        bm.BeginChunkedSnapshotForTest(300, lastRptSeq: 50, ordersExpected: 0);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(50u, bm.Books[300].LastRptSeq);
    }

    [Fact]
    public void NewHeader_MidSnapshot_SupersedesPriorAndResetsCounters()
    {
        var (bm, reg, _) = CreatePerSymbol();
        reg.Observe(securityId: 400, SymbolGapKind.Mbo, 5);

        bm.BeginChunkedSnapshotForTest(400, lastRptSeq: 5, ordersExpected: 30);
        bm.RecordSnapshotChunkForTest(400, ordersInChunk: 10);
        Assert.Equal(0, bm.SnapshotsHealed);

        // Fresh Header_30 supersedes the in-progress snapshot.
        bm.BeginChunkedSnapshotForTest(400, lastRptSeq: 7, ordersExpected: 4);
        bm.RecordSnapshotChunkForTest(400, ordersInChunk: 4);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(7u, bm.Books[400].LastRptSeq);
        Assert.Equal(0, bm.SnapshotChunksOrphaned);
    }

    [Fact]
    public void Staging_LiveBookIntactWhileSnapshotInFlight()
    {
        // New invariant: snapshots are staged in PendingSnapshot.StagedBids/Asks
        // until CompleteSnapshot is called. Live book contents must remain
        // untouched until the heal swap fires.
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 1; r <= 5; r++)
            reg.Observe(securityId: 500, SymbolGapKind.Mbo, r);
        var live = bm.GetOrCreateBook(500);
        live.Bids.Add(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10, SecurityId = 500, Side = BookSideType.Bid });
        live.Asks.Add(new OrderBookEntry { OrderId = 2, Price = 110, Quantity = 5, SecurityId = 500, Side = BookSideType.Ask });

        bm.BeginChunkedSnapshotForTest(500, lastRptSeq: 5, ordersExpected: 4);
        bm.StageSnapshotEntryForTest(500, BookSideType.Bid, orderId: 11, price: 99, quantity: 1);
        bm.StageSnapshotEntryForTest(500, BookSideType.Bid, orderId: 12, price: 98, quantity: 1);
        bm.StageSnapshotEntryForTest(500, BookSideType.Ask, orderId: 13, price: 111, quantity: 1);

        // 3 of 4 staged — book still untouched.
        Assert.Equal(1, live.Bids.OrderCount);
        Assert.Equal(1, live.Asks.OrderCount);
        Assert.True(live.Bids.TryGetOrder(1, out _));

        // 4th entry triggers CompleteSnapshot → swap.
        bm.StageSnapshotEntryForTest(500, BookSideType.Ask, orderId: 14, price: 112, quantity: 1);

        Assert.Equal(2, live.Bids.OrderCount);
        Assert.Equal(2, live.Asks.OrderCount);
        Assert.True(live.Bids.TryGetOrder(11, out _));
        Assert.True(live.Bids.TryGetOrder(12, out _));
        Assert.True(live.Asks.TryGetOrder(13, out _));
        Assert.True(live.Asks.TryGetOrder(14, out _));
        // Pre-snapshot live orders are gone (swap = clear+repopulate).
        Assert.False(live.Bids.TryGetOrder(1, out _));
        Assert.False(live.Asks.TryGetOrder(2, out _));
    }

    [Fact]
    public void Staging_RejectedSnapshot_DoesNotMutateLiveBookOrEmitClear()
    {
        // Rejection invariant: a snapshot whose LastRptSeq is older than
        // MinHealRptSeq must leave the live book bytes untouched and emit
        // NO OnBookCleared event. Pre-staging design clobbered the book on
        // Header_30 then re-cleared on rejection (WINV25 phantom-asks).
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var clears = 0;
        var handler = new ClearCountingHandler(() => clears++);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf, eventHandler: handler);

        // Get the symbol Healthy at high-water = 100, then induce a gap → Stale.
        for (uint r = 50; r <= 100; r++)
            reg.Observe(securityId: 600, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(600, SymbolGapKind.Mbo, 100);
        // Force gap: jump from 100 to 200 → Stale.
        reg.Observe(securityId: 600, SymbolGapKind.Mbo, 200);
        Assert.Equal(SymbolState.Stale, reg.GetState(600, SymbolGapKind.Mbo));

        // Seed live book with content (simulating it was populated before going Stale).
        var live = bm.GetOrCreateBook(600);
        live.Bids.Add(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10, SecurityId = 600, Side = BookSideType.Bid });
        live.Asks.Add(new OrderBookEntry { OrderId = 2, Price = 110, Quantity = 5, SecurityId = 600, Side = BookSideType.Ask });
        live.LastRptSeq = 100;

        // Stale snapshot arrives at lastRptSeq=50 — too old (< MinHeal=199).
        bm.BeginChunkedSnapshotForTest(600, lastRptSeq: 50, ordersExpected: 1);
        bm.StageSnapshotEntryForTest(600, BookSideType.Bid, orderId: 999, price: 50, quantity: 1);

        // Rejection: live book bytes unchanged, no clear emitted.
        Assert.Equal(1, bm.SnapshotsRejectedTooOld);
        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(0, clears);
        Assert.Equal(1, live.Bids.OrderCount);
        Assert.Equal(1, live.Asks.OrderCount);
        Assert.True(live.Bids.TryGetOrder(1, out _));
        Assert.True(live.Asks.TryGetOrder(2, out _));
        Assert.Equal(100u, live.LastRptSeq);
    }

    private sealed class ClearCountingHandler : IBookEventHandler
    {
        private readonly Action _onCleared;
        public ClearCountingHandler(Action onCleared) => _onCleared = onCleared;
        public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) { }
        public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long tradeTimeNs) { }
        public void OnBookCleared(ulong securityId, BookClearSide side) => _onCleared();
    }
}
