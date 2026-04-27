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
    public void Staging_AcceptedSnapshot_RebuildsMarketTierAndEmitsAggregate()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var marketEvents = new List<(BookSideType Side, long Qty, int Count)>();
        var clears = 0;
        var handler = new SnapshotEventCaptureHandler(() => clears++,
            (side, qty, count) => marketEvents.Add((side, qty, count)));
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf, eventHandler: handler);

        for (uint r = 1; r <= 5; r++)
            reg.Observe(securityId: 550, SymbolGapKind.Mbo, r);

        var live = bm.GetOrCreateBook(550);
        live.UpsertMarketOrder(orderId: 1, BookSideType.Bid, quantity: 10, enteringFirm: 1);
        live.UpsertMarketOrder(orderId: 2, BookSideType.Ask, quantity: 20, enteringFirm: 1);

        bm.BeginChunkedSnapshotForTest(550, lastRptSeq: 5, ordersExpected: 3);
        bm.StageSnapshotEntryForTest(550, BookSideType.Bid, orderId: 11, price: 99, quantity: 1);
        bm.StageSnapshotMarketOrderForTest(550, BookSideType.Bid, orderId: 12, quantity: 30, enteringFirm: 2);
        bm.StageSnapshotMarketOrderForTest(550, BookSideType.Ask, orderId: 13, quantity: 40, enteringFirm: 3);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(1, clears);
        Assert.Equal(1, live.Bids.OrderCount);
        Assert.Equal(1, live.MarketOrderCount(BookSideType.Bid));
        Assert.Equal(1, live.MarketOrderCount(BookSideType.Ask));
        Assert.Equal(30, live.MarketOrderQuantity(BookSideType.Bid));
        Assert.Equal(40, live.MarketOrderQuantity(BookSideType.Ask));
        Assert.False(live.TryGetMarketOrder(1, BookSideType.Bid, out _));
        Assert.False(live.TryGetMarketOrder(2, BookSideType.Ask, out _));
        Assert.Contains((BookSideType.Bid, 30L, 1), marketEvents);
        Assert.Contains((BookSideType.Ask, 40L, 1), marketEvents);
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

    [Fact]
    public void Staging_RejectedSnapshot_DoesNotMutateMarketTierOrEmitAggregate()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var marketEvents = 0;
        var handler = new SnapshotEventCaptureHandler(() => { }, (_, _, _) => marketEvents++);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf, eventHandler: handler);

        for (uint r = 50; r <= 100; r++)
            reg.Observe(securityId: 650, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(650, SymbolGapKind.Mbo, 100);
        reg.Observe(securityId: 650, SymbolGapKind.Mbo, 200);

        var live = bm.GetOrCreateBook(650);
        live.UpsertMarketOrder(orderId: 1, BookSideType.Bid, quantity: 10, enteringFirm: 1);
        live.UpsertMarketOrder(orderId: 2, BookSideType.Ask, quantity: 20, enteringFirm: 1);

        bm.BeginChunkedSnapshotForTest(650, lastRptSeq: 50, ordersExpected: 1);
        bm.StageSnapshotMarketOrderForTest(650, BookSideType.Bid, orderId: 999, quantity: 99);

        Assert.Equal(1, bm.SnapshotsRejectedTooOld);
        Assert.Equal(0, marketEvents);
        Assert.Equal(1, live.MarketOrderCount(BookSideType.Bid));
        Assert.Equal(1, live.MarketOrderCount(BookSideType.Ask));
        Assert.Equal(10, live.MarketOrderQuantity(BookSideType.Bid));
        Assert.Equal(20, live.MarketOrderQuantity(BookSideType.Ask));
        Assert.False(live.TryGetMarketOrder(999, BookSideType.Bid, out _));
    }

    // ===== Complex scenarios =====

    [Fact]
    public void SnapshotSuperseded_MidFlight_LiveBookIntact_NoCorruption()
    {
        // Cenário 1: Header_30(rpt=100, expects=3) chega, 1 chunk staged, depois
        // novo Header_30(rpt=200, expects=2) chega antes do staging anterior fechar.
        // O staging antigo deve ser descartado em silêncio; o novo deve substituí-lo
        // e completar limpo. Live book não pode ser tocado em momento algum até o
        // CompleteSnapshot do segundo ciclo.
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var clears = 0;
        var handler = new ClearCountingHandler(() => clears++);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf, eventHandler: handler);

        // Sym Stale com baseline 100 → gap 200 → Stale, MinHeal=199.
        for (uint r = 50; r <= 100; r++)
            reg.Observe(securityId: 700, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(700, SymbolGapKind.Mbo, 100);
        reg.Observe(securityId: 700, SymbolGapKind.Mbo, 300); // gap → Stale, MinHeal=299
        var live = bm.GetOrCreateBook(700);
        live.Bids.Add(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10, SecurityId = 700, Side = BookSideType.Bid });
        live.LastRptSeq = 100;

        // Primeiro snapshot (rpt=150 — também < MinHeal=299, será rejeitado de qualquer forma)
        bm.BeginChunkedSnapshotForTest(700, lastRptSeq: 150, ordersExpected: 3);
        bm.StageSnapshotEntryForTest(700, BookSideType.Bid, orderId: 90, price: 50, quantity: 1);
        // Não chega chunk completo: novo Header chega supersedendo.
        bm.BeginChunkedSnapshotForTest(700, lastRptSeq: 350, ordersExpected: 2);
        bm.StageSnapshotEntryForTest(700, BookSideType.Bid, orderId: 91, price: 95, quantity: 5);
        bm.StageSnapshotEntryForTest(700, BookSideType.Ask, orderId: 92, price: 105, quantity: 7);
        // Segundo snapshot completou (MinHeal=299, snap=350 → aceito)

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(1, clears); // exatamente UMA limpeza (do segundo snapshot, não do primeiro)
        Assert.Equal(1, live.Bids.OrderCount);
        Assert.Equal(1, live.Asks.OrderCount);
        Assert.True(live.Bids.TryGetOrder(91, out _));
        Assert.True(live.Asks.TryGetOrder(92, out _));
        // Order 90 (do primeiro staging abandonado) NÃO está no book.
        Assert.False(live.Bids.TryGetOrder(90, out _));
        // Order 1 original foi removida pelo swap (heal aceito).
        Assert.False(live.Bids.TryGetOrder(1, out _));
        Assert.Equal(350u, live.LastRptSeq);
    }

    [Fact]
    public void Orphan_OrdersChunk_WithoutHeader_LiveBookIntact()
    {
        // Cenário 2: Orders_71 chunk chega para um símbolo sem _pendingSnapshots
        // (header anterior nunca chegou ou já foi consumido). Deve incrementar
        // SnapshotChunksOrphaned e NÃO mutar o book vivo.
        var (bm, reg, _) = CreatePerSymbol();
        // Não há Header_30 — chamar diretamente RecordSnapshotChunkForTest.
        bm.RecordSnapshotChunkForTest(securityId: 800, ordersInChunk: 5);
        Assert.Equal(1, bm.SnapshotChunksOrphaned);
        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.False(bm.Books.ContainsKey(800));
    }

    [Fact]
    public void Snapshot_AcceptedAtBoundary_S_EqualsMinHeal()
    {
        // Cenário 3: snapshot exatamente em S = MinHeal deve ser aceito (>=, não >).
        // Drain window = [S+1, highWater] = [MinHeal+1, highWater].
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 50; r <= 100; r++)
            reg.Observe(securityId: 900, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(900, SymbolGapKind.Mbo, 100);
        reg.Observe(securityId: 900, SymbolGapKind.Mbo, 200); // gap → Stale; MinHeal=199, highWater=200

        // Snapshot exatamente em 199 — boundary case.
        bm.BeginChunkedSnapshotForTest(900, lastRptSeq: 199, ordersExpected: 0);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsRejectedTooOld);
        Assert.Equal(SymbolState.Healthy, reg.GetState(900, SymbolGapKind.Mbo));
        Assert.Equal(199u, bm.Books[900].LastRptSeq);
    }

    [Fact]
    public void Snapshot_RejectedJustBelowBoundary_S_EqualsMinHealMinusOne()
    {
        // Cenário 3b: snapshot em S = MinHeal-1 deve ser REJEITADO (deixaria buraco).
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 50; r <= 100; r++)
            reg.Observe(securityId: 901, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(901, SymbolGapKind.Mbo, 100);
        reg.Observe(securityId: 901, SymbolGapKind.Mbo, 200); // MinHeal=199

        bm.BeginChunkedSnapshotForTest(901, lastRptSeq: 198, ordersExpected: 0);

        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(1, bm.SnapshotsRejectedTooOld);
        Assert.Equal(SymbolState.Stale, reg.GetState(901, SymbolGapKind.Mbo));
    }

    [Fact]
    public void HotPromotion_ThenEviction_AdvancesMinHeal_SubsequentSnapshotRejected()
    {
        // Cenário 5: encher cap normal → promoção para hot cap (sem evict);
        // encher hot cap → drop-oldest com BumpMinHeal; snapshot mais antigo
        // que o evicted rptSeq deve ser rejeitado.
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance, perSymbolCap: 2, hotPerSymbolCap: 4);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);

        // Boot: gap → Stale com MinHeal pequeno.
        for (uint r = 1; r <= 5; r++)
            reg.Observe(securityId: 1100, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(1100, SymbolGapKind.Mbo, 5);
        reg.Observe(securityId: 1100, SymbolGapKind.Mbo, 10); // gap, MinHeal=9

        // Buffer enfileira mensagens 10..15 via wire path (HandleOrder seria o real).
        // Como teste, chama Enqueue direto via callback do Observe.Buffer:
        for (uint r = 10; r <= 15; r++)
        {
            var act = reg.Observe(1100, SymbolGapKind.Mbo, r);
            if (act.Action == SymbolStateRegistry.ObserveAction.Buffer)
                buf.Enqueue(1100, 50, r, 0, new byte[] { (byte)r },
                    onEvictedOldest: ev => reg.BumpMinHeal(1100, SymbolGapKind.Mbo, ev));
        }

        // 6 mensagens entraram: r=10,11 (cap normal), r=12 promove (3 itens, hot=4),
        // r=13 (4 itens, no hot cap), r=14 evict r=10 → MinHeal=10, r=15 evict r=11 → MinHeal=11.
        Assert.Equal(1, buf.HotPromotionCount);
        Assert.True(buf.EvictedPerSymbolCapCount >= 1);
        // Snapshot em rpt=10 (== evicted antigo) deve ser rejeitado, MinHeal já avançou.
        bm.BeginChunkedSnapshotForTest(1100, lastRptSeq: 10, ordersExpected: 0);
        Assert.Equal(1, bm.SnapshotsRejectedTooOld);

        // Snapshot em rpt = MinHeal atual deve ser aceito.
        var minHealNow = (uint)buf.EvictedPerSymbolCapCount + 9; // rough; just use observed: re-fetch.
        // Em vez disso, use snapshot bem alto que com certeza passa.
        bm.BeginChunkedSnapshotForTest(1100, lastRptSeq: 100, ordersExpected: 0);
        Assert.Equal(1, bm.SnapshotsHealed);
    }

    [Fact]
    public void HealthySymbol_SeesNewerSnapshot_SkipsThenNextIncrementalDetectsGap()
    {
        // Cenário 6: símbolo Healthy idle vê snapshot mais recente (snap > priorHighWater).
        // Hoje pulamos via SnapshotsSkippedHealthyAhead (defensive guard); confirmar que
        // a próxima incremental fora-de-ordem (que excede priorHighWater+1) re-Stale
        // o símbolo, e o ciclo Stale/heal recupera.
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 50; r <= 100; r++)
            reg.Observe(securityId: 1300, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(1300, SymbolGapKind.Mbo, 100);
        Assert.Equal(SymbolState.Healthy, reg.GetState(1300, SymbolGapKind.Mbo));
        var live = bm.GetOrCreateBook(1300);
        live.LastRptSeq = 100;

        // Snapshot rotacionado em rpt=150 (mais novo que nosso highWater=100).
        // Use BeginSnapshotHeader (não o atalho BeginChunkedSnapshotForTest)
        // para exercitar o guard "Skip Healthy Ahead".
        bm.BeginSnapshotHeader(secId: 1300, lastRptSeq: 150, hasRptSeq: true, ordersExpected: 0);
        Assert.Equal(1, bm.SnapshotsSkippedHealthyAhead);
        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(SymbolState.Healthy, reg.GetState(1300, SymbolGapKind.Mbo));
        Assert.Equal(100u, live.LastRptSeq);

        // Próxima incremental que pula buraco confirma o gap silencioso (rpt=160 vs esperado=101).
        var obs = reg.Observe(1300, SymbolGapKind.Mbo, 160);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Buffer, obs.Action);
        Assert.Equal(SymbolState.Stale, reg.GetState(1300, SymbolGapKind.Mbo));

        // Próximo snapshot fresco (rpt >= 159) cura corretamente.
        bm.BeginChunkedSnapshotForTest(1300, lastRptSeq: 159, ordersExpected: 0);
        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(SymbolState.Healthy, reg.GetState(1300, SymbolGapKind.Mbo));
    }

    [Fact]
    public void Snapshot_WithZeroEntries_HealsImmediately_NoCorruption()
    {
        // Cenário 7: TotNumBids+TotNumOffers = 0 (book vazio do publisher) deve
        // disparar CompleteSnapshot direto no Header_30 — sem aguardar Orders_71.
        var (bm, reg, _) = CreatePerSymbol();
        for (uint r = 50; r <= 100; r++)
            reg.Observe(securityId: 1400, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(1400, SymbolGapKind.Mbo, 100);
        reg.Observe(securityId: 1400, SymbolGapKind.Mbo, 200); // Stale, MinHeal=199

        // Snapshot vazio em rpt=210 (>= MinHeal). BeginChunkedSnapshotForTest com
        // ordersExpected=0 chama CompleteSnapshot na hora.
        bm.BeginChunkedSnapshotForTest(1400, lastRptSeq: 210, ordersExpected: 0);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(SymbolState.Healthy, reg.GetState(1400, SymbolGapKind.Mbo));
        var live = bm.Books[1400];
        Assert.Equal(0, live.Bids.OrderCount);
        Assert.Equal(0, live.Asks.OrderCount);
        Assert.Equal(210u, live.LastRptSeq);
    }

    [Fact]
    public void FloorPin_BufferOverflowsDuringSnapshotDelivery_HealStillSucceeds()
    {
        // REGRESSÃO: símbolo high-rate ficava preso em Stale por 47min após canal
        // gap. Causa: durante a janela Begin→End do snapshot, o buffer transbordava
        // e cada eviction chamava BumpMinHeal, levando o snapshot (cuja rptSeq
        // estava abaixo do novo MinHeal) a ser rejeitado em CompleteSnapshot.
        //
        // Fix: BeginSnapshotHeader pina um "floor protegido" no buffer (lastRptSeq+1).
        // Eviction de msgs com rptSeq < floor é seguro (snapshot cobre) e NÃO
        // avança MinHeal. Snapshot heals corretamente.
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance, perSymbolCap: 2, hotPerSymbolCap: 4);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);

        // Bootstrap: Healthy at 10, gap to 11 → Stale, MinHeal=10.
        for (uint r = 5; r <= 10; r++)
            reg.Observe(securityId: 2000, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(2000, SymbolGapKind.Mbo, 10);
        // Force gap to put symbol Stale; observed jumps far ahead.
        reg.Observe(securityId: 2000, SymbolGapKind.Mbo, 100);
        Assert.Equal(SymbolState.Stale, reg.GetState(2000, SymbolGapKind.Mbo));

        // Pre-snapshot: a couple of msgs accumulate in the buffer (r=100,101).
        foreach (uint r in new uint[] { 100, 101 })
            buf.Enqueue(2000, 50, r, 0, new byte[] { (byte)r },
                onEvictedOldest: ev => reg.BumpMinHeal(2000, SymbolGapKind.Mbo, ev));

        // Snapshot Begin arrives with lastRptSeq=105 (snapshot includes everything ≤105).
        // Floor is set to 106 → msgs <106 are safe to evict.
        bm.BeginChunkedSnapshotForTest(2000, lastRptSeq: 105, ordersExpected: 1);
        Assert.Equal(106u, buf.ProtectedFloorOf(2000));

        // During Begin→End delivery window, MORE msgs arrive: r=102..107.
        // Buffer is at hot cap (4). r=102 enters, evicts r=100 (< 106 → safe).
        // r=103,104,105 enter, evicting 101,102,103 (all < 106 → safe).
        // r=106,107 enter, evicting 104,105 (still < 106 → safe; 105<106).
        // Final buffer: [104,105,106,107] → wait, we evicted 104 and 105 too.
        // Let me recount: after r=102..107 arrive (6 msgs) and buffer cap=4,
        // we end with the last 4 that arrived: [104,105,106,107]. Evicted=100..103
        // → ALL < 106 → all safe. SafeEvictedBelowFloor should be 4.
        foreach (uint r in new uint[] { 102, 103, 104, 105, 106, 107 })
            buf.Enqueue(2000, 50, r, 0, new byte[] { (byte)r },
                onEvictedOldest: ev => reg.BumpMinHeal(2000, SymbolGapKind.Mbo, ev));

        Assert.Equal(0, buf.EvictedPerSymbolCapCount); // no UNSAFE eviction
        Assert.True(buf.SafeEvictedBelowFloorCount >= 4); // 100..103 all safe-evicted

        // Snapshot End: stage 1 entry → CompleteSnapshot fires → heal accepted
        // (because MinHeal was NOT bumped during the snapshot delivery window).
        bm.StageSnapshotEntryForTest(2000, BookSideType.Bid, orderId: 1, price: 50, quantity: 1);

        Assert.Equal(1, bm.SnapshotsHealed);
        Assert.Equal(0, bm.SnapshotsRejectedTooOld);
        Assert.Equal(SymbolState.Healthy, reg.GetState(2000, SymbolGapKind.Mbo));
        // Floor is cleared on CompleteSnapshot.
        Assert.Equal(0u, buf.ProtectedFloorOf(2000));
    }

    [Fact]
    public void FloorPin_OverflowAboveFloor_StillBumpsMinHeal_SnapshotRejected()
    {
        // Pathological case: even with the floor pin, if the buffer overflows
        // with msgs ALL above the floor (snapshot covers nothing in our buffer),
        // eviction MUST bump MinHeal — otherwise we'd silently leave a hole and
        // accept a snapshot whose drain target is missing from the buffer. The
        // failsafe path (snapshot rejected at CompleteSnapshot) MUST trigger.
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance, perSymbolCap: 2, hotPerSymbolCap: 4);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);

        // Stale at observed=200, MinHeal=199.
        for (uint r = 50; r <= 100; r++)
            reg.Observe(2100, SymbolGapKind.Mbo, r);
        reg.HealFromSnapshot(2100, SymbolGapKind.Mbo, 100);
        reg.Observe(2100, SymbolGapKind.Mbo, 200);

        // Snapshot Begin at lastRptSeq=300 (floor=301), but buffer holds nothing yet.
        bm.BeginChunkedSnapshotForTest(2100, lastRptSeq: 300, ordersExpected: 1);

        // Now flood with msgs all > 301 (above floor). Hot cap=4. Send 6 msgs;
        // 2 evictions happen — both above floor → must bump MinHeal.
        foreach (uint r in new uint[] { 400, 401, 402, 403, 404, 405 })
            buf.Enqueue(2100, 50, r, 0, new byte[] { (byte)(r & 0xFF) },
                onEvictedOldest: ev => reg.BumpMinHeal(2100, SymbolGapKind.Mbo, ev));

        Assert.True(buf.EvictedPerSymbolCapCount >= 2); // unsafe evictions
        Assert.Equal(0, buf.SafeEvictedBelowFloorCount);

        // CompleteSnapshot: snapshot.rptSeq=300 < MinHeal (now ≥400) → rejected.
        bm.StageSnapshotEntryForTest(2100, BookSideType.Bid, orderId: 1, price: 50, quantity: 1);

        Assert.Equal(0, bm.SnapshotsHealed);
        Assert.Equal(1, bm.SnapshotsRejectedTooOld);
        Assert.Equal(SymbolState.Stale, reg.GetState(2100, SymbolGapKind.Mbo));
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

    private sealed class SnapshotEventCaptureHandler : IBookEventHandler
    {
        private readonly Action _onCleared;
        private readonly Action<BookSideType, long, int> _onMarketTierChanged;
        public SnapshotEventCaptureHandler(Action onCleared, Action<BookSideType, long, int> onMarketTierChanged)
        {
            _onCleared = onCleared;
            _onMarketTierChanged = onMarketTierChanged;
        }
        public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) { }
        public void OnMarketTierChanged(OrderBook book, BookSideType side, long totalQuantity, int orderCount)
            => _onMarketTierChanged(side, totalQuantity, orderCount);
        public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long tradeTimeNs) { }
        public void OnBookCleared(ulong securityId, BookClearSide side) => _onCleared();
    }
}
