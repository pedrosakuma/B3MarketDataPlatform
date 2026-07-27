using System.Buffers.Binary;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V17;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class Issue75RecoveryTests
{
    private static (BookManager BookManager, SymbolStateRegistry Registry) Create()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var buffer = new StaleMboBuffer(NullLogger.Instance);
        return (new BookManager(stateRegistry: registry, staleBuffer: buffer), registry);
    }

    [Fact]
    public void EmptyBaseline_MissingRptSeqOne_DoesNotRemainHealthy_AndSnapshotRestoresBothSides()
    {
        var (bookManager, registry) = Create();
        const ulong securityId = 7501;

        bookManager.RecordSnapshotHeader(securityId, lastRptSeq: null);
        bookManager.HealAfterSnapshotForTest(securityId);
        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));

        var observed = registry.Observe(securityId, SymbolGapKind.Mbo, receivedRptSeq: 2);
        Assert.Equal(SymbolState.Stale, observed.NewState);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Buffer, observed.Action);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 2,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 1, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Ask, orderId: 2, price: 101, quantity: 20);

        var book = bookManager.Books[securityId];
        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));
        Assert.Equal(1, book.Bids.OrderCount);
        Assert.Equal(1, book.Asks.OrderCount);
        Assert.Equal(2u, book.LastRptSeq);
    }

    [Fact]
    public void HealthyOneSidedBook_CompatibleSnapshotRepairsSemanticMismatch()
    {
        var (bookManager, registry) = Create();
        const ulong securityId = 7502;

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 2, ordersExpected: 1);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 10, price: 100, quantity: 10);
        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));
        Assert.Equal(0, bookManager.Books[securityId].Asks.OrderCount);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 2,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 10, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Ask, orderId: 11, price: 101, quantity: 10);

        var repaired = bookManager.Books[securityId];
        Assert.Equal(1, repaired.Bids.OrderCount);
        Assert.Equal(1, repaired.Asks.OrderCount);
        Assert.Equal(1L, bookManager.SnapshotsSemanticMismatch);
        Assert.Equal(1L, bookManager.SnapshotsSemanticRepair);
        Assert.Equal(0L, bookManager.SnapshotsSkippedHealthyAhead);
    }

    [Fact]
    public void HealthySemanticRepair_BuffersAndReplaysIncrementalArrivingDuringStaging()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var buffer = new StaleMboBuffer(NullLogger.Instance);
        var bookManager = new BookManager(stateRegistry: registry, staleBuffer: buffer);
        const ulong securityId = 7505;

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 2, ordersExpected: 1);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 40, price: 100, quantity: 10);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 2,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        Assert.Equal(SymbolState.Stale, registry.GetState(securityId, SymbolGapKind.Mbo));

        var incremental = EncodeOrder(
            securityId, orderId: 42, price: 99, quantity: 5, rptSeq: 3);
        bookManager.OnPacket(
            in EmptyPacket,
            incremental,
            Order_MBO_50Data.MESSAGE_ID);
        Assert.Equal(1, buffer.DepthOf(securityId));
        Assert.Equal(1, bookManager.Books[securityId].Bids.OrderCount);

        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 40, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Ask, orderId: 41, price: 101, quantity: 10);

        var repaired = bookManager.Books[securityId];
        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));
        Assert.Equal(2, repaired.Bids.OrderCount);
        Assert.Equal(1, repaired.Asks.OrderCount);
        Assert.Equal(3u, repaired.LastRptSeq);
        Assert.Equal(0, buffer.DepthOf(securityId));
    }

    [Fact]
    public void HealthyBook_WithMatchingSideCounts_KeepsFastSkipPath()
    {
        var (bookManager, _) = Create();
        const ulong securityId = 7503;

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 3, ordersExpected: 1);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 20, price: 100, quantity: 10);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 3,
            expectedBids: 1,
            expectedOffers: 0,
            lastSequenceVersion: null);

        Assert.Equal(1L, bookManager.SnapshotsSkippedHealthyAhead);
        Assert.Equal(0L, bookManager.SnapshotsSemanticMismatch);
        Assert.Equal(0L, bookManager.SnapshotsSemanticRepair);
    }

    [Fact]
    public void HealthyBook_OlderSnapshotWithDifferentCounts_IsReportedAndLaterCompatibleSnapshotRepairs()
    {
        var (bookManager, _) = Create();
        const ulong securityId = 7506;

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 3, ordersExpected: 1);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 21, price: 100, quantity: 10);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 2,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);

        Assert.Equal(0L, bookManager.SnapshotsSkippedHealthyAhead);
        Assert.Equal(1L, bookManager.SnapshotsSemanticMismatch);
        Assert.Equal(0L, bookManager.SnapshotsSemanticRepair);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 3,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 21, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Ask, orderId: 22, price: 101, quantity: 10);

        Assert.Equal(2L, bookManager.SnapshotsSemanticMismatch);
        Assert.Equal(1L, bookManager.SnapshotsSemanticRepair);
        Assert.Equal(1, bookManager.Books[securityId].Asks.OrderCount);
    }

    [Fact]
    public void HealthyBook_NoRptSemanticMismatch_IsNotClassifiedAsHealthySkip()
    {
        var (bookManager, registry) = Create();
        const ulong securityId = 7507;

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 3, ordersExpected: 1);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 23, price: 100, quantity: 10);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 0,
            expectedBids: 0,
            expectedOffers: 0,
            lastSequenceVersion: null);

        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));
        Assert.Equal(0L, bookManager.SnapshotsSkippedHealthyAhead);
        Assert.Equal(1L, bookManager.SnapshotsSemanticMismatch);
        Assert.Equal(0L, bookManager.SnapshotsSemanticRepair);
        Assert.Equal(1L, bookManager.SnapshotsMissingRptSeq);
        Assert.Equal(1, bookManager.Books[securityId].Bids.OrderCount);
    }

    [Fact]
    public void HealthyBook_FutureSemanticSnapshot_DoesNotAdvancePastObservedWatermark()
    {
        var (bookManager, registry) = Create();
        const ulong securityId = 7508;

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 3, ordersExpected: 1);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 24, price: 100, quantity: 10);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 4,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);

        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));
        Assert.Equal(0L, bookManager.SnapshotsSkippedHealthyAhead);
        Assert.Equal(1L, bookManager.SnapshotsSemanticMismatch);
        Assert.Equal(0L, bookManager.SnapshotsSemanticRepair);

        var next = registry.Observe(securityId, SymbolGapKind.Mbo, receivedRptSeq: 4);
        Assert.Equal(SymbolStateRegistry.ObserveAction.Apply, next.Action);
    }

    [Fact]
    public void SemanticRepair_FutureReplacementRemainsWatermarkGuarded()
    {
        var (bookManager, registry) = Create();
        const ulong securityId = 7510;

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 3, ordersExpected: 1);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 27, price: 100, quantity: 10);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 3,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        Assert.Equal(SymbolState.Stale, registry.GetState(securityId, SymbolGapKind.Mbo));

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 100,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        Assert.Equal(SymbolState.Stale, registry.GetState(securityId, SymbolGapKind.Mbo));

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 3,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 27, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Ask, orderId: 28, price: 101, quantity: 10);

        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));
        Assert.Equal(1L, bookManager.SnapshotsSemanticRepair);
        Assert.Equal(3u, bookManager.Books[securityId].LastRptSeq);
    }

    [Fact]
    public void RejectedSemanticRepair_FutureSnapshotRemainsWatermarkGuarded()
    {
        var (bookManager, registry) = Create();
        const ulong securityId = 7511;

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 3, ordersExpected: 1);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 29, price: 100, quantity: 10);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 3,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 29, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 30, price: 99, quantity: 10);
        Assert.Equal(1L, bookManager.SnapshotsRejectedSideCountMismatch);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 100,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        Assert.Equal(SymbolState.Stale, registry.GetState(securityId, SymbolGapKind.Mbo));

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 3,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 29, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Ask, orderId: 31, price: 101, quantity: 10);

        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));
        Assert.Equal(1L, bookManager.SnapshotsSemanticRepair);
        Assert.Equal(3u, bookManager.Books[securityId].LastRptSeq);
    }

    [Fact]
    public void SideCountRejection_PreservesProtectedFloorCoverage()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var buffer = new StaleMboBuffer(NullLogger.Instance, perSymbolCap: 2, hotPerSymbolCap: 4);
        var bookManager = new BookManager(stateRegistry: registry, staleBuffer: buffer);
        const ulong securityId = 7509;

        registry.HealFromSnapshot(securityId, SymbolGapKind.Mbo, snapshotRptSeq: 10);
        registry.Observe(securityId, SymbolGapKind.Mbo, receivedRptSeq: 100);
        foreach (uint rptSeq in new uint[] { 100, 101 })
            Enqueue(rptSeq);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 105,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        foreach (uint rptSeq in new uint[] { 102, 103, 104, 105, 106, 107 })
            Enqueue(rptSeq);

        Assert.True(buffer.SafeEvictedBelowFloorCount > 0);

        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 25, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 26, price: 99, quantity: 10);

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 104, ordersExpected: 0);
        Assert.Equal(1L, bookManager.SnapshotsRejectedTooOld);
        Assert.Equal(SymbolState.Stale, registry.GetState(securityId, SymbolGapKind.Mbo));

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 105, ordersExpected: 0);
        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));

        void Enqueue(uint rptSeq)
        {
            buffer.Enqueue(
                securityId,
                templateId: Order_MBO_50Data.MESSAGE_ID,
                rptSeq,
                sendingTimeNs: 0,
                body: new byte[] { (byte)rptSeq },
                onEvictedOldest: evicted =>
                    registry.BumpMinHeal(securityId, SymbolGapKind.Mbo, evicted));
        }
    }

    [Fact]
    public void ReplacedSnapshot_PreservesProtectedFloorCoverage()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var buffer = new StaleMboBuffer(NullLogger.Instance, perSymbolCap: 2, hotPerSymbolCap: 4);
        var bookManager = new BookManager(stateRegistry: registry, staleBuffer: buffer);
        const ulong securityId = 7512;

        registry.HealFromSnapshot(securityId, SymbolGapKind.Mbo, snapshotRptSeq: 10);
        registry.Observe(securityId, SymbolGapKind.Mbo, receivedRptSeq: 100);
        foreach (uint rptSeq in new uint[] { 100, 101 })
            Enqueue(rptSeq);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 105,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        foreach (uint rptSeq in new uint[] { 102, 103, 104, 105, 106, 107 })
            Enqueue(rptSeq);

        Assert.True(buffer.SafeEvictedBelowFloorCount > 0);

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 104,
            expectedBids: 0,
            expectedOffers: 0,
            lastSequenceVersion: null);

        Assert.Equal(1L, bookManager.SnapshotsAbandoned);
        Assert.Equal(1L, bookManager.SnapshotsRejectedTooOld);
        Assert.Equal(SymbolState.Stale, registry.GetState(securityId, SymbolGapKind.Mbo));

        bookManager.BeginChunkedSnapshotForTest(securityId, lastRptSeq: 105, ordersExpected: 0);
        Assert.Equal(SymbolState.Healthy, registry.GetState(securityId, SymbolGapKind.Mbo));

        void Enqueue(uint rptSeq)
        {
            buffer.Enqueue(
                securityId,
                templateId: Order_MBO_50Data.MESSAGE_ID,
                rptSeq,
                sendingTimeNs: 0,
                body: new byte[] { (byte)rptSeq },
                onEvictedOldest: evicted =>
                    registry.BumpMinHeal(securityId, SymbolGapKind.Mbo, evicted));
        }
    }

    [Fact]
    public void SnapshotWithCorrectTotalButWrongSideComposition_IsRejected()
    {
        var (bookManager, registry) = Create();
        const ulong securityId = 7504;

        bookManager.OnSnapshotHeaderForTest(
            securityId,
            lastRptSeq: 5,
            expectedBids: 1,
            expectedOffers: 1,
            lastSequenceVersion: null);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 30, price: 100, quantity: 10);
        bookManager.StageSnapshotEntryForTest(
            securityId, BookSideType.Bid, orderId: 31, price: 99, quantity: 10);

        Assert.Equal(1L, bookManager.SnapshotsRejectedSideCountMismatch);
        Assert.Equal(SymbolState.Unknown, registry.GetState(securityId, SymbolGapKind.Mbo));
        Assert.False(bookManager.Books.TryGetValue(securityId, out var book)
            && (book.Bids.OrderCount != 0 || book.Asks.OrderCount != 0));
    }

    private static readonly UmdfPacket EmptyPacket = new()
    {
        Data = ReadOnlyMemory<byte>.Empty,
        Channel = ChannelType.IncrementalA,
        ChannelGroup = 1,
        ReceivedTimestampTicks = 0,
    };

    private static byte[] EncodeOrder(
        ulong securityId,
        ulong orderId,
        long price,
        long quantity,
        uint rptSeq)
    {
        const int sbeHeaderSize = 8;
        var buffer = new byte[sbeHeaderSize + Order_MBO_50Data.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(
            buffer.AsSpan(0), (ushort)Order_MBO_50Data.MESSAGE_SIZE);
        BinaryPrimitives.WriteUInt16LittleEndian(
            buffer.AsSpan(2), Order_MBO_50Data.MESSAGE_ID);

        var body = buffer.AsSpan(sbeHeaderSize);
        var message = new Order_MBO_50Data
        {
            SecurityID = (SecurityID)securityId,
            MDUpdateAction = MDUpdateAction.NEW,
            MDEntryType = MDEntryType.BID,
            MDEntrySize = (Quantity)quantity,
            SecondaryOrderID = (OrderID)orderId,
        };
        message.SetRptSeq(rptSeq);
        message.TryEncode(body, out _);
        BinaryPrimitives.WriteInt64LittleEndian(body[12..20], price);
        return buffer;
    }
}
