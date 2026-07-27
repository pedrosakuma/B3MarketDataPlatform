using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class Issue80TradeGapRecoveryTests
{
    private const ulong SecurityId = 80;

    [Fact]
    public void GapExposedByTrade_StalesMboButEmitsTrade()
    {
        var (bookManager, registry, buffer, recorder) = Create();
        Bootstrap(bookManager, rptSeq: 10);

        SendTrade(bookManager, rptSeq: 13, tradeId: 1);

        Assert.Equal(SymbolState.Stale, registry.GetState(SecurityId, SymbolGapKind.Mbo));
        Assert.Equal(1, recorder.TradeCount);
        Assert.Equal(0, buffer.DepthOf(SecurityId));
        Assert.Equal(1, bookManager.MboStaleTransitions);
        Assert.Equal(2, bookManager.MboStaleGapSizeSum);
        Assert.Equal(0, bookManager.TradeRouteRejected);
        Assert.True(bookManager.TryGetTradeState(SecurityId, out var tradeState));
        Assert.Equal(35_3400L, tradeState!.LastTradePrice);
    }

    [Fact]
    public void TradeWhileMboStale_EmitsWhileMboMutationRemainsBuffered()
    {
        var (bookManager, registry, buffer, recorder) = Create();
        Bootstrap(bookManager, rptSeq: 10);

        SendTrade(bookManager, rptSeq: 13, tradeId: 1);
        SendOrder(bookManager, rptSeq: 14, orderId: 100);
        SendTrade(bookManager, rptSeq: 15, tradeId: 2);

        Assert.Equal(SymbolState.Stale, registry.GetState(SecurityId, SymbolGapKind.Mbo));
        Assert.Equal(2, recorder.TradeCount);
        Assert.Equal(0, recorder.OrderAddCount);
        Assert.Equal(1, buffer.DepthOf(SecurityId));
        Assert.Equal(1, bookManager.BufferedMboMessages);
    }

    [Fact]
    public void CoveringSnapshot_PreservesAppliedTradeAndSuppressesDuplicate()
    {
        var (bookManager, registry, buffer, recorder) = Create();
        Bootstrap(bookManager, rptSeq: 10);

        SendTrade(bookManager, rptSeq: 13, tradeId: 1);
        Assert.True(bookManager.TryGetTradeState(SecurityId, out var before));
        Assert.Single(before!.Ring.AsSpan());

        bookManager.BeginChunkedSnapshotForTest(SecurityId, lastRptSeq: 11, ordersExpected: 0);
        Assert.Equal(SymbolState.Stale, registry.GetState(SecurityId, SymbolGapKind.Mbo));
        Assert.Equal(1, bookManager.SnapshotsRejectedTooOld);

        bookManager.BeginChunkedSnapshotForTest(SecurityId, lastRptSeq: 13, ordersExpected: 0);

        Assert.Equal(SymbolState.Healthy, registry.GetState(SecurityId, SymbolGapKind.Mbo));
        Assert.Equal(0, buffer.DepthOf(SecurityId));
        Assert.Equal(1, recorder.TradeCount);
        Assert.True(bookManager.TryGetTradeState(SecurityId, out var after));
        Assert.Single(after!.Ring.AsSpan());
        Assert.Equal(35_3400L, after.LastTradePrice);

        SendTrade(bookManager, rptSeq: 13, tradeId: 1);

        Assert.Equal(1, recorder.TradeCount);
        Assert.Single(after.Ring.AsSpan());
        Assert.Equal(1, bookManager.TradeRouteRejected);
    }

    [Fact]
    public void DuplicateAndReorderedTrades_AreSuppressed()
    {
        var (bookManager, _, _, recorder) = Create();
        Bootstrap(bookManager, rptSeq: 10);

        SendTrade(bookManager, rptSeq: 11, tradeId: 1);
        SendTrade(bookManager, rptSeq: 11, tradeId: 1);
        SendTrade(bookManager, rptSeq: 10, tradeId: 2);

        Assert.Equal(1, recorder.TradeCount);
        Assert.Equal(2, bookManager.TradeRouteRejected);
        Assert.True(bookManager.TryGetTradeState(SecurityId, out var tradeState));
        Assert.Single(tradeState!.Ring.AsSpan());
    }

    [Fact]
    public void TradeFamilies_ShareSemanticWatermarkAndGlobalContinuity()
    {
        var (bookManager, registry, buffer, recorder) = Create();
        Bootstrap(bookManager, rptSeq: 10);

        SendTrade(bookManager, rptSeq: 11, tradeId: 1);
        SendForwardTrade(bookManager, rptSeq: 12, tradeId: 2);
        SendExecutionSummary(bookManager, rptSeq: 13);
        SendTradeBust(bookManager, rptSeq: 14, tradeId: 1);
        SendOrder(bookManager, rptSeq: 15, orderId: 100);

        Assert.Equal(SymbolState.Healthy, registry.GetState(SecurityId, SymbolGapKind.Mbo));
        Assert.False(registry.IsAnyStale(SecurityId));
        Assert.Equal(0, buffer.DepthOf(SecurityId));
        Assert.Equal(1, recorder.TradeCount);
        Assert.Equal(1, recorder.ForwardTradeCount);
        Assert.Equal(1, recorder.ExecutionSummaryCount);
        Assert.Equal(1, recorder.TradeBustCount);
        Assert.Equal(1, recorder.OrderAddCount);
        Assert.Equal(1, bookManager.TradeBustsApplied);
        Assert.Equal(0, bookManager.MboStaleTransitions);
    }

    [Fact]
    public void SemanticFamilies_EmitWhileMboAlreadyStale()
    {
        var (bookManager, registry, buffer, recorder) = Create();
        Bootstrap(bookManager, rptSeq: 10);

        SendOrder(bookManager, rptSeq: 13, orderId: 100);
        SendTrade(bookManager, rptSeq: 14, tradeId: 1);
        SendForwardTrade(bookManager, rptSeq: 15, tradeId: 2);
        SendExecutionSummary(bookManager, rptSeq: 16);
        SendTradeBust(bookManager, rptSeq: 17, tradeId: 1);

        Assert.Equal(SymbolState.Stale, registry.GetState(SecurityId, SymbolGapKind.Mbo));
        Assert.Equal(1, buffer.DepthOf(SecurityId));
        Assert.Equal(1, recorder.TradeCount);
        Assert.Equal(1, recorder.ForwardTradeCount);
        Assert.Equal(1, recorder.ExecutionSummaryCount);
        Assert.Equal(1, recorder.TradeBustCount);
        Assert.Equal(1, bookManager.TradeBustsApplied);
    }

    private static (BookManager Manager, SymbolStateRegistry Registry, StaleMboBuffer Buffer, Recorder Recorder) Create()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var buffer = new StaleMboBuffer(NullLogger.Instance);
        var recorder = new Recorder();
        var manager = new BookManager(
            eventHandler: recorder,
            stateRegistry: registry,
            staleBuffer: buffer);
        return (manager, registry, buffer, recorder);
    }

    private static void Bootstrap(BookManager bookManager, uint rptSeq)
        => bookManager.BeginChunkedSnapshotForTest(SecurityId, rptSeq, ordersExpected: 0);

    private static void SendTrade(BookManager bookManager, uint rptSeq, uint tradeId)
    {
        var message = new Trade_53Data
        {
            SecurityID = SecurityId,
            MDEntryPx = new Price { Mantissa = 35_3400 },
            MDEntrySize = 100,
            TradeID = tradeId,
        };
        message.SetRptSeq(rptSeq);
        message.SetTrdSubType(null);
        Dispatch(bookManager, message);
    }

    private static void SendForwardTrade(BookManager bookManager, uint rptSeq, uint tradeId)
    {
        var message = new ForwardTrade_54Data
        {
            SecurityID = SecurityId,
            MDEntryPx = new Price { Mantissa = 35_3500 },
            MDEntrySize = 50,
            TradeID = tradeId,
        };
        message.SetRptSeq(rptSeq);
        message.SetTrdSubType(null);
        Dispatch(bookManager, message);
    }

    private static void SendExecutionSummary(BookManager bookManager, uint rptSeq)
    {
        var message = new ExecutionSummary_55Data
        {
            SecurityID = SecurityId,
            LastPx = new Price { Mantissa = 35_3500 },
            FillQty = 150,
        };
        message.SetRptSeq(rptSeq);
        Dispatch(bookManager, message);
    }

    private static void SendTradeBust(BookManager bookManager, uint rptSeq, uint tradeId)
    {
        var message = new TradeBust_57Data
        {
            SecurityID = SecurityId,
            MDEntryPx = new Price { Mantissa = 35_3400 },
            MDEntrySize = 100,
            TradeID = tradeId,
        };
        message.SetRptSeq(rptSeq);
        Dispatch(bookManager, message);
    }

    private static void SendOrder(BookManager bookManager, uint rptSeq, ulong orderId)
    {
        var message = new Order_MBO_50Data
        {
            SecurityID = SecurityId,
            MDUpdateAction = MDUpdateAction.NEW,
            MDEntryType = MDEntryType.BID,
            MDEntrySize = 10,
            SecondaryOrderID = orderId,
        };
        message.SetRptSeq(rptSeq);
        message.SetMDEntryPrevSize(null);
        Dispatch(bookManager, message);
    }

    private static void Dispatch(BookManager bookManager, Trade_53Data message)
    {
        var payload = new byte[MessageHeader.MESSAGE_SIZE + Math.Max(Trade_53Data.MESSAGE_SIZE, Trade_53Data.BLOCK_LENGTH)];
        Trade_53Data.WriteHeader(payload);
        message.Encode(payload.AsSpan(MessageHeader.MESSAGE_SIZE));
        bookManager.OnPacket(default, payload, Trade_53Data.MESSAGE_ID);
    }

    private static void Dispatch(BookManager bookManager, ForwardTrade_54Data message)
    {
        var payload = new byte[MessageHeader.MESSAGE_SIZE + Math.Max(ForwardTrade_54Data.MESSAGE_SIZE, ForwardTrade_54Data.BLOCK_LENGTH)];
        ForwardTrade_54Data.WriteHeader(payload);
        message.Encode(payload.AsSpan(MessageHeader.MESSAGE_SIZE));
        bookManager.OnPacket(default, payload, ForwardTrade_54Data.MESSAGE_ID);
    }

    private static void Dispatch(BookManager bookManager, ExecutionSummary_55Data message)
    {
        var payload = new byte[MessageHeader.MESSAGE_SIZE + Math.Max(ExecutionSummary_55Data.MESSAGE_SIZE, ExecutionSummary_55Data.BLOCK_LENGTH)];
        ExecutionSummary_55Data.WriteHeader(payload);
        message.Encode(payload.AsSpan(MessageHeader.MESSAGE_SIZE));
        bookManager.OnPacket(default, payload, ExecutionSummary_55Data.MESSAGE_ID);
    }

    private static void Dispatch(BookManager bookManager, TradeBust_57Data message)
    {
        var payload = new byte[MessageHeader.MESSAGE_SIZE + Math.Max(TradeBust_57Data.MESSAGE_SIZE, TradeBust_57Data.BLOCK_LENGTH)];
        TradeBust_57Data.WriteHeader(payload);
        message.Encode(payload.AsSpan(MessageHeader.MESSAGE_SIZE));
        bookManager.OnPacket(default, payload, TradeBust_57Data.MESSAGE_ID);
    }

    private static void Dispatch(BookManager bookManager, Order_MBO_50Data message)
    {
        var payload = new byte[MessageHeader.MESSAGE_SIZE + Math.Max(Order_MBO_50Data.MESSAGE_SIZE, Order_MBO_50Data.BLOCK_LENGTH)];
        Order_MBO_50Data.WriteHeader(payload);
        message.Encode(payload.AsSpan(MessageHeader.MESSAGE_SIZE));
        bookManager.OnPacket(default, payload, Order_MBO_50Data.MESSAGE_ID);
    }

    private sealed class Recorder : IBookEventHandler
    {
        public int TradeCount;
        public int ForwardTradeCount;
        public int ExecutionSummaryCount;
        public int TradeBustCount;
        public int OrderAddCount;

        public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) => OrderAddCount++;
        public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) { }
        public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
            => TradeCount++;
        public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
            => ForwardTradeCount++;
        public void OnExecutionSummary(ulong securityId, long lastPx, long fillQty)
            => ExecutionSummaryCount++;
        public void OnTradeBust(ulong securityId, long price, long quantity, long tradeId)
            => TradeBustCount++;
        public void OnBookCleared(ulong securityId, BookClearSide side) { }
    }
}
