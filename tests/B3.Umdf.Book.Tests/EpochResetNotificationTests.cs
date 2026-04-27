using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins the <see cref="IBookEventHandler.OnEpochReset"/> notification
/// (recovery improvement #11): downstream consumers (conflation, fanout,
/// stat caches) need an explicit signal that ALL per-symbol derived state
/// is now invalid — not just the order books (which already get per-book
/// <see cref="IBookEventHandler.OnBookCleared"/>).
/// </summary>
public class EpochResetNotificationTests
{
    private sealed class TrackingHandler : IBookEventHandler
    {
        public List<SnapshotClearReason> EpochResets { get; } = new();
        public int BookClearedCount;

        public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) { }
        public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) { }
        public void OnBookCleared(ulong securityId, BookClearSide side) => BookClearedCount++;
        public void OnEpochReset(SnapshotClearReason reason) => EpochResets.Add(reason);
    }

    private static (BookManager bm, TrackingHandler h) CreatePerSymbol()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var h = new TrackingHandler();
        var bm = new BookManager(eventHandler: h, stateRegistry: reg, staleBuffer: buf);
        return (bm, h);
    }

    [Fact]
    public void OnSequenceReset_FiresOnEpochReset_WithSequenceResetReason()
    {
        var (bm, h) = CreatePerSymbol();

        bm.OnSequenceReset();

        Assert.Single(h.EpochResets);
        Assert.Equal(SnapshotClearReason.SequenceReset, h.EpochResets[0]);
    }

    [Fact]
    public void OnSequenceVersionChanged_FiresOnEpochReset_WithVersionChangedReason()
    {
        var (bm, h) = CreatePerSymbol();

        bm.OnSequenceVersionChanged(7);

        Assert.Single(h.EpochResets);
        Assert.Equal(SnapshotClearReason.SequenceVersionChanged, h.EpochResets[0]);
    }

    [Fact]
    public void HandlerThrowing_DoesNotPropagate_ResetStillCompletes()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var h = new ThrowingHandler();
        var bm = new BookManager(eventHandler: h, stateRegistry: reg, staleBuffer: buf);

        bm.OnSequenceReset(); // must not throw
        Assert.Equal(1, bm.EpochResets);
    }

    private sealed class ThrowingHandler : IBookEventHandler
    {
        public void OnOrderAdded(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry) { }
        public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side) { }
        public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs) { }
        public void OnBookCleared(ulong securityId, BookClearSide side) { }
        public void OnEpochReset(SnapshotClearReason reason)
            => throw new InvalidOperationException("simulated downstream failure");
    }
}
