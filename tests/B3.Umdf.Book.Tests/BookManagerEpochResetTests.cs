using B3.Umdf.Feed;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class BookManagerEpochResetTests
{
    private static (BookManager bm, SymbolStateRegistry reg, StaleMboBuffer buf) CreatePerSymbol()
    {
        var reg = new SymbolStateRegistry(NullLogger.Instance);
        var buf = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: reg, staleBuffer: buf);
        return (bm, reg, buf);
    }

    [Fact]
    public void OnSequenceReset_PerSymbol_ResetsRegistryAndClearsBuffer()
    {
        var (bm, reg, buf) = CreatePerSymbol();

        // Seed: heal one symbol so it's Healthy with a baseline.
        reg.Observe(1, SymbolGapKind.OpeningPrice, 100); // AcceptFirst → Healthy
        Assert.Equal(SymbolState.Healthy, reg.GetState(1, SymbolGapKind.OpeningPrice));

        // Seed: enqueue some MBO bodies for symbol 2 (cold start, MBO buffer).
        var body = new byte[16];
        buf.Enqueue(2, templateId: 50, rptSeq: 5, sendingTimeNs: 0, body);
        buf.Enqueue(2, templateId: 50, rptSeq: 6, sendingTimeNs: 0, body);
        Assert.Equal(2, buf.DepthOf(2));

        // Cache a pending snapshot header (would be consumed by next snapshot body).
        bm.RecordSnapshotHeader(3, lastRptSeq: 50);

        // Trigger sequence reset
        bm.OnSequenceReset();

        Assert.Equal(1, bm.EpochResets);
        Assert.Equal(2, bm.EpochResetMessagesDropped);
        Assert.Equal(0, buf.DepthOf(2));
        Assert.Equal(SymbolState.Unknown, reg.GetState(1, SymbolGapKind.OpeningPrice));

        // Pending snapshot header was cleared too — heal attempt for sec 3 must fail.
        bm.HealAfterSnapshotForTest(3);
        Assert.Equal(1, bm.SnapshotsMissingRptSeq);
    }

}
