namespace B3.Umdf.Server.Tests;

public class SubscriptionStateCutoffTests
{
    [Fact]
    public void BookAndTradeSnapshotCutoffs_AreIndependent()
    {
        var state = new SubscriptionState(
            DataFlags.ConflatedMbp | DataFlags.Trades,
            minBookBroadcastSequenceExclusive: 0,
            minTradeBroadcastSequenceExclusive: 0,
            conflationIntervalMs: 250);

        state.AdvanceBookMinBroadcastSequence(10);

        Assert.False(state.WantsConflatedMbpBatch(batchSequence: 5, cadenceMs: 250));
        Assert.True(state.WantsTradesBatch(batchSequence: 5));

        state.AdvanceTradeMinBroadcastSequence(10);

        Assert.False(state.WantsTradesBatch(batchSequence: 5));
    }
}
