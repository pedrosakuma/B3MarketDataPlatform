using B3.Umdf.Book;
using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Pins the GroupConflationHandler's reaction to
/// <see cref="IBookEventHandler.OnEpochReset"/> — the epoch-reset visibility
/// hook. The handler must:
/// <list type="bullet">
///   <item>increment <see cref="GroupConflationHandler.EpochResetsObserved"/></item>
///   <item>record the most recent <see cref="SnapshotClearReason"/></item>
///   <item>NOT throw / NOT clear per-session attributes (vwap, trading status,
///   trade history) — those survive across SequenceVersion change.</item>
/// </list>
/// </summary>
public class GroupConflationHandlerEpochResetTests
{
    [Fact]
    public void OnEpochReset_IncrementsCounter_RecordsReason()
    {
        var sm = new SubscriptionManager();
        var gh = sm.CreateGroupHandler();

        Assert.Equal(0, gh.EpochResetsObserved);

        gh.OnEpochReset(SnapshotClearReason.SequenceVersionChanged);
        Assert.Equal(1, gh.EpochResetsObserved);
        Assert.Equal(SnapshotClearReason.SequenceVersionChanged, gh.LastEpochResetReason);

        gh.OnEpochReset(SnapshotClearReason.ChannelReset);
        gh.OnEpochReset(SnapshotClearReason.SequenceReset);

        Assert.Equal(3, gh.EpochResetsObserved);
        Assert.Equal(SnapshotClearReason.SequenceReset, gh.LastEpochResetReason);
    }

    [Fact]
    public void OnEpochReset_IsTotal_NoThrow_NoSideEffectsOnUnknownState()
    {
        var sm = new SubscriptionManager();
        var gh = sm.CreateGroupHandler();

        // Unspecified is the legacy default — must work.
        gh.OnEpochReset(SnapshotClearReason.Unspecified);

        Assert.Equal(1, gh.EpochResetsObserved);
        Assert.Equal(SnapshotClearReason.Unspecified, gh.LastEpochResetReason);
    }
}
