using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

public class BookManagerRecoveryModeTests
{
    [Fact]
    public void Channel_DefaultMode_NoRegistryRequired()
    {
        var bm = new BookManager();
        Assert.Equal(RecoveryMode.Channel, bm.RecoveryMode);
        Assert.Null(bm.StateRegistry);
        Assert.Null(bm.StaleBuffer);
    }

    [Fact]
    public void PerSymbol_RequiresRegistry()
    {
        var ex = Assert.Throws<ArgumentNullException>(() =>
            new BookManager(recoveryMode: RecoveryMode.PerSymbol));
        Assert.Equal("stateRegistry", ex.ParamName);
    }

    [Fact]
    public void PerSymbol_RequiresStaleBuffer()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var ex = Assert.Throws<ArgumentNullException>(() =>
            new BookManager(stateRegistry: registry, recoveryMode: RecoveryMode.PerSymbol));
        Assert.Equal("staleBuffer", ex.ParamName);
    }

    [Fact]
    public void PerSymbol_FullyConstructed_ExposesComponents()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var buffer = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(
            stateRegistry: registry,
            staleBuffer: buffer,
            recoveryMode: RecoveryMode.PerSymbol);

        Assert.Equal(RecoveryMode.PerSymbol, bm.RecoveryMode);
        Assert.Same(registry, bm.StateRegistry);
        Assert.Same(buffer, bm.StaleBuffer);
        Assert.Equal(0, bm.BufferedMboMessages);
        Assert.Equal(0, bm.ReplayedMboMessages);
    }

    [Fact]
    public void ReplayDeferredMbo_NoOp_InChannelMode()
    {
        var bm = new BookManager();
        Assert.Equal(0, bm.ReplayDeferredMbo(securityId: 1, drainFrom: 1, drainTo: 100));
    }

    [Fact]
    public void ReplayDeferredMbo_EmptyBuffer_ReturnsZero()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var buffer = new StaleMboBuffer(NullLogger.Instance);
        var bm = new BookManager(stateRegistry: registry, staleBuffer: buffer,
            recoveryMode: RecoveryMode.PerSymbol);

        Assert.Equal(0, bm.ReplayDeferredMbo(1, 1, 100));
    }
}
