using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins the EmptyBook_9 distinct counter introduced for diagnostics. EmptyBook
/// is a per-instrument provable empty-state event (B3 spec resets RptSeq to 1
/// immediately after) and was previously indistinguishable from any other
/// book-clear path (MassDelete, ChannelReset). The counter lets operators
/// detect e.g. an unexpected rate of EmptyBook events on instruments that
/// should be quiet.
/// </summary>
public class EmptyBookCounterTests
{
    [Fact]
    public void HandleEmptyBook_IncrementsCounter_PerInstance()
    {
        var bm = new BookManager(
            stateRegistry: new SymbolStateRegistry(NullLogger.Instance),
            staleBuffer: new StaleMboBuffer(NullLogger.Instance));

        Assert.Equal(0L, bm.EmptyBookEventCount);

        ApplyEmptyBook(bm, securityId: 100);
        Assert.Equal(1L, bm.EmptyBookEventCount);

        ApplyEmptyBook(bm, securityId: 100);
        ApplyEmptyBook(bm, securityId: 200);
        Assert.Equal(3L, bm.EmptyBookEventCount);
    }

    [Fact]
    public void HandleEmptyBook_CounterIndependentOfBookExistence()
    {
        // EmptyBook can arrive for an instrument the consumer has never seen
        // (cold start). The counter must still advance — the registry epoch
        // reset side-effect runs unconditionally.
        var bm = new BookManager(
            stateRegistry: new SymbolStateRegistry(NullLogger.Instance),
            staleBuffer: new StaleMboBuffer(NullLogger.Instance));

        ApplyEmptyBook(bm, securityId: 9999);

        Assert.Equal(1L, bm.EmptyBookEventCount);
    }

    private static void ApplyEmptyBook(BookManager bm, ulong securityId)
    {
        Span<byte> body = stackalloc byte[EmptyBook_9Data.MESSAGE_SIZE];
        var msg = new EmptyBook_9Data { SecurityID = (SecurityID)securityId };
        msg.TryEncode(body, out _);
        bm.HandleEmptyBookForTest(body);
    }
}
