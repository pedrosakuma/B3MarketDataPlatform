namespace B3.Umdf.Book;

/// <summary>
/// Monotonic clock abstraction for time-based gauges in the Book layer
/// (forced-heal escape, stale-since latency tracking). Production code uses
/// <see cref="SystemClock.Instance"/>; tests inject <c>FakeClock</c> to drive
/// time deterministically without <c>Thread.Sleep</c>.
/// </summary>
public interface IClock
{
    /// <summary>
    /// Monotonic millisecond counter (semantics of <see cref="System.Environment.TickCount64"/>).
    /// MUST NOT go backwards.
    /// </summary>
    long NowTicks { get; }
}

public sealed class SystemClock : IClock
{
    public static readonly SystemClock Instance = new();
    private SystemClock() { }
    public long NowTicks => Environment.TickCount64;
}
