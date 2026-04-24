using System.Collections.Concurrent;
using System.Collections.Frozen;

namespace B3.Umdf.Book;

/// <summary>
/// Thread-safe registry of <see cref="OrderBook"/> instances keyed by security id.
/// Owns the dual-dictionary fast path used by <see cref="BookManager"/>:
///
/// * <see cref="ConcurrentDictionary{TKey,TValue}"/> for safe lookup-or-create
///   while instruments are still being discovered.
/// * <see cref="FrozenDictionary{TKey,TValue}"/> snapshot installed by
///   <see cref="Freeze"/> for hot-path lookups once the symbol set is stable.
///
/// All public members are safe to call concurrently with feed-thread writes.
/// </summary>
internal sealed class BookStore
{
    private readonly ConcurrentDictionary<ulong, OrderBook> _books = new(Environment.ProcessorCount, 4096);
    private volatile FrozenDictionary<ulong, OrderBook>? _frozen;

    /// <summary>
    /// Live view of all order books. Safe to enumerate concurrently with writes
    /// (the underlying ConcurrentDictionary's enumerator is snapshot-style).
    /// </summary>
    public IReadOnlyDictionary<ulong, OrderBook> Books => _books;

    public int Count => _books.Count;

    /// <summary>
    /// Install a frozen snapshot of the current key set as the fast lookup path.
    /// Call after instrument discovery (InstrDef + initial snapshots) is complete.
    /// </summary>
    public void Freeze() => _frozen = _books.ToFrozenDictionary();

    /// <summary>
    /// Lookup the book for <paramref name="securityId"/> or create a fresh one.
    /// Uses the frozen fast path when available; otherwise falls back to the
    /// concurrent dictionary's lock-free GetOrAdd.
    /// </summary>
    public OrderBook GetOrCreate(ulong securityId)
    {
        if (_frozen is { } frozen && frozen.TryGetValue(securityId, out var book))
            return book;
        return _books.GetOrAdd(securityId, static id => new OrderBook(id));
    }

    /// <summary>
    /// Lookup-only fast path. Checks the frozen snapshot first then falls back
    /// to the mutable dictionary so reads remain correct during setup.
    /// </summary>
    public bool TryGet(ulong securityId, out OrderBook book)
    {
        if (_frozen is { } frozen && frozen.TryGetValue(securityId, out book!))
            return true;
        return _books.TryGetValue(securityId, out book!);
    }
}
