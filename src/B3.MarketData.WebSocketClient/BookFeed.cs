using System.Collections.Concurrent;

namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Phase 1 (issue #43). Opt-in materialized order-book view derived
/// from the raw MBO event stream (<see cref="MarketDataClient.BookSnapshot"/> +
/// <see cref="MarketDataClient.OrderAdded"/> /
/// <see cref="MarketDataClient.OrderUpdated"/> /
/// <see cref="MarketDataClient.OrderDeleted"/> /
/// <see cref="MarketDataClient.BookCleared"/>) and the per-symbol stale
/// signal (<see cref="MarketDataClient.SymbolStaleStatus"/>) the server
/// already emits.
///
/// <para>
/// <b>Design intent.</b> Most consumers just want top-of-book. Reimplementing
/// the MBO→L2 state machine in every host (trading, surveillance, recorders,
/// dashboards) is error-prone, so we ship it here. The low-level MBO events
/// remain available on <see cref="MarketDataClient"/> unchanged — this layer
/// is strictly additive and opt-in.
/// </para>
///
/// <para>
/// <b>Lifecycle.</b> Construct via <see cref="MarketDataClient.CreateBookFeed"/>
/// (or the <c>WithBookFeed</c> DI extension); subscribe handlers are attached
/// in the constructor and detached on <see cref="Dispose"/>. The feed does NOT
/// drive the client's subscription state — callers still issue
/// <see cref="MarketDataClient.SubscribeAsync(string, SubscribeFlags, CancellationToken)"/>
/// with <see cref="SubscribeFlags.Book"/> set.
/// </para>
///
/// <para>
/// <b>Stale gating.</b> When the server reports a symbol as stale
/// (<see cref="SymbolStaleStatusEvent.IsStale"/> = <c>true</c>) the book
/// is flagged via <see cref="IBookView.IsStale"/>. The next
/// <see cref="BookSnapshotEvent"/> (delivered after the server completes
/// recovery and re-snapshots) clears the flag and replaces the state. We do
/// NOT invent client-side RptSeq gap detection — the server already does it
/// and re-emits a snapshot when needed.
/// </para>
///
/// <para>
/// <b>Threading.</b> Per-symbol state lives behind its own lock; the outer
/// dictionary is concurrent. Event handlers run on the client's receive loop
/// (single writer); reads via <see cref="GetBook"/> /
/// <see cref="TryGetTop"/> are safe from any thread.
/// </para>
/// </summary>
public sealed class BookFeed : IBookFeed, IDisposable
{
    private readonly MarketDataClient _client;
    private readonly ConcurrentDictionary<string, BookView> _bySymbol =
        new(StringComparer.OrdinalIgnoreCase);
    private readonly Action<BookSnapshotEvent> _onSnap;
    private readonly Action<OrderAddedEvent> _onAdd;
    private readonly Action<OrderUpdatedEvent> _onUpd;
    private readonly Action<OrderDeletedEvent> _onDel;
    private readonly Action<BookClearedEvent> _onClr;
    private readonly Action<SymbolStaleStatusEvent> _onStale;
    private readonly Action<UnsubscribedEvent> _onUnsubscribed;
    private int _disposed;

    /// <inheritdoc/>
    public event Action<string>? Changed;

    public BookFeed(MarketDataClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _onSnap = OnSnapshot;
        _onAdd = OnAdded;
        _onUpd = OnUpdated;
        _onDel = OnDeleted;
        _onClr = OnCleared;
        _onStale = OnStale;
        _onUnsubscribed = OnUnsubscribed;
        _client.BookSnapshot += _onSnap;
        _client.OrderAdded += _onAdd;
        _client.OrderUpdated += _onUpd;
        _client.OrderDeleted += _onDel;
        _client.BookCleared += _onClr;
        _client.SymbolStaleStatus += _onStale;
        _client.Unsubscribed += _onUnsubscribed;
    }

    /// <inheritdoc/>
    public IBookView? GetBook(string symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol)) return null;
        return _bySymbol.TryGetValue(symbol.Trim(), out var b) ? b : null;
    }

    /// <inheritdoc/>
    public bool TryGetTop(string symbol, out L2TopOfBook top)
    {
        var b = GetBook(symbol);
        if (b is null) { top = default; return false; }
        return b.TryGetTop(out top);
    }

    /// <summary>
    /// Drop the in-memory book for <paramref name="symbol"/>. Normally
    /// <see cref="MarketDataClient.UnsubscribeAsync"/> handles eviction
    /// automatically via the
    /// <see cref="MarketDataClient.Unsubscribed"/> event; call this only when
    /// you want to discard a book without unsubscribing (e.g. memory
    /// reclamation for a symbol you still receive but no longer care about).
    /// </summary>
    public bool Forget(string symbol)
    {
        if (string.IsNullOrWhiteSpace(symbol)) return false;
        return _bySymbol.TryRemove(symbol.Trim(), out _);
    }

    private BookView GetOrCreate(string symbol, ulong securityId) =>
        _bySymbol.GetOrAdd(symbol, sym => new BookView(sym, securityId));

    private void OnSnapshot(BookSnapshotEvent ev)
    {
        var book = GetOrCreate(ev.Symbol, ev.SecurityId);
        book.ApplySnapshot(ev);
        Changed?.Invoke(ev.Symbol);
    }

    private void OnAdded(OrderAddedEvent ev)
    {
        if (ev.Qty <= 0) return;
        var book = GetOrCreate(ev.Symbol, ev.SecurityId);
        book.ApplyAdded(ev);
        Changed?.Invoke(ev.Symbol);
    }

    private void OnUpdated(OrderUpdatedEvent ev)
    {
        var book = GetOrCreate(ev.Symbol, ev.SecurityId);
        book.ApplyUpdated(ev);
        Changed?.Invoke(ev.Symbol);
    }

    private void OnDeleted(OrderDeletedEvent ev)
    {
        if (!_bySymbol.TryGetValue(ev.Symbol, out var book)) return;
        book.ApplyDeleted(ev);
        Changed?.Invoke(ev.Symbol);
    }

    private void OnCleared(BookClearedEvent ev)
    {
        if (!_bySymbol.TryGetValue(ev.Symbol, out var book)) return;
        book.ApplyCleared(ev);
        Changed?.Invoke(ev.Symbol);
    }

    private void OnStale(SymbolStaleStatusEvent ev)
    {
        if (!_bySymbol.TryGetValue(ev.Symbol, out var book)) return;
        book.MarkStale(ev.IsStale, ev.ReceivedUtc);
        Changed?.Invoke(ev.Symbol);
    }

    private void OnUnsubscribed(UnsubscribedEvent ev)
    {
        if (string.IsNullOrWhiteSpace(ev.Symbol)) return;
        if (_bySymbol.TryRemove(ev.Symbol.Trim(), out _))
        {
            Changed?.Invoke(ev.Symbol);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _client.BookSnapshot -= _onSnap;
        _client.OrderAdded -= _onAdd;
        _client.OrderUpdated -= _onUpd;
        _client.OrderDeleted -= _onDel;
        _client.BookCleared -= _onClr;
        _client.SymbolStaleStatus -= _onStale;
        _client.Unsubscribed -= _onUnsubscribed;
    }
}
