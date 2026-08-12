using System.Globalization;
using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

public sealed class FixInitialSnapshotProvider
{
    private readonly IReadOnlyList<BookManager> _bookManagers;
    private readonly IFixMarketDataInstrumentResolver _instrumentResolver;
    private readonly IFixClock _clock;

    public FixInitialSnapshotProvider(
        IReadOnlyList<BookManager> bookManagers,
        IFixMarketDataInstrumentResolver instrumentResolver,
        IFixClock? clock = null)
    {
        _bookManagers = bookManagers ?? throw new ArgumentNullException(nameof(bookManagers));
        _instrumentResolver = instrumentResolver ?? throw new ArgumentNullException(nameof(instrumentResolver));
        _clock = clock ?? SystemFixClock.Instance;
    }

    public IEnumerable<FixMessage> CreateMessages()
    {
        var emitted = new HashSet<ulong>();
        DateTimeOffset snapshotTime = _clock.UtcNow;

        foreach (BookManager manager in _bookManagers)
        {
            foreach (KeyValuePair<ulong, OrderBook> entry in manager.Books)
            {
                if (!emitted.Add(entry.Key))
                    continue;
                if (!_instrumentResolver.TryResolve(entry.Key, out FixMarketDataInstrument instrument))
                    continue;

                yield return FixSnapshotMessageBuilder.Build(
                    new FixMarketDataSnapshotRequest($"SNAP-{entry.Key.ToString(CultureInfo.InvariantCulture)}", instrument),
                    entry.Value,
                    snapshotTime);
            }
        }
    }
}
