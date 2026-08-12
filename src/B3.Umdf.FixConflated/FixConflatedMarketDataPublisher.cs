using System.Collections.Concurrent;
using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

public sealed class FixConflatedMarketDataPublisher : IBookEventHandler, IDisposable
{
    private readonly IFixApplicationMessageSink _sink;
    private readonly IFixApplicationHeaderProvider _headerProvider;
    private readonly IFixMarketDataInstrumentResolver _instrumentResolver;
    private readonly IFixClock _clock;
    private readonly FixApplicationMessageWriter _writer;
    private readonly ConcurrentQueue<QueuedBookDelta> _pendingBookDeltas = new();
    private readonly Dictionary<ConflationKey, List<QueuedBookDelta>> _bufferedBookDeltas = new();
    private readonly List<ConflationKey> _bufferedOrder = new();
    private readonly object _stateGate = new();
    private readonly object _emitGate = new();
    private readonly AutoResetEvent _wakeSignal = new(false);
    private readonly TimeSpan _conflationInterval;
    private readonly long _conflationIntervalMs;
    private readonly Thread? _workerThread;
    private volatile bool _disposed;
    private volatile bool _stopRequested;
    private long _nextFlushTicks;

    public FixConflatedMarketDataPublisher(
        IFixApplicationMessageSink sink,
        IFixApplicationHeaderProvider headerProvider,
        IFixMarketDataInstrumentResolver instrumentResolver,
        FixConflatedMarketDataOptions? options = null,
        IFixClock? clock = null)
    {
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _headerProvider = headerProvider ?? throw new ArgumentNullException(nameof(headerProvider));
        _instrumentResolver = instrumentResolver ?? throw new ArgumentNullException(nameof(instrumentResolver));
        _clock = clock ?? SystemFixClock.Instance;

        var resolvedOptions = options ?? new FixConflatedMarketDataOptions();
        if (resolvedOptions.ConflationInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "ConflationInterval must be positive.");

        _conflationInterval = resolvedOptions.ConflationInterval;
        _conflationIntervalMs = checked((long)Math.Ceiling(_conflationInterval.TotalMilliseconds));
        _writer = new FixApplicationMessageWriter(resolvedOptions.InitialBufferSize);

        if (resolvedOptions.StartBackgroundWorker)
        {
            _workerThread = new Thread(RunWorker)
            {
                IsBackground = true,
                Name = "FixConflatedMarketDataPublisher",
            };
            _workerThread.Start();
        }
    }

    public TimeSpan ConflationInterval => _conflationInterval;

    public void PublishSnapshot(FixMarketDataSnapshotRequest request, OrderBook book)
    {
        ArgumentNullException.ThrowIfNull(book);
        ThrowIfDisposed();

        lock (_emitGate)
        {
            DateTimeOffset now = _clock.UtcNow;
            FixApplicationSessionHeader header = _headerProvider.NextHeader(now);
            ReadOnlyMemory<byte> frame = _writer.WriteSnapshotFullRefresh(header, request, book);
            _sink.OnMessage(frame);
        }
    }

    public void OnOrderAdded(OrderBook book, in OrderBookEntry entry)
        => EnqueueBookDelta(CreateBookDelta(
            book.SecurityId,
            entry.Side,
            FixMdUpdateAction.New,
            entry.Price,
            entry.Quantity,
            entry.OrderId,
            FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.OrderId));

    public void OnOrderUpdated(OrderBook book, in OrderBookEntry entry)
        => EnqueueBookDelta(CreateBookDelta(
            book.SecurityId,
            entry.Side,
            FixMdUpdateAction.Change,
            entry.Price,
            entry.Quantity,
            entry.OrderId,
            FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.OrderId));

    public void OnOrderDeleted(OrderBook book, ulong orderId, BookSideType side)
        => EnqueueBookDelta(CreateBookDelta(
            book.SecurityId,
            side,
            FixMdUpdateAction.Delete,
            0,
            0,
            orderId,
            FixMarketDataEntryFields.OrderId));

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
        => OnTrade(securityId, price, quantity, tradeId, sendingTimeNs, TradeFlags.None);

    public void OnTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs, TradeFlags flags)
        => PublishTrade(securityId, price, quantity, tradeId, sendingTimeNs);

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
        => OnForwardTrade(securityId, price, quantity, tradeId, sendingTimeNs, TradeFlags.None);

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs, TradeFlags flags)
        => PublishTrade(securityId, price, quantity, tradeId, sendingTimeNs);

    public void OnBookCleared(ulong securityId, BookClearSide side)
    {
        long nowTicks = _clock.MonotonicTicks;
        if (side is BookClearSide.Bid or BookClearSide.Both)
        {
            _pendingBookDeltas.Enqueue(new QueuedBookDelta(
                securityId,
                BookSideType.Bid,
                FixMdUpdateAction.DeleteThru,
                FixMarketDataEntryFields.None,
                nowTicks));
        }

        if (side is BookClearSide.Ask or BookClearSide.Both)
        {
            _pendingBookDeltas.Enqueue(new QueuedBookDelta(
                securityId,
                BookSideType.Ask,
                FixMdUpdateAction.DeleteThru,
                FixMarketDataEntryFields.None,
                nowTicks));
        }

        _wakeSignal.Set();
    }

    public void OnEpochReset(SnapshotClearReason reason)
    {
        ThrowIfDisposed();
        lock (_stateGate)
        {
            while (_pendingBookDeltas.TryDequeue(out _))
            {
            }

            _bufferedBookDeltas.Clear();
            _bufferedOrder.Clear();
            _nextFlushTicks = 0;
        }
    }

    public void FlushIfDue()
    {
        ThrowIfDisposed();
        FlushBuffered(force: false);
    }

    public void FlushNow()
    {
        ThrowIfDisposed();
        FlushBuffered(force: true);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _stopRequested = true;
        _wakeSignal.Set();
        _workerThread?.Join(TimeSpan.FromSeconds(2));
        FlushBuffered(force: true);
        _writer.Dispose();
        _wakeSignal.Dispose();
        _disposed = true;
    }

    private void RunWorker()
    {
        while (!_stopRequested)
        {
            FlushBuffered(force: false);
            _wakeSignal.WaitOne(GetWaitDurationMilliseconds());
        }
    }

    private int GetWaitDurationMilliseconds()
    {
        lock (_stateGate)
        {
            if (_bufferedOrder.Count == 0)
                return 50;

            long remaining = _nextFlushTicks - _clock.MonotonicTicks;
            if (remaining <= 0)
                return 1;

            return remaining >= int.MaxValue ? int.MaxValue : (int)remaining;
        }
    }

    private void PublishTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
    {
        ThrowIfDisposed();
        if (!_instrumentResolver.TryResolve(securityId, out FixMarketDataInstrument instrument))
            return;

        DateTimeOffset entryTime = ConvertToTimestamp(sendingTimeNs);
        FixApplicationSessionHeader header = _headerProvider.NextHeader(entryTime);

        Span<FixMarketDataIncrementalEntry> entries = stackalloc FixMarketDataIncrementalEntry[1];
        entries[0] = new FixMarketDataIncrementalEntry(
            FixMdUpdateAction.New,
            FixMdEntryType.Trade,
            entryTime,
            FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.TradeId,
            price,
            quantity,
            TradeId: tradeId);

        lock (_emitGate)
        {
            ReadOnlyMemory<byte> frame = _writer.WriteIncrementalRefresh(header, instrument, entries);
            _sink.OnMessage(frame);
        }
    }

    private void EnqueueBookDelta(QueuedBookDelta delta)
    {
        ThrowIfDisposed();
        _pendingBookDeltas.Enqueue(delta);
        _wakeSignal.Set();
    }

    private QueuedBookDelta CreateBookDelta(
        ulong securityId,
        BookSideType side,
        FixMdUpdateAction updateAction,
        long price,
        long quantity,
        ulong orderId,
        FixMarketDataEntryFields fields)
        => new(
            securityId,
            side,
            updateAction,
            fields,
            _clock.MonotonicTicks,
            price,
            quantity,
            orderId);

    private void FlushBuffered(bool force)
    {
        List<BufferedBatch>? batches = null;

        lock (_stateGate)
        {
            DrainPendingDeltas();

            if (_bufferedOrder.Count == 0)
                return;

            long nowTicks = _clock.MonotonicTicks;
            if (!force && nowTicks < _nextFlushTicks)
                return;

            DateTimeOffset flushTime = _clock.UtcNow;
            batches = new List<BufferedBatch>(_bufferedOrder.Count);
            foreach (ConflationKey key in _bufferedOrder)
            {
                List<QueuedBookDelta> deltas = _bufferedBookDeltas[key];
                var entries = new FixMarketDataIncrementalEntry[deltas.Count];
                for (int i = 0; i < deltas.Count; i++)
                {
                    QueuedBookDelta delta = deltas[i];
                    entries[i] = new FixMarketDataIncrementalEntry(
                        delta.UpdateAction,
                        key.EntryType,
                        flushTime,
                        delta.Fields,
                        delta.Price,
                        delta.Size,
                        delta.OrderId);
                }

                batches.Add(new BufferedBatch(key.SecurityId, entries));
            }

            _bufferedBookDeltas.Clear();
            _bufferedOrder.Clear();
            _nextFlushTicks = 0;
        }

        lock (_emitGate)
        {
            if (batches is null)
                return;

            foreach (BufferedBatch batch in batches)
            {
                if (!_instrumentResolver.TryResolve(batch.SecurityId, out FixMarketDataInstrument instrument))
                    continue;

                FixApplicationSessionHeader header = _headerProvider.NextHeader(batch.Entries[0].EntryTime);
                ReadOnlyMemory<byte> frame = _writer.WriteIncrementalRefresh(header, instrument, batch.Entries);
                _sink.OnMessage(frame);
            }
        }
    }

    private void DrainPendingDeltas()
    {
        while (_pendingBookDeltas.TryDequeue(out QueuedBookDelta delta))
        {
            var key = new ConflationKey(delta.SecurityId, delta.Side);
            if (!_bufferedBookDeltas.TryGetValue(key, out List<QueuedBookDelta>? entries))
            {
                entries = [];
                _bufferedBookDeltas.Add(key, entries);
                _bufferedOrder.Add(key);
            }

            entries.Add(delta);
            if (_nextFlushTicks == 0)
                _nextFlushTicks = delta.EnqueueTicks + _conflationIntervalMs;
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private DateTimeOffset ConvertToTimestamp(long sendingTimeNs)
    {
        if (sendingTimeNs <= 0)
            return _clock.UtcNow;

        return DateTimeOffset.FromUnixTimeMilliseconds(sendingTimeNs / 1_000_000);
    }

    private readonly record struct ConflationKey(ulong SecurityId, BookSideType Side)
    {
        public FixMdEntryType EntryType => Side == BookSideType.Bid ? FixMdEntryType.Bid : FixMdEntryType.Offer;
    }

    private readonly record struct QueuedBookDelta(
        ulong SecurityId,
        BookSideType Side,
        FixMdUpdateAction UpdateAction,
        FixMarketDataEntryFields Fields,
        long EnqueueTicks,
        long Price = 0,
        long Size = 0,
        ulong OrderId = 0);

    private readonly record struct BufferedBatch(
        ulong SecurityId,
        FixMarketDataIncrementalEntry[] Entries);
}
