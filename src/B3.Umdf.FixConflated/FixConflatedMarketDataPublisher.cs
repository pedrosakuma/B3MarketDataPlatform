using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

public sealed class FixConflatedMarketDataPublisher : IBookEventHandler, IDisposable
{
    private const int MaxPooledBufferedBatchLists = 32;
    private const int MaxPooledBufferedBookDeltaLists = 256;
    private const int MaxRetainedBufferedBatchCapacity = 512;
    private const int MaxRetainedBufferedBookDeltaCapacity = 4_096;

    private readonly IFixApplicationMessageSink _sink;
    private readonly IFixApplicationBroadcastSink? _broadcastSink;
    private readonly IFixApplicationHeaderProvider _headerProvider;
    private readonly IFixMarketDataInstrumentResolver _instrumentResolver;
    private readonly IFixClock _clock;
    private readonly FixApplicationMessageWriter _writer;
    private readonly ConcurrentQueue<QueuedBookDelta> _pendingBookDeltas = new();
    private readonly ConcurrentQueue<QueuedTrade> _pendingTrades = new();
    private readonly Dictionary<ConflationKey, List<QueuedBookDelta>> _bufferedBookDeltas = new();
    private readonly List<ConflationKey> _bufferedOrder = new();
    private readonly Stack<List<BufferedBatch>> _bufferedBatchListPool = new();
    private readonly Stack<List<QueuedBookDelta>> _bufferedBookDeltaListPool = new();
    private readonly object _stateGate = new();
    private readonly object _emitGate = new();
    private readonly AutoResetEvent _wakeSignal = new(false);
    private readonly TimeSpan _conflationInterval;
    private readonly long _conflationIntervalMs;
    private readonly int _pendingEventCapacity;
    private readonly Thread? _workerThread;
    private FixMarketDataIncrementalEntry[] _entryBuffer = [];
    private volatile bool _disposed;
    private volatile bool _stopRequested;
    private long _nextFlushTicks;
    private int _pendingEventCount;
    private long _droppedEvents;

    public FixConflatedMarketDataPublisher(
        IFixApplicationMessageSink sink,
        IFixApplicationHeaderProvider headerProvider,
        IFixMarketDataInstrumentResolver instrumentResolver,
        FixConflatedMarketDataOptions? options = null,
        IFixClock? clock = null)
    {
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        _broadcastSink = sink as IFixApplicationBroadcastSink;
        _headerProvider = headerProvider ?? throw new ArgumentNullException(nameof(headerProvider));
        _instrumentResolver = instrumentResolver ?? throw new ArgumentNullException(nameof(instrumentResolver));
        _clock = clock ?? SystemFixClock.Instance;

        var resolvedOptions = options ?? new FixConflatedMarketDataOptions();
        if (resolvedOptions.ConflationInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "ConflationInterval must be positive.");

        _conflationInterval = resolvedOptions.ConflationInterval;
        _conflationIntervalMs = checked((long)Math.Ceiling(_conflationInterval.TotalMilliseconds));
        _pendingEventCapacity = resolvedOptions.PendingEventCapacity;
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
    public int PendingEventCount => Volatile.Read(ref _pendingEventCount);
    public long DroppedEvents => Volatile.Read(ref _droppedEvents);

    public void PublishSnapshot(FixMarketDataSnapshotRequest request, OrderBook book)
    {
        ArgumentNullException.ThrowIfNull(book);
        ThrowIfDisposed();

        lock (_emitGate)
        {
            DateTimeOffset now = _clock.UtcNow;
            if (_broadcastSink is not null)
            {
                _broadcastSink.OnApplicationMessage(FixSnapshotMessageBuilder.Build(request, book, now));
                return;
            }

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
        => EnqueueTrade(new QueuedTrade(securityId, price, quantity, tradeId, sendingTimeNs));

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs)
        => OnForwardTrade(securityId, price, quantity, tradeId, sendingTimeNs, TradeFlags.None);

    public void OnForwardTrade(ulong securityId, long price, long quantity, long tradeId, long sendingTimeNs, TradeFlags flags)
        => EnqueueTrade(new QueuedTrade(securityId, price, quantity, tradeId, sendingTimeNs));

    public void OnBookCleared(ulong securityId, BookClearSide side)
    {
        ThrowIfDisposed();
        long nowTicks = _clock.MonotonicTicks;
        if (side is BookClearSide.Bid or BookClearSide.Both)
        {
            if (TryReservePendingEventSlot())
            {
                _pendingBookDeltas.Enqueue(new QueuedBookDelta(
                    securityId,
                    BookSideType.Bid,
                    FixMdUpdateAction.DeleteThru,
                    FixMarketDataEntryFields.None,
                    nowTicks));
            }
        }

        if (side is BookClearSide.Ask or BookClearSide.Both)
        {
            if (TryReservePendingEventSlot())
            {
                _pendingBookDeltas.Enqueue(new QueuedBookDelta(
                    securityId,
                    BookSideType.Ask,
                    FixMdUpdateAction.DeleteThru,
                    FixMarketDataEntryFields.None,
                    nowTicks));
            }
        }

        SignalWorker();
    }

    public void OnEpochReset(SnapshotClearReason reason)
    {
        ThrowIfDisposed();
        lock (_stateGate)
        {
            while (_pendingBookDeltas.TryDequeue(out _))
            {
                ReleasePendingEventSlot();
            }

            while (_pendingTrades.TryDequeue(out _))
            {
                ReleasePendingEventSlot();
            }

            ResetBufferedBookDeltas();
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

    private void EnqueueBookDelta(QueuedBookDelta delta)
    {
        ThrowIfDisposed();
        if (!TryReservePendingEventSlot())
            return;

        _pendingBookDeltas.Enqueue(delta);
        SignalWorker();
    }

    private void EnqueueTrade(QueuedTrade trade)
    {
        ThrowIfDisposed();
        if (!TryReservePendingEventSlot())
            return;

        _pendingTrades.Enqueue(trade);
        SignalWorker();
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
        List<BufferedBatch>? bookBatches = null;
        List<QueuedTrade>? trades = null;

        lock (_stateGate)
        {
            DrainPendingDeltas();
            trades = DrainPendingTrades();

            if (_bufferedOrder.Count == 0 && (trades is null || trades.Count == 0))
                return;

            long nowTicks = _clock.MonotonicTicks;
            bool emitBookBatches = _bufferedOrder.Count != 0 && (force || nowTicks >= _nextFlushTicks);
            if (!emitBookBatches && (trades is null || trades.Count == 0))
                return;

            if (emitBookBatches)
            {
                DateTimeOffset flushTime = _clock.UtcNow;
                bookBatches = RentBufferedBatchList(_bufferedOrder.Count);
                foreach (ConflationKey key in _bufferedOrder)
                {
                    bookBatches.Add(new BufferedBatch(key, flushTime, _bufferedBookDeltas[key]));
                }

                _bufferedBookDeltas.Clear();
                _bufferedOrder.Clear();
                _nextFlushTicks = 0;
            }
        }

        try
        {
            lock (_emitGate)
            {
                if (trades is not null)
                {
                    foreach (QueuedTrade trade in trades)
                        EmitTrade(trade);
                }

                if (bookBatches is null)
                    return;

                foreach (BufferedBatch batch in bookBatches)
                {
                    if (!_instrumentResolver.TryResolve(batch.Key.SecurityId, out FixMarketDataInstrument instrument))
                        continue;

                    Span<QueuedBookDelta> deltas = CollectionsMarshal.AsSpan(batch.Deltas);
                    Span<FixMarketDataIncrementalEntry> entries = EnsureEntryBufferCapacity(deltas.Length).AsSpan(0, deltas.Length);
                    for (int i = 0; i < deltas.Length; i++)
                    {
                        QueuedBookDelta delta = deltas[i];
                        entries[i] = new FixMarketDataIncrementalEntry(
                            delta.UpdateAction,
                            batch.Key.EntryType,
                            batch.EntryTime,
                            delta.Fields,
                            delta.Price,
                            delta.Size,
                            delta.OrderId);
                    }

                    if (_broadcastSink is not null)
                    {
                        _broadcastSink.OnApplicationMessage(FixIncrementalRefreshMessageBuilder.Build(instrument, entries));
                    }
                    else
                    {
                        FixApplicationSessionHeader header = _headerProvider.NextHeader(batch.EntryTime);
                        ReadOnlyMemory<byte> frame = _writer.WriteIncrementalRefresh(header, instrument, entries);
                        _sink.OnMessage(frame);
                    }
                }
            }
        }
        finally
        {
            ReturnBufferedBatchesToPool(bookBatches);
        }
    }

    private void DrainPendingDeltas()
    {
        while (_pendingBookDeltas.TryDequeue(out QueuedBookDelta delta))
        {
            ReleasePendingEventSlot();
            var key = new ConflationKey(delta.SecurityId, delta.Side);
            if (!_bufferedBookDeltas.TryGetValue(key, out List<QueuedBookDelta>? entries))
            {
                entries = RentBufferedBookDeltaList();
                _bufferedBookDeltas.Add(key, entries);
                _bufferedOrder.Add(key);
            }

            entries.Add(delta);
            if (_nextFlushTicks == 0)
                _nextFlushTicks = delta.EnqueueTicks + _conflationIntervalMs;
        }
    }

    private List<QueuedTrade>? DrainPendingTrades()
    {
        List<QueuedTrade>? trades = null;
        while (_pendingTrades.TryDequeue(out QueuedTrade trade))
        {
            ReleasePendingEventSlot();
            trades ??= new List<QueuedTrade>();
            trades.Add(trade);
        }

        return trades;
    }

    private void EmitTrade(QueuedTrade trade)
    {
        if (!_instrumentResolver.TryResolve(trade.SecurityId, out FixMarketDataInstrument instrument))
            return;

        DateTimeOffset entryTime = ConvertToTimestamp(trade.SendingTimeNs);

        Span<FixMarketDataIncrementalEntry> entries = stackalloc FixMarketDataIncrementalEntry[1];
        entries[0] = new FixMarketDataIncrementalEntry(
            FixMdUpdateAction.New,
            FixMdEntryType.Trade,
            entryTime,
            FixMarketDataEntryFields.Price | FixMarketDataEntryFields.Size | FixMarketDataEntryFields.TradeId,
            trade.Price,
            trade.Quantity,
            TradeId: trade.TradeId);

        if (_broadcastSink is not null)
        {
            _broadcastSink.OnApplicationMessage(FixIncrementalRefreshMessageBuilder.Build(instrument, entries));
            return;
        }

        FixApplicationSessionHeader header = _headerProvider.NextHeader(entryTime);
        ReadOnlyMemory<byte> frame = _writer.WriteIncrementalRefresh(header, instrument, entries);
        _sink.OnMessage(frame);
    }

    private bool TryReservePendingEventSlot()
    {
        if (_pendingEventCapacity <= 0)
        {
            Interlocked.Increment(ref _pendingEventCount);
            return true;
        }

        while (true)
        {
            int snapshot = Volatile.Read(ref _pendingEventCount);
            if (snapshot >= _pendingEventCapacity)
            {
                Interlocked.Increment(ref _droppedEvents);
                return false;
            }

            if (Interlocked.CompareExchange(ref _pendingEventCount, snapshot + 1, snapshot) == snapshot)
                return true;
        }
    }

    private void ReleasePendingEventSlot()
    {
        if (Interlocked.Decrement(ref _pendingEventCount) < 0)
            Interlocked.Exchange(ref _pendingEventCount, 0);
    }

    private void SignalWorker()
    {
        if (_workerThread is not null)
            _wakeSignal.Set();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private void ResetBufferedBookDeltas()
    {
        foreach (List<QueuedBookDelta> deltas in _bufferedBookDeltas.Values)
            ReturnBufferedBookDeltaList(deltas);

        _bufferedBookDeltas.Clear();
        _bufferedOrder.Clear();
    }

    private List<QueuedBookDelta> RentBufferedBookDeltaList()
    {
        if (_bufferedBookDeltaListPool.Count == 0)
            return [];

        return _bufferedBookDeltaListPool.Pop();
    }

    private List<BufferedBatch> RentBufferedBatchList(int minimumCapacity)
    {
        if (_bufferedBatchListPool.Count == 0)
            return new List<BufferedBatch>(minimumCapacity);

        List<BufferedBatch> batches = _bufferedBatchListPool.Pop();
        if (batches.Capacity < minimumCapacity)
            batches.Capacity = minimumCapacity;

        return batches;
    }

    private void ReturnBufferedBatchesToPool(List<BufferedBatch>? bookBatches)
    {
        if (bookBatches is null || bookBatches.Count == 0)
            return;

        lock (_stateGate)
        {
            foreach (BufferedBatch batch in bookBatches)
                ReturnBufferedBookDeltaList(batch.Deltas);

            bookBatches.Clear();
            if (bookBatches.Capacity > MaxRetainedBufferedBatchCapacity
                || _bufferedBatchListPool.Count >= MaxPooledBufferedBatchLists)
            {
                return;
            }

            _bufferedBatchListPool.Push(bookBatches);
        }
    }

    private void ReturnBufferedBookDeltaList(List<QueuedBookDelta> deltas)
    {
        deltas.Clear();
        if (_bufferedBookDeltaListPool.Count >= MaxPooledBufferedBookDeltaLists
            || deltas.Capacity > MaxRetainedBufferedBookDeltaCapacity)
        {
            return;
        }

        _bufferedBookDeltaListPool.Push(deltas);
    }

    private FixMarketDataIncrementalEntry[] EnsureEntryBufferCapacity(int minimumLength)
    {
        if (_entryBuffer.Length >= minimumLength)
            return _entryBuffer;

        _entryBuffer = new FixMarketDataIncrementalEntry[minimumLength];
        return _entryBuffer;
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

    private readonly record struct QueuedTrade(
        ulong SecurityId,
        long Price,
        long Quantity,
        long TradeId,
        long SendingTimeNs);

    private readonly record struct BufferedBatch(
        ConflationKey Key,
        DateTimeOffset EntryTime,
        List<QueuedBookDelta> Deltas);
}
