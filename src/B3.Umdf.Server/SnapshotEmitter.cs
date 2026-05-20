using System.Buffers;
using B3.Umdf.Book;

namespace B3.Umdf.Server;

/// <summary>
/// Pure-static helpers that serialize per-security snapshots into wire frames and
/// hand them to a <see cref="ClientSession"/>'s outbound ring. Every method here is
/// stateless and side-effect-free apart from outbound enqueues —
/// extracted out of <see cref="SubscriptionManager"/> so the manager can focus on
/// orchestration (subscription state, routing, lifecycle) instead of byte layout.
///
/// <para>None of these helpers retains the <paramref name="session"/> reference past
/// the call; pooled buffers passed to <see cref="ClientSession.TryEnqueueBatch"/>
/// are owned by the session afterwards (returned to the pool by the write loop).</para>
/// </summary>
internal static class SnapshotEmitter
{
    /// <summary>
    /// Cached UTF-8 build-version string used in every <see cref="MessageType.ServerHello"/>
    /// frame. Sourced from the assembly version once at first access — no per-connection
    /// reflection cost on the hot connect path.
    /// </summary>
    private static readonly string s_serverBuildVersion =
        typeof(SnapshotEmitter).Assembly.GetName().Version?.ToString() ?? "0.0.0.0";

    /// <summary>
    /// Capabilities advertised in the handshake. Kept as a single source of truth so
    /// tests can assert against it without re-deriving the bitmask.
    /// </summary>
    internal const ServerCapabilities AdvertisedCapabilities =
        ServerCapabilities.SnapshotOnSubscribe | ServerCapabilities.SymbolDelistedNotification;

    /// <summary>
    /// Send a <see cref="MessageType.ServerHello"/> as the very first server-initiated
    /// frame. Carries protocol version + capabilities + server build so clients can
    /// negotiate features and surface the build version to operators.
    /// </summary>
    public static bool SendServerHello(ClientSession session)
    {
        var buf = new byte[WireProtocol.ServerHelloMaxSize];
        int len = WireProtocol.WriteServerHello(
            buf,
            WireProtocol.ProtocolVersion,
            AdvertisedCapabilities,
            s_serverBuildVersion);
        return session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    /// <summary>
    /// Send a server-status frame (ready/initializing) directly. Used both for
    /// fresh-client greeting and for broadcast on RealTime entry.
    /// </summary>
    public static bool SendServerStatus(ClientSession session, bool ready)
    {
        var buf = new byte[5];
        WireProtocol.WriteServerStatus(buf, ready);
        return session.TryEnqueue(buf);
    }

    /// <summary>Send a SubscribeError frame for the given (code, symbol) tuple.</summary>
    public static bool SendError(ClientSession session, SubscribeErrorCode code, string symbol)
    {
        var buf = new byte[WireProtocol.FramingHeaderSize + 1 + 1 + System.Text.Encoding.UTF8.GetMaxByteCount(symbol.Length)];
        int len = WireProtocol.WriteSubscribeError(buf, code, symbol);
        return session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    /// <summary>
    /// Serialize a complete order book (both sides) into a single pooled buffer and
    /// enqueue it as a batch. Pooled buffer ownership transfers to the session on
    /// success; on TryEnqueueBatch failure the session returns it to the pool.
    /// </summary>
    public static bool SendMboSnapshot(ClientSession session, OrderBook book)
    {
        ulong securityId = book.SecurityId;
        uint lastRptSeq = book.LastRptSeq;
        var bids = book.Bids;
        var asks = book.Asks;

        int headerSize = WireProtocol.BookSnapshotSize(0, 0);
        int totalOrders = bids.OrderCount + asks.OrderCount;
        int needed = headerSize + totalOrders * 37;
        var buf = BroadcastBufferPool.Shared.Rent(needed);

        WireProtocol.WriteBookSnapshotHeader(buf, securityId, lastRptSeq, 0, 0);
        int offset = headerSize;

        WriteOrdersDirect(buf, ref offset, securityId, bids);
        WriteOrdersDirect(buf, ref offset, securityId, asks);

        if (!session.TryEnqueueBatch(new ReadOnlyMemory<byte>(buf, 0, offset), 1, pooledArray: buf))
            return false;
        return SendMarketTierSnapshot(session, book);
    }

    /// <summary>
    /// Send an empty book snapshot (header only, zero orders). Used when a
    /// subscriber requests a security that has no <see cref="OrderBook"/> yet —
    /// the empty frame still tells the frontend that the snapshot phase is
    /// complete so it can drop any "loading" placeholders.
    /// </summary>
    public static bool SendEmptyBookSnapshot(ClientSession session, ulong securityId)
    {
        var emptyBuf = new byte[WireProtocol.BookSnapshotSize(0, 0)];
        WireProtocol.WriteBookSnapshotHeader(emptyBuf, securityId, 0, 0, 0);
        return session.TryEnqueue(new ReadOnlyMemory<byte>(emptyBuf));
    }

    /// <summary>
    /// Serialize the current MBP (price-level) snapshot for both sides of an
    /// <see cref="OrderBook"/>. One <see cref="MessageType.LevelSnapshot"/> frame is
    /// produced per security — the wire payload is bid-then-ask price levels, each
    /// 20 bytes (price/totalQty/orderCount). Pooled buffer ownership transfers to
    /// the session on success.
    /// </summary>
    public static bool SendMbpSnapshot(ClientSession session, OrderBook book)
    {
        ulong securityId = book.SecurityId;
        var bids = book.Bids;
        var asks = book.Asks;
        int bidCount = bids.LevelCount;
        int askCount = asks.LevelCount;

        int total = WireProtocol.LevelSnapshotSize(bidCount, askCount);
        var buf = BroadcastBufferPool.Shared.Rent(total);

        int offset = WireProtocol.WriteLevelSnapshotHeader(buf, securityId,
            checked((ushort)bidCount), checked((ushort)askCount));

        // PriceLevels iterates best→worst; for the snapshot we just need every
        // level and the order doesn't carry semantics (frontend keys by price).
        foreach (var lvl in bids.PriceLevelAggregates)
            offset = WireProtocol.WriteLevelSnapshotEntry(buf, offset, lvl.Price, lvl.TotalQty, lvl.Count);
        foreach (var lvl in asks.PriceLevelAggregates)
            offset = WireProtocol.WriteLevelSnapshotEntry(buf, offset, lvl.Price, lvl.TotalQty, lvl.Count);

        return session.TryEnqueueBatch(new ReadOnlyMemory<byte>(buf, 0, offset), 1, pooledArray: buf);
    }

    /// <summary>
    /// Send an empty MBP snapshot (header only) so the frontend can drop any
    /// "loading" placeholder when the security has no live book yet.
    /// </summary>
    public static bool SendEmptyMbpSnapshot(ClientSession session, ulong securityId)
    {
        var emptyBuf = new byte[WireProtocol.LevelSnapshotSize(0, 0)];
        WireProtocol.WriteLevelSnapshotHeader(emptyBuf, securityId, 0, 0);
        return session.TryEnqueue(new ReadOnlyMemory<byte>(emptyBuf));
    }

    private static bool SendMarketTierSnapshot(ClientSession session, OrderBook book)
    {
        Span<byte> buf = stackalloc byte[32];
        return SendMarketTierSide(session, buf, book.SecurityId, BookSideType.Bid, book.MarketOrderQuantity(BookSideType.Bid), book.MarketOrderCount(BookSideType.Bid))
            && SendMarketTierSide(session, buf, book.SecurityId, BookSideType.Ask, book.MarketOrderQuantity(BookSideType.Ask), book.MarketOrderCount(BookSideType.Ask));
    }

    private static bool SendMarketTierSide(ClientSession session, Span<byte> buf, ulong securityId, BookSideType side, long totalQty, int orderCount)
    {
        if (orderCount == 0 && totalQty == 0)
            return true;
        int len = WireProtocol.WriteMarketTierUpdate(buf, securityId, (byte)side, totalQty, orderCount);
        return session.TryEnqueue(new ReadOnlyMemory<byte>(buf[..len].ToArray()));
    }

    /// <summary>Send the current <see cref="InstrumentInfo"/> snapshot for a security.</summary>
    public static bool SendInfoSnapshot(ClientSession session, ulong securityId, InstrumentInfo info)
    {
        var buf = new byte[WireProtocol.InfoSnapshotMaxSize];
        int len = WireProtocol.WriteInfoSnapshot(buf, securityId, info);
        return session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    /// <summary>
    /// Replay every cached trade in the per-security ring buffer to the session,
    /// one Trade frame per entry. Bounded by <see cref="SubscriptionManager.MaxRecentTrades"/>.
    /// </summary>
    public static bool SendTradeHistory(ClientSession session, ulong securityId, TradeRingBuffer ring)
    {
        foreach (var slot in ring.AsSpan())
        {
            // Spec §10: TradeBust_57 cancels a previously-broadcast trade. New
            // subscribers must not see busted trades in their initial history;
            // live subscribers receive the bust via MessageType.TradeBust.
            if (slot.Busted != 0) continue;
            var buf = new byte[37];
            int len = WireProtocol.WriteTrade(buf, securityId, slot.Price, slot.Qty, slot.TradeId);
            if (!session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len)))
                return false;
        }
        return true;
    }

    /// <summary>
    /// Send the per-security candle history in chunks of at most
    /// <see cref="WireProtocol.MaxCandlesPerSnapshot"/> (≈1364 due to u16 framing).
    /// First frame carries CandleFlagFirst, last frame carries CandleFlagLast; a
    /// single-batch history sets both flags. Empty history degrades to
    /// <see cref="SendEmptyCandleSnapshot"/> so the frontend always sees a
    /// snapshot-complete signal.
    /// </summary>
    public static bool SendCandleHistory(ClientSession session, ulong securityId, CandleAggregator agg)
    {
        var candles = agg.GetCandles();
        if (candles.Length == 0)
        {
            return SendEmptyCandleSnapshot(session, securityId);
        }

        int maxPerBatch = WireProtocol.MaxCandlesPerSnapshot;
        for (int i = 0; i < candles.Length; i += maxPerBatch)
        {
            int count = Math.Min(maxPerBatch, candles.Length - i);
            byte flags = 0;
            if (i == 0)
                flags |= WireProtocol.CandleFlagFirst;
            if (i + count >= candles.Length)
                flags |= WireProtocol.CandleFlagLast;

            var batch = candles.AsSpan(i, count);
            var buf = new byte[WireProtocol.FramingHeaderSize + 8 + 2 + 1 + 2 + count * WireProtocol.CandleSize];
            int len = WireProtocol.WriteCandleSnapshot(buf, securityId, agg.Resolution, flags, batch);
            if (!session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len)))
                return false;
        }
        return true;
    }

    /// <summary>Send an empty candle snapshot frame so the frontend can drop loading state.</summary>
    public static bool SendEmptyCandleSnapshot(ClientSession session, ulong securityId)
    {
        var buf = new byte[WireProtocol.FramingHeaderSize + 8 + 2 + 1 + 2];
        int len = WireProtocol.WriteCandleSnapshot(
            buf,
            securityId,
            1,
            (byte)(WireProtocol.CandleFlagFirst | WireProtocol.CandleFlagLast),
            ReadOnlySpan<Candle>.Empty);
        return session.TryEnqueue(new ReadOnlyMemory<byte>(buf, 0, len));
    }

    /// <summary>
    /// Writes every order on the given side directly into <paramref name="buf"/> without
    /// allocating an intermediate tuple array. Iterates over the concrete-typed
    /// <see cref="Dictionary{TKey,TValue}.ValueCollection"/> so the foreach uses a
    /// struct enumerator instead of the boxed <see cref="IEnumerable{T}"/> path.
    /// </summary>
    private static void WriteOrdersDirect(byte[] buf, ref int offset, ulong securityId, BookSide side)
    {
        foreach (var entry in side.SnapshotOrderValues)
        {
            int len = WireProtocol.WriteOrderEvent(buf.AsSpan(offset), MessageType.OrderAdded,
                securityId, entry.OrderId, (byte)entry.Side, entry.Price, entry.Quantity);
            offset += len;
        }
    }
}
