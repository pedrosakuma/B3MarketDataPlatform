using System.Buffers.Binary;
using System.Text;
using B3.Umdf.Book;

namespace B3.Umdf.Server;

/// <summary>Message type identifiers for the binary WebSocket protocol.</summary>
public enum MessageType : ushort
{
    // Client → Server
    Subscribe = 0x0001,
    Unsubscribe = 0x0002,
    Get = 0x0003,

    // Server → Client
    SubscribeOk = 0x0010,
    SubscribeError = 0x0011,
    Unsubscribed = 0x0012,
    BookSnapshot = 0x0020,
    InfoSnapshot = 0x0021,
    OrderAdded = 0x0030,
    OrderUpdated = 0x0031,
    OrderDeleted = 0x0032,
    Trade = 0x0033,
    BookCleared = 0x0034,
    /// <summary>Server → Client: cancellation of a previously-broadcast trade
    /// (TradeBust_57 from B3 spec §10). Carries securityId + tradeId so the
    /// frontend can mark the corresponding entry in the trade history as
    /// cancelled. Sent exactly once per bust observed on the wire.</summary>
    TradeBust = 0x0035,
    RankingsUpdate = 0x0040,

    /// <summary>Server → Client: broadcast of server feed status (ready / not ready).</summary>
    ServerStatus = 0x0050,

    /// <summary>Server → Client: full candle history for a security on subscribe.</summary>
    CandleSnapshot = 0x0060,

    /// <summary>Server → Client: single candle update (latest candle changed or new candle).</summary>
    CandleUpdate = 0x0061,

    /// <summary>Server → Client: per-symbol stale-status transition.
    /// Sent when a subscribed security flips between Healthy and Stale so the UI can dim
    /// rows / show a stale indicator. Coalesced per security in the conflation buffer:
    /// the last value within a packet wins.</summary>
    SymbolStaleStatus = 0x0070,

    /// <summary>Server → Client: aggregate recovery progress.
    /// Periodic broadcast (~250ms) of total stale symbols and per-kind breakdown across
    /// all channel groups. Stops after totalStale=0 has been broadcast once so clients
    /// can clear the dashboard banner. Independent of <see cref="SymbolStaleStatus"/>:
    /// that message targets per-row dimming for subscribed symbols, this one drives the
    /// global "Recovering N/M symbols" banner.</summary>
    RecoveryProgress = 0x0080,
}

public enum SubscribeErrorCode : byte
{
    UnknownSymbol = 0x01,
    NotReady = 0x02,
}

/// <summary>
/// Bitmask for the data channels a client wants to receive.
/// Sent in the Subscribe message; echoed back in SubscribeOk.
/// </summary>
[Flags]
public enum DataFlags : byte
{
    None = 0x00,
    /// <summary>BookSnapshot + order incrementals (OrderAdded/Updated/Deleted, Trade, BookCleared).</summary>
    Book = 0x01,
    /// <summary>InfoSnapshot + incremental market data / security status updates.</summary>
    Info = 0x02,
    All = Book | Info,
}

/// <summary>
/// Binary serialization for the WebSocket protocol.
/// All messages have a 4-byte framing header: [u16 messageLength][u16 messageType].
/// messageLength includes the header itself.
/// </summary>
public static class WireProtocol
{
    public const int FramingHeaderSize = 4;

    public static void WriteFramingHeader(Span<byte> dest, ushort totalLength, MessageType type)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(dest, totalLength);
        BinaryPrimitives.WriteUInt16LittleEndian(dest[2..], (ushort)type);
    }

    public static bool TryReadFramingHeader(ReadOnlySpan<byte> src, out ushort length, out MessageType type)
    {
        if (src.Length < FramingHeaderSize)
        {
            length = 0;
            type = 0;
            return false;
        }
        length = BinaryPrimitives.ReadUInt16LittleEndian(src);
        type = (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(src[2..]);
        return true;
    }

    // --- Client → Server ---

    /// <summary>
    /// Parse a Subscribe message. Returns symbol and data flags.
    /// Format: [flags 1B][symbolLen 1B][symbol...].
    /// </summary>
    public static (string symbol, DataFlags flags) ReadSubscribe(ReadOnlySpan<byte> payload)
    {
        var flags = (DataFlags)payload[0];
        if (flags == DataFlags.None) flags = DataFlags.All; // treat 0 as "all"
        byte symbolLen = payload[1];
        var symbol = Encoding.UTF8.GetString(payload.Slice(2, symbolLen));
        return (symbol, flags);
    }

    /// <summary>Parse an Unsubscribe message. Returns securityId.</summary>
    public static ulong ReadUnsubscribe(ReadOnlySpan<byte> payload)
    {
        return BinaryPrimitives.ReadUInt64LittleEndian(payload);
    }

    // --- Server → Client ---

    /// <summary>Write ServerStatus: 1-byte ready flag. Total: 5 bytes.</summary>
    public static int WriteServerStatus(Span<byte> dest, bool ready)
    {
        const ushort totalLen = FramingHeaderSize + 1;
        WriteFramingHeader(dest, totalLen, MessageType.ServerStatus);
        dest[4] = ready ? (byte)1 : (byte)0;
        return totalLen;
    }

    /// <summary>Write SubscribeOk: securityId + flags + symbol.</summary>
    public static int WriteSubscribeOk(Span<byte> dest, ulong securityId, DataFlags flags, string symbol)
    {
        int symbolLen = Encoding.UTF8.GetBytes(symbol, dest[14..]);
        ushort totalLen = (ushort)(FramingHeaderSize + 8 + 1 + 1 + symbolLen);
        WriteFramingHeader(dest, totalLen, MessageType.SubscribeOk);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[4..], securityId);
        dest[12] = (byte)flags;
        dest[13] = (byte)symbolLen;
        return totalLen;
    }

    /// <summary>Write SubscribeError: errorCode + symbol.</summary>
    public static int WriteSubscribeError(Span<byte> dest, SubscribeErrorCode errorCode, string symbol)
    {
        int symbolLen = Encoding.UTF8.GetBytes(symbol, dest[6..]);
        ushort totalLen = (ushort)(FramingHeaderSize + 1 + 1 + symbolLen);
        WriteFramingHeader(dest, totalLen, MessageType.SubscribeError);
        dest[4] = (byte)errorCode;
        dest[5] = (byte)symbolLen;
        return totalLen;
    }

    /// <summary>Write Unsubscribed: securityId.</summary>
    public static int WriteUnsubscribed(Span<byte> dest, ulong securityId)
    {
        const ushort totalLen = FramingHeaderSize + 8;
        WriteFramingHeader(dest, totalLen, MessageType.Unsubscribed);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[4..], securityId);
        return totalLen;
    }

    /// <summary>Write OrderAdded/Updated: securityId, orderId, side, price, qty.</summary>
    public static int WriteOrderEvent(Span<byte> dest, MessageType type, ulong securityId, ulong orderId, byte side, long price, long qty)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 8 + 1 + 8 + 8; // 37
        WriteFramingHeader(dest, totalLen, type);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], orderId); offset += 8;
        dest[offset++] = side;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], price); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], qty);
        return totalLen;
    }

    /// <summary>Write OrderDeleted: securityId, orderId, side.</summary>
    public static int WriteOrderDeleted(Span<byte> dest, ulong securityId, ulong orderId, byte side)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 8 + 1; // 21
        WriteFramingHeader(dest, totalLen, MessageType.OrderDeleted);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], orderId); offset += 8;
        dest[offset] = side;
        return totalLen;
    }

    /// <summary>Write Trade: securityId, price, qty, tradeId.</summary>
    public static int WriteTrade(Span<byte> dest, ulong securityId, long price, long qty, long tradeId)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 8 + 8 + 8; // 36
        WriteFramingHeader(dest, totalLen, MessageType.Trade);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], price); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], qty); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], tradeId);
        return totalLen;
    }

    /// <summary>Write TradeBust: securityId + tradeId.</summary>
    public static int WriteTradeBust(Span<byte> dest, ulong securityId, long tradeId)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 8; // 20
        WriteFramingHeader(dest, totalLen, MessageType.TradeBust);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], tradeId);
        return totalLen;
    }

    /// <summary>Write BookCleared: securityId + clearSide byte.</summary>
    public static int WriteBookCleared(Span<byte> dest, ulong securityId, byte clearSide)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 1; // 13
        WriteFramingHeader(dest, totalLen, MessageType.BookCleared);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[4..], securityId);
        dest[12] = clearSide;
        return totalLen;
    }

    /// <summary>
    /// Write SymbolStaleStatus: securityId + isStale byte.
    /// Total: 13 bytes. Pushed when a subscribed security flips between
    /// Healthy and Stale.
    /// </summary>
    public static int WriteSymbolStaleStatus(Span<byte> dest, ulong securityId, bool isStale)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 1; // 13
        WriteFramingHeader(dest, totalLen, MessageType.SymbolStaleStatus);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[4..], securityId);
        dest[12] = isStale ? (byte)1 : (byte)0;
        return totalLen;
    }

    /// <summary>
    /// Max buffer size for RecoveryProgress: header(4) + totals(8) + kindCount(1)
    /// + 14 kinds × (1 byte id + 4 byte count) = 83 bytes.
    /// </summary>
    public const int RecoveryProgressMaxSize = FramingHeaderSize + 4 + 4 + 1 + 14 * 5;

    /// <summary>
    /// Write RecoveryProgress aggregate: totalSymbols, totalStaleSymbols, then
    /// (kindId, count) pairs for every kind whose count is non-zero. Caller is
    /// expected to provide a buffer of at least <see cref="RecoveryProgressMaxSize"/>
    /// bytes. Returns the number of bytes written.
    /// </summary>
    public static int WriteRecoveryProgress(
        Span<byte> dest,
        uint totalSymbols,
        uint totalStaleSymbols,
        ReadOnlySpan<int> staleByKind)
    {
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[offset..], totalSymbols); offset += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[offset..], totalStaleSymbols); offset += 4;
        int kindCountOffset = offset;
        offset += 1; // placeholder for kindCount
        byte kindCount = 0;
        for (int i = 0; i < staleByKind.Length; i++)
        {
            int v = staleByKind[i];
            if (v <= 0) continue;
            dest[offset++] = (byte)i;
            BinaryPrimitives.WriteUInt32LittleEndian(dest[offset..], (uint)v); offset += 4;
            kindCount++;
        }
        dest[kindCountOffset] = kindCount;
        WriteFramingHeader(dest, (ushort)offset, MessageType.RecoveryProgress);
        return offset;
    }

    // --- InfoSnapshot (variable-length, bitmask-driven) ---

    // Bit positions in InfoSnapshot field mask (u32).
    // Values are written as i64 in bit order for set bits.
    public const int FieldOpeningPrice = 0;
    public const int FieldClosingPrice = 1;
    public const int FieldHighPrice = 2;
    public const int FieldLowPrice = 3;
    public const int FieldLastTradePrice = 4;
    public const int FieldLastTradeSize = 5;
    public const int FieldSettlementPrice = 6;
    public const int FieldTheoreticalOpeningPrice = 7;
    public const int FieldTheoreticalOpeningSize = 8;
    public const int FieldAuctionImbalanceSize = 9;
    public const int FieldTradeVolume = 10;
    public const int FieldVwapPrice = 11;
    public const int FieldNetChange = 12;
    public const int FieldNumberOfTrades = 13;
    public const int FieldOpenInterest = 14;
    public const int FieldPriceBandLow = 15;
    public const int FieldPriceBandHigh = 16;
    public const int FieldTradingReferencePrice = 17;
    public const int FieldAvgDailyTradedQty = 18;
    public const int FieldMaxTradeVol = 19;
    public const int FieldTradingStatus = 20;
    public const int FieldTradingEvent = 21;
    public const int FieldPriceLimitType = 22;
    public const int FieldMinPriceIncrement = 23;

    /// <summary>Max buffer size for InfoSnapshot: header + securityId + mask + 24 fields × 8.</summary>
    public const int InfoSnapshotMaxSize = FramingHeaderSize + 8 + 4 + 24 * 8; // 208

    /// <summary>
    /// Write InfoSnapshot: securityId + u32 field bitmask + i64 values for present fields.
    /// Returns total message length.
    /// </summary>
    public static int WriteInfoSnapshot(Span<byte> dest, ulong securityId, InstrumentInfo info)
    {
        int offset = FramingHeaderSize + 8 + 4; // skip header + securityId + mask placeholder
        uint mask = 0;

        if (info.OpeningPrice is { } v0) { mask |= 1u << FieldOpeningPrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v0); offset += 8; }
        if (info.ClosingPrice is { } v1) { mask |= 1u << FieldClosingPrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v1); offset += 8; }
        if (info.HighPrice is { } v2) { mask |= 1u << FieldHighPrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v2); offset += 8; }
        if (info.LowPrice is { } v3) { mask |= 1u << FieldLowPrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v3); offset += 8; }
        if (info.LastTradePrice is { } v4) { mask |= 1u << FieldLastTradePrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v4); offset += 8; }
        if (info.LastTradeSize is { } v5) { mask |= 1u << FieldLastTradeSize; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v5); offset += 8; }
        if (info.SettlementPrice is { } v6) { mask |= 1u << FieldSettlementPrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v6); offset += 8; }
        if (info.TheoreticalOpeningPrice is { } v7) { mask |= 1u << FieldTheoreticalOpeningPrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v7); offset += 8; }
        if (info.TheoreticalOpeningSize is { } v8) { mask |= 1u << FieldTheoreticalOpeningSize; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v8); offset += 8; }
        if (info.AuctionImbalanceSize is { } v9) { mask |= 1u << FieldAuctionImbalanceSize; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v9); offset += 8; }
        if (info.TradeVolume is { } v10) { mask |= 1u << FieldTradeVolume; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v10); offset += 8; }
        if (info.VwapPrice is { } v11) { mask |= 1u << FieldVwapPrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v11); offset += 8; }
        if (info.NetChangeFromPrevDay is { } v12) { mask |= 1u << FieldNetChange; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v12); offset += 8; }
        if (info.NumberOfTrades is { } v13) { mask |= 1u << FieldNumberOfTrades; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v13); offset += 8; }
        if (info.OpenInterest is { } v14) { mask |= 1u << FieldOpenInterest; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v14); offset += 8; }
        if (info.PriceBandLow is { } v15) { mask |= 1u << FieldPriceBandLow; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v15); offset += 8; }
        if (info.PriceBandHigh is { } v16) { mask |= 1u << FieldPriceBandHigh; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v16); offset += 8; }
        if (info.TradingReferencePrice is { } v17) { mask |= 1u << FieldTradingReferencePrice; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v17); offset += 8; }
        if (info.AvgDailyTradedQty is { } v18) { mask |= 1u << FieldAvgDailyTradedQty; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v18); offset += 8; }
        if (info.MaxTradeVol is { } v19) { mask |= 1u << FieldMaxTradeVol; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v19); offset += 8; }
        if (info.TradingStatus is { } v20) { mask |= 1u << FieldTradingStatus; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v20); offset += 8; }
        if (info.TradingEvent is { } v21) { mask |= 1u << FieldTradingEvent; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v21); offset += 8; }
        // PriceLimitType is a u8 enum; we widen to i64 to keep the wire format
        // homogeneous (one slot = 8 bytes). Decoder reads low 8 bits.
        if (info.PriceLimitType is { } v22) { mask |= 1u << FieldPriceLimitType; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v22); offset += 8; }
        if (info.MinPriceIncrement is { } v23) { mask |= 1u << FieldMinPriceIncrement; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v23); offset += 8; }

        // Write framing header, securityId, and mask
        ushort totalLen = (ushort)offset;
        WriteFramingHeader(dest, totalLen, MessageType.InfoSnapshot);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[FramingHeaderSize..], securityId);
        BinaryPrimitives.WriteUInt32LittleEndian(dest[(FramingHeaderSize + 8)..], mask);
        return totalLen;
    }

    // --- BookSnapshot (variable-length) ---

    /// <summary>
    /// Computes the total message size for a BookSnapshot.
    /// Each level = price(8) + totalQty(8) + orderCount(2) = 18 bytes.
    /// </summary>
    public static int BookSnapshotSize(int bidLevels, int askLevels)
    {
        return FramingHeaderSize + 8 + 4 + 2 + 2 + (bidLevels + askLevels) * 18;
    }

    /// <summary>
    /// Write BookSnapshot header. Returns offset where levels should be written.
    /// Caller writes levels using WritePriceLevel.
    /// </summary>
    public static int WriteBookSnapshotHeader(Span<byte> dest, ulong securityId, uint rptSeq, ushort bidCount, ushort askCount)
    {
        ushort totalLen = (ushort)BookSnapshotSize(bidCount, askCount);
        WriteFramingHeader(dest, totalLen, MessageType.BookSnapshot);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[offset..], rptSeq); offset += 4;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], bidCount); offset += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], askCount); offset += 2;
        return offset;
    }

    /// <summary>Write one price level (18 bytes). Returns new offset.</summary>
    public static int WritePriceLevel(Span<byte> dest, int offset, long price, long totalQty, ushort orderCount)
    {
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], price); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], totalQty); offset += 8;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], orderCount); offset += 2;
        return offset;
    }

    // --- RankingsUpdate (variable-length, 3 categories) ---

    /// <summary>
    /// Write RankingsUpdate: 3 categories (volume, gainers, losers), each with N entries.
    /// Format: [header][volCount:u8][entries...][gainerCount:u8][entries...][loserCount:u8][entries...]
    /// Each entry: [securityId:u64][value:i64][symLen:u8][symbol bytes]
    /// </summary>
    public static int WriteRankingsUpdate(Span<byte> dest,
        ReadOnlySpan<RankingEntry> volume,
        ReadOnlySpan<RankingEntry> gainers,
        ReadOnlySpan<RankingEntry> losers)
    {
        int offset = FramingHeaderSize;

        offset = WriteRankingCategory(dest, offset, volume);
        offset = WriteRankingCategory(dest, offset, gainers);
        offset = WriteRankingCategory(dest, offset, losers);

        ushort totalLen = (ushort)offset;
        WriteFramingHeader(dest, totalLen, MessageType.RankingsUpdate);
        return totalLen;
    }

    private static int WriteRankingCategory(Span<byte> dest, int offset, ReadOnlySpan<RankingEntry> entries)
    {
        dest[offset++] = (byte)entries.Length;
        foreach (ref readonly var e in entries)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], e.SecurityId); offset += 8;
            BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], e.Value); offset += 8;
            int symLen = Encoding.UTF8.GetBytes(e.Symbol, dest[(offset + 1)..]);
            dest[offset] = (byte)symLen;
            offset += 1 + symLen;
        }
        return offset;
    }

    /// <summary>Max buffer for rankings: 3 categories × 10 entries × (8+8+1+32) ≈ 1500 bytes + header.</summary>
    public const int RankingsUpdateMaxSize = FramingHeaderSize + 3 * (1 + 10 * (8 + 8 + 1 + 64));

    // --- CandleSnapshot / CandleUpdate ---

    /// <summary>Candle wire size: time(8) + open(8) + high(8) + low(8) + close(8) + volume(8) + avg(8) = 56 bytes.</summary>
    public const int CandleSize = 56;

    /// <summary>CandleSnapshot header overhead: framing(4) + secId(8) + resolution(2) + flags(1) + count(2) = 17.</summary>
    private const int CandleSnapshotHeaderSize = FramingHeaderSize + 8 + 2 + 1 + 2;

    /// <summary>Max candles per CandleSnapshot message, limited by u16 framing length.</summary>
    public const int MaxCandlesPerSnapshot = (ushort.MaxValue - CandleSnapshotHeaderSize) / CandleSize; // 1364

    /// <summary>CandleSnapshot flags.</summary>
    internal const byte CandleFlagFirst = 0x01; // first batch (replace)
    internal const byte CandleFlagLast = 0x02;  // final batch of the snapshot

    /// <summary>
    /// Write CandleSnapshot batch: securityId + resolution + flags + count + candle data.
    /// Format: [header][u64 secId][u16 resolution][u8 flags][u16 count][candle × N]
    /// Caller must ensure candles.Length &lt;= MaxCandlesPerSnapshot.
    /// </summary>
    internal static int WriteCandleSnapshot(Span<byte> dest, ulong securityId, int resolution, byte flags, ReadOnlySpan<Candle> candles)
    {
        ushort totalLen = (ushort)(CandleSnapshotHeaderSize + candles.Length * CandleSize);
        WriteFramingHeader(dest, totalLen, MessageType.CandleSnapshot);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], (ushort)resolution); offset += 2;
        dest[offset++] = flags;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], (ushort)candles.Length); offset += 2;
        foreach (ref readonly var c in candles)
            offset = WriteCandle(dest, offset, c);
        return totalLen;
    }

    /// <summary>CandleUpdate wire size: framing(4) + secId(8) + resolution(2) + candle(56) = 70 bytes.</summary>
    public const int CandleUpdateMessageSize = FramingHeaderSize + 8 + 2 + CandleSize;

    /// <summary>
    /// Write CandleUpdate: securityId + resolution + single candle.
    /// Format: [header][u64 secId][u16 resolution][candle]
    /// </summary>
    internal static int WriteCandleUpdate(Span<byte> dest, ulong securityId, int resolution, in Candle candle)
    {
        const ushort totalLen = (ushort)CandleUpdateMessageSize;
        WriteFramingHeader(dest, totalLen, MessageType.CandleUpdate);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], (ushort)resolution); offset += 2;
        WriteCandle(dest, offset, candle);
        return totalLen;
    }

    private static int WriteCandle(Span<byte> dest, int offset, in Candle c)
    {
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], c.Time); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], c.Open); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], c.High); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], c.Low); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], c.Close); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], c.Volume); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], c.Avg); offset += 8;
        return offset;
    }

}

public readonly struct RankingEntry
{
    public readonly ulong SecurityId;
    public readonly long Value;
    public readonly string Symbol;

    public RankingEntry(ulong securityId, long value, string symbol)
    {
        SecurityId = securityId;
        Value = value;
        Symbol = symbol;
    }
}
