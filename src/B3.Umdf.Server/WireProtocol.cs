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
    /// <summary>Server → Client: aggregated price-level snapshot (MBP) for a security.
    /// Carries every live price level as (price, totalQty, orderCount). Sent on
    /// initial subscribe when <see cref="DataFlags.Mbp"/> is requested. Paired
    /// with incremental <see cref="LevelUpdate"/> / <see cref="LevelDeleted"/>.</summary>
    LevelSnapshot = 0x0022,
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
    /// <summary>Server → Client: aggregate null-price MOA/MOC market tier for one side.
    /// Carries total quantity and order count; it is deliberately separate from
    /// OrderAdded/Updated so no sentinel price is needed.</summary>
    MarketTierUpdate = 0x0036,
    /// <summary>Server → Client: incremental MBP price-level update.
    /// Carries (securityId, side, price, totalQty, orderCount) for a level whose
    /// aggregate just changed. Conflated last-write-wins by (secId, side, price)
    /// in the per-group MBP buffer so a level touched many times in one packet
    /// still emits at most one frame per packet.</summary>
    LevelUpdate = 0x0037,
    /// <summary>Server → Client: incremental MBP level removal.
    /// Sent when a price level was fully drained (last order deleted/moved away).
    /// Carries (securityId, side, price). Frontend MUST drop the level from its
    /// per-symbol map.</summary>
    LevelDeleted = 0x0038,
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

    /// <summary>Server → Client: terminal notification that a previously-subscribed
    /// security has been delisted by the venue. Carries only the securityId; clients
    /// MUST stop expecting further data for that symbol and SHOULD remove it from
    /// their UI. The server cleans up the per-symbol subscription map after sending,
    /// so subsequent <c>Subscribe</c> attempts for the same symbol will fail with
    /// <see cref="SubscribeErrorCode.UnknownSymbol"/>.
    /// Issued via <c>SubscriptionManager.NotifyDelisted</c>; integration with the
    /// upstream SBE delisting trigger is intentionally a follow-up.</summary>
    SymbolDelisted = 0x0071,

    /// <summary>Server → Client: aggregate recovery progress.
    /// Periodic broadcast (~250ms) of total stale symbols and per-kind breakdown across
    /// all channel groups. Stops after totalStale=0 has been broadcast once so clients
    /// can clear the dashboard banner. Independent of <see cref="SymbolStaleStatus"/>:
    /// that message targets per-row dimming for subscribed symbols, this one drives the
    /// global "Recovering N/M symbols" banner.</summary>
    RecoveryProgress = 0x0080,

    /// <summary>Server → Client: start of a News delivery (template SBE 5).
    /// Carries the metadata header (securityId, newsId, source, lang, origTime,
    /// total lengths) but NOT the variable-length text payloads — those follow
    /// as zero or more <see cref="NewsChunk"/> messages and a final
    /// <see cref="NewsEnd"/>. Fragmentation is required because the framing
    /// header uses u16 length (max ~65 KB) but a reassembled News may exceed
    /// that. All News fragments for a given delivery share the same newsId.</summary>
    NewsBegin = 0x0090,
    /// <summary>Server → Client: a single payload fragment for an in-flight News
    /// delivery. Carries newsId + field discriminator (0=Headline,1=Text,2=URL)
    /// + bytes. Up to 60 KB per fragment.</summary>
    NewsChunk = 0x0091,
    /// <summary>Server → Client: explicit terminator for a News delivery.
    /// Lets clients release per-news buffers and surface the assembled news to
    /// the UI exactly once.</summary>
    NewsEnd = 0x0092,

    /// <summary>Server → Client: protocol/version handshake. Sent as the very first
    /// frame after a client connects (before <see cref="ServerStatus"/>) so consumers
    /// can negotiate features and surface the server build to operators.
    /// Payload: <c>[u32 protocolVersion][u32 capabilities][u8 buildVerLen][buildVer UTF-8]</c>.
    /// The MessageType is purely additive — older clients that don't recognise it MUST
    /// skip the frame (length-prefixed) and continue parsing the next message.</summary>
    ServerHello = 0x00A0,

    /// <summary>Client → Server: optional version-negotiation handshake. If sent it MUST
    /// arrive before any <see cref="Subscribe"/> / <see cref="Get"/> / <see cref="Unsubscribe"/>
    /// frame. Payload: <c>[u32 protocolVersion]</c> — the version the client intends to
    /// speak. Servers reject (WS close 1003 "protocol_version_unsupported") any value
    /// outside <see cref="WireProtocol.SupportedProtocolVersionMin"/>..<see cref="WireProtocol.SupportedProtocolVersionMax"/>.
    /// Backwards compatible: clients that never send ClientHello are assumed to speak
    /// the current <see cref="WireProtocol.ProtocolVersion"/>.</summary>
    ClientHello = 0x00A1,

    /// <summary>Server → Client: full <c>SecurityDefinition</c> (static instrument metadata
    /// — tick, lot, identity, classification) for a single security. Pushed (a) on
    /// initial Subscribe when <see cref="DataFlags.SecurityDefinition"/> is set and
    /// the server already has the definition cached, and (b) on every subsequent
    /// real change (identity, tick, lot, …). Re-broadcasts that don't change any
    /// field are short-circuited upstream by
    /// <c>MarketDataManager.HandleSecurityDefinition</c>'s validity-timestamp
    /// fast-path, so this frame only flows on true deltas.
    /// Payload (little-endian, after the 4-byte framing header):
    /// <c>[u64 securityId][u8 symbolLen][symbol UTF-8][u32 numericFieldMask]
    /// [i64 values for set bits in bit order][u32 stringFieldMask][per set bit: u16 len][bytes]</c>.
    /// Field mask bit positions are defined by <c>SecurityDefinitionField*</c>
    /// constants on <see cref="WireProtocol"/>. New fields are append-only at
    /// new bit positions; older SDKs MUST consume slots for unknown bits without
    /// alignment damage.</summary>
    SecurityDefinition = 0x00B0,

    /// <summary>Server → Client: dynamic per-symbol price band (tunnel) snapshot
    /// for a single security. Pushed (a) on initial Subscribe when
    /// <see cref="DataFlags.PriceBand"/> is set and the server has already
    /// observed at least one <c>PriceBand_22</c>, and (b) on every real change
    /// to low/high limits, limit-type, midpoint-type, or trading reference
    /// price. Idempotent re-broadcasts (the venue may emit the same band
    /// periodically) are short-circuited upstream by
    /// <c>MarketDataManager.HandlePriceBand</c>'s diff check, so this frame
    /// only flows when the band actually moves.
    /// Payload (little-endian, after the 4-byte framing header):
    /// <c>[u64 securityId][u8 symbolLen][symbol UTF-8][u32 fieldMask][i64 values
    /// for set bits in bit order]</c>.
    /// Field mask bit positions are defined by <c>PriceBandField*</c>
    /// constants on <see cref="WireProtocol"/>. New fields are append-only at
    /// new bit positions; older SDKs MUST consume slots for unknown bits without
    /// alignment damage.</summary>
    PriceBand = 0x00B1,

    /// <summary>Server → Client: auction state for one security (imbalance +
    /// trading phase). Pushed on Subscribe (when the server has observed
    /// imbalance or group-phase data) and on every subsequent real delta.
    /// Layout: <c>[securityId u64][symLen u8][symbol ...][fieldMask u8][i64 values
    /// for set bits in bit order]</c>.
    /// Field mask bit positions are defined by <c>AuctionField*</c>
    /// constants on <see cref="WireProtocol"/>. New fields are append-only at
    /// new bit positions.</summary>
    Auction = 0x00B2,
}

/// <summary>
/// Bitfield of optional server-side features advertised in <see cref="MessageType.ServerHello"/>.
/// Clients MUST treat unknown bits as reserved (ignore + log). New flags are appended
/// only — bits MUST NOT be reused or repurposed once shipped.
/// </summary>
[Flags]
public enum ServerCapabilities : uint
{
    None = 0,
    /// <summary>Server pushes initial book/info/MBP snapshot frames immediately on
    /// successful Subscribe. Always set today; documented as a flag so clients can
    /// branch cleanly once a future server might allow snapshot opt-out.</summary>
    SnapshotOnSubscribe = 0x0001,
    /// <summary>Server emits <see cref="MessageType.SymbolDelisted"/> as the terminal
    /// notification when a subscribed security is delisted mid-session.</summary>
    SymbolDelistedNotification = 0x0002,
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
    /// <summary>News deliveries (NewsBegin/Chunk/End). Opt-in: clients without this
    /// bit receive no news, even for symbols they're subscribed to. Both
    /// instrument-scoped and global news flow through this flag.</summary>
    News = 0x04,
    /// <summary>Aggregated price-level (MBP) stream: <see cref="MessageType.LevelSnapshot"/>
    /// + incremental <see cref="MessageType.LevelUpdate"/>/<see cref="MessageType.LevelDeleted"/>.
    /// Independent of <see cref="Book"/>: a client may request only MBP, only
    /// MBO (Book), or both. MBP is the recommended default for UI consumers
    /// since the wire is conflated per (secId, side, price) instead of per
    /// orderId, dramatically reducing bandwidth on hot levels.</summary>
    Mbp = 0x08,
    /// <summary>Trade prints (live <see cref="MessageType.Trade"/>) and corrections
    /// (<see cref="MessageType.TradeBust"/>), plus the per-symbol recent-trades
    /// history sent on subscribe. Opt-in (default OFF): clients that only want
    /// quotes (Book and/or Mbp) avoid trade-stream bandwidth entirely. Note that
    /// <c>LastTradePrice</c> in <see cref="MessageType.InfoSnapshot"/> is part of
    /// <see cref="Info"/> and is not gated by this flag.</summary>
    Trades = 0x10,
    /// <summary>Static instrument metadata stream
    /// (<see cref="MessageType.SecurityDefinition"/>): tick, lot, identity,
    /// classification fields from <c>SecurityDefinition_12</c>. Pushed on
    /// Subscribe (when the server already has the definition cached) and on
    /// every subsequent real delta. Opt-in so legacy clients that only need
    /// price data don't pay metadata-frame bandwidth.</summary>
    SecurityDefinition = 0x20,
    /// <summary>Dynamic per-symbol price band (tunnel) stream
    /// (<see cref="MessageType.PriceBand"/>): low/high limits, limit-type,
    /// midpoint-type, and trading reference price from <c>PriceBand_22</c>.
    /// Pushed on Subscribe (when the server has already observed the band)
    /// and on every subsequent real delta. Opt-in so legacy clients that
    /// don't enforce pre-trade band checks don't pay the bandwidth.</summary>
    PriceBand = 0x40,
    /// <summary>Auction state stream (<see cref="MessageType.Auction"/>):
    /// imbalance quantity/condition from <c>AuctionImbalance_19</c> and
    /// group trading phase from <c>SecurityGroupPhase_10</c>. Pushed on
    /// Subscribe (when the server has observed imbalance or phase data)
    /// and on every subsequent real delta. Opt-in so legacy clients that
    /// don't trade in auctions don't pay the bandwidth.</summary>
    Auction = 0x80,
    /// <summary>Legacy convenience: Book + Info. Kept stable for compatibility;
    /// does NOT include News, MBP, Trades, SecurityDefinition, PriceBand, or Auction.</summary>
    All = Book | Info,
    /// <summary>Convenience alias for "every data class": Book + Info + News + MBP + Trades + SecurityDefinition + PriceBand + Auction.</summary>
    Everything = Book | Info | News | Mbp | Trades | SecurityDefinition | PriceBand | Auction,
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

    /// <summary>
    /// Write Trade: securityId, price, qty, tradeId, flags.
    /// Wire layout: <c>[hdr 4][secId 8][price 8][qty 8][tradeId 8][flags 1]</c> = 37 bytes.
    /// The trailing <paramref name="flags"/> byte (default 0) carries
    /// <see cref="B3.Umdf.Book.TradeFlags"/> bits. Older clients that read the
    /// previous 36-byte layout simply ignore the trailing byte — the framing
    /// header always reports the true length so frame alignment stays intact.
    /// </summary>
    public static int WriteTrade(Span<byte> dest, ulong securityId, long price, long qty, long tradeId, byte flags = 0)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 8 + 8 + 8 + 1; // 37
        WriteFramingHeader(dest, totalLen, MessageType.Trade);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], price); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], qty); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], tradeId); offset += 8;
        dest[offset] = flags;
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

    /// <summary>Write MarketTierUpdate: securityId + side + aggregate qty + order count.</summary>
    public static int WriteMarketTierUpdate(Span<byte> dest, ulong securityId, byte side, long totalQty, int orderCount)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 1 + 8 + 4; // 25
        WriteFramingHeader(dest, totalLen, MessageType.MarketTierUpdate);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        dest[offset++] = side;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], totalQty); offset += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[offset..], checked((uint)orderCount));
        return totalLen;
    }

    /// <summary>
    /// Write LevelUpdate (MBP incremental). Layout:
    /// header(4) + securityId(8) + side(1) + price(8) + totalQty(8) + orderCount(4) = 33 bytes.
    /// </summary>
    public const int LevelUpdateSize = FramingHeaderSize + 8 + 1 + 8 + 8 + 4;
    public static int WriteLevelUpdate(Span<byte> dest, ulong securityId, byte side, long price, long totalQty, int orderCount)
    {
        const ushort totalLen = LevelUpdateSize;
        WriteFramingHeader(dest, totalLen, MessageType.LevelUpdate);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        dest[offset++] = side;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], price); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], totalQty); offset += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[offset..], checked((uint)orderCount));
        return totalLen;
    }

    /// <summary>
    /// Write LevelDeleted (MBP incremental). Layout:
    /// header(4) + securityId(8) + side(1) + price(8) = 21 bytes.
    /// </summary>
    public const int LevelDeletedSize = FramingHeaderSize + 8 + 1 + 8;
    public static int WriteLevelDeleted(Span<byte> dest, ulong securityId, byte side, long price)
    {
        const ushort totalLen = LevelDeletedSize;
        WriteFramingHeader(dest, totalLen, MessageType.LevelDeleted);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        dest[offset++] = side;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], price);
        return totalLen;
    }

    /// <summary>
    /// Per-level entry size inside <see cref="MessageType.LevelSnapshot"/>:
    /// price(8) + totalQty(8) + orderCount(4) = 20 bytes.
    /// </summary>
    public const int LevelSnapshotEntrySize = 8 + 8 + 4;

    /// <summary>Total bytes a LevelSnapshot occupies for the given level counts.</summary>
    public static int LevelSnapshotSize(int bidLevels, int askLevels)
        => FramingHeaderSize + 8 + 2 + 2 + (bidLevels + askLevels) * LevelSnapshotEntrySize;

    /// <summary>
    /// Write LevelSnapshot header. Layout:
    /// header(4) + securityId(8) + bidCount(2) + askCount(2) + bid entries + ask entries.
    /// Each entry is <see cref="LevelSnapshotEntrySize"/> bytes (price, totalQty, orderCount).
    /// Caller writes entries via <see cref="WriteLevelSnapshotEntry"/>.
    /// </summary>
    public static int WriteLevelSnapshotHeader(Span<byte> dest, ulong securityId, ushort bidCount, ushort askCount)
    {
        ushort totalLen = (ushort)LevelSnapshotSize(bidCount, askCount);
        WriteFramingHeader(dest, totalLen, MessageType.LevelSnapshot);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], bidCount); offset += 2;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], askCount); offset += 2;
        return offset;
    }

    /// <summary>Write one MBP snapshot entry (20 bytes). Returns new offset.</summary>
    public static int WriteLevelSnapshotEntry(Span<byte> dest, int offset, long price, long totalQty, int orderCount)
    {
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], price); offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], totalQty); offset += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[offset..], checked((uint)orderCount)); offset += 4;
        return offset;
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
    /// Current server-side wire protocol version. Bumped only on a breaking change to
    /// the framing or to the semantics of an existing <see cref="MessageType"/>.
    /// Additive new MessageTypes do NOT bump this field.
    /// </summary>
    public const uint ProtocolVersion = 1;

    /// <summary>
    /// Minimum protocol version the server can speak with a client. A
    /// <see cref="MessageType.ClientHello"/> payload below this value triggers a
    /// WS close (1003 "protocol_version_unsupported"). Today identical to
    /// <see cref="ProtocolVersion"/> (no historic versions supported); kept as a
    /// distinct constant so the negotiation path is shaped correctly for the day
    /// the server bumps <see cref="SupportedProtocolVersionMax"/>.
    /// </summary>
    public const uint SupportedProtocolVersionMin = 1;

    /// <summary>
    /// Maximum protocol version the server can speak with a client. Mirrors
    /// <see cref="ProtocolVersion"/> today; future server releases that gain a new
    /// wire format should bump <see cref="ProtocolVersion"/> + this constant.
    /// </summary>
    public const uint SupportedProtocolVersionMax = ProtocolVersion;

    /// <summary>
    /// Total bytes of a <see cref="MessageType.ClientHello"/> frame: header(4) + version(4).
    /// </summary>
    public const int ClientHelloSize = FramingHeaderSize + 4;

    /// <summary>
    /// Write a <see cref="MessageType.ClientHello"/> frame: <c>[u32 protocolVersion]</c>.
    /// Used by the C# SDK (and tests) to advertise the version the client intends to speak.
    /// </summary>
    public static int WriteClientHello(Span<byte> dest, uint protocolVersion)
    {
        const ushort totalLen = ClientHelloSize;
        WriteFramingHeader(dest, totalLen, MessageType.ClientHello);
        BinaryPrimitives.WriteUInt32LittleEndian(dest[FramingHeaderSize..], protocolVersion);
        return totalLen;
    }

    /// <summary>
    /// Parse a <see cref="MessageType.ClientHello"/> payload (everything after the 4-byte
    /// framing header). Returns the version the client claims to support.
    /// </summary>
    public static uint ReadClientHello(ReadOnlySpan<byte> payload)
        => BinaryPrimitives.ReadUInt32LittleEndian(payload);


    /// <summary>
    /// Maximum buffer needed for a <see cref="MessageType.ServerHello"/> frame:
    /// header(4) + protocolVersion(4) + capabilities(4) + buildVerLen(1) + 255 UTF-8 bytes.
    /// </summary>
    public const int ServerHelloMaxSize = FramingHeaderSize + 4 + 4 + 1 + 255;

    /// <summary>
    /// Write a <see cref="MessageType.ServerHello"/> frame. <paramref name="buildVersion"/>
    /// is truncated to 255 UTF-8 bytes (the on-wire length prefix is a single byte) so
    /// callers may safely pass an assembly version, semver string, or git SHA without
    /// pre-validation. Returns the total number of bytes written.
    /// </summary>
    public static int WriteServerHello(
        Span<byte> dest,
        uint protocolVersion,
        ServerCapabilities capabilities,
        string buildVersion)
    {
        // UTF-8-encode into a scratch buffer first so we can clamp to 255 bytes
        // without truncating mid-codepoint awkwardness — buildVersion is short
        // (assembly version / sha) so the alloc is negligible.
        var encoded = Encoding.UTF8.GetBytes(buildVersion ?? string.Empty);
        if (encoded.Length > 255)
        {
            // Defensive: drop trailing bytes; build strings should never exceed 255.
            var trimmed = new byte[255];
            Array.Copy(encoded, trimmed, 255);
            encoded = trimmed;
        }
        ushort totalLen = (ushort)(FramingHeaderSize + 4 + 4 + 1 + encoded.Length);
        WriteFramingHeader(dest, totalLen, MessageType.ServerHello);
        BinaryPrimitives.WriteUInt32LittleEndian(dest[4..], protocolVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(dest[8..], (uint)capabilities);
        dest[12] = (byte)encoded.Length;
        encoded.CopyTo(dest[13..]);
        return totalLen;
    }

    /// <summary>
    /// Parse a <see cref="MessageType.ServerHello"/> payload (everything after the
    /// 4-byte framing header). Returns the negotiated tuple. Used by the C# client
    /// SDK and useful to round-trip in tests.
    /// </summary>
    public static (uint ProtocolVersion, ServerCapabilities Capabilities, string BuildVersion) ReadServerHello(
        ReadOnlySpan<byte> payload)
    {
        uint version = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        var caps = (ServerCapabilities)BinaryPrimitives.ReadUInt32LittleEndian(payload[4..]);
        byte buildLen = payload[8];
        string build = Encoding.UTF8.GetString(payload.Slice(9, buildLen));
        return (version, caps, build);
    }

    /// <summary>
    /// Write a <see cref="MessageType.SymbolDelisted"/> frame: <c>[securityId u64]</c>.
    /// Total: 12 bytes. Sent once per subscriber when a security is delisted.
    /// </summary>
    public static int WriteSymbolDelisted(Span<byte> dest, ulong securityId)
    {
        const ushort totalLen = FramingHeaderSize + 8; // 12
        WriteFramingHeader(dest, totalLen, MessageType.SymbolDelisted);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[4..], securityId);
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
    /// <summary>Raw <c>ImbalanceCondition</c> bitfield from the upstream
    /// <c>AuctionImbalance_19</c> message: bit 8 = ImbalanceMoreBuyers (P),
    /// bit 9 = ImbalanceMoreSellers (Q), all bits off = BALANCED. Widened
    /// to i64 like every other slot. Decoder masks low 16 bits.</summary>
    public const int FieldAuctionImbalanceCondition = 24;

    /// <summary>Max buffer size for InfoSnapshot: header + securityId + mask + 25 fields × 8.</summary>
    public const int InfoSnapshotMaxSize = FramingHeaderSize + 8 + 4 + 25 * 8; // 216

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
        // AuctionImbalanceCondition is a ushort bitfield; widened to i64 so the
        // wire keeps one slot = 8 bytes. Decoder masks low 16 bits.
        if (info.AuctionImbalanceCondition is { } v24) { mask |= 1u << FieldAuctionImbalanceCondition; BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v24); offset += 8; }

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

    // --- News (fragmented, version=1) ---

    /// <summary>Current wire version for News frames. Increment on schema changes
    /// so old clients can detect and reject (or upgrade) frames they don't grok.</summary>
    public const byte NewsFrameVersion = 1;

    /// <summary>Field discriminator carried in each <see cref="MessageType.NewsChunk"/>
    /// to tell the client which logical buffer (Headline / Text / URL) the bytes belong to.</summary>
    public enum NewsField : byte
    {
        Headline = 0,
        Text = 1,
        Url = 2,
    }

    /// <summary>Fixed payload size of a NewsBegin (after framing header).
    /// 1 (version) + 8 (securityId) + 8 (newsId) + 1 (source) + 2 (lang) + 8 (origTime)
    /// + 4 (totalHeadlineLen) + 4 (totalTextLen) + 4 (totalUrlLen) = 40 bytes.</summary>
    public const int NewsBeginPayloadSize = 1 + 8 + 8 + 1 + 2 + 8 + 4 + 4 + 4;

    /// <summary>Total wire size of a NewsBegin (framing + payload).</summary>
    public const int NewsBeginTotalSize = FramingHeaderSize + NewsBeginPayloadSize;

    /// <summary>Maximum bytes allowed in a single NewsChunk fragment payload.
    /// Sized to leave comfortable headroom under the u16 framing length cap
    /// (max ~65 KB) after accounting for chunk header.</summary>
    public const int NewsChunkMaxFragment = 60 * 1024;

    /// <summary>Header overhead of a NewsChunk/NewsEnd payload (after framing).
    /// 1 (version) + 8 (newsId) + 1 (field) + 2 (fragmentLen) = 12 bytes.</summary>
    public const int NewsChunkHeaderSize = 1 + 8 + 1 + 2;

    /// <summary>Total wire size for a NewsChunk/NewsEnd carrying <paramref name="fragmentLen"/> payload bytes.</summary>
    public static int NewsChunkTotalSize(int fragmentLen) => FramingHeaderSize + NewsChunkHeaderSize + fragmentLen;

    /// <summary>
    /// Write a NewsBegin frame. <paramref name="securityIdOrZero"/> = 0 means global news.
    /// Total length: <see cref="NewsBeginTotalSize"/>. Returns bytes written.
    /// </summary>
    public static int WriteNewsBegin(
        Span<byte> dest,
        ulong securityIdOrZero,
        ulong newsId,
        byte source,
        ushort language,
        long origTimeNanos,
        uint totalHeadlineLen,
        uint totalTextLen,
        uint totalUrlLen)
    {
        const ushort totalLen = (ushort)NewsBeginTotalSize;
        WriteFramingHeader(dest, totalLen, MessageType.NewsBegin);
        int o = FramingHeaderSize;
        dest[o++] = NewsFrameVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[o..], securityIdOrZero); o += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[o..], newsId); o += 8;
        dest[o++] = source;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[o..], language); o += 2;
        BinaryPrimitives.WriteInt64LittleEndian(dest[o..], origTimeNanos); o += 8;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[o..], totalHeadlineLen); o += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[o..], totalTextLen); o += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(dest[o..], totalUrlLen); o += 4;
        return totalLen;
    }

    /// <summary>
    /// Write a NewsChunk (intermediate fragment) or NewsEnd (final fragment).
    /// <paramref name="fragment"/> length must be ≤ <see cref="NewsChunkMaxFragment"/>.
    /// Use <paramref name="isFinal"/>=true to emit the terminator.
    /// Returns total bytes written.
    /// </summary>
    public static int WriteNewsChunk(
        Span<byte> dest,
        ulong newsId,
        NewsField field,
        ReadOnlySpan<byte> fragment,
        bool isFinal)
    {
        if (fragment.Length > NewsChunkMaxFragment)
            throw new ArgumentOutOfRangeException(nameof(fragment), $"News fragment exceeds {NewsChunkMaxFragment} bytes");
        ushort totalLen = (ushort)(FramingHeaderSize + NewsChunkHeaderSize + fragment.Length);
        WriteFramingHeader(dest, totalLen, isFinal ? MessageType.NewsEnd : MessageType.NewsChunk);
        int o = FramingHeaderSize;
        dest[o++] = NewsFrameVersion;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[o..], newsId); o += 8;
        dest[o++] = (byte)field;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[o..], (ushort)fragment.Length); o += 2;
        fragment.CopyTo(dest[o..]);
        return totalLen;
    }

    /// <summary>Parse a NewsBegin payload (the bytes after the 4-byte framing header).</summary>
    public static bool TryReadNewsBegin(
        ReadOnlySpan<byte> payload,
        out byte version,
        out ulong securityIdOrZero,
        out ulong newsId,
        out byte source,
        out ushort language,
        out long origTimeNanos,
        out uint totalHeadlineLen,
        out uint totalTextLen,
        out uint totalUrlLen)
    {
        version = 0; securityIdOrZero = 0; newsId = 0; source = 0; language = 0;
        origTimeNanos = 0; totalHeadlineLen = 0; totalTextLen = 0; totalUrlLen = 0;
        if (payload.Length < NewsBeginPayloadSize) return false;
        int o = 0;
        version = payload[o++];
        securityIdOrZero = BinaryPrimitives.ReadUInt64LittleEndian(payload[o..]); o += 8;
        newsId = BinaryPrimitives.ReadUInt64LittleEndian(payload[o..]); o += 8;
        source = payload[o++];
        language = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        origTimeNanos = BinaryPrimitives.ReadInt64LittleEndian(payload[o..]); o += 8;
        totalHeadlineLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]); o += 4;
        totalTextLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]); o += 4;
        totalUrlLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]);
        return true;
    }

    /// <summary>Parse a NewsChunk or NewsEnd payload. Returns the raw fragment as a slice
    /// of the input span — caller must copy if it must outlive the buffer.</summary>
    public static bool TryReadNewsChunk(
        ReadOnlySpan<byte> payload,
        out byte version,
        out ulong newsId,
        out NewsField field,
        out ReadOnlySpan<byte> fragment)
    {
        version = 0; newsId = 0; field = NewsField.Headline; fragment = default;
        if (payload.Length < NewsChunkHeaderSize) return false;
        int o = 0;
        version = payload[o++];
        newsId = BinaryPrimitives.ReadUInt64LittleEndian(payload[o..]); o += 8;
        field = (NewsField)payload[o++];
        ushort fragLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        if (payload.Length < o + fragLen) return false;
        fragment = payload.Slice(o, fragLen);
        return true;
    }

    // --- SecurityDefinition (variable-length, dual-bitmask) ---

    // Numeric field bit positions in SecurityDefinition.numericFieldMask (u32).
    // Each present bit consumes one i64 slot (8 bytes), in bit-ascending order.
    // New numeric fields are append-only — bumping the bit position is a wire
    // break. SDKs MUST consume slots for unknown bits without alignment damage.
    public const int SecurityDefinitionFieldMinPriceIncrement = 0;
    public const int SecurityDefinitionFieldMinTradeVolume = 1;
    public const int SecurityDefinitionFieldPriceDivisor = 2;
    public const int SecurityDefinitionFieldContractMultiplier = 3;
    public const int SecurityDefinitionFieldStrikePrice = 4;
    public const int SecurityDefinitionFieldMaturityDate = 5;
    public const int SecurityDefinitionFieldPutOrCall = 6;
    public const int SecurityDefinitionFieldExerciseStyle = 7;
    public const int SecurityDefinitionFieldSecurityType = 8;
    public const int SecurityDefinitionFieldSecuritySubType = 9;
    public const int SecurityDefinitionFieldProduct = 10;
    public const int SecurityDefinitionFieldMarketSegmentID = 11;
    public const int SecurityDefinitionFieldTickSizeDenominator = 12;

    // String field bit positions in SecurityDefinition.stringFieldMask (u32).
    // Each present bit consumes [u16 len][bytes]; len is the UTF-8 byte length
    // (Latin1 also fits since it's a subset). New string fields are append-only.
    public const int SecurityDefinitionStringIsin = 0;
    public const int SecurityDefinitionStringCurrency = 1;
    public const int SecurityDefinitionStringAsset = 2;
    public const int SecurityDefinitionStringCfiCode = 3;
    public const int SecurityDefinitionStringSecurityGroup = 4;
    public const int SecurityDefinitionStringSecurityDescription = 5;

    /// <summary>
    /// Conservative upper bound for a SecurityDefinition frame. Symbol up to 32 bytes,
    /// 13 numeric slots × 8 bytes, 6 string slots × (2-byte len + up to ~512 bytes for
    /// SecurityDescription, smaller for the rest). Stays comfortably below the u16
    /// framing-length ceiling.
    /// </summary>
    public const int SecurityDefinitionMaxSize =
        FramingHeaderSize       // 4
        + 8                     // securityId
        + 1 + 32                // symbol len + bytes
        + 4                     // numeric mask
        + 13 * 8                // numeric slots
        + 4                     // string mask
        + 6 * (2 + 512);        // string slots (worst case: each up to 512 bytes)

    /// <summary>
    /// Write a <see cref="MessageType.SecurityDefinition"/> frame for one security.
    /// <paramref name="info"/> is the cached <c>InstrumentInfo</c> populated by
    /// <c>MarketDataManager.HandleSecurityDefinition</c>. Strings are encoded as
    /// UTF-8; numeric fields are written as i64 (widened from native types) in
    /// ascending bit order. Returns total bytes written. Caller is expected to
    /// provide a buffer of at least <see cref="SecurityDefinitionMaxSize"/> bytes.
    /// </summary>
    public static int WriteSecurityDefinition(Span<byte> dest, ulong securityId, InstrumentInfo info)
    {
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;

        // Symbol — always present (single-byte length prefix; SBE Symbol is at most 20 bytes).
        string symbol = info.Symbol ?? string.Empty;
        var symBytes = Encoding.UTF8.GetBytes(symbol);
        if (symBytes.Length > 255)
        {
            // Defensive: B3 Symbol field is bounded but clamp anyway.
            var trimmed = new byte[255];
            Array.Copy(symBytes, trimmed, 255);
            symBytes = trimmed;
        }
        dest[offset++] = (byte)symBytes.Length;
        symBytes.CopyTo(dest[offset..]); offset += symBytes.Length;

        // Numeric mask + slots (mask placeholder filled after we know which bits are set).
        int numericMaskOffset = offset;
        offset += 4;
        uint numericMask = 0;

        if (info.MinPriceIncrement is { } v0)
        { numericMask |= 1u << SecurityDefinitionFieldMinPriceIncrement;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v0); offset += 8; }
        if (info.MinTradeVolume is { } v1)
        { numericMask |= 1u << SecurityDefinitionFieldMinTradeVolume;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v1); offset += 8; }
        if (info.PriceDivisor is { } v2)
        { numericMask |= 1u << SecurityDefinitionFieldPriceDivisor;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v2); offset += 8; }
        if (info.ContractMultiplier is { } v3)
        { numericMask |= 1u << SecurityDefinitionFieldContractMultiplier;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v3); offset += 8; }
        if (info.StrikePrice is { } v4)
        { numericMask |= 1u << SecurityDefinitionFieldStrikePrice;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v4); offset += 8; }
        if (info.MaturityDate is { } v5)
        { numericMask |= 1u << SecurityDefinitionFieldMaturityDate;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v5); offset += 8; }
        if (info.PutOrCall is { } v6)
        { numericMask |= 1u << SecurityDefinitionFieldPutOrCall;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v6); offset += 8; }
        if (info.ExerciseStyle is { } v7)
        { numericMask |= 1u << SecurityDefinitionFieldExerciseStyle;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v7); offset += 8; }
        if (info.SecurityType is { } v8)
        { numericMask |= 1u << SecurityDefinitionFieldSecurityType;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v8); offset += 8; }
        if (info.SecuritySubType is { } v9)
        { numericMask |= 1u << SecurityDefinitionFieldSecuritySubType;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v9); offset += 8; }
        if (info.Product is { } v10)
        { numericMask |= 1u << SecurityDefinitionFieldProduct;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v10); offset += 8; }
        if (info.MarketSegmentID is { } v11)
        { numericMask |= 1u << SecurityDefinitionFieldMarketSegmentID;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v11); offset += 8; }
        if (info.TickSizeDenominator is { } v12)
        { numericMask |= 1u << SecurityDefinitionFieldTickSizeDenominator;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v12); offset += 8; }

        BinaryPrimitives.WriteUInt32LittleEndian(dest[numericMaskOffset..], numericMask);

        // String mask + slots.
        int stringMaskOffset = offset;
        offset += 4;
        uint stringMask = 0;

        offset = WriteOptionalString(dest, offset, info.IsinNumber, SecurityDefinitionStringIsin, ref stringMask);
        offset = WriteOptionalString(dest, offset, info.Currency, SecurityDefinitionStringCurrency, ref stringMask);
        offset = WriteOptionalString(dest, offset, info.Asset, SecurityDefinitionStringAsset, ref stringMask);
        offset = WriteOptionalString(dest, offset, info.CfiCode, SecurityDefinitionStringCfiCode, ref stringMask);
        offset = WriteOptionalString(dest, offset, info.SecurityGroup, SecurityDefinitionStringSecurityGroup, ref stringMask);
        offset = WriteOptionalString(dest, offset, info.SecurityDescription, SecurityDefinitionStringSecurityDescription, ref stringMask);

        BinaryPrimitives.WriteUInt32LittleEndian(dest[stringMaskOffset..], stringMask);

        ushort totalLen = (ushort)offset;
        WriteFramingHeader(dest, totalLen, MessageType.SecurityDefinition);
        return totalLen;
    }

    private static int WriteOptionalString(Span<byte> dest, int offset, string? value, int bit, ref uint mask)
    {
        if (string.IsNullOrEmpty(value)) return offset;
        var bytes = Encoding.UTF8.GetBytes(value);
        if (bytes.Length > ushort.MaxValue)
        {
            // Defensive: SecurityDescription is bounded by the SBE schema; clamp anyway.
            var trimmed = new byte[ushort.MaxValue];
            Array.Copy(bytes, trimmed, ushort.MaxValue);
            bytes = trimmed;
        }
        mask |= 1u << bit;
        BinaryPrimitives.WriteUInt16LittleEndian(dest[offset..], (ushort)bytes.Length);
        offset += 2;
        bytes.CopyTo(dest[offset..]);
        offset += bytes.Length;
        return offset;
    }

    // PriceBand field-mask bit positions. Mirrored verbatim by the SDK's
    // WireFormat.PriceBand* constants — never reorder. New fields are
    // append-only at new bit positions; older SDKs MUST consume slots for
    // unknown bits without surfacing them.
    public const int PriceBandFieldLowerBand = 0;            // i64 (Price, /1e4)
    public const int PriceBandFieldUpperBand = 1;            // i64 (Price, /1e4)
    public const int PriceBandFieldTradingReferencePrice = 2;// i64 (Fixed8, /1e8)
    public const int PriceBandFieldPriceLimitType = 3;       // i64 (byte enum)
    public const int PriceBandFieldPriceBandType = 4;        // i64 (byte enum)
    public const int PriceBandFieldPriceBandMidpointPriceType = 5; // i64 (byte enum)
    public const int PriceBandFieldAsOfTimestampNanos = 6;   // i64 (UTC nanos)
    public const int PriceBandFieldRptSeq = 7;               // i64 (widened uint)

    /// <summary>
    /// Maximum body size of a <see cref="MessageType.PriceBand"/> frame:
    /// header(4) + secId(8) + symLen(1) + symbol(≤255) + fieldMask(4) + 8 numeric slots × 8.
    /// </summary>
    public const int PriceBandMaxSize =
        FramingHeaderSize + 8 + 1 + 255 + 4 + 8 * 8;

    /// <summary>
    /// Serialize a <c>PriceBand</c> frame. <paramref name="dest"/> must
    /// provide a buffer of at least <see cref="PriceBandMaxSize"/> bytes.
    /// </summary>
    public static int WritePriceBand(Span<byte> dest, ulong securityId, InstrumentInfo info)
    {
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;

        // Symbol — always present (single-byte length prefix; SBE Symbol is at most 20 bytes).
        string symbol = info.Symbol ?? string.Empty;
        var symBytes = Encoding.UTF8.GetBytes(symbol);
        if (symBytes.Length > 255)
        {
            var trimmed = new byte[255];
            Array.Copy(symBytes, trimmed, 255);
            symBytes = trimmed;
        }
        dest[offset++] = (byte)symBytes.Length;
        symBytes.CopyTo(dest[offset..]); offset += symBytes.Length;

        // Field mask + slots — placeholder filled after we know which bits are set.
        int maskOffset = offset;
        offset += 4;
        uint mask = 0;

        if (info.PriceBandLow is { } v0)
        { mask |= 1u << PriceBandFieldLowerBand;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v0); offset += 8; }
        if (info.PriceBandHigh is { } v1)
        { mask |= 1u << PriceBandFieldUpperBand;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v1); offset += 8; }
        if (info.TradingReferencePrice is { } v2)
        { mask |= 1u << PriceBandFieldTradingReferencePrice;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v2); offset += 8; }
        if (info.PriceLimitType is { } v3)
        { mask |= 1u << PriceBandFieldPriceLimitType;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v3); offset += 8; }
        if (info.PriceBandType is { } v4)
        { mask |= 1u << PriceBandFieldPriceBandType;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v4); offset += 8; }
        if (info.PriceBandMidpointPriceType is { } v5)
        { mask |= 1u << PriceBandFieldPriceBandMidpointPriceType;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v5); offset += 8; }
        if (info.PriceBandTimestamp is { } v6)
        { mask |= 1u << PriceBandFieldAsOfTimestampNanos;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v6); offset += 8; }
        if (info.LastRptSeqPriceBand != 0)
        { mask |= 1u << PriceBandFieldRptSeq;
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], info.LastRptSeqPriceBand); offset += 8; }

        BinaryPrimitives.WriteUInt32LittleEndian(dest[maskOffset..], mask);

        ushort totalLen = (ushort)offset;
        WriteFramingHeader(dest, totalLen, MessageType.PriceBand);
        return totalLen;
    }

    // ───────────────────────────────────────────────────────────────────────
    //  Auction frame (0x00B2) — imbalance + group phase
    // ───────────────────────────────────────────────────────────────────────

    public const int AuctionFieldImbalanceQty = 0;           // i64
    public const int AuctionFieldImbalanceCondition = 1;     // i64 (ushort widened)
    public const int AuctionFieldTradingStatus = 2;          // i64 (int widened)
    public const int AuctionFieldTradSesOpenTime = 3;        // i64 (UTC nanos)
    public const int AuctionFieldAsOfTimestampNanos = 4;     // i64 (UTC nanos)
    public const int AuctionFieldRptSeq = 5;                 // i64 (widened uint)

    /// <summary>
    /// Maximum body size of a <see cref="MessageType.Auction"/> frame:
    /// header(4) + secId(8) + symLen(1) + symbol(≤255) + fieldMask(1) + 6 numeric slots × 8.
    /// </summary>
    public const int AuctionMaxSize =
        FramingHeaderSize + 8 + 1 + 255 + 1 + 6 * 8;

    /// <summary>
    /// Serialize an <see cref="MessageType.Auction"/> frame.
    /// <paramref name="dest"/> must provide at least <see cref="AuctionMaxSize"/> bytes.
    /// </summary>
    public static int WriteAuction(Span<byte> dest, ulong securityId, InstrumentInfo info)
    {
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;

        // Symbol — single-byte length prefix
        string symbol = info.Symbol ?? string.Empty;
        var symBytes = Encoding.UTF8.GetBytes(symbol);
        if (symBytes.Length > 255)
        {
            var trimmed = new byte[255];
            Array.Copy(symBytes, trimmed, 255);
            symBytes = trimmed;
        }
        dest[offset++] = (byte)symBytes.Length;
        symBytes.CopyTo(dest[offset..]); offset += symBytes.Length;

        // Field mask (1 byte) + slots
        int maskOffset = offset;
        offset += 1;
        byte mask = 0;

        if (info.AuctionImbalanceSize is { } v0)
        { mask |= (byte)(1 << AuctionFieldImbalanceQty);
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v0); offset += 8; }
        if (info.AuctionImbalanceCondition is { } v1)
        { mask |= (byte)(1 << AuctionFieldImbalanceCondition);
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v1); offset += 8; }
        if (info.TradingStatus is { } v2)
        { mask |= (byte)(1 << AuctionFieldTradingStatus);
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v2); offset += 8; }
        if (info.TradSesOpenTime is { } v3)
        { mask |= (byte)(1 << AuctionFieldTradSesOpenTime);
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], (long)v3); offset += 8; }
        if (info.AuctionTimestamp is { } v4)
        { mask |= (byte)(1 << AuctionFieldAsOfTimestampNanos);
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], v4); offset += 8; }
        if (info.LastRptSeqAuctionImbalance != 0)
        { mask |= (byte)(1 << AuctionFieldRptSeq);
          BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], info.LastRptSeqAuctionImbalance); offset += 8; }

        dest[maskOffset] = mask;

        ushort totalLen = (ushort)offset;
        WriteFramingHeader(dest, totalLen, MessageType.Auction);
        return totalLen;
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
