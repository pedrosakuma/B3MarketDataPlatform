using System.Buffers.Binary;
using System.Text;

namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Binary wire format for the B3MarketDataPlatform WebSocket protocol.
/// Mirrors a minimal subset of the server's <c>WireProtocol</c> + the
/// <c>MessageType</c> enum, just for the v1 SDK surface.
/// Layout: <c>[length u16][type u16][payload]</c>, little-endian; length
/// includes the 4-byte header.
/// </summary>
internal static class WireFormat
{
    public const int FramingHeaderSize = 4;

    /// <summary>SBE schema's <c>Price</c>/<c>PriceOptional</c> exponent (1e-4).</summary>
    public const decimal PriceScale = 10_000m;

    /// <summary>SBE schema's <c>Fixed8</c> exponent (1e-8) — used by
    /// <c>SecurityDefinition_12.MinPriceIncrement</c>.</summary>
    public const decimal PriceScale8 = 100_000_000m;

    public enum MessageType : ushort
    {
        Subscribe = 0x0001,
        Unsubscribe = 0x0002,
        SubscribeOk = 0x0010,
        SubscribeError = 0x0011,
        Unsubscribed = 0x0012,
        BookSnapshot = 0x0020,
        InfoSnapshot = 0x0021,
        LevelSnapshot = 0x0022,
        OrderAdded = 0x0030,
        OrderUpdated = 0x0031,
        OrderDeleted = 0x0032,
        Trade = 0x0033,
        BookCleared = 0x0034,
        TradeBust = 0x0035,
        MarketTierUpdate = 0x0036,
        LevelUpdate = 0x0037,
        LevelDeleted = 0x0038,
        RankingsUpdate = 0x0040,
        ServerStatus = 0x0050,
        CandleSnapshot = 0x0060,
        CandleUpdate = 0x0061,
        SymbolStaleStatus = 0x0070,
        SymbolDelisted = 0x0071,
        RecoveryProgress = 0x0080,
        NewsBegin = 0x0090,
        NewsChunk = 0x0091,
        NewsEnd = 0x0092,
        ServerHello = 0x00A0,
        ClientHello = 0x00A1,
        SecurityDefinition = 0x00B0,
    }

    // InfoSnapshot field-mask bit positions. Must match the server's
    // WireProtocol field constants (Field*). Only the bits the SDK
    // surfaces are listed here; unknown bits are ignored at decode time
    // (their 8 bytes are still consumed in mask-bit order).
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
    /// <summary>Raw <c>ImbalanceCondition</c> bitfield from upstream
    /// <c>AuctionImbalance_19</c>. Surfaced as
    /// <see cref="InfoSnapshotEvent.AuctionImbalanceCondition"/>.</summary>
    public const int FieldAuctionImbalanceCondition = 24;

    // SecurityDefinition numeric field-mask bit positions. Must match the
    // server's WireProtocol.SecurityDefinitionField* constants. New fields
    // are append-only at new bit positions; older SDKs MUST consume slots
    // for unknown bits without surfacing them (the decoder walks the mask
    // in bit order and skips 8 bytes per set bit it does not recognise).
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

    // SecurityDefinition string-mask bit positions (mirrors server).
    public const int SecurityDefinitionStringIsin = 0;
    public const int SecurityDefinitionStringCurrency = 1;
    public const int SecurityDefinitionStringAsset = 2;
    public const int SecurityDefinitionStringCfiCode = 3;
    public const int SecurityDefinitionStringSecurityGroup = 4;
    public const int SecurityDefinitionStringSecurityDescription = 5;

    public static bool TryReadHeader(ReadOnlySpan<byte> src, out ushort length, out MessageType type)
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

    /// <summary>
    /// Wire-protocol version this SDK speaks. Sent in <see cref="MessageType.ClientHello"/>
    /// on every (re)connect; servers that do not understand the version close the
    /// connection with WS code 1003.
    /// </summary>
    public const uint ProtocolVersion = 1;

    /// <summary>Encode a <c>ClientHello</c> frame: <c>[u32 protocolVersion]</c>.</summary>
    public static int WriteClientHello(Span<byte> dest, uint protocolVersion)
    {
        const ushort totalLen = FramingHeaderSize + 4;
        BinaryPrimitives.WriteUInt16LittleEndian(dest, totalLen);
        BinaryPrimitives.WriteUInt16LittleEndian(dest[2..], (ushort)MessageType.ClientHello);
        BinaryPrimitives.WriteUInt32LittleEndian(dest[FramingHeaderSize..], protocolVersion);
        return totalLen;
    }

    /// <summary>Encode a <c>Subscribe</c> frame: <c>[flags u8][symLen u8][symbol UTF-8…]</c>.</summary>
    public static int WriteSubscribe(Span<byte> dest, SubscribeFlags flags, string symbol)
    {
        int symbolLen = Encoding.UTF8.GetBytes(symbol, dest[(FramingHeaderSize + 2)..]);
        ushort totalLen = (ushort)(FramingHeaderSize + 1 + 1 + symbolLen);
        BinaryPrimitives.WriteUInt16LittleEndian(dest, totalLen);
        BinaryPrimitives.WriteUInt16LittleEndian(dest[2..], (ushort)MessageType.Subscribe);
        dest[FramingHeaderSize] = (byte)flags;
        dest[FramingHeaderSize + 1] = (byte)symbolLen;
        return totalLen;
    }

    /// <summary>Encode an <c>Unsubscribe</c> frame: <c>[securityId u64]</c>.</summary>
    public static int WriteUnsubscribe(Span<byte> dest, ulong securityId)
    {
        const ushort totalLen = FramingHeaderSize + 8;
        BinaryPrimitives.WriteUInt16LittleEndian(dest, totalLen);
        BinaryPrimitives.WriteUInt16LittleEndian(dest[2..], (ushort)MessageType.Unsubscribe);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[FramingHeaderSize..], securityId);
        return totalLen;
    }

    public static (ulong SecurityId, byte Flags, string Symbol) ReadSubscribeOk(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        byte flags = payload[8];
        byte symLen = payload[9];
        string sym = Encoding.UTF8.GetString(payload.Slice(10, symLen));
        return (secId, flags, sym);
    }

    public static (string Symbol, byte ErrorCode) ReadSubscribeError(ReadOnlySpan<byte> payload)
    {
        byte errorCode = payload[0];
        byte symLen = payload[1];
        string sym = Encoding.UTF8.GetString(payload.Slice(2, symLen));
        return (sym, errorCode);
    }

    public static bool ReadServerStatus(ReadOnlySpan<byte> payload) => payload[0] != 0;

    /// <summary>
    /// Decode a <c>ServerHello</c> payload (everything after the 4-byte framing
    /// header). Layout: <c>[u32 protocolVersion][u32 capabilities][u8 buildVerLen][buildVer UTF-8…]</c>.
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

    /// <summary>Decode a <c>SymbolDelisted</c> payload: <c>[securityId u64]</c>.</summary>
    public static ulong ReadSymbolDelisted(ReadOnlySpan<byte> payload)
        => BinaryPrimitives.ReadUInt64LittleEndian(payload);

    /// <summary>
    /// Decode a Trade payload. Layout: <c>[secId u64][price i64][qty i64][tradeId i64][flags u8?]</c>.
    /// The trailing flags byte was added in a later server build; when the
    /// payload is the legacy 32 bytes (no flags), <see cref="TradeFlags.None"/>
    /// is returned.
    /// </summary>
    public static (ulong SecurityId, long Price, long Qty, long TradeId, TradeFlags Flags) ReadTrade(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        long price = BinaryPrimitives.ReadInt64LittleEndian(payload[8..]);
        long qty = BinaryPrimitives.ReadInt64LittleEndian(payload[16..]);
        long tradeId = BinaryPrimitives.ReadInt64LittleEndian(payload[24..]);
        var flags = payload.Length >= 33 ? (TradeFlags)payload[32] : TradeFlags.None;
        return (secId, price, qty, tradeId, flags);
    }

    public static (ulong SecurityId, long TradeId) ReadTradeBust(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        long tradeId = BinaryPrimitives.ReadInt64LittleEndian(payload[8..]);
        return (secId, tradeId);
    }

    /// <summary>
    /// Decode an <c>InfoSnapshot</c> body into a populated event.
    /// Only fields whose bit is set in <c>fieldMask</c> are present in
    /// the payload, in bit order, as <c>i64</c>. Unknown bits (above
    /// <see cref="FieldMinPriceIncrement"/>) are still consumed so the
    /// SDK keeps reading future fields without alignment damage.
    /// </summary>
    public static InfoSnapshotEvent ReadInfoSnapshot(ReadOnlySpan<byte> payload, string symbol, DateTime receivedUtc)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        uint mask = BinaryPrimitives.ReadUInt32LittleEndian(payload[8..]);

        decimal? opening = null, closing = null, high = null, low = null;
        decimal? lastTradePrice = null;
        long? lastTradeSize = null;
        decimal? settlement = null, vwap = null;
        long? trades = null, openInterest = null;
        decimal? bandLow = null, bandHigh = null, refPx = null;
        long? volume = null, status = null, evt = null;
        decimal? theoreticalOpeningPrice = null;
        long? theoreticalOpeningSize = null;
        long? auctionImbalanceSize = null;
        AuctionImbalanceCondition? auctionImbalanceCondition = null;

        int offset = 12;
        for (int bit = 0; bit < 32; bit++)
        {
            if ((mask & (1u << bit)) == 0) continue;
            long v = BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]);
            offset += 8;
            switch (bit)
            {
                case FieldOpeningPrice: opening = v / PriceScale; break;
                case FieldClosingPrice: closing = v / PriceScale; break;
                case FieldHighPrice: high = v / PriceScale; break;
                case FieldLowPrice: low = v / PriceScale; break;
                case FieldLastTradePrice: lastTradePrice = v / PriceScale; break;
                case FieldLastTradeSize: lastTradeSize = v; break;
                case FieldSettlementPrice: settlement = v / PriceScale; break;
                case FieldTheoreticalOpeningPrice: theoreticalOpeningPrice = v / PriceScale; break;
                case FieldTheoreticalOpeningSize: theoreticalOpeningSize = v; break;
                case FieldAuctionImbalanceSize: auctionImbalanceSize = v; break;
                case FieldVwapPrice: vwap = v / PriceScale; break;
                case FieldNumberOfTrades: trades = v; break;
                case FieldOpenInterest: openInterest = v; break;
                case FieldPriceBandLow: bandLow = v / PriceScale; break;
                case FieldPriceBandHigh: bandHigh = v / PriceScale; break;
                case FieldTradingReferencePrice: refPx = v / PriceScale; break;
                case FieldTradeVolume: volume = v; break;
                case FieldTradingStatus: status = v; break;
                case FieldTradingEvent: evt = v; break;
                case FieldAuctionImbalanceCondition:
                    // SBE ImbalanceCondition bits: 0x0100 = MoreBuyers,
                    // 0x0200 = MoreSellers. Decoder masks low 16 bits and
                    // translates to the clean SDK enum.
                    auctionImbalanceCondition = DecodeImbalanceCondition((ushort)v);
                    break;
                // Other bits (NetChange, AvgDailyTradedQty, MaxTradeVol,
                // PriceLimitType, MinPriceIncrement) are consumed but not
                // surfaced in the v1 typed event.
            }
        }

        return new InfoSnapshotEvent
        {
            SecurityId = secId,
            Symbol = symbol,
            ReceivedUtc = receivedUtc,
            OpeningPrice = opening,
            ClosingPrice = closing,
            HighPrice = high,
            LowPrice = low,
            LastTradePrice = lastTradePrice,
            LastTradeSize = lastTradeSize,
            SettlementPrice = settlement,
            VwapPrice = vwap,
            NumberOfTrades = trades,
            OpenInterest = openInterest,
            PriceBandLow = bandLow,
            PriceBandHigh = bandHigh,
            TradingReferencePrice = refPx,
            TradeVolume = volume,
            TradingStatus = status,
            TradingEvent = evt,
            TheoreticalOpeningPrice = theoreticalOpeningPrice,
            TheoreticalOpeningSize = theoreticalOpeningSize,
            AuctionImbalanceSize = auctionImbalanceSize,
            AuctionImbalanceCondition = auctionImbalanceCondition,
        };
    }

    /// <summary>
    /// Decode a <see cref="MessageType.SecurityDefinition"/> body. Wire layout
    /// is: <c>[u64 secId][u8 symLen][symbol UTF-8][u32 numericMask][i64 slots…]
    /// [u32 stringMask][per set bit: u16 len][bytes UTF-8]</c>. Both masks are
    /// walked in bit order; unknown numeric bits consume 8 bytes, unknown
    /// string bits consume a length-prefixed slot — that's how the format
    /// stays append-only forward-compatible.
    /// </summary>
    public static SecurityDefinitionEvent ReadSecurityDefinition(ReadOnlySpan<byte> payload, DateTime receivedUtc)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        int offset = 8;

        int symLen = payload[offset++];
        string symbol = symLen == 0
            ? string.Empty
            : Encoding.UTF8.GetString(payload.Slice(offset, symLen));
        offset += symLen;

        uint numericMask = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
        offset += 4;

        decimal? minPriceIncrement = null;
        long? minTradeVolume = null;
        long? priceDivisor = null;
        long? contractMultiplier = null;
        long? strikePrice = null;
        long? maturityDate = null;
        long? putOrCall = null;
        long? exerciseStyle = null;
        long? securityType = null;
        long? securitySubType = null;
        long? product = null;
        long? marketSegmentID = null;
        long? tickSizeDenominator = null;

        for (int bit = 0; bit < 32; bit++)
        {
            if ((numericMask & (1u << bit)) == 0) continue;
            long v = BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]);
            offset += 8;
            switch (bit)
            {
                case SecurityDefinitionFieldMinPriceIncrement:
                    minPriceIncrement = v / PriceScale8;
                    break;
                case SecurityDefinitionFieldMinTradeVolume: minTradeVolume = v; break;
                case SecurityDefinitionFieldPriceDivisor: priceDivisor = v; break;
                case SecurityDefinitionFieldContractMultiplier: contractMultiplier = v; break;
                case SecurityDefinitionFieldStrikePrice: strikePrice = v; break;
                case SecurityDefinitionFieldMaturityDate: maturityDate = v; break;
                case SecurityDefinitionFieldPutOrCall: putOrCall = v; break;
                case SecurityDefinitionFieldExerciseStyle: exerciseStyle = v; break;
                case SecurityDefinitionFieldSecurityType: securityType = v; break;
                case SecurityDefinitionFieldSecuritySubType: securitySubType = v; break;
                case SecurityDefinitionFieldProduct: product = v; break;
                case SecurityDefinitionFieldMarketSegmentID: marketSegmentID = v; break;
                case SecurityDefinitionFieldTickSizeDenominator: tickSizeDenominator = v; break;
            }
        }

        uint stringMask = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
        offset += 4;

        string? isin = null, currency = null, asset = null, cfiCode = null;
        string? securityGroup = null, securityDescription = null;

        for (int bit = 0; bit < 32; bit++)
        {
            if ((stringMask & (1u << bit)) == 0) continue;
            ushort len = BinaryPrimitives.ReadUInt16LittleEndian(payload[offset..]);
            offset += 2;
            string s = len == 0 ? string.Empty : Encoding.UTF8.GetString(payload.Slice(offset, len));
            offset += len;
            switch (bit)
            {
                case SecurityDefinitionStringIsin: isin = s; break;
                case SecurityDefinitionStringCurrency: currency = s; break;
                case SecurityDefinitionStringAsset: asset = s; break;
                case SecurityDefinitionStringCfiCode: cfiCode = s; break;
                case SecurityDefinitionStringSecurityGroup: securityGroup = s; break;
                case SecurityDefinitionStringSecurityDescription: securityDescription = s; break;
            }
        }

        return new SecurityDefinitionEvent
        {
            SecurityId = secId,
            Symbol = symbol,
            ReceivedUtc = receivedUtc,
            MinPriceIncrement = minPriceIncrement,
            MinTradeVolume = minTradeVolume,
            PriceDivisor = priceDivisor,
            ContractMultiplier = contractMultiplier,
            StrikePrice = strikePrice,
            MaturityDate = maturityDate,
            PutOrCall = putOrCall,
            ExerciseStyle = exerciseStyle,
            SecurityType = securityType,
            SecuritySubType = securitySubType,
            Product = product,
            MarketSegmentID = marketSegmentID,
            TickSizeDenominator = tickSizeDenominator,
            IsinNumber = isin,
            Currency = currency,
            Asset = asset,
            CfiCode = cfiCode,
            SecurityGroup = securityGroup,
            SecurityDescription = securityDescription,
        };
    }

    // SBE ImbalanceCondition bit positions (uint16):
    //   bit 8 (0x0100) = ImbalanceMoreBuyers
    //   bit 9 (0x0200) = ImbalanceMoreSellers
    private const ushort ImbalanceBitMoreBuyers = 0x0100;
    private const ushort ImbalanceBitMoreSellers = 0x0200;

    private static AuctionImbalanceCondition DecodeImbalanceCondition(ushort raw)
    {
        bool buyers = (raw & ImbalanceBitMoreBuyers) != 0;
        bool sellers = (raw & ImbalanceBitMoreSellers) != 0;
        if (buyers && sellers) return WebSocketClient.AuctionImbalanceCondition.Unknown;
        if (buyers) return WebSocketClient.AuctionImbalanceCondition.MoreBuyers;
        if (sellers) return WebSocketClient.AuctionImbalanceCondition.MoreSellers;
        return WebSocketClient.AuctionImbalanceCondition.Balanced;
    }

    // ── MBO / order events ──────────────────────────────────────────

    /// <summary>Read OrderAdded/OrderUpdated payload (33 bytes after framing).</summary>
    public static (ulong SecurityId, ulong OrderId, byte Side, long Price, long Qty) ReadOrderEvent(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        ulong orderId = BinaryPrimitives.ReadUInt64LittleEndian(payload[8..]);
        byte side = payload[16];
        long price = BinaryPrimitives.ReadInt64LittleEndian(payload[17..]);
        long qty = BinaryPrimitives.ReadInt64LittleEndian(payload[25..]);
        return (secId, orderId, side, price, qty);
    }

    /// <summary>Read OrderDeleted payload (17 bytes after framing).</summary>
    public static (ulong SecurityId, ulong OrderId, byte Side) ReadOrderDeleted(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        ulong orderId = BinaryPrimitives.ReadUInt64LittleEndian(payload[8..]);
        byte side = payload[16];
        return (secId, orderId, side);
    }

    /// <summary>Read BookCleared payload (9 bytes after framing).</summary>
    public static (ulong SecurityId, byte ClearSide) ReadBookCleared(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        byte clearSide = payload[8];
        return (secId, clearSide);
    }

    /// <summary>Read MarketTierUpdate payload (21 bytes after framing).</summary>
    public static (ulong SecurityId, byte Side, long TotalQty, int OrderCount) ReadMarketTierUpdate(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        byte side = payload[8];
        long totalQty = BinaryPrimitives.ReadInt64LittleEndian(payload[9..]);
        int orderCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(payload[17..]);
        return (secId, side, totalQty, orderCount);
    }

    /// <summary>
    /// Decode BookSnapshot into a populated event. Layout (after framing):
    /// <c>[u64 secId][u32 rptSeq][u16 bidCount][u16 askCount][orders…]</c>.
    /// Each order = <c>[u64 orderId][u64 price][u16 qty]</c> wait — server
    /// writes <c>[i64 price][i64 totalQty][u16 orderCount]</c> for aggregated
    /// snapshot per the server's <c>WritePriceLevel</c>; we re-export it as
    /// per-order surface here for MBO consumers.
    /// </summary>
    /// <remarks>BookSnapshot on the wire today aggregates by price level
    /// (price, totalQty, orderCount) — same shape as <see cref="MessageType.LevelSnapshot"/>.
    /// We expose it via <see cref="BookSnapshotEvent"/> with one entry per
    /// price level (OrderId is set to 0 since the server does not include
    /// individual order ids in BookSnapshot).</remarks>
    public static BookSnapshotEvent ReadBookSnapshot(ReadOnlySpan<byte> payload, string symbol, DateTime receivedUtc)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        uint rptSeq = BinaryPrimitives.ReadUInt32LittleEndian(payload[8..]);
        ushort bidCount = BinaryPrimitives.ReadUInt16LittleEndian(payload[12..]);
        ushort askCount = BinaryPrimitives.ReadUInt16LittleEndian(payload[14..]);
        int offset = 16;

        var bids = bidCount == 0 ? Array.Empty<BookSnapshotOrder>() : new BookSnapshotOrder[bidCount];
        for (int i = 0; i < bidCount; i++)
        {
            long price = BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]);
            long qty = BinaryPrimitives.ReadInt64LittleEndian(payload[(offset + 8)..]);
            // orderCount (u16) lives in payload[offset+16..offset+18]; aggregated
            // snapshot — surface as a synthetic per-level entry (OrderId = 0).
            offset += 18;
            bids[i] = new BookSnapshotOrder(0UL, price / PriceScale, qty);
        }

        var asks = askCount == 0 ? Array.Empty<BookSnapshotOrder>() : new BookSnapshotOrder[askCount];
        for (int i = 0; i < askCount; i++)
        {
            long price = BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]);
            long qty = BinaryPrimitives.ReadInt64LittleEndian(payload[(offset + 8)..]);
            offset += 18;
            asks[i] = new BookSnapshotOrder(0UL, price / PriceScale, qty);
        }

        return new BookSnapshotEvent
        {
            SecurityId = secId,
            Symbol = symbol,
            RptSeq = rptSeq,
            Bids = bids,
            Asks = asks,
            ReceivedUtc = receivedUtc,
        };
    }

    // ── MBP / level events ──────────────────────────────────────────

    /// <summary>Read LevelUpdate payload (29 bytes after framing).</summary>
    public static (ulong SecurityId, byte Side, long Price, long TotalQty, int OrderCount) ReadLevelUpdate(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        byte side = payload[8];
        long price = BinaryPrimitives.ReadInt64LittleEndian(payload[9..]);
        long totalQty = BinaryPrimitives.ReadInt64LittleEndian(payload[17..]);
        int orderCount = (int)BinaryPrimitives.ReadUInt32LittleEndian(payload[25..]);
        return (secId, side, price, totalQty, orderCount);
    }

    /// <summary>Read LevelDeleted payload (17 bytes after framing).</summary>
    public static (ulong SecurityId, byte Side, long Price) ReadLevelDeleted(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        byte side = payload[8];
        long price = BinaryPrimitives.ReadInt64LittleEndian(payload[9..]);
        return (secId, side, price);
    }

    /// <summary>
    /// Decode LevelSnapshot. Layout: <c>[u64 secId][u16 bidCount][u16 askCount]</c>
    /// + N + M entries × <c>[i64 price][i64 totalQty][u32 orderCount]</c> (20 B each).
    /// </summary>
    public static LevelSnapshotEvent ReadLevelSnapshot(ReadOnlySpan<byte> payload, string symbol, DateTime receivedUtc)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        ushort bidCount = BinaryPrimitives.ReadUInt16LittleEndian(payload[8..]);
        ushort askCount = BinaryPrimitives.ReadUInt16LittleEndian(payload[10..]);
        int offset = 12;

        var bids = bidCount == 0 ? Array.Empty<PriceLevel>() : new PriceLevel[bidCount];
        for (int i = 0; i < bidCount; i++)
        {
            long price = BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]);
            long totalQty = BinaryPrimitives.ReadInt64LittleEndian(payload[(offset + 8)..]);
            uint orderCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(offset + 16)..]);
            offset += 20;
            bids[i] = new PriceLevel(price / PriceScale, totalQty, (int)orderCount);
        }

        var asks = askCount == 0 ? Array.Empty<PriceLevel>() : new PriceLevel[askCount];
        for (int i = 0; i < askCount; i++)
        {
            long price = BinaryPrimitives.ReadInt64LittleEndian(payload[offset..]);
            long totalQty = BinaryPrimitives.ReadInt64LittleEndian(payload[(offset + 8)..]);
            uint orderCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(offset + 16)..]);
            offset += 20;
            asks[i] = new PriceLevel(price / PriceScale, totalQty, (int)orderCount);
        }

        return new LevelSnapshotEvent
        {
            SecurityId = secId,
            Symbol = symbol,
            Bids = bids,
            Asks = asks,
            ReceivedUtc = receivedUtc,
        };
    }

    // ── Stale + recovery ────────────────────────────────────────────

    /// <summary>Read SymbolStaleStatus payload (9 bytes after framing).</summary>
    public static (ulong SecurityId, bool IsStale) ReadSymbolStaleStatus(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        bool isStale = payload[8] != 0;
        return (secId, isStale);
    }

    /// <summary>Read RecoveryProgress. Layout: <c>[u32 totalSymbols][u32 totalStale][u8 kindCount]
    /// + kindCount × ([u8 kindId][u32 count])</c>.</summary>
    public static RecoveryProgressEvent ReadRecoveryProgress(ReadOnlySpan<byte> payload, DateTime receivedUtc)
    {
        uint total = BinaryPrimitives.ReadUInt32LittleEndian(payload);
        uint totalStale = BinaryPrimitives.ReadUInt32LittleEndian(payload[4..]);
        byte kindCount = payload[8];
        int offset = 9;
        var kinds = kindCount == 0 ? Array.Empty<RecoveryProgressKind>() : new RecoveryProgressKind[kindCount];
        for (int i = 0; i < kindCount; i++)
        {
            byte kind = payload[offset++];
            uint count = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
            offset += 4;
            kinds[i] = new RecoveryProgressKind(kind, count);
        }
        return new RecoveryProgressEvent
        {
            TotalSymbols = total,
            TotalStaleSymbols = totalStale,
            StaleByKind = kinds,
            ReceivedUtc = receivedUtc,
        };
    }

    // ── Candles ─────────────────────────────────────────────────────

    private const int CandleWireSize = 56;

    /// <summary>Read CandleSnapshot. Layout (after framing):
    /// <c>[u64 secId][u16 resolution][u8 flags][u16 count][candle × N]</c>.
    /// flags bit 0 = first, bit 1 = last.</summary>
    public static CandleSnapshotEvent ReadCandleSnapshot(ReadOnlySpan<byte> payload, string symbol, DateTime receivedUtc)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        ushort resolution = BinaryPrimitives.ReadUInt16LittleEndian(payload[8..]);
        byte flags = payload[10];
        ushort count = BinaryPrimitives.ReadUInt16LittleEndian(payload[11..]);
        int offset = 13;
        var candles = count == 0 ? Array.Empty<Candle>() : new Candle[count];
        for (int i = 0; i < count; i++)
        {
            candles[i] = ReadCandle(payload[offset..]);
            offset += CandleWireSize;
        }
        return new CandleSnapshotEvent
        {
            SecurityId = secId,
            Symbol = symbol,
            Resolution = resolution,
            IsFirst = (flags & 0x01) != 0,
            IsLast = (flags & 0x02) != 0,
            Candles = candles,
            ReceivedUtc = receivedUtc,
        };
    }

    /// <summary>Read CandleUpdate payload: <c>[u64 secId][u16 resolution][candle]</c>.</summary>
    public static (ulong SecurityId, int Resolution, Candle Candle) ReadCandleUpdate(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        ushort resolution = BinaryPrimitives.ReadUInt16LittleEndian(payload[8..]);
        var candle = ReadCandle(payload[10..]);
        return (secId, resolution, candle);
    }

    private static Candle ReadCandle(ReadOnlySpan<byte> src)
    {
        long time = BinaryPrimitives.ReadInt64LittleEndian(src);
        long open = BinaryPrimitives.ReadInt64LittleEndian(src[8..]);
        long high = BinaryPrimitives.ReadInt64LittleEndian(src[16..]);
        long low = BinaryPrimitives.ReadInt64LittleEndian(src[24..]);
        long close = BinaryPrimitives.ReadInt64LittleEndian(src[32..]);
        long volume = BinaryPrimitives.ReadInt64LittleEndian(src[40..]);
        long avg = BinaryPrimitives.ReadInt64LittleEndian(src[48..]);
        return new Candle(
            time,
            open / PriceScale,
            high / PriceScale,
            low / PriceScale,
            close / PriceScale,
            volume,
            avg / PriceScale);
    }

    // ── Rankings ────────────────────────────────────────────────────

    /// <summary>Read RankingsUpdate. Three back-to-back categories
    /// (Volume, Gainers, Losers); each is <c>[u8 count] + count × entry</c>
    /// where entry = <c>[u64 secId][i64 value][u8 symLen][symbol UTF-8…]</c>.</summary>
    public static RankingsUpdateEvent ReadRankingsUpdate(ReadOnlySpan<byte> payload, DateTime receivedUtc)
    {
        int offset = 0;
        var volume = ReadRankingCategory(payload, ref offset);
        var gainers = ReadRankingCategory(payload, ref offset);
        var losers = ReadRankingCategory(payload, ref offset);
        return new RankingsUpdateEvent
        {
            Volume = volume,
            Gainers = gainers,
            Losers = losers,
            ReceivedUtc = receivedUtc,
        };
    }

    private static IReadOnlyList<RankingEntry> ReadRankingCategory(ReadOnlySpan<byte> payload, ref int offset)
    {
        byte count = payload[offset++];
        if (count == 0) return Array.Empty<RankingEntry>();
        var list = new RankingEntry[count];
        for (int i = 0; i < count; i++)
        {
            ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
            long value = BinaryPrimitives.ReadInt64LittleEndian(payload[(offset + 8)..]);
            byte symLen = payload[offset + 16];
            string sym = Encoding.UTF8.GetString(payload.Slice(offset + 17, symLen));
            offset += 17 + symLen;
            list[i] = new RankingEntry(secId, sym, value);
        }
        return list;
    }

    // ── News ────────────────────────────────────────────────────────

    public const byte NewsFrameVersion = 1;

    public enum NewsField : byte
    {
        Headline = 0,
        Text = 1,
        Url = 2,
    }

    /// <summary>Parse a NewsBegin payload (after framing). Layout:
    /// <c>[u8 version][u64 secIdOrZero][u64 newsId][u8 source][u16 language]
    /// [i64 origTimeNanos][u32 totalHeadlineLen][u32 totalTextLen][u32 totalUrlLen]</c>.</summary>
    public static (byte Version, ulong SecurityIdOrZero, ulong NewsId, byte Source, ushort Language,
        long OrigTimeNanos, uint TotalHeadlineLen, uint TotalTextLen, uint TotalUrlLen)
        ReadNewsBegin(ReadOnlySpan<byte> payload)
    {
        int o = 0;
        byte version = payload[o++];
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload[o..]); o += 8;
        ulong newsId = BinaryPrimitives.ReadUInt64LittleEndian(payload[o..]); o += 8;
        byte source = payload[o++];
        ushort language = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        long origTime = BinaryPrimitives.ReadInt64LittleEndian(payload[o..]); o += 8;
        uint hLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]); o += 4;
        uint tLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]); o += 4;
        uint uLen = BinaryPrimitives.ReadUInt32LittleEndian(payload[o..]);
        return (version, secId, newsId, source, language, origTime, hLen, tLen, uLen);
    }

    /// <summary>Parse a NewsChunk / NewsEnd payload (after framing). Layout:
    /// <c>[u8 version][u64 newsId][u8 field][u16 fragmentLen][bytes…]</c>.
    /// Returned <paramref name="fragment"/> aliases the input buffer — caller
    /// MUST copy if it needs to outlive the receive scratch.</summary>
    public static (byte Version, ulong NewsId, NewsField Field) ReadNewsChunk(
        ReadOnlySpan<byte> payload, out ReadOnlySpan<byte> fragment)
    {
        int o = 0;
        byte version = payload[o++];
        ulong newsId = BinaryPrimitives.ReadUInt64LittleEndian(payload[o..]); o += 8;
        byte field = payload[o++];
        ushort fragLen = BinaryPrimitives.ReadUInt16LittleEndian(payload[o..]); o += 2;
        fragment = payload.Slice(o, fragLen);
        return (version, newsId, (NewsField)field);
    }
}
