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

    public enum MessageType : ushort
    {
        Subscribe = 0x0001,
        Unsubscribe = 0x0002,
        SubscribeOk = 0x0010,
        SubscribeError = 0x0011,
        Unsubscribed = 0x0012,
        InfoSnapshot = 0x0021,
        Trade = 0x0033,
        TradeBust = 0x0035,
        ServerStatus = 0x0050,
        SymbolDelisted = 0x0071,
        ServerHello = 0x00A0,
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
    /// Encode a <c>Subscribe</c> frame: <c>[flags u8][symLen u8][symbol UTF-8…]</c>.
    /// Returns the total frame length written.
    /// </summary>
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

    public static (ulong SecurityId, long Price, long Qty, long TradeId) ReadTrade(ReadOnlySpan<byte> payload)
    {
        ulong secId = BinaryPrimitives.ReadUInt64LittleEndian(payload);
        long price = BinaryPrimitives.ReadInt64LittleEndian(payload[8..]);
        long qty = BinaryPrimitives.ReadInt64LittleEndian(payload[16..]);
        long tradeId = BinaryPrimitives.ReadInt64LittleEndian(payload[24..]);
        return (secId, price, qty, tradeId);
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
                case FieldVwapPrice: vwap = v / PriceScale; break;
                case FieldNumberOfTrades: trades = v; break;
                case FieldOpenInterest: openInterest = v; break;
                case FieldPriceBandLow: bandLow = v / PriceScale; break;
                case FieldPriceBandHigh: bandHigh = v / PriceScale; break;
                case FieldTradingReferencePrice: refPx = v / PriceScale; break;
                case FieldTradeVolume: volume = v; break;
                case FieldTradingStatus: status = v; break;
                case FieldTradingEvent: evt = v; break;
                // Other bits (TheoreticalOpening*, Auction*, NetChange,
                // AvgDailyTradedQty, MaxTradeVol, PriceLimitType,
                // MinPriceIncrement) are consumed but not surfaced in
                // the v1 typed event.
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
        };
    }
}
