using System.Buffers.Binary;
using System.Text;

namespace B3.Umdf.Server;

/// <summary>Message type identifiers for the binary WebSocket protocol.</summary>
public enum MessageType : ushort
{
    // Client → Server
    Subscribe = 0x0001,
    Unsubscribe = 0x0002,

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
    MarketData = 0x0040,
    SecurityStatus = 0x0041,
}

public enum SubscribeErrorCode : byte
{
    UnknownSymbol = 0x01,
    NotReady = 0x02,
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

    /// <summary>Parse a Subscribe message. Returns symbol string.</summary>
    public static string ReadSubscribe(ReadOnlySpan<byte> payload)
    {
        // payload starts after framing header
        byte symbolLen = payload[0];
        return Encoding.UTF8.GetString(payload.Slice(1, symbolLen));
    }

    /// <summary>Parse an Unsubscribe message. Returns securityId.</summary>
    public static ulong ReadUnsubscribe(ReadOnlySpan<byte> payload)
    {
        return BinaryPrimitives.ReadUInt64LittleEndian(payload);
    }

    // --- Server → Client ---

    /// <summary>Write SubscribeOk: securityId + symbol.</summary>
    public static int WriteSubscribeOk(Span<byte> dest, ulong securityId, string symbol)
    {
        byte[] symbolBytes = Encoding.UTF8.GetBytes(symbol);
        ushort totalLen = (ushort)(FramingHeaderSize + 8 + 1 + symbolBytes.Length);
        WriteFramingHeader(dest, totalLen, MessageType.SubscribeOk);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[4..], securityId);
        dest[12] = (byte)symbolBytes.Length;
        symbolBytes.CopyTo(dest[13..]);
        return totalLen;
    }

    /// <summary>Write SubscribeError: errorCode + symbol.</summary>
    public static int WriteSubscribeError(Span<byte> dest, SubscribeErrorCode errorCode, string symbol)
    {
        byte[] symbolBytes = Encoding.UTF8.GetBytes(symbol);
        ushort totalLen = (ushort)(FramingHeaderSize + 1 + 1 + symbolBytes.Length);
        WriteFramingHeader(dest, totalLen, MessageType.SubscribeError);
        dest[4] = (byte)errorCode;
        dest[5] = (byte)symbolBytes.Length;
        symbolBytes.CopyTo(dest[6..]);
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

    /// <summary>Write BookCleared: securityId.</summary>
    public static int WriteBookCleared(Span<byte> dest, ulong securityId)
    {
        const ushort totalLen = FramingHeaderSize + 8; // 12
        WriteFramingHeader(dest, totalLen, MessageType.BookCleared);
        BinaryPrimitives.WriteUInt64LittleEndian(dest[4..], securityId);
        return totalLen;
    }

    /// <summary>Write MarketData: securityId, fieldId, value.</summary>
    public static int WriteMarketData(Span<byte> dest, ulong securityId, byte fieldId, long value)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 1 + 8; // 21
        WriteFramingHeader(dest, totalLen, MessageType.MarketData);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        dest[offset++] = fieldId;
        BinaryPrimitives.WriteInt64LittleEndian(dest[offset..], value);
        return totalLen;
    }

    /// <summary>Write SecurityStatus: securityId, tradingStatus, tradingEvent.</summary>
    public static int WriteSecurityStatus(Span<byte> dest, ulong securityId, int tradingStatus, int tradingEvent)
    {
        const ushort totalLen = FramingHeaderSize + 8 + 4 + 4; // 20
        WriteFramingHeader(dest, totalLen, MessageType.SecurityStatus);
        int offset = FramingHeaderSize;
        BinaryPrimitives.WriteUInt64LittleEndian(dest[offset..], securityId); offset += 8;
        BinaryPrimitives.WriteInt32LittleEndian(dest[offset..], tradingStatus); offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(dest[offset..], tradingEvent);
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

    // --- InfoSnapshot ---

    // Field IDs for InfoSnapshot
    public const byte FieldOpeningPrice = 1;
    public const byte FieldClosingPrice = 2;
    public const byte FieldHighPrice = 3;
    public const byte FieldLowPrice = 4;
    public const byte FieldLastTradePrice = 5;
    public const byte FieldLastTradeSize = 6;
    public const byte FieldSettlementPrice = 7;
    public const byte FieldTheoreticalOpeningPrice = 8;
    public const byte FieldTradeVolume = 9;
    public const byte FieldVwapPrice = 10;
    public const byte FieldNetChange = 11;
    public const byte FieldNumberOfTrades = 12;
    public const byte FieldOpenInterest = 13;
    public const byte FieldPriceBandLow = 14;
    public const byte FieldPriceBandHigh = 15;
    public const byte FieldTradingReferencePrice = 16;
}
