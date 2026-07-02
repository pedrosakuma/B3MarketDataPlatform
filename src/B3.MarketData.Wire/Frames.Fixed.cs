using System.Runtime.InteropServices;

namespace B3.MarketData.Wire;

// Fixed-layout ("v2 core") frames. Each struct embeds the 8-byte framing header
// at offsets 0/4/6 and lays payload fields out largest-first with natural
// internal alignment and NO tail padding. The published WireSize is a MINIMUM
// length: decoders must accept longer frames and ignore trailing bytes. Fields
// are little-endian (host-LE fast path; see WireFrame.EnsureLittleEndian).

/// <summary>Base helpers shared by fixed frame structs.</summary>
internal static class FrameHeaderFields
{
    public const int LengthOffset = 0;
    public const int TypeOffset = 4;
    public const int HeaderFlagsOffset = 6;
}

/// <summary><see cref="MessageType.ClientHello"/> — 16 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct ClientHelloFrame
{
    public const int WireSize = 16;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly uint ProtocolVersion;
    [FieldOffset(12)] public readonly uint ClientCapabilities;

    public ClientHelloFrame(uint protocolVersion, ClientCapabilities capabilities)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.ClientHello;
        HeaderFlagsRaw = 0;
        ProtocolVersion = protocolVersion;
        ClientCapabilities = (uint)capabilities;
    }
}

/// <summary><see cref="MessageType.ServerStatus"/> — 9 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct ServerStatusFrame
{
    public const int WireSize = 9;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly byte Ready;

    public ServerStatusFrame(bool ready)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.ServerStatus;
        HeaderFlagsRaw = 0;
        Ready = ready ? (byte)1 : (byte)0;
    }
}

/// <summary><see cref="MessageType.Unsubscribe"/> / <see cref="MessageType.Unsubscribed"/>
/// / <see cref="MessageType.SymbolDelisted"/> — 16 bytes, single securityId.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct SecurityIdFrame
{
    public const int WireSize = 16;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;

    public SecurityIdFrame(MessageType type, ulong securityId)
    {
        Length = WireSize;
        TypeRaw = (ushort)type;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
    }
}

/// <summary><see cref="MessageType.SymbolStaleStatus"/> — 17 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct SymbolStaleStatusFrame
{
    public const int WireSize = 17;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly byte IsStale;

    public SymbolStaleStatusFrame(ulong securityId, bool isStale)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.SymbolStaleStatus;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        IsStale = isStale ? (byte)1 : (byte)0;
    }
}

/// <summary><see cref="MessageType.OrderAdded"/> / <see cref="MessageType.OrderUpdated"/> — 41 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct OrderEventFrame
{
    public const int WireSize = 41;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly ulong OrderId;
    [FieldOffset(24)] public readonly long Price;
    [FieldOffset(32)] public readonly long Quantity;
    [FieldOffset(40)] public readonly byte Side;

    public OrderEventFrame(MessageType type, ulong securityId, ulong orderId, long price, long qty, byte side)
    {
        Length = WireSize;
        TypeRaw = (ushort)type;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        OrderId = orderId;
        Price = price;
        Quantity = qty;
        Side = side;
    }

    public MessageType Type => (MessageType)TypeRaw;
}

/// <summary><see cref="MessageType.OrderDeleted"/> — 25 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct OrderDeletedFrame
{
    public const int WireSize = 25;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly ulong OrderId;
    [FieldOffset(24)] public readonly byte Side;

    public OrderDeletedFrame(ulong securityId, ulong orderId, byte side)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.OrderDeleted;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        OrderId = orderId;
        Side = side;
    }
}

/// <summary><see cref="MessageType.Trade"/> — 41 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct TradeFrame
{
    public const int WireSize = 41;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly long Price;
    [FieldOffset(24)] public readonly long Quantity;
    [FieldOffset(32)] public readonly long TradeId;
    [FieldOffset(40)] public readonly byte Flags;

    public TradeFrame(ulong securityId, long price, long qty, long tradeId, byte flags)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.Trade;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        Price = price;
        Quantity = qty;
        TradeId = tradeId;
        Flags = flags;
    }
}

/// <summary><see cref="MessageType.TradeBust"/> — 24 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct TradeBustFrame
{
    public const int WireSize = 24;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly long TradeId;

    public TradeBustFrame(ulong securityId, long tradeId)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.TradeBust;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        TradeId = tradeId;
    }
}

/// <summary><see cref="MessageType.BookCleared"/> — 17 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct BookClearedFrame
{
    public const int WireSize = 17;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly byte ClearSide;

    public BookClearedFrame(ulong securityId, byte clearSide)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.BookCleared;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        ClearSide = clearSide;
    }
}

/// <summary><see cref="MessageType.MarketTierUpdate"/> — 29 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct MarketTierUpdateFrame
{
    public const int WireSize = 29;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly long TotalQty;
    [FieldOffset(24)] public readonly uint OrderCount;
    [FieldOffset(28)] public readonly byte Side;

    public MarketTierUpdateFrame(ulong securityId, byte side, long totalQty, uint orderCount)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.MarketTierUpdate;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        TotalQty = totalQty;
        OrderCount = orderCount;
        Side = side;
    }
}

/// <summary><see cref="MessageType.LevelUpdate"/> — 37 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct LevelUpdateFrame
{
    public const int WireSize = 37;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly long Price;
    [FieldOffset(24)] public readonly long TotalQty;
    [FieldOffset(32)] public readonly uint OrderCount;
    [FieldOffset(36)] public readonly byte Side;

    public LevelUpdateFrame(ulong securityId, byte side, long price, long totalQty, uint orderCount)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.LevelUpdate;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        Price = price;
        TotalQty = totalQty;
        OrderCount = orderCount;
        Side = side;
    }
}

/// <summary><see cref="MessageType.LevelDeleted"/> — 25 bytes.</summary>
[StructLayout(LayoutKind.Explicit, Size = WireSize)]
public readonly struct LevelDeletedFrame
{
    public const int WireSize = 25;
    [FieldOffset(0)] public readonly uint Length;
    [FieldOffset(4)] public readonly ushort TypeRaw;
    [FieldOffset(6)] public readonly ushort HeaderFlagsRaw;
    [FieldOffset(8)] public readonly ulong SecurityId;
    [FieldOffset(16)] public readonly long Price;
    [FieldOffset(24)] public readonly byte Side;

    public LevelDeletedFrame(ulong securityId, byte side, long price)
    {
        Length = WireSize;
        TypeRaw = (ushort)MessageType.LevelDeleted;
        HeaderFlagsRaw = 0;
        SecurityId = securityId;
        Price = price;
        Side = side;
    }
}
