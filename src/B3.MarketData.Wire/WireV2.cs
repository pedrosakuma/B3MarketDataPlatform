using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace B3.MarketData.Wire;

/// <summary>
/// Single canonical definition of the B3MarketDataPlatform WebSocket binary
/// protocol, version 2. This file is <b>compile-linked into both</b> the server
/// (<c>B3.Umdf.Server</c>) and the published SDK
/// (<c>B3.MarketData.WebSocketClient</c>) so the two C# sides can never drift.
/// The frontend JS mirror (<c>frontend/js/protocol.js</c>) is guarded against
/// drift by committed golden hex vectors decoded identically in C# and JS.
///
/// <para><b>Framing.</b> Every frame starts with an 8-byte header:
/// <c>[u32 length][u16 type][u16 headerFlags]</c>, little-endian, where
/// <c>length</c> includes the header. The payload therefore starts at byte 8.</para>
///
/// <para><b>Endianness.</b> The wire is little-endian only. The zero-copy
/// blittable fast path (<see cref="WireFrame.Write{T}"/> /
/// <see cref="WireFrame.Read{T}"/>) is valid only on little-endian runtimes; a
/// guard throws otherwise. See <see cref="WireFrame.EnsureLittleEndian"/>.</para>
///
/// <para><b>Forward-compatibility contract.</b> Fixed frames publish a
/// <c>WireSize</c> that is a <i>minimum</i> length: decoders MUST accept frames
/// whose <c>length</c> is greater than that size and MUST ignore trailing bytes,
/// never validating an exact length. New fields may only be appended after the
/// published core and only read after checking
/// <c>length &gt;= fieldEndOffset</c>. Existing offsets and meanings are
/// immutable. Message types and flag bits are append-only and never reused or
/// renumbered. Unknown <see cref="DataFlags"/> / capability bits are ignored;
/// unknown <b>non-zero</b> <c>headerFlags</c> MUST cause the frame to be rejected
/// (they may change payload interpretation, e.g. compression).</para>
/// </summary>
public static class WireV2
{
    /// <summary>Size of the fixed framing header: <c>[u32 length][u16 type][u16 headerFlags]</c>.</summary>
    public const int HeaderSize = 8;

    /// <summary>Current wire protocol version.</summary>
    public const uint ProtocolVersion = 2;

    /// <summary>Lowest protocol version this build can speak.</summary>
    public const uint SupportedProtocolVersionMin = 2;

    /// <summary>Highest protocol version this build can speak.</summary>
    public const uint SupportedProtocolVersionMax = 2;

    /// <summary>
    /// Hard cap on a single frame's advertised <c>length</c>. Guards against a
    /// malformed/hostile header claiming a huge length and forcing an unbounded
    /// buffer allocation. 16 MiB comfortably exceeds any legitimate frame
    /// (deep snapshots, news bodies) while bounding worst-case memory.
    /// </summary>
    public const int MaxFrameLength = 16 * 1024 * 1024;
}

/// <summary>Message type discriminator (u16 on the wire, header offset 4).</summary>
public enum MessageType : ushort
{
    // Client -> Server
    ClientHello = 0x00A1,
    Subscribe = 0x0001,
    Unsubscribe = 0x0002,
    Get = 0x0003,

    // Server -> Client: handshake / lifecycle
    ServerHello = 0x00A0,
    ServerStatus = 0x0050,
    SubscribeOk = 0x0010,
    SubscribeError = 0x0011,
    Unsubscribed = 0x0012,
    SymbolStaleStatus = 0x0070,
    SymbolDelisted = 0x0071,
    RecoveryProgress = 0x0080,

    // Server -> Client: snapshots
    BookSnapshot = 0x0020,
    InfoSnapshot = 0x0021,
    LevelSnapshot = 0x0022,

    // Server -> Client: incrementals
    OrderAdded = 0x0030,
    OrderUpdated = 0x0031,
    OrderDeleted = 0x0032,
    Trade = 0x0033,
    BookCleared = 0x0034,
    TradeBust = 0x0035,
    MarketTierUpdate = 0x0036,
    LevelUpdate = 0x0037,
    LevelDeleted = 0x0038,

    // Server -> Client: aggregates / metadata
    RankingsUpdate = 0x0040,
    CandleSnapshot = 0x0060,
    CandleUpdate = 0x0061,
    SecurityDefinition = 0x00B0,
    PriceBand = 0x00B1,
    Auction = 0x00B2,

    // Server -> Client: news (fragmented)
    NewsBegin = 0x0090,
    NewsChunk = 0x0091,
    NewsEnd = 0x0092,
}

/// <summary>
/// Data channels a client can subscribe to (u32 bitmask). Append-only; bits are
/// never reused. Do not publish an all-ones "everything" constant — that would
/// implicitly opt legacy clients into future channels. Use
/// <see cref="AllKnown"/>; the server masks off unknown requested bits and
/// echoes only the accepted set in <see cref="MessageType.SubscribeOk"/>.
/// </summary>
[Flags]
public enum DataFlags : uint
{
    None = 0,
    Book = 0x0001,
    Info = 0x0002,
    News = 0x0004,
    Mbp = 0x0008,
    Trades = 0x0010,
    SecurityDefinition = 0x0020,
    PriceBand = 0x0040,
    Auction = 0x0080,

    /// <summary>Legacy convenience: Book + Info.</summary>
    All = Book | Info,

    /// <summary>Every channel this build knows about. NOT all-ones — new
    /// channels are added here explicitly so unknown bits stay unrequested.</summary>
    AllKnown = Book | Info | News | Mbp | Trades | SecurityDefinition | PriceBand | Auction,
}

/// <summary>Optional server features advertised in <see cref="MessageType.ServerHello"/>. Append-only.</summary>
[Flags]
public enum ServerCapabilities : uint
{
    None = 0,
    SnapshotOnSubscribe = 0x0001,
    SymbolDelistedNotification = 0x0002,
}

/// <summary>Optional client features advertised in <see cref="MessageType.ClientHello"/>. Append-only; 0 today.</summary>
[Flags]
public enum ClientCapabilities : uint
{
    None = 0,
}

/// <summary>Header-flag bits (u16 at header offset 6). All zero in v2 base.
/// Receivers MUST reject frames carrying any bit they do not understand — such
/// a bit may change how the payload is interpreted (e.g. compression).</summary>
[Flags]
public enum HeaderFlags : ushort
{
    None = 0,
}

/// <summary><see cref="MessageType.SubscribeError"/> reason codes.</summary>
public enum SubscribeErrorCode : byte
{
    UnknownSymbol = 0x01,
    NotReady = 0x02,
}

/// <summary>Framing-header read/write helpers and the blittable fast path.</summary>
public static class WireFrame
{
    /// <summary>Throws if the current runtime is big-endian, where the blittable
    /// struct fast path would produce/consume the wrong byte order.</summary>
    public static void EnsureLittleEndian()
    {
        if (!BitConverter.IsLittleEndian)
            throw new PlatformNotSupportedException(
                "B3 WS wire protocol v2 requires a little-endian runtime for the blittable fast path.");
    }

    /// <summary>Write the 8-byte framing header. <paramref name="length"/> is the
    /// total frame length including this header.</summary>
    public static void WriteHeader(Span<byte> dest, int length, MessageType type, HeaderFlags flags = HeaderFlags.None)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(dest, (uint)length);
        BinaryPrimitives.WriteUInt16LittleEndian(dest[4..], (ushort)type);
        BinaryPrimitives.WriteUInt16LittleEndian(dest[6..], (ushort)flags);
    }

    /// <summary>Try to read the framing header. Returns false if fewer than
    /// <see cref="WireV2.HeaderSize"/> bytes are available.</summary>
    public static bool TryReadHeader(ReadOnlySpan<byte> src, out uint length, out MessageType type, out HeaderFlags flags)
    {
        if (src.Length < WireV2.HeaderSize)
        {
            length = 0; type = 0; flags = HeaderFlags.None;
            return false;
        }
        length = BinaryPrimitives.ReadUInt32LittleEndian(src);
        type = (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(src[4..]);
        flags = (HeaderFlags)BinaryPrimitives.ReadUInt16LittleEndian(src[6..]);
        return true;
    }

    /// <summary>Write a fixed blittable frame struct (header embedded) and return
    /// its size in bytes. Little-endian runtime required.</summary>
    public static int Write<T>(Span<byte> dest, in T frame) where T : unmanaged
    {
        MemoryMarshal.Write(dest, in frame);
        return Unsafe.SizeOf<T>();
    }

    /// <summary>Read a fixed blittable frame struct from the start of
    /// <paramref name="src"/>. The caller must have already validated that the
    /// frame's advertised length is at least <c>sizeof(T)</c> (min-length rule);
    /// trailing bytes beyond <c>sizeof(T)</c> are ignored.</summary>
    public static T Read<T>(ReadOnlySpan<byte> src) where T : unmanaged
        => MemoryMarshal.Read<T>(src);
}
