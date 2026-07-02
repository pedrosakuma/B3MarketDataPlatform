using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using B3.MarketData.Wire;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// Locks the v2 blittable wire layout: sizes, field offsets, and header framing.
/// These assertions are the guardrail that keeps the on-wire layout from
/// silently changing (Explicit layout can otherwise be edited by accident).
/// </summary>
public class WireV2LayoutTests
{
    [Fact]
    public void Header_IsEightBytes()
    {
        Assert.Equal(8, WireV2.HeaderSize);
    }

    [Theory]
    [InlineData(typeof(ClientHelloFrame), 16)]
    [InlineData(typeof(ServerStatusFrame), 9)]
    [InlineData(typeof(SecurityIdFrame), 16)]
    [InlineData(typeof(SymbolStaleStatusFrame), 17)]
    [InlineData(typeof(OrderEventFrame), 41)]
    [InlineData(typeof(OrderDeletedFrame), 25)]
    [InlineData(typeof(TradeFrame), 41)]
    [InlineData(typeof(TradeBustFrame), 24)]
    [InlineData(typeof(BookClearedFrame), 17)]
    [InlineData(typeof(MarketTierUpdateFrame), 29)]
    [InlineData(typeof(LevelUpdateFrame), 37)]
    [InlineData(typeof(LevelDeletedFrame), 25)]
    public void FrameSize_MatchesSpec(Type frameType, int expectedSize)
    {
        int size = Marshal.SizeOf(frameType);
        Assert.Equal(expectedSize, size);
    }

    [Fact]
    public void OrderEvent_FieldOffsets_AreAligned()
    {
        Assert.Equal(0, (int)Marshal.OffsetOf<OrderEventFrame>(nameof(OrderEventFrame.Length)));
        Assert.Equal(4, (int)Marshal.OffsetOf<OrderEventFrame>(nameof(OrderEventFrame.TypeRaw)));
        Assert.Equal(6, (int)Marshal.OffsetOf<OrderEventFrame>(nameof(OrderEventFrame.HeaderFlagsRaw)));
        Assert.Equal(8, (int)Marshal.OffsetOf<OrderEventFrame>(nameof(OrderEventFrame.SecurityId)));
        Assert.Equal(16, (int)Marshal.OffsetOf<OrderEventFrame>(nameof(OrderEventFrame.OrderId)));
        Assert.Equal(24, (int)Marshal.OffsetOf<OrderEventFrame>(nameof(OrderEventFrame.Price)));
        Assert.Equal(32, (int)Marshal.OffsetOf<OrderEventFrame>(nameof(OrderEventFrame.Quantity)));
        Assert.Equal(40, (int)Marshal.OffsetOf<OrderEventFrame>(nameof(OrderEventFrame.Side)));
    }

    [Fact]
    public void OrderEvent_WriteThenRead_RoundTrips()
    {
        Span<byte> buf = stackalloc byte[64];
        var frame = new OrderEventFrame(MessageType.OrderAdded, 42, 9001, 12_3456, 250, side: 0);
        int n = WireFrame.Write(buf, in frame);
        Assert.Equal(OrderEventFrame.WireSize, n);

        Assert.True(WireFrame.TryReadHeader(buf, out uint len, out var type, out var flags));
        Assert.Equal((uint)OrderEventFrame.WireSize, len);
        Assert.Equal(MessageType.OrderAdded, type);
        Assert.Equal(HeaderFlags.None, flags);

        var read = WireFrame.Read<OrderEventFrame>(buf);
        Assert.Equal(42UL, read.SecurityId);
        Assert.Equal(9001UL, read.OrderId);
        Assert.Equal(12_3456L, read.Price);
        Assert.Equal(250L, read.Quantity);
        Assert.Equal((byte)0, read.Side);
    }

    [Fact]
    public void Read_IgnoresTrailingBytes_MinLengthRule()
    {
        // A forward-compatible producer appended 8 unknown trailing bytes.
        Span<byte> buf = stackalloc byte[OrderEventFrame.WireSize + 8];
        var frame = new OrderEventFrame(MessageType.OrderUpdated, 7, 1, 100, 5, side: 1);
        WireFrame.Write(buf, in frame);
        // Simulate a longer advertised length.
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(buf, (uint)buf.Length);

        // Old decoder still reads its known core without error.
        var read = WireFrame.Read<OrderEventFrame>(buf);
        Assert.Equal(7UL, read.SecurityId);
        Assert.Equal(1UL, read.OrderId);
    }
}
