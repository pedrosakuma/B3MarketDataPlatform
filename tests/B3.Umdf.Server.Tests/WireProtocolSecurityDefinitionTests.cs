using System.Buffers.Binary;
using System.Text;
using B3.Umdf.Book;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Wire-format tests for the SecurityDefinition channel (issue #55). Pins the
/// dual-bitmask layout (numericMask + stringMask), bit-position contract, and
/// MessageType code so changes that would silently break the SDK decoder fail
/// loudly here first.
/// </summary>
public class WireProtocolSecurityDefinitionTests
{
    private static (int length, MessageType type) ReadFraming(Span<byte> buf) =>
        ((int)BinaryPrimitives.ReadUInt32LittleEndian(buf), (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(buf[4..]));

    [Fact]
    public void WriteSecurityDefinition_MessageTypeIs0x00B0()
    {
        var buf = new byte[WireProtocol.SecurityDefinitionMaxSize];
        var info = new InstrumentInfo { Symbol = "PETR4" };
        WireProtocol.WriteSecurityDefinition(buf, securityId: 1, info);
        var (_, type) = ReadFraming(buf);
        Assert.Equal(MessageType.SecurityDefinition, type);
        Assert.Equal((ushort)0x00B0, (ushort)type);
    }

    [Fact]
    public void WriteSecurityDefinition_OnlySymbol_BothMasksZero()
    {
        var buf = new byte[WireProtocol.SecurityDefinitionMaxSize];
        var info = new InstrumentInfo { Symbol = "PETR4" };
        int len = WireProtocol.WriteSecurityDefinition(buf, securityId: 99, info);

        var (msgLen, _) = ReadFraming(buf);
        Assert.Equal(len, msgLen);

        int off = WireProtocol.FramingHeaderSize;
        Assert.Equal(99UL, BinaryPrimitives.ReadUInt64LittleEndian(buf.AsSpan(off))); off += 8;
        int symLen = buf[off++];
        Assert.Equal(5, symLen);
        Assert.Equal("PETR4", Encoding.UTF8.GetString(buf.AsSpan(off, symLen))); off += symLen;
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off))); off += 4;
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off))); off += 4;
        Assert.Equal(len, off);
    }

    [Fact]
    public void WriteSecurityDefinition_TickAndLot_MaskAndValuesCorrect()
    {
        var buf = new byte[WireProtocol.SecurityDefinitionMaxSize];
        var info = new InstrumentInfo
        {
            Symbol = "PETR4",
            // 0.01 in Fixed8 units (1e-8 exponent) -> 1_000_000
            MinPriceIncrement = 1_000_000L,
            MinTradeVolume = 100L,
        };
        int len = WireProtocol.WriteSecurityDefinition(buf, securityId: 42, info);

        int off = WireProtocol.FramingHeaderSize + 8 + 1 + 5; // header + secId + symLen + "PETR4"
        uint numericMask = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off));
        off += 4;

        Assert.True((numericMask & (1u << WireProtocol.SecurityDefinitionFieldMinPriceIncrement)) != 0);
        Assert.True((numericMask & (1u << WireProtocol.SecurityDefinitionFieldMinTradeVolume)) != 0);

        // Bit order on the wire: MinPriceIncrement(0), MinTradeVolume(1).
        Assert.Equal(1_000_000L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(100L, BinaryPrimitives.ReadInt64LittleEndian(buf.AsSpan(off))); off += 8;
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off))); off += 4; // empty stringMask
        Assert.Equal(len, off);
    }

    [Fact]
    public void WriteSecurityDefinition_AllStrings_StringMaskCorrect()
    {
        var buf = new byte[WireProtocol.SecurityDefinitionMaxSize];
        var info = new InstrumentInfo
        {
            Symbol = "PETR4",
            IsinNumber = "BRPETRACNOR9",
            Currency = "BRL",
            Asset = "PETR",
            CfiCode = "ESVUFR",
            SecurityGroup = "EQTY",
            SecurityDescription = "Petrobras PN",
        };
        int len = WireProtocol.WriteSecurityDefinition(buf, securityId: 1, info);

        int off = WireProtocol.FramingHeaderSize + 8 + 1 + 5 + 4; // skip up through numericMask (=0)
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off - 4)));

        uint stringMask = BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(off));
        off += 4;
        Assert.Equal(0b11_1111u, stringMask & 0xFFu);

        // Strings appear in bit order: Isin(0), Currency(1), Asset(2), CfiCode(3),
        // SecurityGroup(4), SecurityDescription(5).
        foreach (var expected in new[] { "BRPETRACNOR9", "BRL", "PETR", "ESVUFR", "EQTY", "Petrobras PN" })
        {
            ushort sLen = BinaryPrimitives.ReadUInt16LittleEndian(buf.AsSpan(off));
            off += 2;
            Assert.Equal(expected, Encoding.UTF8.GetString(buf.AsSpan(off, sLen)));
            off += sLen;
        }
        Assert.Equal(len, off);
    }

    [Fact]
    public void DataFlags_SecurityDefinition_IsInEverythingButNotInAll()
    {
        Assert.True(DataFlags.AllKnown.HasFlag(DataFlags.SecurityDefinition));
        Assert.False(DataFlags.All.HasFlag(DataFlags.SecurityDefinition));
        Assert.Equal((byte)0x20, (byte)DataFlags.SecurityDefinition);
    }
}
