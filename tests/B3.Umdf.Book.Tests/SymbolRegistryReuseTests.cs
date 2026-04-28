using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Pins the SymbolRegistry behavior when the exchange reuses a SecurityID
/// for a different ticker (delisting recycle): the old <c>_bySymbol</c>
/// entry must be dropped so a downstream <c>TryResolve(oldSymbol)</c>
/// returns false instead of silently mapping to the new instrument.
/// </summary>
public class SymbolRegistryReuseTests
{
    [Fact]
    public void SecurityIdReuse_RemovesOldSymbolMapping()
    {
        var reg = new SymbolRegistry();

        reg.OnPacket(in EmptyPacket, BuildSecDefPacket(securityId: 42, symbol: "PETR4"),
            SecurityDefinition_12Data.MESSAGE_ID);

        Assert.True(reg.TryResolve("PETR4", out var id1));
        Assert.Equal(42UL, id1);

        // Same SecurityID re-bound to a different symbol (post-delisting reuse).
        reg.OnPacket(in EmptyPacket, BuildSecDefPacket(securityId: 42, symbol: "VALE3"),
            SecurityDefinition_12Data.MESSAGE_ID);

        Assert.True(reg.TryResolve("VALE3", out var id2));
        Assert.Equal(42UL, id2);
        Assert.False(reg.TryResolve("PETR4", out _),
            "Old symbol must NOT resolve after SecurityID reuse — would be silent corruption.");

        Assert.True(reg.TryGetSymbol(42UL, out var sym));
        Assert.Equal("VALE3", sym);
    }

    [Fact]
    public void SecurityIdReuse_FrozenSnapshotRefreshedOnNextPromote()
    {
        // Regression: TryPromote was guarded by `liveCount <= _lastFrozenCount`,
        // so a same-count overwrite (reuse) left the frozen layer serving the
        // stale mapping forever. Force a promote and re-read via the frozen path.
        var reg = new SymbolRegistry();
        reg.OnPacket(in EmptyPacket, BuildSecDefPacket(securityId: 7, symbol: "OLD"),
            SecurityDefinition_12Data.MESSAGE_ID);
        reg.OnInstrumentDefinitionsComplete(1); // promote

        reg.OnPacket(in EmptyPacket, BuildSecDefPacket(securityId: 7, symbol: "NEW"),
            SecurityDefinition_12Data.MESSAGE_ID);
        Assert.True(reg.TryPromote(), "Promote must rebuild after content change even when count is unchanged.");

        Assert.True(reg.TryGetSymbol(7UL, out var sym));
        Assert.Equal("NEW", sym);
        Assert.False(reg.TryResolve("OLD", out _));
    }

    private static readonly UmdfPacket EmptyPacket = new()
    {
        Data = ReadOnlyMemory<byte>.Empty,
        Channel = ChannelType.InstrumentDefinition,
        ChannelGroup = 1,
        ReceivedTimestampTicks = 0,
    };

    private static byte[] BuildSecDefPacket(ulong securityId, string symbol)
    {
        const int sbeHeaderSize = 8;
        var payload = new SecurityDefinition_12Data
        {
            SecurityID = (SecurityID)securityId,
        };

        var buf = new byte[1024];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)SecurityDefinition_12Data.MESSAGE_SIZE);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), SecurityDefinition_12Data.MESSAGE_ID);

        bool ok = SecurityDefinition_12Data.TryEncode(
            payload,
            buf.AsSpan(sbeHeaderSize),
            ReadOnlySpan<SecurityDefinition_12Data.NoUnderlyingsData>.Empty,
            ReadOnlySpan<SecurityDefinition_12Data.NoLegsData>.Empty,
            ReadOnlySpan<SecurityDefinition_12Data.NoInstrAttribsData>.Empty,
            ReadOnlySpan<byte>.Empty,
            out int bytesWritten);
        Assert.True(ok);

        var src = Encoding.Latin1.GetBytes(symbol);
        var body = buf.AsSpan(sbeHeaderSize);
        body.Slice(16, 20).Clear();
        src.AsSpan(0, Math.Min(src.Length, 20)).CopyTo(body.Slice(16));

        var result = new byte[sbeHeaderSize + bytesWritten];
        buf.AsSpan(0, result.Length).CopyTo(result);
        return result;
    }
}
