using System.Buffers.Binary;
using System.Text;
using B3.Umdf.Sbe;

namespace B3.Umdf.Sbe.Tests;

public class SafeSbeTextTests
{
    // ── TextEncoding (1-byte length prefix + UTF-8) ──────────────────────────────

    [Fact]
    public void TryReadTextEncoding_EmptyBuffer_ReturnsFalse()
    {
        Assert.False(SafeSbeText.TryReadTextEncoding(ReadOnlySpan<byte>.Empty, out var s));
        Assert.Equal(string.Empty, s);
    }

    [Fact]
    public void TryReadTextEncoding_TruncatedPayload_ReturnsFalse()
    {
        // Length prefix says 5 bytes but only 2 follow.
        ReadOnlySpan<byte> buf = new byte[] { 5, (byte)'a', (byte)'b' };
        Assert.False(SafeSbeText.TryReadTextEncoding(buf, out var s));
        Assert.Equal(string.Empty, s);
    }

    [Fact]
    public void TryReadTextEncoding_ZeroLength_ReturnsTrueWithEmpty()
    {
        ReadOnlySpan<byte> buf = new byte[] { 0 };
        Assert.True(SafeSbeText.TryReadTextEncoding(buf, out var s));
        Assert.Equal(string.Empty, s);
    }

    [Fact]
    public void TryReadTextEncoding_ValidPayload_DecodesString()
    {
        var data = Encoding.UTF8.GetBytes("PETR4");
        var buf = new byte[1 + data.Length];
        buf[0] = (byte)data.Length;
        data.CopyTo(buf, 1);

        Assert.True(SafeSbeText.TryReadTextEncoding(buf, out var s));
        Assert.Equal("PETR4", s);
    }

    [Fact]
    public void TryReadTextEncoding_ExtraTrailingBytes_StillDecodesDeclaredLength()
    {
        // 3 declared bytes followed by garbage; we must only decode the 3 bytes.
        ReadOnlySpan<byte> buf = new byte[] { 3, (byte)'A', (byte)'B', (byte)'C', 0xFF, 0xFE };
        Assert.True(SafeSbeText.TryReadTextEncoding(buf, out var s));
        Assert.Equal("ABC", s);
    }

    // ── VarString (2-byte LE length prefix + UTF-8) ──────────────────────────────

    [Fact]
    public void TryReadVarString_EmptyBuffer_ReturnsFalse()
    {
        Assert.False(SafeSbeText.TryReadVarString(ReadOnlySpan<byte>.Empty, out var s));
        Assert.Equal(string.Empty, s);
    }

    [Fact]
    public void TryReadVarString_OnlyOnePrefixByte_ReturnsFalse()
    {
        ReadOnlySpan<byte> buf = new byte[] { 0x05 };
        Assert.False(SafeSbeText.TryReadVarString(buf, out var s));
        Assert.Equal(string.Empty, s);
    }

    [Fact]
    public void TryReadVarString_TruncatedPayload_ReturnsFalse()
    {
        // Length prefix says 10 bytes; only 4 follow.
        var buf = new byte[6];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, 10);
        Encoding.UTF8.GetBytes("data").CopyTo(buf, 2);

        Assert.False(SafeSbeText.TryReadVarString(buf, out var s));
        Assert.Equal(string.Empty, s);
    }

    [Fact]
    public void TryReadVarString_ZeroLength_ReturnsTrueWithEmpty()
    {
        var buf = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, 0);

        Assert.True(SafeSbeText.TryReadVarString(buf, out var s));
        Assert.Equal(string.Empty, s);
    }

    [Fact]
    public void TryReadVarString_ValidUtf8_DecodesString()
    {
        // "Ação" is multi-byte in UTF-8 — exercises real UTF-8 decoding.
        var payload = Encoding.UTF8.GetBytes("Ação");
        var buf = new byte[2 + payload.Length];
        BinaryPrimitives.WriteUInt16LittleEndian(buf, (ushort)payload.Length);
        payload.CopyTo(buf, 2);

        Assert.True(SafeSbeText.TryReadVarString(buf, out var s));
        Assert.Equal("Ação", s);
    }
}
