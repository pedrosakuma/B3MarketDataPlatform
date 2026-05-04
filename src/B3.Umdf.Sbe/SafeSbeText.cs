using System.Text;

namespace B3.Umdf.Sbe;

/// <summary>
/// Defensive readers for SBE variable-length string composites (issue #15).
///
/// The generated <c>TextEncoding.Create</c> / <c>VarString.Create</c> helpers slice the buffer
/// using the on-wire length prefix without verifying that the buffer actually contains that
/// many bytes. A malformed or truncated UMDF packet can therefore trigger an
/// <see cref="ArgumentOutOfRangeException"/> deep inside the parser. These helpers replicate
/// the same wire layout but bounds-check first and return <c>false</c> instead of throwing,
/// so the caller can drop the message and bump a parse-error counter.
///
/// Wire layout (must mirror <c>TextEncoding.cs</c> / <c>VarString.cs</c> in the generated SBE
/// composites — keep in sync if the schema ever changes):
/// <list type="bullet">
///   <item><c>TextEncoding</c>: <c>byte length</c> (1-byte prefix) + <c>length</c> ASCII/UTF-8 bytes.</item>
///   <item><c>VarString</c>:    <c>ushort length</c> (2-byte LE prefix) + <c>length</c> UTF-8 bytes.</item>
/// </list>
/// </summary>
public static class SafeSbeText
{
    internal const int TextEncodingLengthPrefix = 1;
    internal const int VarStringLengthPrefix = 2;

    /// <summary>
    /// Safely decode a <c>TextEncoding</c> composite (1-byte length prefix + UTF-8 data).
    /// Returns <c>false</c> if the buffer is too small to contain the prefix or the declared
    /// payload. On failure <paramref name="value"/> is set to <see cref="string.Empty"/>.
    /// </summary>
    public static bool TryReadTextEncoding(ReadOnlySpan<byte> buffer, out string value)
    {
        if (buffer.Length < TextEncodingLengthPrefix)
        {
            value = string.Empty;
            return false;
        }

        int length = buffer[0];
        int total = TextEncodingLengthPrefix + length;
        if (buffer.Length < total)
        {
            value = string.Empty;
            return false;
        }

        value = length == 0
            ? string.Empty
            : Encoding.UTF8.GetString(buffer.Slice(TextEncodingLengthPrefix, length));
        return true;
    }

    /// <summary>
    /// Safely decode a <c>VarString</c> composite (2-byte little-endian length prefix + UTF-8 data).
    /// Returns <c>false</c> if the buffer is too small to contain the prefix or the declared
    /// payload. On failure <paramref name="value"/> is set to <see cref="string.Empty"/>.
    /// </summary>
    public static bool TryReadVarString(ReadOnlySpan<byte> buffer, out string value)
    {
        if (buffer.Length < VarStringLengthPrefix)
        {
            value = string.Empty;
            return false;
        }

        // Wire format is little-endian (SBE default and what MemoryMarshal.AsRef<ushort> produces
        // on the LE platforms we run on); use BinaryPrimitives for an explicit, portable read.
        ushort length = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(buffer);
        int total = VarStringLengthPrefix + length;
        if (buffer.Length < total)
        {
            value = string.Empty;
            return false;
        }

        value = length == 0
            ? string.Empty
            : Encoding.UTF8.GetString(buffer.Slice(VarStringLengthPrefix, length));
        return true;
    }
}
