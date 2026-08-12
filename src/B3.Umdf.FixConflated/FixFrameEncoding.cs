using System.Buffers.Text;

namespace B3.Umdf.FixConflated;

internal static class FixFrameEncoding
{
    public const int ChecksumFieldLength = 7;

    public static int GetFrameLength(int beginStringLength, int bodyLength)
        => GetPrefixLength(beginStringLength, bodyLength) + bodyLength + ChecksumFieldLength;

    public static int GetPrefixLength(int beginStringLength, int bodyLength)
        => 2 + beginStringLength + 1 + 2 + CountDigits(bodyLength) + 1;

    public static int WritePrefix(Span<byte> destination, ReadOnlySpan<byte> beginString, int bodyLength)
    {
        int offset = 0;
        destination[offset++] = (byte)'8';
        destination[offset++] = (byte)'=';
        beginString.CopyTo(destination[offset..]);
        offset += beginString.Length;
        destination[offset++] = FixMessageCodec.Soh;

        destination[offset++] = (byte)'9';
        destination[offset++] = (byte)'=';
        if (!Utf8Formatter.TryFormat(bodyLength, destination[offset..], out int digitsWritten))
            throw new InvalidOperationException("Unable to format FIX body length.");

        offset += digitsWritten;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    public static int CalculateChecksum(ReadOnlySpan<byte> payload)
    {
        int checksum = 0;
        for (int i = 0; i < payload.Length; i++)
            checksum = (checksum + payload[i]) & 0xFF;

        return checksum;
    }

    public static int WriteChecksumField(Span<byte> destination, int checksum)
    {
        destination[0] = (byte)'1';
        destination[1] = (byte)'0';
        destination[2] = (byte)'=';
        destination[3] = (byte)('0' + (checksum / 100));
        destination[4] = (byte)('0' + ((checksum / 10) % 10));
        destination[5] = (byte)('0' + (checksum % 10));
        destination[6] = FixMessageCodec.Soh;
        return ChecksumFieldLength;
    }

    private static int CountDigits(int value)
    {
        if (value == 0)
            return 1;

        int digits = 0;
        int remaining = value;
        while (remaining != 0)
        {
            remaining /= 10;
            digits++;
        }

        return digits;
    }
}
