using System.Buffers.Text;
using System.Globalization;
using System.Text;

namespace B3.Umdf.FixConflated;

public enum FixDecodeError
{
    None = 0,
    Incomplete,
    MissingBeginString,
    InvalidBeginString,
    MissingBodyLength,
    InvalidBodyLength,
    MissingMsgType,
    MissingCheckSum,
    InvalidCheckSum,
    CheckSumMismatch,
    BodyLengthMismatch,
    MalformedField,
    InvalidTag,
}

public readonly record struct FixDecodeResult(
    bool Success,
    FixMessage? Message,
    int BytesConsumed,
    FixDecodeError Error)
{
    public static FixDecodeResult Incomplete => new(false, null, 0, FixDecodeError.Incomplete);
    public static FixDecodeResult Failure(FixDecodeError error) => new(false, null, 0, error);
    public static FixDecodeResult Completed(FixMessage message, int bytesConsumed) => new(true, message, bytesConsumed, FixDecodeError.None);
}

public static class FixMessageCodec
{
    public const byte Soh = 0x01;
    public const string BeginString = "FIX.4.4";
    private static readonly Encoding Ascii = Encoding.ASCII;

    public static byte[] Encode(FixMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        string beginString = message.TryGetString(FixTags.BeginString, out var begin)
            ? begin!
            : BeginString;

        if (!message.TryGetString(FixTags.MsgType, out var msgType) || string.IsNullOrEmpty(msgType))
            throw new InvalidOperationException("FIX message requires MsgType (35) before encoding.");

        int bodyLength = CalculateBodyLength(message, msgType);
        byte[] payload = new byte[FixFrameEncoding.GetFrameLength(beginString.Length, bodyLength)];
        int offset = WritePrefix(payload, beginString, bodyLength);
        offset += WriteField(payload.AsSpan(offset), FixTags.MsgType, msgType);

        IReadOnlyList<FixField> fields = message.Fields;
        for (int i = 0; i < fields.Count; i++)
        {
            FixField field = fields[i];
            if (ShouldSkipEncodeField(field))
                continue;

            offset += WriteField(payload.AsSpan(offset), field.Tag, field.Value);
        }

        int checksumOffset = offset;
        int checksum = FixFrameEncoding.CalculateChecksum(payload.AsSpan(0, checksumOffset));
        FixFrameEncoding.WriteChecksumField(payload.AsSpan(checksumOffset), checksum);
        return payload;
    }

    public static FixDecodeResult Decode(ReadOnlySpan<byte> buffer)
    {
        if (buffer.IsEmpty)
            return FixDecodeResult.Incomplete;

        int firstEnd = buffer.IndexOf(Soh);
        if (firstEnd < 0)
            return FixDecodeResult.Incomplete;

        if (!TryParseField(buffer[..firstEnd], out var beginField))
            return FixDecodeResult.Failure(FixDecodeError.MalformedField);
        if (beginField.Tag != FixTags.BeginString)
            return FixDecodeResult.Failure(FixDecodeError.MissingBeginString);
        if (!string.Equals(beginField.Value, BeginString, StringComparison.Ordinal))
            return FixDecodeResult.Failure(FixDecodeError.InvalidBeginString);

        int secondStart = firstEnd + 1;
        int secondEndOffset = buffer[secondStart..].IndexOf(Soh);
        if (secondEndOffset < 0)
            return FixDecodeResult.Incomplete;
        int secondEnd = secondStart + secondEndOffset;

        if (!TryParseField(buffer[secondStart..secondEnd], out var bodyLengthField))
            return FixDecodeResult.Failure(FixDecodeError.MalformedField);
        if (bodyLengthField.Tag != FixTags.BodyLength)
            return FixDecodeResult.Failure(FixDecodeError.MissingBodyLength);
        if (!int.TryParse(bodyLengthField.Value, NumberStyles.None, CultureInfo.InvariantCulture, out int bodyLength) || bodyLength < 0)
            return FixDecodeResult.Failure(FixDecodeError.InvalidBodyLength);

        int bodyStart = secondEnd + 1;
        int checksumFieldStart = bodyStart + bodyLength;
        if (checksumFieldStart + 7 > buffer.Length)
            return FixDecodeResult.Incomplete;
        if (buffer[checksumFieldStart] != (byte)'1' || buffer[checksumFieldStart + 1] != (byte)'0' || buffer[checksumFieldStart + 2] != (byte)'=')
            return FixDecodeResult.Failure(FixDecodeError.BodyLengthMismatch);

        int checksumFieldEndOffset = buffer[checksumFieldStart..].IndexOf(Soh);
        if (checksumFieldEndOffset < 0)
            return FixDecodeResult.Incomplete;
        int checksumFieldEnd = checksumFieldStart + checksumFieldEndOffset;
        int totalLength = checksumFieldEnd + 1;
        if (totalLength > buffer.Length)
            return FixDecodeResult.Incomplete;

        ReadOnlySpan<byte> checksumSpan = buffer[(checksumFieldStart + 3)..checksumFieldEnd];
        if (checksumSpan.Length != 3 ||
            !int.TryParse(Encoding.ASCII.GetString(checksumSpan), NumberStyles.None, CultureInfo.InvariantCulture, out int expectedChecksum))
            return FixDecodeResult.Failure(FixDecodeError.InvalidCheckSum);

        int actualChecksum = FixFrameEncoding.CalculateChecksum(buffer[..checksumFieldStart]);
        if (actualChecksum != expectedChecksum)
            return FixDecodeResult.Failure(FixDecodeError.CheckSumMismatch);

        var message = new FixMessage();
        message.Add(beginField.Tag, beginField.Value);
        message.Add(bodyLengthField.Tag, bodyLengthField.Value);

        int cursor = bodyStart;
        bool sawMsgType = false;
        while (cursor < checksumFieldStart)
        {
            int fieldEndOffset = buffer[cursor..checksumFieldStart].IndexOf(Soh);
            if (fieldEndOffset < 0)
                return FixDecodeResult.Failure(FixDecodeError.BodyLengthMismatch);

            int fieldEnd = cursor + fieldEndOffset;
            if (!TryParseField(buffer[cursor..fieldEnd], out var field))
                return FixDecodeResult.Failure(FixDecodeError.MalformedField);

            if (!sawMsgType)
            {
                if (field.Tag != FixTags.MsgType)
                    return FixDecodeResult.Failure(FixDecodeError.MissingMsgType);
                sawMsgType = true;
            }

            message.Add(field.Tag, field.Value);
            cursor = fieldEnd + 1;
        }

        if (!sawMsgType)
            return FixDecodeResult.Failure(FixDecodeError.MissingMsgType);

        message.Add(FixTags.CheckSum, expectedChecksum.ToString("000", CultureInfo.InvariantCulture));
        return FixDecodeResult.Completed(message, totalLength);
    }

    private static int CalculateBodyLength(FixMessage message, string msgType)
    {
        int bodyLength = GetFieldLength(FixTags.MsgType, msgType);
        IReadOnlyList<FixField> fields = message.Fields;
        for (int i = 0; i < fields.Count; i++)
        {
            FixField field = fields[i];
            if (ShouldSkipEncodeField(field))
                continue;

            bodyLength += GetFieldLength(field.Tag, field.Value);
        }

        return bodyLength;
    }

    private static bool TryParseField(ReadOnlySpan<byte> span, out FixField field)
    {
        int equals = span.IndexOf((byte)'=');
        if (equals <= 0)
        {
            field = default;
            return false;
        }

        if (!int.TryParse(Encoding.ASCII.GetString(span[..equals]), NumberStyles.None, CultureInfo.InvariantCulture, out int tag))
        {
            field = default;
            return false;
        }

        field = new FixField(tag, Encoding.ASCII.GetString(span[(equals + 1)..]));
        return true;
    }

    private static bool ShouldSkipEncodeField(FixField field)
    {
        if (field.Tag is FixTags.BeginString or FixTags.BodyLength or FixTags.CheckSum)
            return true;

        return field.Tag == FixTags.MsgType;
    }

    private static int GetFieldLength(int tag, string value)
        => CountDigits(tag) + 1 + value.Length + 1;

    private static int WritePrefix(Span<byte> destination, string beginString, int bodyLength)
    {
        int offset = 0;
        destination[offset++] = (byte)'8';
        destination[offset++] = (byte)'=';
        offset += Ascii.GetBytes(beginString.AsSpan(), destination[offset..]);
        destination[offset++] = Soh;
        destination[offset++] = (byte)'9';
        destination[offset++] = (byte)'=';
        if (!Utf8Formatter.TryFormat(bodyLength, destination[offset..], out int bodyLengthDigits))
            throw new InvalidOperationException("Unable to format FIX body length.");

        offset += bodyLengthDigits;
        destination[offset++] = Soh;
        return offset;
    }

    private static int WriteField(Span<byte> destination, int tag, string value)
    {
        if (!Utf8Formatter.TryFormat(tag, destination, out int offset))
            throw new InvalidOperationException("Unable to format FIX tag.");

        destination[offset++] = (byte)'=';
        offset += Ascii.GetBytes(value.AsSpan(), destination[offset..]);
        destination[offset++] = Soh;
        return offset;
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
