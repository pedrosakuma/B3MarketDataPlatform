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

    public static byte[] Encode(FixMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        string beginString = message.TryGetString(FixTags.BeginString, out var begin)
            ? begin!
            : BeginString;

        if (!message.TryGetString(FixTags.MsgType, out var msgType) || string.IsNullOrEmpty(msgType))
            throw new InvalidOperationException("FIX message requires MsgType (35) before encoding.");

        List<FixField> fields = CollectFields(message, msgType);

        var bodyBuilder = new StringBuilder();
        bodyBuilder.Append("35=").Append(msgType).Append((char)Soh);
        foreach (var field in fields)
            AppendField(bodyBuilder, field);

        string body = bodyBuilder.ToString();
        string prefix = $"8={beginString}{(char)Soh}9={Encoding.ASCII.GetByteCount(body).ToString(CultureInfo.InvariantCulture)}{(char)Soh}";
        string messageWithoutChecksum = prefix + body;

        int checksum = 0;
        byte[] payloadBytes = Encoding.ASCII.GetBytes(messageWithoutChecksum);
        for (int i = 0; i < payloadBytes.Length; i++)
            checksum = (checksum + payloadBytes[i]) & 0xFF;

        string finalMessage = string.Create(
            CultureInfo.InvariantCulture,
            $"{messageWithoutChecksum}10={checksum:000}{(char)Soh}");

        return Encoding.ASCII.GetBytes(finalMessage);
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

        int actualChecksum = 0;
        for (int i = 0; i < checksumFieldStart; i++)
            actualChecksum = (actualChecksum + buffer[i]) & 0xFF;
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

    private static List<FixField> CollectFields(FixMessage message, string msgType)
    {
        var fields = new List<FixField>(message.Fields.Count);
        for (int i = 0; i < message.Fields.Count; i++)
        {
            var field = message.Fields[i];
            if (field.Tag is FixTags.BeginString or FixTags.BodyLength or FixTags.CheckSum)
                continue;
            if (field.Tag == FixTags.MsgType)
            {
                if (!string.Equals(field.Value, msgType, StringComparison.Ordinal))
                    continue;
                continue;
            }

            fields.Add(field);
        }

        return fields;
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

    private static void AppendField(StringBuilder builder, FixField field)
    {
        builder
            .Append(field.Tag.ToString(CultureInfo.InvariantCulture))
            .Append('=')
            .Append(field.Value)
            .Append((char)Soh);
    }
}
