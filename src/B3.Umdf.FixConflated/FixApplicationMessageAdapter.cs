namespace B3.Umdf.FixConflated;

internal static class FixApplicationMessageAdapter
{
    public static FixMessage FromEncodedFrame(ReadOnlySpan<byte> encodedFrame)
    {
        FixDecodeResult decoded = FixMessageCodec.Decode(encodedFrame);
        if (!decoded.Success || decoded.Message is null)
            throw new InvalidOperationException($"Unable to decode FIX application frame: {decoded.Error}.");

        return StripSessionEnvelope(decoded.Message);
    }

    public static FixMessage StripSessionEnvelope(FixMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var applicationMessage = new FixMessage();
        foreach (FixField field in message.Fields)
        {
            if (field.Tag is FixTags.BeginString
                or FixTags.BodyLength
                or FixTags.SenderCompId
                or FixTags.TargetCompId
                or FixTags.MsgSeqNum
                or FixTags.SendingTime
                or FixTags.CheckSum
                or FixTags.PossDupFlag)
            {
                continue;
            }

            applicationMessage.Add(field.Tag, field.Value);
        }

        return applicationMessage;
    }
}
