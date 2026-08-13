using System.Text;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixMessageBatchEncoderTests
{
    [Fact]
    public void Append_Concatenates_EncodedFrames()
    {
        FixMessage first = CreateMessage("md-1", 7);
        FixMessage second = CreateMessage("md-2", 8);

        byte[] expected = [.. FixMessageCodec.Encode(first), .. FixMessageCodec.Encode(second)];

        using var encoder = new FixMessageBatchEncoder();
        encoder.Append(first);
        encoder.Append(second);

        Assert.Equal(Encoding.ASCII.GetString(expected), Encoding.ASCII.GetString(encoder.WrittenMemory.Span));
    }

    private static FixMessage CreateMessage(string mdReqId, int seqNum)
    {
        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixMsgTypes.MarketDataIncrementalRefresh);
        message.Add(FixTags.SenderCompId, "SERVER");
        message.Add(FixTags.TargetCompId, "CLIENT");
        message.Add(FixTags.MsgSeqNum, seqNum);
        message.Add(FixTags.SendingTime, "20260813-20:55:00.123");
        message.Add(FixTags.MDReqId, mdReqId);
        message.Add(FixTags.NoMDEntries, 0);
        return message;
    }
}
