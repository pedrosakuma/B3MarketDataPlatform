using System.Text;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixMessageEncoderTests
{
    [Fact]
    public void Encode_Matches_Codec_Output()
    {
        var message = new FixMessage();
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, FixMsgTypes.MarketDataIncrementalRefresh);
        message.Add(FixTags.SenderCompId, "SERVER");
        message.Add(FixTags.TargetCompId, "CLIENT");
        message.Add(FixTags.MsgSeqNum, 7);
        message.Add(FixTags.SendingTime, "20260813-17:30:00.123");
        message.Add(FixTags.MDReqId, "md-1");
        message.Add(FixTags.NoMDEntries, 1);
        message.Add(FixTags.MDUpdateAction, "0");
        message.Add(FixTags.MDEntryType, "0");
        message.Add(FixTags.Symbol, "PETR4");
        message.Add(FixTags.SecurityId, "1234");
        message.Add(FixTags.MDEntryPx, "28.10");
        message.Add(FixTags.MDEntrySize, "100");
        message.Add(FixTags.MDEntryDate, "20260813");
        message.Add(FixTags.MDEntryTime, "17:30:00.123");

        byte[] encoded = FixMessageCodec.Encode(message);

        using var encoder = new FixMessageEncoder();
        ReadOnlyMemory<byte> reusable = encoder.Encode(message);

        Assert.Equal(Encoding.ASCII.GetString(encoded), Encoding.ASCII.GetString(reusable.Span));
    }
}
