using System.IO.Compression;
using System.Text;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixMessageCodecTests
{
    [Fact]
    public void EncodeDecode_RoundTrips_WithValidBodyLengthAndChecksum()
    {
        var message = new FixMessage();
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, FixMsgTypes.Logon);
        message.Add(FixTags.SenderCompId, "CLIENT");
        message.Add(FixTags.TargetCompId, "SERVER");
        message.Add(FixTags.MsgSeqNum, 1);
        message.Add(FixTags.SendingTime, "20260812-18:32:01.333");
        message.Add(FixTags.EncryptMethod, 0);
        message.Add(FixTags.HeartBtInt, 30);

        byte[] encoded = FixMessageCodec.Encode(message);
        string encodedText = Encoding.ASCII.GetString(encoded);

        Assert.StartsWith($"8=FIX.4.4{(char)FixMessageCodec.Soh}9=", encodedText, StringComparison.Ordinal);
        Assert.Contains($"{(char)FixMessageCodec.Soh}35=A{(char)FixMessageCodec.Soh}", encodedText, StringComparison.Ordinal);
        Assert.EndsWith(((char)FixMessageCodec.Soh).ToString(), encodedText, StringComparison.Ordinal);

        var decoded = FixMessageCodec.Decode(encoded);
        Assert.True(decoded.Success);
        Assert.Equal(encoded.Length, decoded.BytesConsumed);
        Assert.NotNull(decoded.Message);
        Assert.Equal("CLIENT", GetRequired(decoded.Message!, FixTags.SenderCompId));
        Assert.Equal("SERVER", GetRequired(decoded.Message!, FixTags.TargetCompId));
        Assert.Equal("30", GetRequired(decoded.Message!, FixTags.HeartBtInt));
    }

    [Fact]
    public void Decode_Rejects_InvalidChecksum()
    {
        var message = CreateHeartbeat(seqNum: 1);
        byte[] encoded = FixMessageCodec.Encode(message);
        encoded[^2] = encoded[^2] == (byte)'0' ? (byte)'1' : (byte)'0';

        var decoded = FixMessageCodec.Decode(encoded);

        Assert.False(decoded.Success);
        Assert.Equal(FixDecodeError.CheckSumMismatch, decoded.Error);
    }

    [Fact]
    public void Decode_Rejects_InvalidBodyLength()
    {
        var message = CreateHeartbeat(seqNum: 1);
        byte[] encoded = FixMessageCodec.Encode(message);
        int bodyLengthIndex = Array.IndexOf(encoded, (byte)'9');
        encoded[bodyLengthIndex + 2] = (byte)'0';

        var decoded = FixMessageCodec.Decode(encoded);

        Assert.False(decoded.Success);
        Assert.Equal(FixDecodeError.BodyLengthMismatch, decoded.Error);
    }

    [Fact]
    public void Compress_UsesZlibWrapper()
    {
        byte[] raw = FixMessageCodec.Encode(CreateHeartbeat(seqNum: 7));

        byte[] compressed = FixZlibCompression.Compress(raw);
        using var input = new MemoryStream(compressed);
        using var zlib = new ZLibStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        zlib.CopyTo(output);

        Assert.Equal(raw, output.ToArray());
    }

    private static FixMessage CreateHeartbeat(int seqNum)
    {
        var message = new FixMessage();
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, FixMsgTypes.Heartbeat);
        message.Add(FixTags.SenderCompId, "SERVER");
        message.Add(FixTags.TargetCompId, "CLIENT");
        message.Add(FixTags.MsgSeqNum, seqNum);
        message.Add(FixTags.SendingTime, "20260812-18:32:01.333");
        return message;
    }

    private static string GetRequired(FixMessage message, int tag)
    {
        Assert.True(message.TryGetString(tag, out string? value));
        return value!;
    }
}
