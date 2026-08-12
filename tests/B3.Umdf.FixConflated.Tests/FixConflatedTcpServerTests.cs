using System.Globalization;
using System.Net;
using System.Net.Sockets;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixConflatedTcpServerTests
{
    [Fact]
    public async Task Logon_And_Broadcast_Are_Delivered_Over_Tcp()
    {
        var hub = new FixConflatedSessionHub();
        int port = GetFreeTcpPort();
        await using var server = new FixConflatedTcpServer(
            hub,
            new FixConflatedTcpServerOptions
            {
                OutboundQueueCapacity = 64,
                SessionOptions = new FixSessionOptions { ApplicationResendBufferCapacity = 8 },
            });
        await server.StartAsync(port);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, port);
        using NetworkStream stream = client.GetStream();

        await stream.WriteAsync(FixMessageCodec.Encode(CreateLogon("CLIENT-A", "SANDBOX", 1)));

        FixMessage logonAck = await ReadMessageAsync(stream);
        Assert.Equal(FixMsgTypes.Logon, GetRequired(logonAck, FixTags.MsgType));
        Assert.Equal("1", GetRequired(logonAck, FixTags.MsgSeqNum));
        Assert.Equal("SANDBOX", GetRequired(logonAck, FixTags.SenderCompId));
        Assert.Equal("CLIENT-A", GetRequired(logonAck, FixTags.TargetCompId));

        hub.BroadcastApplication(CreateApplicationMessage("md-1"));

        FixMessage incremental = await ReadMessageAsync(stream);
        Assert.Equal(FixMsgTypes.MarketDataIncrementalRefresh, GetRequired(incremental, FixTags.MsgType));
        Assert.Equal("2", GetRequired(incremental, FixTags.MsgSeqNum));
        Assert.Equal("md-1", GetRequired(incremental, FixTags.MDReqId));
    }

    private static FixMessage CreateLogon(string senderCompId, string targetCompId, int seqNum)
    {
        var message = new FixMessage();
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, FixMsgTypes.Logon);
        message.Add(FixTags.SenderCompId, senderCompId);
        message.Add(FixTags.TargetCompId, targetCompId);
        message.Add(FixTags.MsgSeqNum, seqNum);
        message.Add(FixTags.SendingTime, "20260812-19:30:00.000");
        message.Add(FixTags.EncryptMethod, 0);
        message.Add(FixTags.HeartBtInt, 30);
        return message;
    }

    private static FixMessage CreateApplicationMessage(string mdReqId)
    {
        var message = new FixMessage();
        message.Add(FixTags.MsgType, FixMsgTypes.MarketDataIncrementalRefresh);
        message.Add(FixTags.MDReqId, mdReqId);
        message.Add(FixTags.NoMDEntries, 0);
        return message;
    }

    private static async Task<FixMessage> ReadMessageAsync(NetworkStream stream)
    {
        byte[] buffer = new byte[4096];
        int buffered = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (true)
        {
            FixDecodeResult decoded = FixMessageCodec.Decode(buffer.AsSpan(0, buffered));
            if (decoded.Success)
                return decoded.Message!;
            if (decoded.Error != FixDecodeError.Incomplete)
                throw new Xunit.Sdk.XunitException($"Expected a full FIX frame but decode failed with {decoded.Error}.");

            int read = await stream.ReadAsync(buffer.AsMemory(buffered), cts.Token);
            Assert.True(read > 0, "Expected the server to send a FIX frame before closing the socket.");
            buffered += read;
        }
    }

    private static int GetFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static string GetRequired(FixMessage message, int tag)
    {
        Assert.True(message.TryGetString(tag, out string? value));
        return value!;
    }
}
