using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

public sealed class FixSessionConnectionTests
{
    [Fact]
    public void Session_Flows_Logon_Heartbeat_TestRequest_Logout()
    {
        var clock = new FakeFixClock();
        var store = new FixSessionStateStore();
        var session = new FixSessionConnection(store, clock: clock);

        var logon = CreateInbound(FixMsgTypes.Logon, 1, message =>
        {
            message.Add(FixTags.EncryptMethod, 0);
            message.Add(FixTags.HeartBtInt, 30);
        });

        FixSessionUpdate logonUpdate = session.Receive(logon);

        var outboundLogon = Assert.Single(logonUpdate.OutboundMessages);
        Assert.False(logonUpdate.DisconnectTransport);
        Assert.Equal(FixMsgTypes.Logon, GetRequired(outboundLogon, FixTags.MsgType));
        Assert.Equal("SERVER", GetRequired(outboundLogon, FixTags.SenderCompId));
        Assert.Equal("CLIENT", GetRequired(outboundLogon, FixTags.TargetCompId));
        Assert.Equal("1", GetRequired(outboundLogon, FixTags.MsgSeqNum));

        clock.AdvanceSeconds(31);
        FixSessionUpdate heartbeatUpdate = session.Advance();
        var heartbeat = Assert.Single(heartbeatUpdate.OutboundMessages);
        Assert.Equal(FixMsgTypes.Heartbeat, GetRequired(heartbeat, FixTags.MsgType));
        Assert.Equal("2", GetRequired(heartbeat, FixTags.MsgSeqNum));

        clock.AdvanceSeconds(1);
        FixSessionUpdate inboundHeartbeat = session.Receive(CreateInbound(FixMsgTypes.Heartbeat, 2));
        Assert.Empty(inboundHeartbeat.OutboundMessages);

        clock.AdvanceSeconds(1);
        FixSessionUpdate testRequestUpdate = session.Receive(CreateInbound(FixMsgTypes.TestRequest, 3, message => message.Add(FixTags.TestReqId, "ping-1")));
        var echoedHeartbeat = Assert.Single(testRequestUpdate.OutboundMessages);
        Assert.Equal(FixMsgTypes.Heartbeat, GetRequired(echoedHeartbeat, FixTags.MsgType));
        Assert.Equal("ping-1", GetRequired(echoedHeartbeat, FixTags.TestReqId));

        clock.AdvanceSeconds(1);
        FixSessionUpdate logoutUpdate = session.Receive(CreateInbound(FixMsgTypes.Logout, 4));
        var logout = Assert.Single(logoutUpdate.OutboundMessages);
        Assert.True(logoutUpdate.DisconnectTransport);
        Assert.Equal(FixMsgTypes.Logout, GetRequired(logout, FixTags.MsgType));
        Assert.Equal(FixSessionState.Disconnected, session.State);
    }

    [Fact]
    public void ResendRequest_Replays_Buffered_ApplicationMessages()
    {
        var clock = new FakeFixClock();
        var session = CreateLoggedOnSession(clock, new FixSessionOptions { ApplicationResendBufferCapacity = 8 });

        Assert.True(session.TrySendApplication(CreateApplicationMessage("m1"), out FixSessionUpdate firstSend));
        Assert.True(session.TrySendApplication(CreateApplicationMessage("m2"), out _));
        Assert.True(session.TrySendApplication(CreateApplicationMessage("m3"), out _));

        clock.AdvanceSeconds(1);
        FixSessionUpdate resendUpdate = session.Receive(CreateInbound(FixMsgTypes.ResendRequest, 2, message =>
        {
            message.Add(FixTags.BeginSeqNo, 2);
            message.Add(FixTags.EndSeqNo, 4);
        }));

        Assert.Equal(3, resendUpdate.OutboundMessages.Count);
        Assert.All(resendUpdate.OutboundMessages, message => Assert.Equal("Y", GetRequired(message, FixTags.PossDupFlag)));
        Assert.Equal(new[] { "2", "3", "4" }, resendUpdate.OutboundMessages.Select(m => GetRequired(m, FixTags.MsgSeqNum)).ToArray());
        Assert.False(firstSend.OutboundMessages[0].TryGetString(FixTags.PossDupFlag, out _));
    }

    [Fact]
    public void ResendRequest_GapFills_Messages_Evicted_FromCurrentSessionBuffer()
    {
        var clock = new FakeFixClock();
        var session = CreateLoggedOnSession(clock, new FixSessionOptions { ApplicationResendBufferCapacity = 2 });

        Assert.True(session.TrySendApplication(CreateApplicationMessage("m1"), out _));
        Assert.True(session.TrySendApplication(CreateApplicationMessage("m2"), out _));
        Assert.True(session.TrySendApplication(CreateApplicationMessage("m3"), out _));

        clock.AdvanceSeconds(1);
        FixSessionUpdate resendUpdate = session.Receive(CreateInbound(FixMsgTypes.ResendRequest, 2, message =>
        {
            message.Add(FixTags.BeginSeqNo, 2);
            message.Add(FixTags.EndSeqNo, 4);
        }));

        Assert.Equal(3, resendUpdate.OutboundMessages.Count);

        FixMessage gapFill = resendUpdate.OutboundMessages[0];
        Assert.Equal(FixMsgTypes.SequenceReset, GetRequired(gapFill, FixTags.MsgType));
        Assert.Equal("Y", GetRequired(gapFill, FixTags.PossDupFlag));
        Assert.Equal("2", GetRequired(gapFill, FixTags.MsgSeqNum));
        Assert.Equal("3", GetRequired(gapFill, FixTags.NewSeqNo));

        Assert.Equal("3", GetRequired(resendUpdate.OutboundMessages[1], FixTags.MsgSeqNum));
        Assert.Equal("4", GetRequired(resendUpdate.OutboundMessages[2], FixTags.MsgSeqNum));
    }

    [Fact]
    public void Reconnect_WithLowerSequence_Disconnects_Immediately_WithoutLogout()
    {
        var clock = new FakeFixClock();
        var store = new FixSessionStateStore();
        var firstSession = new FixSessionConnection(store, clock: clock);

        _ = firstSession.Receive(CreateInbound(FixMsgTypes.Logon, 1, message =>
        {
            message.Add(FixTags.EncryptMethod, 0);
            message.Add(FixTags.HeartBtInt, 30);
        }));
        clock.AdvanceSeconds(1);
        _ = firstSession.Receive(CreateInbound(FixMsgTypes.Heartbeat, 2));
        clock.AdvanceSeconds(1);
        _ = firstSession.Receive(CreateInbound(FixMsgTypes.Logout, 3));

        var secondSession = new FixSessionConnection(store, clock: clock);
        FixSessionUpdate reconnect = secondSession.Receive(CreateInbound(FixMsgTypes.Logon, 1, message =>
        {
            message.Add(FixTags.EncryptMethod, 0);
            message.Add(FixTags.HeartBtInt, 30);
        }));

        Assert.True(reconnect.DisconnectTransport);
        Assert.Empty(reconnect.OutboundMessages);
        Assert.Equal(FixSessionState.Disconnected, secondSession.State);
    }

    private static FixSessionConnection CreateLoggedOnSession(FakeFixClock clock, FixSessionOptions options)
    {
        var session = new FixSessionConnection(new FixSessionStateStore(), options, clock);
        FixSessionUpdate logonUpdate = session.Receive(CreateInbound(FixMsgTypes.Logon, 1, message =>
        {
            message.Add(FixTags.EncryptMethod, 0);
            message.Add(FixTags.HeartBtInt, 30);
        }));
        Assert.Single(logonUpdate.OutboundMessages);
        return session;
    }

    private static FixMessage CreateInbound(string msgType, int seqNum, Action<FixMessage>? configure = null)
    {
        var message = new FixMessage();
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, msgType);
        message.Add(FixTags.SenderCompId, "CLIENT");
        message.Add(FixTags.TargetCompId, "SERVER");
        message.Add(FixTags.MsgSeqNum, seqNum);
        message.Add(FixTags.SendingTime, "20260812-18:32:01.333");
        configure?.Invoke(message);
        return message;
    }

    private static FixMessage CreateApplicationMessage(string mdReqId)
    {
        var message = new FixMessage();
        message.Add(FixTags.MsgType, "X");
        message.Add(262, mdReqId);
        message.Add(55, "PETR4");
        return message;
    }

    private static string GetRequired(FixMessage message, int tag)
    {
        Assert.True(message.TryGetString(tag, out string? value));
        return value!;
    }

    private sealed class FakeFixClock : IFixClock
    {
        private DateTimeOffset _utcNow = new(2026, 8, 12, 18, 32, 1, TimeSpan.Zero);
        private long _monotonicTicks;

        public DateTimeOffset UtcNow => _utcNow;
        public long MonotonicTicks => _monotonicTicks;

        public void AdvanceSeconds(int seconds)
        {
            _utcNow = _utcNow.AddSeconds(seconds);
            _monotonicTicks += seconds * 1000L;
        }
    }
}
