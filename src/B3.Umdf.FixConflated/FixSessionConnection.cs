using System.Globalization;
using System.Diagnostics.CodeAnalysis;

namespace B3.Umdf.FixConflated;

/// <summary>
/// Minimal acceptor-side FIX session engine for the sandbox channel. It keeps a
/// bounded in-memory resend buffer only for the current process/session and does
/// not attempt any cross-session replay beyond the B3-documented immediate
/// disconnect rule for reconnects that arrive with a lower-than-expected
/// inbound sequence number.
/// </summary>
public sealed class FixSessionConnection
{
    private readonly FixSessionStateStore _stateStore;
    private readonly IFixClock _clock;
    private readonly FixSessionOptions _options;
    private readonly Queue<int> _applicationSequenceOrder = new();
    private readonly Dictionary<int, FixMessage> _applicationMessages = new();

    private FixSessionIdentity? _identity;
    private FixPersistentSessionState _state;
    private int _heartbeatIntervalSeconds;
    private long _lastReceivedTicks;
    private long _lastSentTicks;

    public FixSessionConnection(
        FixSessionStateStore stateStore,
        FixSessionOptions? options = null,
        IFixClock? clock = null)
    {
        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _options = options ?? new FixSessionOptions();
        _clock = clock ?? SystemFixClock.Instance;
        _heartbeatIntervalSeconds = _options.DefaultHeartbeatIntervalSecondsValue;
        State = FixSessionState.AwaitingLogon;
    }

    public FixSessionState State { get; private set; }
    public FixSessionIdentity? Identity => _identity;
    public int NextExpectedInboundSeqNum => _state.NextExpectedInboundSeqNum;
    public int NextOutboundSeqNum => _state.NextOutboundSeqNum;
    public int HeartbeatIntervalSeconds => _heartbeatIntervalSeconds;

    public FixSessionUpdate ReceiveFrame(ReadOnlySpan<byte> frame)
    {
        var decode = FixMessageCodec.Decode(frame);
        if (!decode.Success)
        {
            State = FixSessionState.Disconnected;
            return new FixSessionUpdate([], true, "Invalid FIX frame.", decode.Error);
        }

        return Receive(decode.Message!);
    }

    public FixSessionUpdate Receive(FixMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (State == FixSessionState.Disconnected)
            return FixSessionUpdate.None;

        var nowUtc = _clock.UtcNow;
        long nowTicks = _clock.MonotonicTicks;
        _lastReceivedTicks = nowTicks;

        if (!TryGetRequiredString(message, FixTags.MsgType, out string? msgType) ||
            !TryGetRequiredInt(message, FixTags.MsgSeqNum, out int inboundSeqNum))
            return DisconnectWithLogout("Missing session header.", nowUtc, nowTicks);

        if (State == FixSessionState.AwaitingLogon)
        {
            if (!string.Equals(msgType, FixMsgTypes.Logon, StringComparison.Ordinal))
                return DisconnectWithLogout("First message must be Logon.", nowUtc, nowTicks);

            return HandleLogon(message, inboundSeqNum, nowUtc, nowTicks);
        }

        if (inboundSeqNum != _state.NextExpectedInboundSeqNum)
            return DisconnectWithLogout($"Unexpected inbound sequence number {inboundSeqNum.ToString(CultureInfo.InvariantCulture)}.", nowUtc, nowTicks);

        _state = _state with { NextExpectedInboundSeqNum = inboundSeqNum + 1 };
        PersistState();

        return msgType switch
        {
            FixMsgTypes.Heartbeat => FixSessionUpdate.None,
            FixMsgTypes.TestRequest => HandleTestRequest(message, nowUtc, nowTicks),
            FixMsgTypes.ResendRequest => HandleResendRequest(message),
            FixMsgTypes.Logout => HandleLogout(nowUtc, nowTicks),
            FixMsgTypes.Reject => FixSessionUpdate.None,
            FixMsgTypes.Logon => DisconnectImmediately("Unexpected secondary Logon."),
            _ => DisconnectWithLogout($"Unsupported session MsgType {msgType}.", nowUtc, nowTicks),
        };
    }

    public FixSessionUpdate Advance()
    {
        if (State is FixSessionState.AwaitingLogon or FixSessionState.Disconnected)
            return FixSessionUpdate.None;

        var nowUtc = _clock.UtcNow;
        long nowTicks = _clock.MonotonicTicks;
        long heartbeatWindow = _heartbeatIntervalSeconds * 1000L;

        if (nowTicks - _lastReceivedTicks >= heartbeatWindow * 2)
            return DisconnectWithLogout("Peer heartbeat timeout.", nowUtc, nowTicks);

        if (nowTicks - _lastSentTicks < heartbeatWindow)
            return FixSessionUpdate.None;

        var heartbeat = CreateLiveMessage(FixMsgTypes.Heartbeat, nowUtc, static _ => { });
        return UpdateWith(heartbeat, disconnect: false, reason: null);
    }

    public bool TrySendApplication(FixMessage applicationMessage, out FixSessionUpdate update)
    {
        ArgumentNullException.ThrowIfNull(applicationMessage);
        if (State != FixSessionState.Active)
        {
            update = FixSessionUpdate.None;
            return false;
        }

        var nowUtc = _clock.UtcNow;
        long nowTicks = _clock.MonotonicTicks;
        int seqNum = _state.NextOutboundSeqNum;
        _state = _state with { NextOutboundSeqNum = seqNum + 1 };
        PersistState();

        var outbound = CreateOutgoingMessage(
            GetRequiredIdentity(),
            applicationMessage,
            seqNum,
            nowUtc,
            possDup: false);

        StoreApplicationMessage(seqNum, outbound);
        _lastSentTicks = nowTicks;
        update = UpdateWith(outbound, disconnect: false, reason: null);
        return true;
    }

    private FixSessionUpdate HandleLogon(FixMessage message, int inboundSeqNum, DateTimeOffset nowUtc, long nowTicks)
    {
        if (!TryGetRequiredString(message, FixTags.SenderCompId, out string? senderCompId) ||
            !TryGetRequiredString(message, FixTags.TargetCompId, out string? targetCompId) ||
            !TryGetRequiredInt(message, FixTags.HeartBtInt, out int heartbeatIntervalSeconds) ||
            !TryGetRequiredInt(message, FixTags.EncryptMethod, out int encryptMethod))
            return DisconnectImmediately("Malformed Logon.");

        if (heartbeatIntervalSeconds <= 0 || encryptMethod != 0)
            return DisconnectImmediately("Malformed Logon.");

        var identity = new FixSessionIdentity(senderCompId!, targetCompId!);
        FixPersistentSessionState persistent = _stateStore.GetOrCreate(identity);
        if (inboundSeqNum < persistent.NextExpectedInboundSeqNum)
            return DisconnectImmediately("Reconnect sequence number lower than expected.");

        _identity = identity;
        _state = persistent with { NextExpectedInboundSeqNum = inboundSeqNum + 1 };
        _heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        _lastReceivedTicks = nowTicks;

        bool resetRequested = message.TryGetBoolean(FixTags.ResetSeqNumFlag, out bool resetValue) && resetValue;
        if (resetRequested && persistent.NextExpectedInboundSeqNum == 1 && persistent.NextOutboundSeqNum == 1)
            _state = new FixPersistentSessionState(2, 1);

        PersistState();
        State = FixSessionState.Active;

        var logon = CreateLiveMessage(FixMsgTypes.Logon, nowUtc, outbound =>
        {
            outbound.Add(FixTags.EncryptMethod, 0);
            outbound.Add(FixTags.HeartBtInt, heartbeatIntervalSeconds);
            if (resetRequested && persistent.NextExpectedInboundSeqNum == 1 && persistent.NextOutboundSeqNum == 1)
                outbound.AddBoolean(FixTags.ResetSeqNumFlag, true);
        });

        return UpdateWith(logon, disconnect: false, reason: null);
    }

    private FixSessionUpdate HandleTestRequest(FixMessage message, DateTimeOffset nowUtc, long nowTicks)
    {
        if (!TryGetRequiredString(message, FixTags.TestReqId, out string? testReqId))
            return DisconnectWithLogout("Malformed TestRequest.", nowUtc, nowTicks);

        var heartbeat = CreateLiveMessage(FixMsgTypes.Heartbeat, nowUtc, outbound => outbound.Add(FixTags.TestReqId, testReqId!));
        return UpdateWith(heartbeat, disconnect: false, reason: null);
    }

    private FixSessionUpdate HandleResendRequest(FixMessage message)
    {
        if (!TryGetRequiredInt(message, FixTags.BeginSeqNo, out int beginSeqNo) ||
            !TryGetRequiredInt(message, FixTags.EndSeqNo, out int endSeqNo))
            return DisconnectImmediately("Malformed ResendRequest.");

        if (beginSeqNo <= 0)
            return DisconnectImmediately("Malformed ResendRequest.");

        int highestSentSeqNum = _state.NextOutboundSeqNum - 1;
        if (highestSentSeqNum <= 0)
            return FixSessionUpdate.None;

        int requestedEnd = endSeqNo == 0 ? highestSentSeqNum : Math.Min(endSeqNo, highestSentSeqNum);
        if (requestedEnd < beginSeqNo)
            return FixSessionUpdate.None;

        List<FixMessage> outbound = new((requestedEnd - beginSeqNo) + 1);
        int cursor = beginSeqNo;
        while (cursor <= requestedEnd)
        {
            if (_applicationMessages.TryGetValue(cursor, out var stored))
            {
                outbound.Add(CloneForReplay(stored));
                cursor++;
                continue;
            }

            int gapStart = cursor;
            while (cursor <= requestedEnd && !_applicationMessages.ContainsKey(cursor))
                cursor++;

            outbound.Add(CreateGapFill(gapStart, cursor));
        }

        return outbound.Count == 0
            ? FixSessionUpdate.None
            : new FixSessionUpdate(outbound, false, null, FixDecodeError.None);
    }

    private FixSessionUpdate HandleLogout(DateTimeOffset nowUtc, long nowTicks)
    {
        if (State == FixSessionState.LogoutSent)
        {
            State = FixSessionState.Disconnected;
            return new FixSessionUpdate([], true, null, FixDecodeError.None);
        }

        var logout = CreateLiveMessage(FixMsgTypes.Logout, nowUtc, static _ => { });
        State = FixSessionState.Disconnected;
        _lastSentTicks = nowTicks;
        return new FixSessionUpdate([logout], true, null, FixDecodeError.None);
    }

    private FixSessionUpdate DisconnectWithLogout(string reason, DateTimeOffset nowUtc, long nowTicks)
    {
        if (State == FixSessionState.Disconnected)
            return new FixSessionUpdate([], true, reason, FixDecodeError.None);

        if (_identity is null)
            return DisconnectImmediately(reason);

        var logout = CreateLiveMessage(FixMsgTypes.Logout, nowUtc, outbound => outbound.Add(58, reason));
        State = FixSessionState.Disconnected;
        _lastSentTicks = nowTicks;
        return new FixSessionUpdate([logout], true, reason, FixDecodeError.None);
    }

    private FixSessionUpdate DisconnectImmediately(string reason)
    {
        State = FixSessionState.Disconnected;
        return new FixSessionUpdate([], true, reason, FixDecodeError.None);
    }

    private FixMessage CreateLiveMessage(string msgType, DateTimeOffset nowUtc, Action<FixMessage> populate)
    {
        int seqNum = _state.NextOutboundSeqNum;
        _state = _state with { NextOutboundSeqNum = seqNum + 1 };
        PersistState();

        var outbound = CreateOutgoingMessage(GetRequiredIdentity(), CreateBareMessage(msgType, populate), seqNum, nowUtc, possDup: false);
        _lastSentTicks = _clock.MonotonicTicks;
        return outbound;
    }

    private FixMessage CreateGapFill(int gapStart, int newSeqNo)
    {
        var outbound = CreateBareMessage(FixMsgTypes.SequenceReset, message =>
        {
            message.AddBoolean(FixTags.GapFillFlag, true);
            message.Add(FixTags.NewSeqNo, newSeqNo);
        });

        return CreateOutgoingMessage(GetRequiredIdentity(), outbound, gapStart, _clock.UtcNow, possDup: true);
    }

    private static FixMessage CloneForReplay(FixMessage message)
    {
        var replay = new FixMessage(message, capacity: 1);
        replay.AddBoolean(FixTags.PossDupFlag, true);
        return replay;
    }

    private static FixMessage CreateBareMessage(string msgType, Action<FixMessage> populate)
    {
        var message = new FixMessage();
        message.Add(FixTags.MsgType, msgType);
        populate(message);
        return message;
    }

    private static FixMessage CreateOutgoingMessage(
        FixSessionIdentity identity,
        FixMessage payload,
        int seqNum,
        DateTimeOffset nowUtc,
        bool possDup)
    {
        var outbound = new FixMessage(payload, capacity: 5 + (possDup ? 1 : 0));
        outbound.Add(FixTags.MsgType, GetRequiredString(payload, FixTags.MsgType));
        outbound.Add(FixTags.SenderCompId, identity.TargetCompId);
        outbound.Add(FixTags.TargetCompId, identity.SenderCompId);
        outbound.Add(FixTags.MsgSeqNum, seqNum);
        outbound.Add(FixTags.SendingTime, FixValueFormatting.FormatUtcTimestamp(nowUtc));
        if (possDup)
            outbound.AddBoolean(FixTags.PossDupFlag, true);

        return outbound;
    }

    private void StoreApplicationMessage(int seqNum, FixMessage message)
    {
        if (_options.ApplicationResendBufferCapacity <= 0)
            return;

        _applicationMessages[seqNum] = message;
        _applicationSequenceOrder.Enqueue(seqNum);
        while (_applicationSequenceOrder.Count > _options.ApplicationResendBufferCapacity)
        {
            int evicted = _applicationSequenceOrder.Dequeue();
            _applicationMessages.Remove(evicted);
        }
    }

    private FixSessionIdentity GetRequiredIdentity()
        => _identity ?? throw new InvalidOperationException("Session identity not established.");

    private void PersistState()
    {
        if (_identity is { } identity)
            _stateStore.Save(identity, _state);
    }

    private static bool TryGetRequiredString(FixMessage message, int tag, [NotNullWhen(true)] out string? value)
    {
        if (message.TryGetString(tag, out value) && !string.IsNullOrEmpty(value))
            return true;

        value = null;
        return false;
    }

    private static string GetRequiredString(FixMessage message, int tag)
    {
        if (TryGetRequiredString(message, tag, out var value))
            return value!;

        throw new InvalidOperationException($"Missing FIX field {tag.ToString(CultureInfo.InvariantCulture)}.");
    }

    private static bool TryGetRequiredInt(FixMessage message, int tag, out int value)
    {
        if (message.TryGetInt32(tag, out value))
            return true;

        value = 0;
        return false;
    }

    private static FixSessionUpdate UpdateWith(FixMessage outbound, bool disconnect, string? reason)
        => new([outbound], disconnect, reason, FixDecodeError.None);
}
