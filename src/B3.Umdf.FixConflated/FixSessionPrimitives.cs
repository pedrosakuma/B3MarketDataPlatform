using System.Collections.Concurrent;
using System.Globalization;

namespace B3.Umdf.FixConflated;

public interface IFixClock
{
    DateTimeOffset UtcNow { get; }
    long MonotonicTicks { get; }
}

public sealed class SystemFixClock : IFixClock
{
    public static readonly SystemFixClock Instance = new();
    private SystemFixClock() { }
    public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
    public long MonotonicTicks => Environment.TickCount64;
}

public sealed class FixSessionOptions
{
    public const int DefaultHeartbeatIntervalSeconds = 30;
    public const int DefaultApplicationResendBufferCapacity = 10_000;

    public int DefaultHeartbeatIntervalSecondsValue { get; init; } = DefaultHeartbeatIntervalSeconds;
    public int ApplicationResendBufferCapacity { get; init; } = DefaultApplicationResendBufferCapacity;
}

public enum FixSessionState
{
    AwaitingLogon = 0,
    Active,
    LogoutSent,
    Disconnected,
}

public readonly record struct FixSessionIdentity(string SenderCompId, string TargetCompId);

public readonly record struct FixPersistentSessionState(int NextExpectedInboundSeqNum, int NextOutboundSeqNum);

public sealed class FixSessionStateStore
{
    private readonly ConcurrentDictionary<FixSessionIdentity, PersistentState> _states = new();

    public FixPersistentSessionState GetOrCreate(FixSessionIdentity identity)
    {
        PersistentState state = _states.GetOrAdd(identity, static _ => new PersistentState());
        lock (state.Sync)
            return new FixPersistentSessionState(state.NextExpectedInboundSeqNum, state.NextOutboundSeqNum);
    }

    public void Save(FixSessionIdentity identity, FixPersistentSessionState state)
    {
        PersistentState entry = _states.GetOrAdd(identity, static _ => new PersistentState());
        lock (entry.Sync)
        {
            entry.NextExpectedInboundSeqNum = state.NextExpectedInboundSeqNum;
            entry.NextOutboundSeqNum = state.NextOutboundSeqNum;
        }
    }

    private sealed class PersistentState
    {
        public object Sync { get; } = new();
        public int NextExpectedInboundSeqNum { get; set; } = 1;
        public int NextOutboundSeqNum { get; set; } = 1;
    }
}

public sealed class FixSessionUpdate
{
    public static readonly FixSessionUpdate None = new([], false, null, FixDecodeError.None);

    public FixSessionUpdate(
        IReadOnlyList<FixMessage> outboundMessages,
        bool disconnectTransport,
        string? disconnectReason,
        FixDecodeError decodeError)
    {
        OutboundMessages = outboundMessages;
        DisconnectTransport = disconnectTransport;
        DisconnectReason = disconnectReason;
        DecodeError = decodeError;
    }

    public IReadOnlyList<FixMessage> OutboundMessages { get; }
    public bool DisconnectTransport { get; }
    public string? DisconnectReason { get; }
    public FixDecodeError DecodeError { get; }
}

internal static class FixTags
{
    public const int BeginString = 8;
    public const int BodyLength = 9;
    public const int MsgType = 35;
    public const int SenderCompId = 49;
    public const int TargetCompId = 56;
    public const int MsgSeqNum = 34;
    public const int SendingTime = 52;
    public const int PossDupFlag = 43;
    public const int EncryptMethod = 98;
    public const int HeartBtInt = 108;
    public const int TestReqId = 112;
    public const int GapFillFlag = 123;
    public const int ResetSeqNumFlag = 141;
    public const int BeginSeqNo = 7;
    public const int EndSeqNo = 16;
    public const int NewSeqNo = 36;
    public const int NextExpectedMsgSeqNum = 789;
    public const int CheckSum = 10;
}

internal static class FixMsgTypes
{
    public const string Heartbeat = "0";
    public const string TestRequest = "1";
    public const string ResendRequest = "2";
    public const string Reject = "3";
    public const string SequenceReset = "4";
    public const string Logout = "5";
    public const string Logon = "A";
}

internal static class FixValueFormatting
{
    public static string FormatUtcTimestamp(DateTimeOffset value)
        => value.UtcDateTime.ToString("yyyyMMdd-HH:mm:ss.fff", CultureInfo.InvariantCulture);
}
