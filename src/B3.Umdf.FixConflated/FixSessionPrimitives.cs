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
    public const int OrderId = 37;
    public const int SecurityId = 48;
    public const int SenderCompId = 49;
    public const int Symbol = 55;
    public const int Text = 58;
    public const int TargetCompId = 56;
    public const int MsgSeqNum = 34;
    public const int SendingTime = 52;
    public const int PossDupFlag = 43;
    public const int EncryptMethod = 98;
    public const int HeartBtInt = 108;
    public const int TestReqId = 112;
    public const int DeliverToCompID = 128;
    public const int GapFillFlag = 123;
    public const int ResetSeqNumFlag = 141;
    public const int BeginSeqNo = 7;
    public const int EndSeqNo = 16;
    public const int NewSeqNo = 36;
    public const int NextExpectedMsgSeqNum = 789;
    public const int MDReqId = 262;
    public const int NoMDEntries = 268;
    public const int MDEntryType = 269;
    public const int MDEntryPx = 270;
    public const int MDEntrySize = 271;
    public const int MDEntryDate = 272;
    public const int MDEntryTime = 273;
    public const int QuoteCondition = 276;
    public const int TradeCondition = 277;
    public const int MDUpdateAction = 279;
    public const int OpenCloseSettlFlag = 286;
    public const int MDEntrySeller = 289;
    public const int MDEntryPositionNo = 290;
    public const int SecurityExchange = 207;
    public const int SecurityIdSource = 22;
    public const int TradeDate = 75;
    public const int TradingSessionId = 336;
    public const int NumberOfOrders = 346;
    public const int TradingSessionSubId = 625;
    public const int TotNumReports = 911;
    public const int MDBookType = 1021;
    public const int SecurityGroup = 1151;
    public const int MDStreamId = 1500;
    public const int CheckSum = 10;
    public const int TradeId = 1003;
    public const int LastTradeDate = 9325;
    public const int MDInsertDate = 37016;
    public const int MDInsertTime = 37017;
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
    public const string MarketDataSnapshotFullRefresh = "W";
    public const string MarketDataIncrementalRefresh = "X";
}

internal static class FixValueFormatting
{
    public static string FormatUtcTimestamp(DateTimeOffset value)
        => value.UtcDateTime.ToString("yyyyMMdd-HH:mm:ss.fff", CultureInfo.InvariantCulture);

    public static bool TryFormatUtcTimestamp(DateTimeOffset value, Span<byte> destination, out int written)
    {
        if (destination.Length < 21)
        {
            written = 0;
            return false;
        }

        DateTime utc = value.UtcDateTime;
        Write4(destination, utc.Year);
        Write2(destination[4..], utc.Month);
        Write2(destination[6..], utc.Day);
        destination[8] = (byte)'-';
        Write2(destination[9..], utc.Hour);
        destination[11] = (byte)':';
        Write2(destination[12..], utc.Minute);
        destination[14] = (byte)':';
        Write2(destination[15..], utc.Second);
        destination[17] = (byte)'.';
        Write3(destination[18..], utc.Millisecond);
        written = 21;
        return true;
    }

    public static bool TryFormatUtcDate(DateTimeOffset value, Span<byte> destination, out int written)
    {
        if (destination.Length < 8)
        {
            written = 0;
            return false;
        }

        DateTime utc = value.UtcDateTime;
        Write4(destination, utc.Year);
        Write2(destination[4..], utc.Month);
        Write2(destination[6..], utc.Day);
        written = 8;
        return true;
    }

    public static bool TryFormatUtcTime(DateTimeOffset value, Span<byte> destination, out int written)
    {
        if (destination.Length < 12)
        {
            written = 0;
            return false;
        }

        DateTime utc = value.UtcDateTime;
        Write2(destination, utc.Hour);
        destination[2] = (byte)':';
        Write2(destination[3..], utc.Minute);
        destination[5] = (byte)':';
        Write2(destination[6..], utc.Second);
        destination[8] = (byte)'.';
        Write3(destination[9..], utc.Millisecond);
        written = 12;
        return true;
    }

    private static void Write2(Span<byte> destination, int value)
    {
        destination[0] = (byte)('0' + (value / 10));
        destination[1] = (byte)('0' + (value % 10));
    }

    private static void Write3(Span<byte> destination, int value)
    {
        destination[0] = (byte)('0' + (value / 100));
        destination[1] = (byte)('0' + ((value / 10) % 10));
        destination[2] = (byte)('0' + (value % 10));
    }

    private static void Write4(Span<byte> destination, int value)
    {
        destination[0] = (byte)('0' + ((value / 1000) % 10));
        destination[1] = (byte)('0' + ((value / 100) % 10));
        destination[2] = (byte)('0' + ((value / 10) % 10));
        destination[3] = (byte)('0' + (value % 10));
    }
}
