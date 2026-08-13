using BenchmarkDotNet.Attributes;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class FixSessionConnectionBenchmarks
{
    [Params(0, 16, 64)]
    public int ApplicationFieldCount;

    private AdvancingFixClock _clock = null!;
    private FixSessionConnection _session = null!;
    private FixMessage _applicationMessage = null!;

    [GlobalSetup]
    public void Setup()
    {
        _clock = new AdvancingFixClock();
        _session = new FixSessionConnection(
            new FixSessionStateStore(),
            new FixSessionOptions { ApplicationResendBufferCapacity = 128 },
            _clock);
        _applicationMessage = CreateApplicationMessage(ApplicationFieldCount);

        FixSessionUpdate logonUpdate = _session.Receive(CreateInboundLogon());
        if (logonUpdate.OutboundMessages.Count != 1)
            throw new InvalidOperationException("Benchmark setup expected a successful FIX logon.");
    }

    [Benchmark]
    public int TrySendApplication()
    {
        _clock.AdvanceMilliseconds(1);
        if (!_session.TrySendApplication(_applicationMessage, out FixSessionUpdate update))
            throw new InvalidOperationException("Expected FIX application send to succeed.");

        return update.OutboundMessages[0].Fields.Count;
    }

    private static FixMessage CreateInboundLogon()
    {
        var message = new FixMessage(8);
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, FixMsgTypes.Logon);
        message.Add(FixTags.SenderCompId, "CLIENT");
        message.Add(FixTags.TargetCompId, "SERVER");
        message.Add(FixTags.MsgSeqNum, 1);
        message.Add(FixTags.SendingTime, "20260813-20:10:00.123");
        message.Add(FixTags.EncryptMethod, 0);
        message.Add(FixTags.HeartBtInt, 30);
        return message;
    }

    private static FixMessage CreateApplicationMessage(int applicationFieldCount)
    {
        var message = new FixMessage(2 + applicationFieldCount);
        message.Add(FixTags.MsgType, FixMsgTypes.MarketDataIncrementalRefresh);
        message.Add(FixTags.MDReqId, "md-bench");

        for (int i = 0; i < applicationFieldCount; i++)
            message.Add(1000 + i, $"value-{i}");

        return message;
    }
}
