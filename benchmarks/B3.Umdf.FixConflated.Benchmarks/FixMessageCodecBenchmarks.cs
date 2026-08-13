using System.Globalization;
using BenchmarkDotNet.Attributes;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class FixMessageCodecBenchmarks
{
    [Params(0, 16, 64)]
    public int EntryCount;

    private FixMessage _message = null!;

    [GlobalSetup]
    public void Setup()
        => _message = CreateMessage(EntryCount);

    internal static FixMessage CreateMessage(int entryCount)
    {
        var message = new FixMessage(8 + (entryCount * 8));
        message.Add(FixTags.BeginString, FixMessageCodec.BeginString);
        message.Add(FixTags.MsgType, FixMsgTypes.MarketDataIncrementalRefresh);
        message.Add(FixTags.SenderCompId, "SANDBOX");
        message.Add(FixTags.TargetCompId, "CLIENT-A");
        message.Add(FixTags.MsgSeqNum, 42);
        message.Add(FixTags.SendingTime, "20260813-17:30:00.123");
        message.Add(FixTags.MDReqId, "md-1234");
        message.Add(FixTags.NoMDEntries, entryCount);

        for (int i = 0; i < entryCount; i++)
        {
            message.Add(FixTags.MDUpdateAction, i % 3 == 0 ? "0" : "1");
            message.Add(FixTags.MDEntryType, (i & 1) == 0 ? "0" : "1");
            message.Add(FixTags.Symbol, "PETR4");
            message.Add(FixTags.SecurityId, "1234");
            message.Add(FixTags.MDEntryPx, (28.10m + (i * 0.01m)).ToString("F2", CultureInfo.InvariantCulture));
            message.Add(FixTags.MDEntrySize, (100 + i).ToString(CultureInfo.InvariantCulture));
            message.Add(FixTags.MDEntryDate, "20260813");
            message.Add(FixTags.MDEntryTime, "17:30:00.123");
        }

        return message;
    }

    [Benchmark]
    public int Encode()
        => FixMessageCodec.Encode(_message).Length;
}
