using BenchmarkDotNet.Attributes;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class FixMessageBatchEncoderBenchmarks
{
    [Params(1, 8, 32)]
    public int MessageCount;

    [Params(0, 16)]
    public int EntryCount;

    private FixMessage[] _messages = null!;
    private FixMessageEncoder _singleEncoder = null!;
    private FixMessageBatchEncoder _batchEncoder = null!;

    [GlobalSetup]
    public void Setup()
    {
        _singleEncoder = new FixMessageEncoder();
        _batchEncoder = new FixMessageBatchEncoder();
        _messages = new FixMessage[MessageCount];
        for (int i = 0; i < _messages.Length; i++)
            _messages[i] = FixMessageCodecBenchmarks.CreateMessage(EntryCount);
    }

    [Benchmark(Baseline = true)]
    public int EncodeIndividually()
    {
        int totalBytes = 0;
        for (int i = 0; i < _messages.Length; i++)
            totalBytes += _singleEncoder.Encode(_messages[i]).Length;

        return totalBytes;
    }

    [Benchmark]
    public int EncodeBatch()
    {
        _batchEncoder.Reset();
        for (int i = 0; i < _messages.Length; i++)
            _batchEncoder.Append(_messages[i]);

        return _batchEncoder.WrittenCount;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _singleEncoder.Dispose();
        _batchEncoder.Dispose();
    }
}
