using BenchmarkDotNet.Attributes;
using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class FixMessageEncoderBenchmarks
{
    [Params(0, 16, 64)]
    public int EntryCount;

    private FixMessage _message = null!;
    private FixMessageEncoder _encoder = null!;

    [GlobalSetup]
    public void Setup()
    {
        _encoder = new FixMessageEncoder();
        _message = FixMessageCodecBenchmarks.CreateMessage(EntryCount);
    }

    [Benchmark]
    public int EncodeReusable()
        => _encoder.Encode(_message).Length;

    [GlobalCleanup]
    public void Cleanup()
        => _encoder.Dispose();
}
