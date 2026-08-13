using System.Threading.Channels;
using BenchmarkDotNet.Attributes;

namespace B3.Umdf.FixConflated.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class FixOutboundFrameQueueBenchmarks
{
    [Params(64, 256, 1024)]
    public int MessageCount;

    private byte[][] _payloads = null!;
    private Channel<byte[]> _channel = null!;
    private FixOutboundRing<byte[]> _ring = null!;

    [GlobalSetup]
    public void Setup()
    {
        _payloads = new byte[MessageCount][];
        for (int i = 0; i < _payloads.Length; i++)
        {
            _payloads[i] = new byte[192];
            _payloads[i][0] = (byte)i;
        }
    }

    [IterationSetup(Targets = [nameof(ChannelRoundTrip), nameof(RingRoundTrip)])]
    public void IterationSetup()
    {
        _channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(MessageCount)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
        _ring = new FixOutboundRing<byte[]>(MessageCount);
    }

    [IterationCleanup(Targets = [nameof(ChannelRoundTrip), nameof(RingRoundTrip)])]
    public void IterationCleanup()
    {
        _channel.Writer.TryComplete();
        _ring.Dispose();
    }

    [Benchmark(Baseline = true)]
    public int ChannelRoundTrip()
    {
        int totalBytes = 0;
        for (int i = 0; i < _payloads.Length; i++)
            _channel.Writer.TryWrite(_payloads[i]);

        while (_channel.Reader.TryRead(out byte[]? payload))
            totalBytes += payload.Length;

        return totalBytes;
    }

    [Benchmark]
    public int RingRoundTrip()
    {
        int totalBytes = 0;
        for (int i = 0; i < _payloads.Length; i++)
            _ring.TryEnqueue(_payloads[i]);

        while (_ring.TryDequeue(out byte[]? payload))
            totalBytes += payload.Length;

        return totalBytes;
    }
}
