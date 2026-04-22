using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using B3.Umdf.Feed;
using B3.Umdf.Transport;

BenchmarkRunner.Run<MpscPacketRingBenchmarks>();

[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class MpscPacketRingBenchmarks
{
    // Synthetic packet payloads (small + large flavours to mirror real UMDF traffic).
    private static readonly byte[] SmallBuf = new byte[64];
    private static readonly byte[] LargeBuf = new byte[1400];

    [Params(1, 2, 4)]
    public int ProducerCount;

    [Params(100_000)]
    public int PacketsPerProducer;

    [Benchmark(Description = "MpscPacketRing: N producers -> 1 consumer")]
    public long Run()
    {
        var ring = new MpscPacketRing(capacity: 65_536);
        long total = ProducerCount * (long)PacketsPerProducer;

        var producers = new Thread[ProducerCount];
        for (int p = 0; p < ProducerCount; p++)
        {
            producers[p] = new Thread(() =>
            {
                for (int i = 0; i < PacketsPerProducer; i++)
                {
                    var pkt = new UmdfPacket
                    {
                        Data = (i & 1) == 0 ? SmallBuf : LargeBuf,
                        Channel = ChannelType.IncrementalA,
                        ChannelGroup = 0,
                        ReceivedTimestampTicks = i,
                    };
                    while (!ring.TryEnqueue(pkt)) Thread.SpinWait(8);
                }
            });
        }

        var cts = new CancellationTokenSource();
        long consumed = 0;
        var consumer = new Thread(() =>
        {
            while (consumed < total)
            {
                if (ring.TryDequeue(out _)) { consumed++; continue; }
                ring.WaitForItems(cts.Token);
            }
        });

        consumer.Start();
        foreach (var t in producers) t.Start();
        foreach (var t in producers) t.Join();
        // Wake the consumer in case it parked on the last iteration.
        ring.SignalShutdown();
        consumer.Join();
        ring.Dispose();
        return consumed;
    }
}
