using System.Buffers.Binary;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed.Tests;

/// <summary>
/// Pins the dispatch-thread liveness contract: a single misbehaving packet
/// (handler throws) must not kill the per-group dispatch loop and silently
/// degrade the consumer until ring overflow drops packets. The pre-fix
/// behavior caught the exception OUTSIDE the loop, so the thread exited and
/// the producer-side lease backlog grew unobserved.
/// </summary>
public class MultiFeedManagerDispatchResilienceTests
{
    [Fact]
    public async Task HandlerException_DoesNotKillDispatchThread_AndIsCounted()
    {
        var handler = new ThrowingHandler();
        var packets = new[]
        {
            MakePacket(seqNum: 1),
            MakePacket(seqNum: 2),
            MakePacket(seqNum: 3),
        };
        using var source = new ListPacketSource(packets);
        var manager = new MultiFeedManager(source, new[] { 0 }, handler);
        // Bypass WaitInstrumentDefinition so IncrementalA packets are actually
        // dispatched (and the throwing OnPacketProcessed fires).
        manager.Handlers[0].SetStateForTesting(FeedState.Streaming);
        try
        {
            await manager.StartAsync();
            await manager.WaitForCompletionAsync();
            // Wait briefly for dispatch thread to fully drain; WaitForCompletion
            // returns once rings are empty but processing of the last packet
            // may complete a few microseconds later.
            for (int i = 0; i < 100 && handler.AttemptedPackets < 3; i++)
                await Task.Delay(2);
        }
        finally
        {
            await manager.DisposeAsync();
        }

        Assert.Equal(3, handler.AttemptedPackets);
        Assert.Equal(3, manager.DispatchHandlerExceptionCount(0));
    }

    [Fact]
    public async Task LiveDispatchThread_ReportsAliveAfterStart_AndNotAliveAfterStop()
    {
        var manager = new MultiFeedManager(new[] { 0 }, new NoopHandler());
        await manager.StartAsync();
        try
        {
            // Give the OS thread a moment to actually start.
            for (int i = 0; i < 50 && !manager.IsDispatchThreadAlive(0); i++)
                await Task.Delay(2);
            Assert.True(manager.IsDispatchThreadAlive(0));
        }
        finally
        {
            await manager.StopAsync();
            await manager.DisposeAsync();
        }
        Assert.False(manager.IsDispatchThreadAlive(0));
    }

    private static UmdfPacket MakePacket(uint seqNum)
    {
        var buf = new byte[PacketHeader.MESSAGE_SIZE];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(4), seqNum);
        return new UmdfPacket
        {
            Data = buf,
            Channel = ChannelType.IncrementalA,
            ChannelGroup = 0,
            ReceivedTimestampTicks = 1L,
        };
    }

    private sealed class ThrowingHandler : IFeedEventHandler
    {
        public int AttemptedPackets;
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        // OnPacketProcessed is called for every dispatched packet; throwing here
        // surfaces the exception up through FeedHandler.FeedPacket to the
        // dispatch loop, which is exactly the path we are hardening.
        public void OnPacketProcessed()
        {
            AttemptedPackets++;
            throw new InvalidOperationException("boom");
        }
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }

    private sealed class NoopHandler : IFeedEventHandler
    {
        public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId) { }
        public void OnPacketProcessed() { }
        public void OnSequenceReset() { }
        public void OnInstrumentDefinitionsComplete(int instrumentCount) { }
        public void OnSequenceVersionChanged(ushort newVersion) { }
    }

    private sealed class ListPacketSource : IPacketSource, ISyncPacketSource
    {
        private readonly Queue<UmdfPacket> _packets;
        public ListPacketSource(IEnumerable<UmdfPacket> packets)
        {
            _packets = new Queue<UmdfPacket>(packets);
        }
        public bool TryReceive(out UmdfPacket packet)
        {
            lock (_packets)
            {
                if (_packets.Count > 0)
                {
                    packet = _packets.Dequeue();
                    return true;
                }
            }
            packet = default;
            return false;
        }
        public ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
            => TryReceive(out var p)
                ? ValueTask.FromResult(p)
                : ValueTask.FromException<UmdfPacket>(new InvalidOperationException("drained"));
        public void Dispose() { }
    }
}
