using B3.Umdf.Book;
using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Pins issue #48: per-trade <see cref="TradeFlags"/> survive the
/// recent-trades snapshot replay path. The <see cref="TradeRingBuffer"/>
/// slot stores the flag byte and <see cref="SnapshotEmitter.SendTradeHistory"/>
/// writes it into the replayed Trade frame (offset 36 = trailing flags byte).
/// </summary>
public class TradeHistoryFlagsTests
{
    [Fact]
    public void TradeRingBuffer_Add_RoundTripsFlags()
    {
        var ring = new TradeRingBuffer(4);
        ring.Add(price: 100, qty: 1, tradeId: 1, flags: (byte)TradeFlags.None);
        ring.Add(price: 101, qty: 2, tradeId: 2, flags: (byte)TradeFlags.AuctionPrint);
        ring.Add(price: 102, qty: 3, tradeId: 3, flags: 0xFF); // future bits preserved verbatim

        var slots = ring.AsSpan();
        Assert.Equal(3, slots.Length);
        Assert.Equal(0, slots[0].Flags);
        Assert.Equal((byte)TradeFlags.AuctionPrint, slots[1].Flags);
        Assert.Equal(0xFF, slots[2].Flags);
    }

    [Fact]
    public void TradeRingBuffer_Add_DefaultsFlagsToZero()
    {
        // Optional-parameter back-compat: existing callers that don't supply
        // flags must still get a zero-initialized slot.
        var ring = new TradeRingBuffer(2);
        ring.Add(price: 100, qty: 1, tradeId: 1);
        Assert.Equal(0, ring.AsSpan()[0].Flags);
    }

    [Fact]
    public async Task SnapshotEmitter_SendTradeHistory_EmitsFlagsByte()
    {
        // Direct test: populate a ring with mixed flags, replay through
        // SnapshotEmitter, and assert each Trade frame carries the slot's
        // flag byte at offset 36 (the trailing flags byte added in #47).
        const ulong SecurityId = 7777;
        var ring = new TradeRingBuffer(4);
        ring.Add(price: 100, qty: 1, tradeId: 10, flags: (byte)TradeFlags.None);
        ring.Add(price: 101, qty: 2, tradeId: 11, flags: (byte)TradeFlags.AuctionPrint);
        ring.Add(price: 102, qty: 3, tradeId: 12, flags: (byte)TradeFlags.AuctionPrint);

        var recorder = new RecordingWebSocket();
        var session = new ClientSession(recorder, channelCapacity: 16);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var writeLoop = Task.Run(() => session.RunWriteLoopAsync());

        Assert.True(SnapshotEmitter.SendTradeHistory(session, SecurityId, ring));

        // Drain: wait until all 3 Trade frames are flushed to the recorder.
        await WaitUntil(() => recorder.CountByType(MessageType.Trade) >= 3, TimeSpan.FromSeconds(2));
        session.Dispose();
        await writeLoop;

        Assert.Equal(3, recorder.CountByType(MessageType.Trade));
        var frames = recorder.AllFrames(MessageType.Trade);
        Assert.All(frames, f => Assert.Equal(41, f.Length));
        Assert.Equal(0, frames[0][40]);
        Assert.Equal((byte)TradeFlags.AuctionPrint, frames[1][40]);
        Assert.Equal((byte)TradeFlags.AuctionPrint, frames[2][40]);
    }

    private static async Task WaitUntil(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }
        Assert.True(predicate(), "timed out waiting for predicate");
    }
}
