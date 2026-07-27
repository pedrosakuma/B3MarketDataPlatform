using System.Buffers.Binary;
using B3.MarketData.Wire;

namespace B3.Umdf.Server.Tests;

public class CadenceConflationBufferTests
{
    [Fact]
    public void Flush_LastValueWinsAndClearPrecedesPostClearLevel()
    {
        const ulong securityId = 77;
        var buffer = new CadenceConflationBuffer(100);
        Span<byte> frame = stackalloc byte[WireProtocol.LevelUpdateSize];

        int len = WireProtocol.WriteLevelUpdate(frame, securityId, side: 0, price: 1000, totalQty: 5, orderCount: 1);
        buffer.Buffer(securityId, frame[..len], batchSequence: 1, epoch: 0);

        len = WireProtocol.WriteBookCleared(frame, securityId, clearSide: 1);
        buffer.Buffer(securityId, frame[..len], batchSequence: 2, epoch: 0);

        len = WireProtocol.WriteLevelUpdate(frame, securityId, side: 0, price: 999, totalQty: 8, orderCount: 2);
        buffer.Buffer(securityId, frame[..len], batchSequence: 3, epoch: 0);
        len = WireProtocol.WriteLevelUpdate(frame, securityId, side: 0, price: 999, totalQty: 10, orderCount: 3);
        buffer.Buffer(securityId, frame[..len], batchSequence: 4, epoch: 0);

        var emitted = new List<(MessageType Type, long Price, long Qty, long Sequence)>();
        buffer.Flush(Environment.TickCount64 + 100, (secId, bytes, sequence, _) =>
        {
            var type = (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(bytes[4..]);
            long price = type == MessageType.LevelUpdate
                ? BinaryPrimitives.ReadInt64LittleEndian(bytes[16..])
                : 0;
            long qty = type == MessageType.LevelUpdate
                ? BinaryPrimitives.ReadInt64LittleEndian(bytes[24..])
                : 0;
            emitted.Add((type, price, qty, sequence));
        });

        Assert.Equal(2, emitted.Count);
        Assert.Equal(MessageType.BookCleared, emitted[0].Type);
        Assert.Equal((MessageType.LevelUpdate, 999L, 10L, 4L), emitted[1]);
    }

    [Fact]
    public void Buffer_LevelDeleteReplacesPriorUpdateForSameKey()
    {
        const ulong securityId = 88;
        var buffer = new CadenceConflationBuffer(100);
        Span<byte> frame = stackalloc byte[WireProtocol.LevelUpdateSize];
        int len = WireProtocol.WriteLevelUpdate(frame, securityId, side: 1, price: 1010, totalQty: 5, orderCount: 1);
        buffer.Buffer(securityId, frame[..len], batchSequence: 1, epoch: 0);
        len = WireProtocol.WriteLevelDeleted(frame, securityId, side: 1, price: 1010);
        buffer.Buffer(securityId, frame[..len], batchSequence: 2, epoch: 0);

        MessageType emitted = 0;
        buffer.Flush(Environment.TickCount64 + 100, (_, bytes, _, _) =>
            emitted = (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(bytes[4..]));

        Assert.Equal(MessageType.LevelDeleted, emitted);
    }
}
