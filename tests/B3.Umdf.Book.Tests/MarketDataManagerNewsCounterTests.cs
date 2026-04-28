using System.Buffers.Binary;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// P12-9 — every SBE template defined in the schema is now explicitly
/// accounted for at the dispatcher (compile-time enforced via
/// <c>ISbeMessageHandler</c>). The only template that arrives in
/// production flows but has no application-level handler is
/// <see cref="News_5Data"/>; this test pins the
/// <see cref="MarketDataManager.NewsMessagesObserved"/> visibility
/// counter so a future regression cannot silently drop news messages
/// without operators noticing.
/// </summary>
public class MarketDataManagerNewsCounterTests
{
    [Fact]
    public void OnPacket_NewsMessage_IncrementsObservedCounter()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var mdm = new MarketDataManager(stateRegistry: registry);

        Assert.Equal(0L, mdm.NewsMessagesObserved);

        var packet = BuildNewsPacket();
        for (int i = 0; i < 5; i++)
            mdm.OnPacket(in EmptyPacket, packet, News_5Data.MESSAGE_ID);

        Assert.Equal(5L, mdm.NewsMessagesObserved);
    }

    [Fact]
    public void OnPacket_NonNewsMessage_LeavesNewsCounterUntouched()
    {
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var mdm = new MarketDataManager(stateRegistry: registry);

        // Send a SecurityStatus_3 packet — should not touch the news counter.
        var packet = BuildEmptyTemplatePacket(SecurityStatus_3Data.MESSAGE_ID, SecurityStatus_3Data.MESSAGE_SIZE);
        mdm.OnPacket(in EmptyPacket, packet, SecurityStatus_3Data.MESSAGE_ID);

        Assert.Equal(0L, mdm.NewsMessagesObserved);
    }

    private static readonly UmdfPacket EmptyPacket = new()
    {
        Data = ReadOnlyMemory<byte>.Empty,
        Channel = ChannelType.InstrumentDefinition,
        ChannelGroup = 1,
        ReceivedTimestampTicks = 0,
    };

    private static byte[] BuildNewsPacket()
    {
        const int sbeHeaderSize = 8;
        var payload = new News_5Data();

        var buf = new byte[256];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)News_5Data.MESSAGE_SIZE);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), News_5Data.MESSAGE_ID);

        bool ok = News_5Data.TryEncode(
            payload,
            buf.AsSpan(sbeHeaderSize),
            ReadOnlySpan<byte>.Empty,
            ReadOnlySpan<byte>.Empty,
            ReadOnlySpan<byte>.Empty,
            out int bytesWritten);
        Assert.True(ok, "News_5 TryEncode failed");

        var result = new byte[sbeHeaderSize + bytesWritten];
        buf.AsSpan(0, result.Length).CopyTo(result);
        return result;
    }

    private static byte[] BuildEmptyTemplatePacket(ushort templateId, int blockLength)
    {
        const int sbeHeaderSize = 8;
        var buf = new byte[sbeHeaderSize + blockLength];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)blockLength);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), templateId);
        return buf;
    }
}
