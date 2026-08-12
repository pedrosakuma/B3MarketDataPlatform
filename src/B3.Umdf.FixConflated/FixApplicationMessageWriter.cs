using System.Buffers;
using System.Buffers.Text;
using System.Text;
using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

public sealed class FixApplicationMessageWriter : IDisposable
{
    private const int MaxReservedPrefixLength = 32;
    private static readonly Encoding s_ascii = Encoding.ASCII;

    private readonly ArrayPool<byte> _bufferPool;
    private byte[] _buffer;
    private bool _disposed;
    private int _writtenLength;

    public FixApplicationMessageWriter(int initialBufferSize = 4 * 1024, ArrayPool<byte>? bufferPool = null)
    {
        if (initialBufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(initialBufferSize));

        _bufferPool = bufferPool ?? ArrayPool<byte>.Shared;
        _buffer = _bufferPool.Rent(initialBufferSize);
    }

    public ReadOnlyMemory<byte> WriteIncrementalRefresh(
        FixApplicationSessionHeader header,
        FixMarketDataInstrument instrument,
        ReadOnlySpan<FixMarketDataIncrementalEntry> entries,
        string? mdReqId = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (entries.IsEmpty)
            throw new ArgumentException("At least one incremental entry is required.", nameof(entries));

        EnsureCapacity(EstimateIncrementalFrameSize(header, instrument, entries.Length, mdReqId));

        int bodyStart = MaxReservedPrefixLength;
        int bodyLength = WriteIncrementalBody(
            _buffer.AsSpan(bodyStart),
            header,
            instrument,
            entries,
            mdReqId);

        return FinalizeFrame(header.BeginString, bodyStart, bodyLength);
    }

    public ReadOnlyMemory<byte> WriteSnapshotFullRefresh(
        FixApplicationSessionHeader header,
        FixMarketDataSnapshotRequest request,
        OrderBook book)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        int entryCount = book.Bids.OrderCount + book.Asks.OrderCount;
        EnsureCapacity(EstimateSnapshotFrameSize(header, request, entryCount));

        int bodyStart = MaxReservedPrefixLength;
        int bodyLength = WriteSnapshotBody(
            _buffer.AsSpan(bodyStart),
            header,
            request,
            book,
            entryCount);

        return FinalizeFrame(header.BeginString, bodyStart, bodyLength);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _bufferPool.Return(_buffer);
        _buffer = [];
        _writtenLength = 0;
        _disposed = true;
    }

    private ReadOnlyMemory<byte> FinalizeFrame(string beginString, int bodyStart, int bodyLength)
    {
        Span<byte> beginStringBytes = stackalloc byte[Math.Max(beginString.Length, 1)];
        int beginStringLength = s_ascii.GetBytes(beginString, beginStringBytes);
        int prefixLength = FixFrameEncoding.WritePrefix(_buffer, beginStringBytes[..beginStringLength], bodyLength);

        if (prefixLength != bodyStart)
            _buffer.AsSpan(bodyStart, bodyLength).CopyTo(_buffer.AsSpan(prefixLength));

        int checksumOffset = prefixLength + bodyLength;
        int checksum = FixFrameEncoding.CalculateChecksum(_buffer.AsSpan(0, checksumOffset));
        int checksumLength = FixFrameEncoding.WriteChecksumField(_buffer.AsSpan(checksumOffset), checksum);

        _writtenLength = checksumOffset + checksumLength;
        return _buffer.AsMemory(0, _writtenLength);
    }

    private static int WriteIncrementalBody(
        Span<byte> destination,
        FixApplicationSessionHeader header,
        FixMarketDataInstrument instrument,
        ReadOnlySpan<FixMarketDataIncrementalEntry> entries,
        string? mdReqId)
    {
        int offset = 0;
        offset += WriteSessionHeader(destination[offset..], header, FixMsgTypes.MarketDataIncrementalRefresh);

        if (!string.IsNullOrEmpty(mdReqId))
            offset += WriteStringField(destination[offset..], FixTags.MDReqId, mdReqId);

        offset += WriteIntField(destination[offset..], FixTags.NoMDEntries, entries.Length);

        foreach (ref readonly var entry in entries)
        {
            offset += WriteCharField(destination[offset..], FixTags.MDUpdateAction, (char)entry.UpdateAction);
            offset += WriteCharField(destination[offset..], FixTags.MDEntryType, (char)entry.EntryType);
            offset += WriteStringField(destination[offset..], FixTags.Symbol, instrument.Symbol);
            offset += WriteUInt64Field(destination[offset..], FixTags.SecurityId, instrument.SecurityId);

            if ((entry.Fields & FixMarketDataEntryFields.Price) != 0)
                offset += WriteScaledInt64Field(destination[offset..], FixTags.MDEntryPx, entry.Price, instrument.PriceScale);
            if ((entry.Fields & FixMarketDataEntryFields.Size) != 0)
                offset += WriteInt64Field(destination[offset..], FixTags.MDEntrySize, entry.Size);

            offset += WriteUtcDateField(destination[offset..], FixTags.MDEntryDate, entry.EntryTime);
            offset += WriteUtcTimeField(destination[offset..], FixTags.MDEntryTime, entry.EntryTime);

            if ((entry.Fields & FixMarketDataEntryFields.OrderId) != 0)
                offset += WriteUInt64Field(destination[offset..], FixTags.OrderId, entry.OrderId);
            if ((entry.Fields & FixMarketDataEntryFields.TradeId) != 0)
                offset += WriteInt64Field(destination[offset..], FixTags.TradeId, entry.TradeId);
        }

        return offset;
    }

    private static int WriteSnapshotBody(
        Span<byte> destination,
        FixApplicationSessionHeader header,
        FixMarketDataSnapshotRequest request,
        OrderBook book,
        int entryCount)
    {
        int offset = 0;
        offset += WriteSessionHeader(destination[offset..], header, FixMsgTypes.MarketDataSnapshotFullRefresh);
        offset += WriteStringField(destination[offset..], FixTags.MDReqId, request.MdReqId);
        offset += WriteStringField(destination[offset..], FixTags.Symbol, request.Instrument.Symbol);
        offset += WriteUInt64Field(destination[offset..], FixTags.SecurityId, request.Instrument.SecurityId);
        offset += WriteIntField(destination[offset..], FixTags.NoMDEntries, entryCount);

        offset += WriteSnapshotSide(destination[offset..], book.Bids, FixMdEntryType.Bid, request.Instrument.PriceScale, header.SendingTime);
        offset += WriteSnapshotSide(destination[offset..], book.Asks, FixMdEntryType.Offer, request.Instrument.PriceScale, header.SendingTime);
        return offset;
    }

    private static int WriteSnapshotSide(
        Span<byte> destination,
        BookSide side,
        FixMdEntryType entryType,
        int priceScale,
        DateTimeOffset entryTime)
    {
        int offset = 0;
        foreach (var (_, orders) in side.PriceLevels)
        {
            foreach (var order in orders)
            {
                offset += WriteCharField(destination[offset..], FixTags.MDEntryType, (char)entryType);
                offset += WriteScaledInt64Field(destination[offset..], FixTags.MDEntryPx, order.Price, priceScale);
                offset += WriteInt64Field(destination[offset..], FixTags.MDEntrySize, order.Quantity);
                offset += WriteUtcDateField(destination[offset..], FixTags.MDEntryDate, entryTime);
                offset += WriteUtcTimeField(destination[offset..], FixTags.MDEntryTime, entryTime);
                offset += WriteUInt64Field(destination[offset..], FixTags.OrderId, order.OrderId);
            }
        }

        return offset;
    }

    private void EnsureCapacity(int required)
    {
        if (_buffer.Length >= required)
            return;

        byte[] grown = _bufferPool.Rent(required);
        _bufferPool.Return(_buffer);
        _buffer = grown;
    }

    private static int EstimateIncrementalFrameSize(
        FixApplicationSessionHeader header,
        FixMarketDataInstrument instrument,
        int entryCount,
        string? mdReqId)
    {
        int beginStringLength = header.BeginString.Length;
        int mdReqIdLength = string.IsNullOrEmpty(mdReqId) ? 0 : mdReqId!.Length + 16;
        int bodyLength = 96 + header.SenderCompId.Length + header.TargetCompId.Length + mdReqIdLength + entryCount * (160 + instrument.Symbol.Length);
        return MaxReservedPrefixLength + bodyLength + FixFrameEncoding.ChecksumFieldLength + beginStringLength;
    }

    private static int EstimateSnapshotFrameSize(
        FixApplicationSessionHeader header,
        FixMarketDataSnapshotRequest request,
        int entryCount)
    {
        int beginStringLength = header.BeginString.Length;
        int bodyLength = 96 + header.SenderCompId.Length + header.TargetCompId.Length + request.MdReqId.Length + request.Instrument.Symbol.Length + entryCount * 96;
        return MaxReservedPrefixLength + bodyLength + FixFrameEncoding.ChecksumFieldLength + beginStringLength;
    }

    private static int WriteSessionHeader(Span<byte> destination, FixApplicationSessionHeader header, string msgType)
    {
        int offset = 0;
        offset += WriteStringField(destination[offset..], FixTags.MsgType, msgType);
        offset += WriteStringField(destination[offset..], FixTags.SenderCompId, header.SenderCompId);
        offset += WriteStringField(destination[offset..], FixTags.TargetCompId, header.TargetCompId);
        offset += WriteIntField(destination[offset..], FixTags.MsgSeqNum, header.MsgSeqNum);
        offset += WriteUtcTimestampField(destination[offset..], FixTags.SendingTime, header.SendingTime);
        return offset;
    }

    private static int WriteStringField(Span<byte> destination, int tag, string value)
    {
        int offset = WriteTag(destination, tag);
        offset += s_ascii.GetBytes(value, destination[offset..]);
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteCharField(Span<byte> destination, int tag, char value)
    {
        int offset = WriteTag(destination, tag);
        destination[offset++] = (byte)value;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteIntField(Span<byte> destination, int tag, int value)
    {
        int offset = WriteTag(destination, tag);
        if (!Utf8Formatter.TryFormat(value, destination[offset..], out int written))
            throw new InvalidOperationException($"Unable to format FIX int field {tag}.");

        offset += written;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteInt64Field(Span<byte> destination, int tag, long value)
    {
        int offset = WriteTag(destination, tag);
        if (!Utf8Formatter.TryFormat(value, destination[offset..], out int written))
            throw new InvalidOperationException($"Unable to format FIX long field {tag}.");

        offset += written;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteUInt64Field(Span<byte> destination, int tag, ulong value)
    {
        int offset = WriteTag(destination, tag);
        if (!Utf8Formatter.TryFormat(value, destination[offset..], out int written))
            throw new InvalidOperationException($"Unable to format FIX ulong field {tag}.");

        offset += written;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteScaledInt64Field(Span<byte> destination, int tag, long value, int scale)
    {
        int offset = WriteTag(destination, tag);
        if (!TryFormatScaledInt64(value, scale, destination[offset..], out int written))
            throw new InvalidOperationException($"Unable to format FIX decimal field {tag}.");

        offset += written;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteUtcTimestampField(Span<byte> destination, int tag, DateTimeOffset value)
    {
        int offset = WriteTag(destination, tag);
        if (!FixValueFormatting.TryFormatUtcTimestamp(value, destination[offset..], out int written))
            throw new InvalidOperationException($"Unable to format FIX timestamp field {tag}.");

        offset += written;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteUtcDateField(Span<byte> destination, int tag, DateTimeOffset value)
    {
        int offset = WriteTag(destination, tag);
        if (!FixValueFormatting.TryFormatUtcDate(value, destination[offset..], out int written))
            throw new InvalidOperationException($"Unable to format FIX date field {tag}.");

        offset += written;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteUtcTimeField(Span<byte> destination, int tag, DateTimeOffset value)
    {
        int offset = WriteTag(destination, tag);
        if (!FixValueFormatting.TryFormatUtcTime(value, destination[offset..], out int written))
            throw new InvalidOperationException($"Unable to format FIX time field {tag}.");

        offset += written;
        destination[offset++] = FixMessageCodec.Soh;
        return offset;
    }

    private static int WriteTag(Span<byte> destination, int tag)
    {
        if (!Utf8Formatter.TryFormat(tag, destination, out int written))
            throw new InvalidOperationException($"Unable to format FIX tag {tag}.");

        destination[written++] = (byte)'=';
        return written;
    }

    private static bool TryFormatScaledInt64(long value, int scale, Span<byte> destination, out int written)
    {
        if (scale == 0)
            return Utf8Formatter.TryFormat(value, destination, out written);

        ulong divisor = Pow10(scale);
        bool negative = value < 0;
        ulong absolute = negative
            ? unchecked((ulong)(-(value + 1)) + 1UL)
            : (ulong)value;

        ulong whole = absolute / divisor;
        ulong fractional = absolute % divisor;

        int offset = 0;
        if (negative)
        {
            if (destination.IsEmpty)
            {
                written = 0;
                return false;
            }

            destination[offset++] = (byte)'-';
        }

        if (!Utf8Formatter.TryFormat(whole, destination[offset..], out int wholeWritten))
        {
            written = 0;
            return false;
        }

        offset += wholeWritten;
        if (destination.Length <= offset)
        {
            written = 0;
            return false;
        }

        destination[offset++] = (byte)'.';

        Span<byte> fractionalBytes = stackalloc byte[20];
        if (!Utf8Formatter.TryFormat(fractional, fractionalBytes, out int fractionalWritten))
        {
            written = 0;
            return false;
        }

        int zeroPadding = scale - fractionalWritten;
        if (destination.Length < offset + zeroPadding + fractionalWritten)
        {
            written = 0;
            return false;
        }

        destination.Slice(offset, zeroPadding).Fill((byte)'0');
        offset += zeroPadding;
        fractionalBytes[..fractionalWritten].CopyTo(destination[offset..]);
        offset += fractionalWritten;
        written = offset;
        return true;
    }

    private static ulong Pow10(int power)
    {
        ulong result = 1;
        for (int i = 0; i < power; i++)
            result *= 10;

        return result;
    }
}
