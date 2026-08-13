using System.Buffers;

namespace B3.Umdf.FixConflated;

internal sealed class FixMessageBatchEncoder : IDisposable
{
    private readonly ArrayPool<byte> _bufferPool;
    private byte[] _buffer;

    public FixMessageBatchEncoder(int initialBufferSize = 16 * 1024, ArrayPool<byte>? bufferPool = null)
    {
        if (initialBufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(initialBufferSize));

        _bufferPool = bufferPool ?? ArrayPool<byte>.Shared;
        _buffer = _bufferPool.Rent(initialBufferSize);
    }

    public int WrittenCount { get; private set; }
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, WrittenCount);

    public void Reset() => WrittenCount = 0;

    public void Append(FixMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        int encodedLength = FixMessageCodec.GetEncodedLength(message);
        Append(message, encodedLength);
    }

    public void Append(FixMessage message, int encodedLength)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (encodedLength < 0)
            throw new ArgumentOutOfRangeException(nameof(encodedLength));

        EnsureCapacity(WrittenCount + encodedLength);
        WrittenCount += FixMessageCodec.EncodeInto(_buffer.AsSpan(WrittenCount), message);
    }

    public void Dispose()
        => _bufferPool.Return(_buffer);

    private void EnsureCapacity(int needed)
    {
        if (_buffer.Length >= needed)
            return;

        byte[] next = _bufferPool.Rent(needed);
        _buffer.AsSpan(0, WrittenCount).CopyTo(next);
        _bufferPool.Return(_buffer);
        _buffer = next;
    }
}
