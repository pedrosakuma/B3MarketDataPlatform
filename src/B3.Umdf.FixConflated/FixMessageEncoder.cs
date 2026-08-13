using System.Buffers;

namespace B3.Umdf.FixConflated;

internal sealed class FixMessageEncoder : IDisposable
{
    private readonly ArrayPool<byte> _bufferPool;
    private byte[] _buffer;

    public FixMessageEncoder(int initialBufferSize = 4 * 1024, ArrayPool<byte>? bufferPool = null)
    {
        if (initialBufferSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(initialBufferSize));

        _bufferPool = bufferPool ?? ArrayPool<byte>.Shared;
        _buffer = _bufferPool.Rent(initialBufferSize);
    }

    public ReadOnlyMemory<byte> Encode(FixMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        int encodedLength = FixMessageCodec.GetEncodedLength(message);
        EnsureCapacity(encodedLength);
        int written = FixMessageCodec.EncodeInto(_buffer, message);
        return _buffer.AsMemory(0, written);
    }

    public void Dispose()
        => _bufferPool.Return(_buffer);

    private void EnsureCapacity(int needed)
    {
        if (_buffer.Length >= needed)
            return;

        byte[] next = _bufferPool.Rent(needed);
        _bufferPool.Return(_buffer);
        _buffer = next;
    }
}
