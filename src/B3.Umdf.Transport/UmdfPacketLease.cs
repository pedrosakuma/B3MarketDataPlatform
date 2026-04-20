using System.Buffers;

namespace B3.Umdf.Transport;

internal abstract class UmdfPacketLease
{
    public abstract void Retain();
    public abstract void Release();
}

internal sealed class ArrayPoolPacketLease : UmdfPacketLease
{
    private byte[]? _buffer;
    private int _refCount = 1;

    public ArrayPoolPacketLease(byte[] buffer)
    {
        _buffer = buffer;
    }

    public override void Retain()
    {
        while (true)
        {
            int current = Volatile.Read(ref _refCount);
            if (current <= 0)
                throw new InvalidOperationException("Cannot retain a released packet lease.");

            if (Interlocked.CompareExchange(ref _refCount, current + 1, current) == current)
                return;
        }
    }

    public override void Release()
    {
        int remaining = Interlocked.Decrement(ref _refCount);
        if (remaining > 0)
            return;

        if (remaining < 0)
            throw new InvalidOperationException("Packet lease released more than once.");

        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is not null)
            ArrayPool<byte>.Shared.Return(buffer);
    }
}
