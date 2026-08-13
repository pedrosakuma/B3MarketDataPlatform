using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace B3.Umdf.FixConflated;

/// <summary>
/// Multi-producer, single-consumer bounded ring used by FIX session fanout paths.
/// </summary>
internal sealed class FixOutboundRing<T> : IDisposable
    where T : class
{
    private readonly T?[] _slots;
    private readonly long[] _seqs;
    private readonly int _mask;

    private FixPaddedLong _producerSeq;
    private FixPaddedLong _consumerSeq;

    private int _consumerWaiting;
    private readonly ManualResetEventSlim _itemsAvailable = new(initialState: false, spinCount: 0);

    public FixOutboundRing(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 2);
        capacity = NextPow2(capacity);
        _slots = new T?[capacity];
        _seqs = new long[capacity];
        for (int i = 0; i < capacity; i++)
            _seqs[i] = i;
        _mask = capacity - 1;
    }

    public int Capacity => _slots.Length;

    public bool TryEnqueue(T payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        long pos = Volatile.Read(ref _producerSeq.Value);
        while (true)
        {
            int idx = (int)(pos & _mask);
            long seq = Volatile.Read(ref _seqs[idx]);
            long diff = seq - pos;

            if (diff == 0)
            {
                if (Interlocked.CompareExchange(ref _producerSeq.Value, pos + 1, pos) == pos)
                {
                    _slots[idx] = payload;
                    Volatile.Write(ref _seqs[idx], pos + 1);
                    if (Interlocked.CompareExchange(ref _consumerWaiting, 0, 1) == 1)
                        _itemsAvailable.Set();
                    return true;
                }

                pos = Volatile.Read(ref _producerSeq.Value);
            }
            else if (diff < 0)
            {
                return false;
            }
            else
            {
                pos = Volatile.Read(ref _producerSeq.Value);
            }
        }
    }

    public bool TryDequeue([MaybeNullWhen(false)] out T payload)
    {
        long pos = _consumerSeq.Value;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);
        long diff = seq - (pos + 1);
        if (diff == 0)
        {
            payload = _slots[idx];
            _slots[idx] = null;
            Volatile.Write(ref _seqs[idx], pos + _slots.Length);
            _consumerSeq.Value = pos + 1;
            return payload is not null;
        }

        payload = null;
        return false;
    }

    public void WaitForItems(CancellationToken ct)
    {
        _itemsAvailable.Reset();
        Interlocked.Exchange(ref _consumerWaiting, 1);
        if (HasItemsAvailable())
        {
            Volatile.Write(ref _consumerWaiting, 0);
            return;
        }

        try
        {
            _itemsAvailable.Wait(ct);
        }
        finally
        {
            Volatile.Write(ref _consumerWaiting, 0);
        }
    }

    public void SignalShutdown() => _itemsAvailable.Set();

    public void Dispose() => _itemsAvailable.Dispose();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool HasItemsAvailable()
    {
        long pos = _consumerSeq.Value;
        int idx = (int)(pos & _mask);
        long seq = Volatile.Read(ref _seqs[idx]);
        return seq - (pos + 1) == 0;
    }

    private static int NextPow2(int v)
    {
        if ((v & (v - 1)) == 0)
            return v;

        int n = 1;
        while (n < v)
            n <<= 1;
        return n;
    }
}

[StructLayout(LayoutKind.Explicit, Size = 128)]
internal struct FixPaddedLong
{
    [FieldOffset(64)]
    public long Value;
}
