using System.Buffers.Binary;
using B3.MarketData.Wire;

namespace B3.Umdf.Server;

/// <summary>
/// Broadcaster-thread-owned, fixed-cadence last-value-wins buffer for the
/// existing MBP/book-context wire frames. No new payload vocabulary is created.
/// </summary>
internal sealed class CadenceConflationBuffer
{
    private readonly Dictionary<FrameKey, BufferedFrame> _frames = new();
    private long _nextFlushTicks;

    public int CadenceMs { get; }
    public bool HasPending => _frames.Count != 0;

    public CadenceConflationBuffer(int cadenceMs)
    {
        CadenceMs = cadenceMs;
        _nextFlushTicks = Environment.TickCount64 + cadenceMs;
    }

    public void Buffer(
        ulong securityId,
        ReadOnlySpan<byte> bytes,
        long batchSequence,
        long epoch)
    {
        if (bytes.Length < WireV2.HeaderSize) return;
        AdvanceIdleSchedule(Environment.TickCount64);

        var type = (MessageType)BinaryPrimitives.ReadUInt16LittleEndian(bytes[4..]);
        var key = CreateKey(securityId, type, bytes);
        if (type == MessageType.BookCleared)
            ApplyClear(key);

        if (_frames.TryGetValue(key, out var existing))
        {
            if (existing.Bytes.Length != bytes.Length)
                existing.Bytes = new byte[bytes.Length];
            bytes.CopyTo(existing.Bytes);
            existing.Sequence = batchSequence;
            existing.Epoch = epoch;
            _frames[key] = existing;
            MetricsRegistry.WsMessagesConflated.Add(1);
        }
        else
        {
            _frames.Add(key, new BufferedFrame(bytes.ToArray(), batchSequence, epoch));
        }
        MetricsRegistry.ConflatedCadenceFramesBuffered.Add(1);
    }

    public bool IsDue(long nowTicks) => HasPending && nowTicks >= _nextFlushTicks;

    public void Flush(long nowTicks, Action<ulong, ReadOnlySpan<byte>, long, long> emit)
    {
        if (_frames.Count == 0)
        {
            AdvanceIdleSchedule(nowTicks);
            return;
        }

        // Reset markers must precede post-clear level deltas retained in the same window.
        EmitPass(MessageType.BookCleared, emit);
        foreach (var (key, frame) in _frames)
        {
            if (key.Type == MessageType.BookCleared) continue;
            emit(key.SecurityId, frame.Bytes, frame.Sequence, frame.Epoch);
            MetricsRegistry.ConflatedCadenceFramesEmitted.Add(1);
        }
        _frames.Clear();
        do { _nextFlushTicks += CadenceMs; }
        while (_nextFlushTicks <= nowTicks);
    }

    public void Discard()
    {
        _frames.Clear();
        _nextFlushTicks = Environment.TickCount64 + CadenceMs;
    }

    private void EmitPass(
        MessageType type,
        Action<ulong, ReadOnlySpan<byte>, long, long> emit)
    {
        foreach (var (key, frame) in _frames)
        {
            if (key.Type != type) continue;
            emit(key.SecurityId, frame.Bytes, frame.Sequence, frame.Epoch);
            MetricsRegistry.ConflatedCadenceFramesEmitted.Add(1);
        }
    }

    private void AdvanceIdleSchedule(long nowTicks)
    {
        if (HasPending || _nextFlushTicks > nowTicks) return;
        long elapsed = nowTicks - _nextFlushTicks;
        _nextFlushTicks += (elapsed / CadenceMs + 1) * CadenceMs;
    }

    private void ApplyClear(FrameKey clear)
    {
        List<FrameKey>? remove = null;
        foreach (var key in _frames.Keys)
        {
            if (key.SecurityId != clear.SecurityId) continue;

            if (key.Type == MessageType.BookCleared)
            {
                if (clear.Side == 0 || key.Side == clear.Side)
                    (remove ??= new()).Add(key);
                continue;
            }

            bool sameBookSide = clear.Side == 0 || key.Side == clear.Side - 1;
            if (sameBookSide &&
                key.Type is MessageType.LevelUpdate or MessageType.MarketTierUpdate)
                (remove ??= new()).Add(key);
        }
        if (remove is not null)
            foreach (var key in remove)
                _frames.Remove(key);
    }

    private static FrameKey CreateKey(ulong securityId, MessageType type, ReadOnlySpan<byte> bytes)
    {
        return type switch
        {
            MessageType.LevelUpdate => new FrameKey(
                securityId,
                MessageType.LevelUpdate,
                bytes[36],
                BinaryPrimitives.ReadInt64LittleEndian(bytes[16..])),
            MessageType.LevelDeleted => new FrameKey(
                securityId,
                MessageType.LevelUpdate,
                bytes[24],
                BinaryPrimitives.ReadInt64LittleEndian(bytes[16..])),
            MessageType.MarketTierUpdate => new FrameKey(
                securityId,
                MessageType.MarketTierUpdate,
                bytes[28],
                0),
            MessageType.BookCleared => new FrameKey(
                securityId,
                MessageType.BookCleared,
                bytes[16],
                0),
            _ => new FrameKey(securityId, type, 0, 0),
        };
    }

    private readonly record struct FrameKey(
        ulong SecurityId,
        MessageType Type,
        byte Side,
        long Price);

    private struct BufferedFrame
    {
        public byte[] Bytes;
        public long Sequence;
        public long Epoch;

        public BufferedFrame(byte[] bytes, long sequence, long epoch)
        {
            Bytes = bytes;
            Sequence = sequence;
            Epoch = epoch;
        }
    }
}
