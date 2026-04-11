namespace B3.Umdf.PcapReplay;

public struct PcapPacket
{
    public long TimestampMicros { get; init; }
    public ReadOnlyMemory<byte> Data { get; init; }

    /// <summary>
    /// When non-null, this array was rented from ArrayPool and must be returned.
    /// Managed by TimestampMergedReplayer lifecycle.
    /// </summary>
    internal byte[]? PooledArray { get; init; }
}
