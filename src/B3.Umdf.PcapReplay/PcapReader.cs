using System.Buffers;
using System.Buffers.Binary;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.PcapReplay;

public sealed class PcapReader : IDisposable
{
    /// <summary>
    /// Hard upper bound on a PCAP record's <c>incl_len</c>. Sized at the largest plausible
    /// link-layer frame (Ethernet 1500 + 14B header, plus jumbo headroom up to a 16-bit length).
    /// Records with a larger declared length are treated as malformed: we refuse to allocate
    /// (preventing OOM from a corrupted header) and stop the stream cleanly.
    /// </summary>
    public const int MaxPcapRecordBytes = 65535;

    private readonly Stream _stream;
    private readonly bool _swapBytes;
    private readonly uint _linkType;
    private readonly byte[] _recordHeader = new byte[16];
    private readonly ILogger _logger;

    private long _malformedPcapRecords;
    private long _truncatedPcapRecords;

    public uint LinkType => _linkType;

    /// <summary>
    /// Number of records dropped because the declared <c>incl_len</c> exceeded
    /// <see cref="MaxPcapRecordBytes"/>. Indicates capture-file corruption upstream.
    /// </summary>
    public long MalformedPcapRecords => Volatile.Read(ref _malformedPcapRecords);

    /// <summary>
    /// Number of records dropped because the underlying stream ended mid-record
    /// (truncated header or short payload). Distinct from a clean EOF on a record boundary,
    /// which simply terminates iteration without incrementing this counter.
    /// </summary>
    public long TruncatedPcapRecords => Volatile.Read(ref _truncatedPcapRecords);

    public PcapReader(Stream stream, ILogger<PcapReader>? logger = null)
    {
        _stream = stream;
        _logger = logger ?? NullLogger<PcapReader>.Instance;
        Span<byte> header = stackalloc byte[24];
        stream.ReadExactly(header);
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
        _swapBytes = magic == 0xD4C3B2A1;
        if (magic != 0xA1B2C3D4 && magic != 0xD4C3B2A1)
            throw new InvalidDataException($"Invalid PCAP magic: 0x{magic:X8}");
        _linkType = ReadUInt32(header[20..]);
    }

    public PcapReader(string filePath, ILogger<PcapReader>? logger = null)
        : this(new BufferedStream(File.OpenRead(filePath), 1024 * 1024), logger) { }

    public bool TryReadNext(out PcapPacket packet)
    {
        // Read the 16-byte record header. A clean stream end on a record boundary is normal
        // termination (return false, no counter bump). A short read mid-header indicates a
        // truncated capture file — log + count, do not throw.
        int headerRead = ReadFully(_stream, _recordHeader);
        if (headerRead == 0)
        {
            packet = default;
            return false;
        }
        if (headerRead < 16)
        {
            long n = Interlocked.Increment(ref _truncatedPcapRecords);
            LogRateLimited(n, "Truncated PCAP record header: stream ended after {Bytes}/16 bytes (TruncatedPcapRecords={Count}).", headerRead, n);
            packet = default;
            return false;
        }

        uint tsSec = ReadUInt32(_recordHeader);
        uint tsUsec = ReadUInt32(_recordHeader.AsSpan(4));
        uint inclLen = ReadUInt32(_recordHeader.AsSpan(8));

        // Sanity-cap inclLen before any allocation. A bogus value here would otherwise
        // ArrayPool.Rent(int.MaxValue) and OOM the process.
        if (inclLen > MaxPcapRecordBytes)
        {
            long n = Interlocked.Increment(ref _malformedPcapRecords);
            LogRateLimited(n, "Malformed PCAP record: inclLen={InclLen} exceeds cap {Cap}; dropping record and stopping stream (MalformedPcapRecords={Count}).", inclLen, MaxPcapRecordBytes, n);
            packet = default;
            return false;
        }

        int len = (int)inclLen;
        byte[] data = ArrayPool<byte>.Shared.Rent(len);
        try
        {
            int payloadRead = ReadFully(_stream, data.AsSpan(0, len));
            if (payloadRead < len)
            {
                long n = Interlocked.Increment(ref _truncatedPcapRecords);
                LogRateLimited(n, "Truncated PCAP record payload: read {Read}/{Expected} bytes (TruncatedPcapRecords={Count}).", payloadRead, len, n);
                ArrayPool<byte>.Shared.Return(data);
                packet = default;
                return false;
            }
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(data);
            throw;
        }

        packet = new PcapPacket
        {
            TimestampMicros = (long)tsSec * 1_000_000 + tsUsec,
            Data = new ReadOnlyMemory<byte>(data, 0, len),
            PooledArray = data
        };
        return true;
    }

    private static int ReadFully(Stream stream, Span<byte> buffer)
    {
        int total = 0;
        while (total < buffer.Length)
        {
            int n = stream.Read(buffer[total..]);
            if (n == 0) break;
            total += n;
        }
        return total;
    }

    private void LogRateLimited(long counter, string format, params object?[] args)
    {
        // Rate-limit warnings: log the first occurrence and then every 64th to keep
        // operator log volume bounded under a sustained corruption pattern.
        if (counter == 1 || (counter & 63) == 0)
            _logger.LogWarning(format, args);
    }

    private uint ReadUInt32(ReadOnlySpan<byte> span) =>
        _swapBytes ? BinaryPrimitives.ReadUInt32BigEndian(span) : BinaryPrimitives.ReadUInt32LittleEndian(span);

    public void Dispose() => _stream.Dispose();
}
