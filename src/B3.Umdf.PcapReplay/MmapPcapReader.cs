using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;

namespace B3.Umdf.PcapReplay;

/// <summary>
/// Zero-copy PCAP reader backed by memory-mapped files.
/// Packet data points directly into the mmap'd region — no allocations, no copies.
/// Uses madvise(MADV_SEQUENTIAL) on Linux for aggressive kernel read-ahead.
/// For files &gt;2GB, the mmap is split into ~1GB Memory&lt;byte&gt; segments
/// (required because Memory&lt;byte&gt; uses int for offset/length).
/// </summary>
public sealed class MmapPcapReader : IDisposable
{
    private const int SegmentSize = 1 << 30; // 1 GB

    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _view;
    private readonly unsafe byte* _basePtr;
    private readonly long _fileLength;
    private readonly bool _swapBytes;
    private readonly uint _linkType;
    private long _offset;

    // Segmented Memory<byte> views into the mmap region, each ≤1GB.
    private readonly Memory<byte>[] _segments;
    private readonly UnmanagedMemoryManager[] _managers;

    public uint LinkType => _linkType;

    [DllImport("libc", SetLastError = true)]
    private static extern int madvise(nint addr, nuint length, int advice);
    private const int MADV_SEQUENTIAL = 2;

    public unsafe MmapPcapReader(string filePath)
    {
        _fileLength = new FileInfo(filePath).Length;
        if (_fileLength < 24)
            throw new InvalidDataException("File too small for PCAP global header");

        _mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        _view = _mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);

        byte* ptr = null;
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
        ptr += _view.PointerOffset;
        _basePtr = ptr;

        // Tell OS this is a sequential scan — enables aggressive read-ahead
        madvise((nint)_basePtr, (nuint)_fileLength, MADV_SEQUENTIAL);

        // Create segmented Memory<byte> views (each ≤SegmentSize)
        int segCount = (int)((_fileLength + SegmentSize - 1) / SegmentSize);
        _managers = new UnmanagedMemoryManager[segCount];
        _segments = new Memory<byte>[segCount];
        for (int i = 0; i < segCount; i++)
        {
            long segStart = (long)i * SegmentSize;
            int segLen = (int)Math.Min(SegmentSize, _fileLength - segStart);
            _managers[i] = new UnmanagedMemoryManager(ptr + segStart, segLen);
            _segments[i] = _managers[i].Memory;
        }

        // Parse PCAP global header (24 bytes)
        var header = new ReadOnlySpan<byte>(_basePtr, 24);
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
        _swapBytes = magic == 0xD4C3B2A1;
        if (magic != 0xA1B2C3D4 && !_swapBytes)
            throw new InvalidDataException($"Invalid PCAP magic: 0x{magic:X8}");
        _linkType = ReadUInt32(header[20..]);
        _offset = 24;
    }

    public unsafe bool TryReadNext(out PcapPacket packet)
    {
        if (_offset + 16 > _fileLength)
        {
            packet = default;
            return false;
        }

        var recordHeader = new ReadOnlySpan<byte>(_basePtr + _offset, 16);
        uint tsSec = ReadUInt32(recordHeader);
        uint tsUsec = ReadUInt32(recordHeader[4..]);
        uint inclLen = ReadUInt32(recordHeader[8..]);
        _offset += 16;

        if (_offset + inclLen > _fileLength)
        {
            packet = default;
            return false;
        }

        // Zero-copy: slice directly from the mmap'd Memory<byte> segment
        int segIndex = (int)(_offset / SegmentSize);
        int segOffset = (int)(_offset % SegmentSize);

        ReadOnlyMemory<byte> data;
        if (segOffset + (int)inclLen <= _segments[segIndex].Length)
        {
            data = _segments[segIndex].Slice(segOffset, (int)inclLen);
        }
        else
        {
            // Packet spans segment boundary (~64KB packet crossing 1GB boundary — extremely rare)
            var buf = new byte[(int)inclLen];
            new ReadOnlySpan<byte>(_basePtr + _offset, (int)inclLen).CopyTo(buf);
            data = buf;
        }

        _offset += inclLen;

        packet = new PcapPacket
        {
            TimestampMicros = (long)tsSec * 1_000_000 + tsUsec,
            Data = data,
            PooledArray = null
        };
        return true;
    }

    private uint ReadUInt32(ReadOnlySpan<byte> span) =>
        _swapBytes ? BinaryPrimitives.ReadUInt32BigEndian(span) : BinaryPrimitives.ReadUInt32LittleEndian(span);

    public unsafe void Dispose()
    {
        _view.SafeMemoryMappedViewHandle.ReleasePointer();
        _view.Dispose();
        _mmf.Dispose();
    }
}
