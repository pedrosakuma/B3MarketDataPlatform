using System.Buffers.Binary;

namespace B3.Umdf.PcapReplay;

public sealed class PcapReader : IDisposable
{
    private readonly Stream _stream;
    private readonly bool _swapBytes;
    private readonly uint _linkType;
    private readonly byte[] _recordHeader = new byte[16];

    public uint LinkType => _linkType;

    public PcapReader(Stream stream)
    {
        _stream = stream;
        Span<byte> header = stackalloc byte[24];
        stream.ReadExactly(header);
        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
        _swapBytes = magic == 0xD4C3B2A1;
        if (magic != 0xA1B2C3D4 && magic != 0xD4C3B2A1)
            throw new InvalidDataException($"Invalid PCAP magic: 0x{magic:X8}");
        _linkType = ReadUInt32(header[20..]);
    }

    public PcapReader(string filePath) : this(File.OpenRead(filePath)) { }

    public bool TryReadNext(out PcapPacket packet)
    {
        int bytesRead = _stream.Read(_recordHeader);
        if (bytesRead < 16)
        {
            packet = default;
            return false;
        }

        uint tsSec = ReadUInt32(_recordHeader);
        uint tsUsec = ReadUInt32(_recordHeader.AsSpan(4));
        uint inclLen = ReadUInt32(_recordHeader.AsSpan(8));

        byte[] data = new byte[inclLen];
        _stream.ReadExactly(data);

        packet = new PcapPacket
        {
            TimestampMicros = (long)tsSec * 1_000_000 + tsUsec,
            Data = data
        };
        return true;
    }

    private uint ReadUInt32(ReadOnlySpan<byte> span) =>
        _swapBytes ? BinaryPrimitives.ReadUInt32BigEndian(span) : BinaryPrimitives.ReadUInt32LittleEndian(span);

    public void Dispose() => _stream.Dispose();
}
