using System.IO.Compression;

namespace B3.Umdf.FixConflated;

public static class FixZlibCompression
{
    public static byte[] Compress(ReadOnlySpan<byte> payload, CompressionLevel compressionLevel = CompressionLevel.Fastest)
    {
        using var output = new MemoryStream();
        using (var zlib = CreateCompressionStream(output, leaveOpen: true, compressionLevel))
        {
            zlib.Write(payload);
        }

        return output.ToArray();
    }

    public static ZLibStream CreateCompressionStream(Stream destination, bool leaveOpen = false, CompressionLevel compressionLevel = CompressionLevel.Fastest)
    {
        ArgumentNullException.ThrowIfNull(destination);
        return new ZLibStream(destination, compressionLevel, leaveOpen);
    }

    public static ZLibStream CreateDecompressionStream(Stream source, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new ZLibStream(source, CompressionMode.Decompress, leaveOpen);
    }
}
