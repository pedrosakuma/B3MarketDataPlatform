using System.IO.Compression;

namespace B3.Umdf.FixConflated;

public static class FixZlibCompression
{
    public static byte[] Compress(ReadOnlySpan<byte> payload, CompressionLevel compressionLevel = CompressionLevel.Fastest)
    {
        using var output = new MemoryStream();
        using (var zlib = new ZLibStream(output, compressionLevel, leaveOpen: true))
        {
            zlib.Write(payload);
        }

        return output.ToArray();
    }
}
