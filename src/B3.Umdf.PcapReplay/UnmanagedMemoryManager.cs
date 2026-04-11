using System.Buffers;
using System.Runtime.InteropServices;

namespace B3.Umdf.PcapReplay;

/// <summary>
/// MemoryManager that wraps an unmanaged (e.g. mmap'd) pointer as Memory&lt;byte&gt;.
/// The caller is responsible for ensuring the pointer remains valid for the
/// lifetime of any Memory/Span derived from this manager.
/// </summary>
internal sealed unsafe class UnmanagedMemoryManager : MemoryManager<byte>
{
    private byte* _pointer;
    private readonly int _length;

    public UnmanagedMemoryManager(byte* pointer, int length)
    {
        _pointer = pointer;
        _length = length;
    }

    public override Span<byte> GetSpan() => new(_pointer, _length);

    public override MemoryHandle Pin(int elementIndex = 0) =>
        new(_pointer + elementIndex);

    public override void Unpin() { }

    protected override void Dispose(bool disposing) { }
}
