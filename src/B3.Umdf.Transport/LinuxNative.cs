using System.Runtime.InteropServices;

namespace B3.Umdf.Transport;

/// <summary>
/// Linux libc bindings for batched UDP receive via recvmmsg(2).
/// Layouts target glibc/musl on 64-bit Linux. Do not use on other OSes.
/// </summary>
internal static class LinuxNative
{
    public const int MSG_WAITFORONE = 0x10000;
    public const int MSG_TRUNC = 0x20;
    public const int EINTR = 4;
    public const int EAGAIN = 11;

    [StructLayout(LayoutKind.Sequential, Pack = 8)]
    public struct Iovec
    {
        public IntPtr iov_base;
        public nuint iov_len;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 8)]
    public struct Msghdr
    {
        public IntPtr msg_name;
        public uint msg_namelen;
        // 4 bytes padding inserted by alignment of msg_iov
        public IntPtr msg_iov;
        public nuint msg_iovlen;
        public IntPtr msg_control;
        public nuint msg_controllen;
        public int msg_flags;
        // 4 bytes trailing padding for 8-alignment
    }

    [StructLayout(LayoutKind.Sequential, Pack = 8)]
    public struct Mmsghdr
    {
        public Msghdr msg_hdr;
        public uint msg_len;
        // 4 bytes trailing padding
    }

    [DllImport("libc", SetLastError = true, EntryPoint = "recvmmsg")]
    public static extern int recvmmsg(int sockfd, IntPtr msgvec, uint vlen, int flags, IntPtr timeout);

    /// <summary>
    /// Batched UDP send. Returns number of messages successfully transmitted from <paramref name="msgvec"/>.
    /// The msg_len field of each successfully-sent mmsghdr is updated by the kernel.
    /// </summary>
    [DllImport("libc", SetLastError = true, EntryPoint = "sendmmsg")]
    public static extern int sendmmsg(int sockfd, IntPtr msgvec, uint vlen, int flags);
}
