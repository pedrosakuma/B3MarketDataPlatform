using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Transport;

public sealed class MulticastPacketSource : IPacketSource
{
    private const int SourceMembershipRequestSize = 12;
    // UMDF over UDP datagrams are bounded by Ethernet MTU (~1500 bytes); 9 KiB jumbo is the realistic upper bound.
    // Using 64 KiB per rent multiplied by hundreds of thousands of in-flight pooled buffers caused OOM on bursts.
    private const int MaxDatagramSize = 9 * 1024;
    public const int MaxBatchSize = 64;

    private readonly Socket _socket;
    private readonly ChannelType _channelType;
    private readonly int _channelGroup;
    private readonly ILogger _logger;

    // Membership info retained so we can leave and rejoin the multicast group on demand
    // (used to suspend SnapshotRecovery / InstrumentDefinition feeds while in RealTime).
    private readonly IPAddress _multicastGroup;
    private readonly int _port;
    private readonly IPAddress? _localAddress;
    private readonly IPAddress? _sourceAddress;
    private readonly object _membershipLock = new();
    private bool _isJoined;

    // Lazy-initialized state for ReceiveBatch (Linux recvmmsg). Allocated on first call from the
    // owning receive thread and reused for the lifetime of the source.
    private PinnedBufferPool? _batchBufferPool;
    private GCHandle _batchIovecsHandle;
    private GCHandle _batchMmsghdrsHandle;
    private LinuxNative.Iovec[]? _batchIovecs;
    private LinuxNative.Mmsghdr[]? _batchMmsghdrs;
    private byte[]?[]? _batchPendingBuffers;

    // Observable counters (read by the metrics layer; written from the receive thread / membership ops).
    private long _batchedSyscalls;
    private long _batchedDatagrams;
    private long _membershipJoins;
    private long _membershipLeaves;

    /// <summary>Number of recvmmsg(2) calls that returned at least one datagram.</summary>
    public long BatchedSyscalls => Volatile.Read(ref _batchedSyscalls);
    /// <summary>Total datagrams received via ReceiveBatch (sum of all batches). Average batch = Datagrams / Syscalls.</summary>
    public long BatchedDatagrams => Volatile.Read(ref _batchedDatagrams);
    /// <summary>Count of successful multicast membership joins (including the initial one).</summary>
    public long MembershipJoins => Volatile.Read(ref _membershipJoins);
    /// <summary>Count of successful multicast membership leaves.</summary>
    public long MembershipLeaves => Volatile.Read(ref _membershipLeaves);

    public ChannelType ChannelType => _channelType;
    public int ChannelGroup => _channelGroup;
    public bool IsJoined { get { lock (_membershipLock) return _isJoined; } }

    public MulticastPacketSource(ChannelConfig config, ILogger<MulticastPacketSource>? logger = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(config.ReceiveBufferBytes, 1);
        ValidateIPv4(config.MulticastGroup, nameof(config.MulticastGroup));
        if (config.SourceAddress is not null)
            ValidateIPv4(config.SourceAddress, nameof(config.SourceAddress));
        if (config.LocalAddress is not null)
            ValidateIPv4(config.LocalAddress, nameof(config.LocalAddress));

        _channelType = config.Type;
        _channelGroup = config.ChannelGroup;
        _multicastGroup = config.MulticastGroup;
        _port = config.Port;
        _localAddress = config.LocalAddress;
        _sourceAddress = config.SourceAddress;
        _logger = logger ?? NullLogger<MulticastPacketSource>.Instance;
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

        var socket = _socket;
        socket.ExclusiveAddressUse = false;
        socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

        // SO_REUSEPORT (Linux) — required when multiple sockets bind the same multicast (group, port).
        // The kernel load-balances datagrams across all sockets, multiplying the effective
        // per-socket receive buffer and parallelizing receive work across threads.
        if (config.ReceiveSocketCount > 1 && OperatingSystem.IsLinux())
        {
            const int SOL_SOCKET = 1;
            const int SO_REUSEPORT = 15;
            try
            {
                Span<byte> one = stackalloc byte[sizeof(int)];
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(one, 1);
                socket.SetRawSocketOption(SOL_SOCKET, SO_REUSEPORT, one);
            }
            catch (SocketException ex)
            {
                _logger.LogWarning(ex,
                    "SO_REUSEPORT not available for {ChannelType} (group {ChannelGroup}); receive will fall back to a single socket per channel.",
                    config.Type, config.ChannelGroup);
            }
        }

        socket.ReceiveBufferSize = config.ReceiveBufferBytes;
        int actualReceiveBufferBytes = socket.ReceiveBufferSize;
        socket.Bind(new IPEndPoint(IPAddress.Any, config.Port));

        if (actualReceiveBufferBytes < config.ReceiveBufferBytes)
        {
            _logger.LogWarning(
                "UDP receive buffer was clamped by the OS for {ChannelType} (group {ChannelGroup}): requested {RequestedBytes} bytes, got {ActualBytes} bytes. " +
                "This causes packet loss under burst. Raise the host kernel limit with `sudo sysctl -w net.core.rmem_max={RequiredMax}` (and rmem_default), persisted in /etc/sysctl.d/. " +
                "On WSL2, persist via systemd or wsl.conf [boot] command.",
                _channelType,
                _channelGroup,
                config.ReceiveBufferBytes,
                actualReceiveBufferBytes,
                config.ReceiveBufferBytes);
        }

        if (config.SourceAddress is not null)
        {
            var localAddress = config.LocalAddress ?? IPAddress.Any;
            socket.SetSocketOption(
                SocketOptionLevel.IP,
                SocketOptionName.AddSourceMembership,
                BuildSourceMembershipRequest(config.MulticastGroup, localAddress, config.SourceAddress));

            _logger.LogInformation(
                "Joined SSM group {Group}:{Port} source {SourceAddress} via {LocalAddress} ({ChannelType}, group {ChannelGroup}, recvBuffer={ReceiveBufferBytes})",
                config.MulticastGroup, config.Port, config.SourceAddress, localAddress, _channelType, _channelGroup, actualReceiveBufferBytes);
        }
        else if (config.LocalAddress is not null)
        {
            socket.SetSocketOption(
                SocketOptionLevel.IP,
                SocketOptionName.AddMembership,
                new MulticastOption(config.MulticastGroup, config.LocalAddress));
            _logger.LogInformation(
                "Joined multicast group {Group}:{Port} via {LocalAddress} ({ChannelType}, group {ChannelGroup}, recvBuffer={ReceiveBufferBytes})",
                config.MulticastGroup, config.Port, config.LocalAddress, _channelType, _channelGroup, actualReceiveBufferBytes);
        }
        else
        {
            socket.SetSocketOption(
                SocketOptionLevel.IP,
                SocketOptionName.AddMembership,
                new MulticastOption(config.MulticastGroup));
            _logger.LogInformation(
                "Joined multicast group {Group}:{Port} on any local interface ({ChannelType}, group {ChannelGroup}, recvBuffer={ReceiveBufferBytes})",
                config.MulticastGroup, config.Port, _channelType, _channelGroup, actualReceiveBufferBytes);
        }
        _isJoined = true;
        Interlocked.Increment(ref _membershipJoins);
    }

    /// <summary>
    /// Drops the multicast membership so the kernel stops delivering datagrams to this socket.
    /// The receive thread will block in Receive() until the membership is rejoined or the socket is disposed.
    /// Idempotent and thread-safe.
    /// </summary>
    public void LeaveMulticastGroup()
    {
        lock (_membershipLock)
        {
            if (!_isJoined) return;
            try
            {
                if (_sourceAddress is not null)
                {
                    var localAddress = _localAddress ?? IPAddress.Any;
                    _socket.SetSocketOption(
                        SocketOptionLevel.IP,
                        SocketOptionName.DropSourceMembership,
                        BuildSourceMembershipRequest(_multicastGroup, localAddress, _sourceAddress));
                }
                else if (_localAddress is not null)
                {
                    _socket.SetSocketOption(
                        SocketOptionLevel.IP,
                        SocketOptionName.DropMembership,
                        new MulticastOption(_multicastGroup, _localAddress));
                }
                else
                {
                    _socket.SetSocketOption(
                        SocketOptionLevel.IP,
                        SocketOptionName.DropMembership,
                        new MulticastOption(_multicastGroup));
                }
                _isJoined = false;
                Interlocked.Increment(ref _membershipLeaves);
                _logger.LogInformation(
                    "Left multicast group {Group}:{Port} ({ChannelType}, group {ChannelGroup})",
                    _multicastGroup, _port, _channelType, _channelGroup);
            }
            catch (ObjectDisposedException) { /* socket disposed during shutdown */ }
            catch (SocketException ex)
            {
                _logger.LogWarning(ex,
                    "Failed to leave multicast group {Group}:{Port} ({ChannelType}, group {ChannelGroup})",
                    _multicastGroup, _port, _channelType, _channelGroup);
            }
        }
    }

    /// <summary>
    /// Rejoins the multicast group using the same parameters as the initial join.
    /// IGMP join takes ~100-300ms before the kernel begins forwarding datagrams.
    /// Idempotent and thread-safe.
    /// </summary>
    public void RejoinMulticastGroup()
    {
        lock (_membershipLock)
        {
            if (_isJoined) return;
            try
            {
                if (_sourceAddress is not null)
                {
                    var localAddress = _localAddress ?? IPAddress.Any;
                    _socket.SetSocketOption(
                        SocketOptionLevel.IP,
                        SocketOptionName.AddSourceMembership,
                        BuildSourceMembershipRequest(_multicastGroup, localAddress, _sourceAddress));
                }
                else if (_localAddress is not null)
                {
                    _socket.SetSocketOption(
                        SocketOptionLevel.IP,
                        SocketOptionName.AddMembership,
                        new MulticastOption(_multicastGroup, _localAddress));
                }
                else
                {
                    _socket.SetSocketOption(
                        SocketOptionLevel.IP,
                        SocketOptionName.AddMembership,
                        new MulticastOption(_multicastGroup));
                }
                _isJoined = true;
                Interlocked.Increment(ref _membershipJoins);
                _logger.LogInformation(
                    "Rejoined multicast group {Group}:{Port} ({ChannelType}, group {ChannelGroup})",
                    _multicastGroup, _port, _channelType, _channelGroup);
            }
            catch (ObjectDisposedException) { /* socket disposed during shutdown */ }
            catch (SocketException ex)
            {
                _logger.LogWarning(ex,
                    "Failed to rejoin multicast group {Group}:{Port} ({ChannelType}, group {ChannelGroup})",
                    _multicastGroup, _port, _channelType, _channelGroup);
            }
        }
    }

    public async ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(MaxDatagramSize);
        try
        {
            int received = await _socket.ReceiveAsync(buffer.AsMemory(), SocketFlags.None, ct);
            var lease = new ArrayPoolPacketLease(buffer);
            return UmdfPacket.CreateOwned(
                new ReadOnlyMemory<byte>(buffer, 0, received),
                _channelType,
                _channelGroup,
                Environment.TickCount64,
                lease);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    /// <summary>
    /// Blocking synchronous receive intended for a dedicated receive thread.
    /// Avoids the async state machine and continuation scheduling per datagram, which keeps
    /// the kernel UDP buffer drained as fast as possible during bursts.
    /// Cancellation is signalled by disposing the socket; this method will then throw
    /// <see cref="ObjectDisposedException"/> or <see cref="SocketException"/>.
    /// </summary>
    public UmdfPacket Receive()
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(MaxDatagramSize);
        try
        {
            int received = _socket.Receive(buffer, 0, MaxDatagramSize, SocketFlags.None);
            var lease = new ArrayPoolPacketLease(buffer);
            return UmdfPacket.CreateOwned(
                new ReadOnlyMemory<byte>(buffer, 0, received),
                _channelType,
                _channelGroup,
                Environment.TickCount64,
                lease);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    /// <summary>
    /// True if batched receive (recvmmsg) is supported on the current OS. Linux only.
    /// </summary>
    public static bool IsBatchReceiveSupported => OperatingSystem.IsLinux();

    /// <summary>
    /// Blocks until at least one datagram is available, then drains up to destination.Length packets
    /// (max <see cref="MaxBatchSize"/>) in a single recvmmsg(2) syscall. Linux-only; on other
    /// platforms throws PlatformNotSupportedException — call <see cref="Receive"/> instead.
    /// Returns the number of packets written to destination.
    /// Cancellation is signalled by disposing the socket; this method will then throw
    /// <see cref="ObjectDisposedException"/> or <see cref="SocketException"/>.
    /// </summary>
    public int ReceiveBatch(Span<UmdfPacket> destination)
    {
        if (!OperatingSystem.IsLinux())
            throw new PlatformNotSupportedException("ReceiveBatch requires Linux (recvmmsg).");
        if (destination.IsEmpty)
            return 0;

        EnsureBatchStateInitialized();
        int batchSize = Math.Min(destination.Length, MaxBatchSize);

        var mmsghdrs = _batchMmsghdrs!;
        var pending = _batchPendingBuffers!;
        var pool = _batchBufferPool!;

        // Buffers are pre-rented and bound to iovecs in EnsureBatchStateInitialized,
        // and re-bound only for slots consumed by the previous call. The hot path
        // here is just the syscall plus per-received-packet ownership transfer —
        // no per-syscall over-rent (was costing ~62 Rent/Return ops per call when
        // avg batch was ~2 datagrams).

        IntPtr fd = _socket.SafeHandle.DangerousGetHandle();
        int n;
        while (true)
        {
            unsafe
            {
                fixed (LinuxNative.Mmsghdr* hdrPtr = mmsghdrs)
                {
                    n = LinuxNative.recvmmsg((int)fd, (IntPtr)hdrPtr, (uint)batchSize, LinuxNative.MSG_WAITFORONE, IntPtr.Zero);
                }
            }
            if (n >= 0) break;
            int err = Marshal.GetLastPInvokeError();
            if (err == LinuxNative.EINTR) continue;
            throw new SocketException(err);
        }

        long ticks = Environment.TickCount64;
        var iovecs = _batchIovecs!;
        for (int i = 0; i < n; i++)
        {
            var buf = pending[i]!;
            int received = (int)mmsghdrs[i].msg_len;
            var lease = new PinnedPoolPacketLease(buf, pool);
            destination[i] = UmdfPacket.CreateOwned(
                new ReadOnlyMemory<byte>(buf, 0, received),
                _channelType,
                _channelGroup,
                ticks,
                lease);

            // Refill the slot for the next syscall: rent a fresh buffer and rebind iovec.
            // Address is stable for POH arrays, so we capture it once via fixed and store.
            var newBuf = pool.Rent();
            pending[i] = newBuf;
            unsafe
            {
                fixed (byte* p = newBuf)
                {
                    iovecs[i].iov_base = (IntPtr)p;
                }
            }
        }
        if (n > 0)
        {
            Interlocked.Increment(ref _batchedSyscalls);
            Interlocked.Add(ref _batchedDatagrams, n);
        }
        return n;
    }

    private void EnsureBatchStateInitialized()
    {
        if (_batchBufferPool is not null) return;
        _batchBufferPool = new PinnedBufferPool(MaxDatagramSize);
        _batchIovecs = new LinuxNative.Iovec[MaxBatchSize];
        _batchMmsghdrs = new LinuxNative.Mmsghdr[MaxBatchSize];
        _batchPendingBuffers = new byte[MaxBatchSize][];
        _batchIovecsHandle = GCHandle.Alloc(_batchIovecs, GCHandleType.Pinned);
        _batchMmsghdrsHandle = GCHandle.Alloc(_batchMmsghdrs, GCHandleType.Pinned);

        // Wire each mmsghdr to its iovec (one iovec per datagram), pre-rent a buffer
        // for each slot, and bind the iovec base/length once. POH addresses are stable,
        // so we never need to re-pin in the hot path; the slot is only re-bound when
        // the buffer is consumed by a packet (in ReceiveBatch).
        IntPtr iovBase = _batchIovecsHandle.AddrOfPinnedObject();
        int iovecSize = Marshal.SizeOf<LinuxNative.Iovec>();
        for (int i = 0; i < MaxBatchSize; i++)
        {
            _batchMmsghdrs[i].msg_hdr.msg_iov = iovBase + i * iovecSize;
            _batchMmsghdrs[i].msg_hdr.msg_iovlen = (nuint)1;

            var buf = _batchBufferPool.Rent();
            _batchPendingBuffers[i] = buf;
            unsafe
            {
                fixed (byte* p = buf)
                {
                    _batchIovecs[i].iov_base = (IntPtr)p;
                }
            }
            _batchIovecs[i].iov_len = (nuint)MaxDatagramSize;
        }
    }

    internal static byte[] BuildSourceMembershipRequest(IPAddress multicastGroup, IPAddress localAddress, IPAddress sourceAddress)
    {
        ValidateIPv4(multicastGroup, nameof(multicastGroup));
        ValidateIPv4(localAddress, nameof(localAddress));
        ValidateIPv4(sourceAddress, nameof(sourceAddress));

        var request = new byte[SourceMembershipRequestSize];
        multicastGroup.GetAddressBytes().CopyTo(request, 0);
        localAddress.GetAddressBytes().CopyTo(request, 4);
        sourceAddress.GetAddressBytes().CopyTo(request, 8);
        return request;
    }

    private static void ValidateIPv4(IPAddress address, string paramName)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork)
            throw new NotSupportedException($"{paramName} must be an IPv4 address for multicast UDP.");
    }

    public void Dispose()
    {
        _socket.Dispose();
        if (_batchIovecsHandle.IsAllocated) _batchIovecsHandle.Free();
        if (_batchMmsghdrsHandle.IsAllocated) _batchMmsghdrsHandle.Free();
    }
}
