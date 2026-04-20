using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Transport;

public sealed class MulticastPacketPublisher : IDisposable
{
    private const int DefaultSendBufferBytes = 4 * 1024 * 1024;
    private const int DefaultSendTimeoutMs = 1_000;
    /// <summary>
    /// Consecutive fatal-network errors per route before <see cref="Publish"/> throws and
    /// terminates the publisher loop. "Fatal" = the kernel says the destination is unreachable
    /// (no route, interface down). These don't recover on their own — usually the network
    /// namespace went away (in our compose setup the publisher shares it with the consumer,
    /// so when consumer dies we get ENETUNREACH on every send).
    /// </summary>
    private const int FatalNetworkErrorThreshold = 100;
    private const int LogThrottleMs = 1_000;
    /// <summary>
    /// Default batch size used when batching is enabled. 8 messages is a deliberate balance:
    /// large enough to cut the syscall rate by ~8× vs per-datagram <c>sendto</c>, but small
    /// enough that the resulting microburst on the wire fits comfortably inside the consumer's
    /// kernel UDP receive buffer between drain cycles. Larger batches (32+) tend to overrun the
    /// consumer's recvmmsg loop in loopback configurations and induce kernel-side packet drops.
    /// </summary>
    public const int DefaultBatchSize = 8;

    private readonly Dictionary<(int ChannelGroup, ChannelType Type), IPublishSocket> _channels;
    private readonly ILogger _logger;
    private long _publishedPackets;
    private long _publishedBytes;
    private long _lastPublishTimestampTicks;
    private long _droppedPackets;
    private long _lastWarnTickMs;
    private int _consecutiveFatalNetErrors;

    public MulticastPacketPublisher(
        IReadOnlyList<MulticastPublishChannelConfig> configs,
        ILogger<MulticastPacketPublisher>? logger = null,
        int batchSize = 1)
        : this(configs, logger, MakeDefaultFactory(batchSize))
    {
    }

    internal MulticastPacketPublisher(
        IReadOnlyList<MulticastPublishChannelConfig> configs,
        ILogger<MulticastPacketPublisher>? logger,
        SocketFactory socketFactory)
    {
        ArgumentNullException.ThrowIfNull(configs);
        ArgumentNullException.ThrowIfNull(socketFactory);
        ArgumentOutOfRangeException.ThrowIfLessThan(configs.Count, 1);

        _logger = logger ?? NullLogger<MulticastPacketPublisher>.Instance;
        _channels = new Dictionary<(int ChannelGroup, ChannelType Type), IPublishSocket>(configs.Count);

        foreach (var config in configs)
        {
            ValidateIPv4(config.MulticastGroup, nameof(config.MulticastGroup));
            if (config.LocalAddress is not null)
                ValidateIPv4(config.LocalAddress, nameof(config.LocalAddress));

            var key = (config.ChannelGroup, config.Type);
            if (_channels.ContainsKey(key))
                throw new InvalidOperationException(
                    $"Duplicate multicast publish route for group {config.ChannelGroup}, channel {config.Type}.");

            var endpoint = new IPEndPoint(config.MulticastGroup, config.Port);
            _channels[key] = socketFactory(config, endpoint);
        }

        _logger.LogInformation("Configured {RouteCount} multicast publish routes", _channels.Count);
    }

    public long PublishedPackets => Volatile.Read(ref _publishedPackets);
    public long PublishedBytes => Volatile.Read(ref _publishedBytes);
    public long LastPublishTimestampTicks => Volatile.Read(ref _lastPublishTimestampTicks);
    public long DroppedPackets => Volatile.Read(ref _droppedPackets);

    public void Publish(in UmdfPacket packet)
    {
        if (!_channels.TryGetValue((packet.ChannelGroup, packet.Channel), out var socket))
        {
            throw new InvalidOperationException(
                $"No multicast publish route configured for group {packet.ChannelGroup}, channel {packet.Channel}.");
        }

        // Snapshot pending count before the call so we can attribute drops correctly when an
        // auto-flush triggered by this Send() throws (the whole batch is considered dropped).
        int pendingBefore = socket.Pending;
        int messagesFlushed;
        int bytesFlushed;
        try
        {
            socket.Send(packet.Data, out messagesFlushed, out bytesFlushed);
        }
        catch (Exception ex) when (ex is SocketException or IOException)
        {
            int attempted = pendingBefore + 1;
            HandleSendError(ex, packet.ChannelGroup, packet.Channel, attempted);
            return;
        }

        if (messagesFlushed > 0)
            OnFlushed(messagesFlushed, bytesFlushed);
    }

    /// <summary>
    /// Force-flush any buffered datagrams across all sockets. Call before shutdown to drain
    /// partial batches; otherwise the last few datagrams from each route would be lost.
    /// </summary>
    public void Flush()
    {
        foreach (var kv in _channels)
        {
            var socket = kv.Value;
            int pendingBefore = socket.Pending;
            if (pendingBefore == 0) continue;
            int messagesFlushed;
            int bytesFlushed;
            try
            {
                socket.Flush(out messagesFlushed, out bytesFlushed);
            }
            catch (Exception ex) when (ex is SocketException or IOException)
            {
                HandleSendError(ex, kv.Key.ChannelGroup, kv.Key.Type, pendingBefore);
                continue;
            }
            if (messagesFlushed > 0)
                OnFlushed(messagesFlushed, bytesFlushed);
        }
    }

    private void OnFlushed(int messages, int bytes)
    {
        if (Volatile.Read(ref _consecutiveFatalNetErrors) != 0)
            Interlocked.Exchange(ref _consecutiveFatalNetErrors, 0);
        Interlocked.Add(ref _publishedPackets, messages);
        Interlocked.Add(ref _publishedBytes, bytes);
        Volatile.Write(ref _lastPublishTimestampTicks, Environment.TickCount64);
    }

    private void HandleSendError(Exception ex, int channelGroup, ChannelType channel, int attempted)
    {
        long dropped = Interlocked.Add(ref _droppedPackets, attempted);
        bool isFatalNet = IsFatalNetworkError(ex);
        int consecutive = isFatalNet
            ? Interlocked.Add(ref _consecutiveFatalNetErrors, attempted)
            : Interlocked.Exchange(ref _consecutiveFatalNetErrors, 0);

        if (ShouldLog(dropped))
        {
            _logger.LogWarning(ex,
                "Multicast publish dropped {Attempted} datagram(s) for group {ChannelGroup}, channel {Channel} (total dropped: {Dropped}, consecutive fatal-net: {ConsecFatal}). Continuing.",
                attempted, channelGroup, channel, dropped, consecutive);
        }

        if (isFatalNet && consecutive >= FatalNetworkErrorThreshold)
        {
            _logger.LogError(ex,
                "Multicast publish has hit {Threshold} consecutive fatal network errors for group {ChannelGroup}, channel {Channel}. " +
                "Aborting publisher — the underlying interface or route appears to be gone.",
                FatalNetworkErrorThreshold, channelGroup, channel);
            throw new NetworkInterfaceLostException(
                $"Multicast publish unreachable for {FatalNetworkErrorThreshold}+ consecutive datagrams " +
                $"(group {channelGroup}, channel {channel}). Last error: {ex.Message}",
                ex);
        }
    }

    private static bool IsFatalNetworkError(Exception ex)
    {
        var sock = ex as SocketException ?? ex.InnerException as SocketException;
        return sock?.SocketErrorCode is SocketError.NetworkUnreachable
                                       or SocketError.HostUnreachable
                                       or SocketError.NetworkDown
                                       or SocketError.HostDown;
    }

    private bool ShouldLog(long dropped)
    {
        // Always log the first warning; afterwards rate-limit to ~1/second to avoid flooding.
        if (dropped == 1)
            return true;
        long now = Environment.TickCount64;
        long last = Volatile.Read(ref _lastWarnTickMs);
        if (now - last < LogThrottleMs)
            return false;
        return Interlocked.CompareExchange(ref _lastWarnTickMs, now, last) == last;
    }

    public void Dispose()
    {
        // Best-effort drain on dispose so callers that forget to Flush() don't silently lose
        // the last partial batch. Errors here are swallowed — Dispose must not throw.
        foreach (var kv in _channels)
        {
            var socket = kv.Value;
            if (socket.Pending == 0) continue;
            try { socket.Flush(out _, out _); }
            catch (Exception ex) when (ex is SocketException or IOException) { /* ignore */ }
        }
        foreach (var socket in _channels.Values)
            socket.Dispose();
    }

    private static SocketFactory MakeDefaultFactory(int batchSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        if (batchSize == 1 || !OperatingSystem.IsLinux())
            return CreateSocket;
        return (config, endpoint) => new SendmmsgPublishSocket(config, endpoint, batchSize);
    }

    private static IPublishSocket CreateSocket(MulticastPublishChannelConfig config, IPEndPoint endpoint) =>
        new SocketPublishSocket(config, endpoint);

    private static void ValidateIPv4(IPAddress address, string paramName)
    {
        if (address.AddressFamily != AddressFamily.InterNetwork)
            throw new NotSupportedException($"{paramName} must be an IPv4 address for multicast UDP.");
    }

    internal delegate IPublishSocket SocketFactory(MulticastPublishChannelConfig config, IPEndPoint endpoint);

    internal interface IPublishSocket : IDisposable
    {
        /// <summary>
        /// Enqueue <paramref name="payload"/> for sending. May trigger an auto-flush of the
        /// internal batch. Sets <paramref name="messagesFlushed"/> and <paramref name="bytesFlushed"/>
        /// to whatever was actually transmitted on the wire during this call (0/0 if the payload
        /// was just buffered).
        /// On error the implementation throws; the caller should treat <c>Pending + 1</c>
        /// (snapshotted before the call) as dropped.
        /// </summary>
        void Send(ReadOnlyMemory<byte> payload, out int messagesFlushed, out int bytesFlushed);

        /// <summary>Force-flush any buffered messages. Same accounting contract as <see cref="Send"/>.</summary>
        void Flush(out int messagesFlushed, out int bytesFlushed);

        /// <summary>Number of messages buffered awaiting flush. Always 0 for non-batching sockets.</summary>
        int Pending { get; }
    }

    private sealed class SocketPublishSocket : IPublishSocket
    {
        private readonly Socket _socket;
        private readonly IPEndPoint _endpoint;

        public SocketPublishSocket(MulticastPublishChannelConfig config, IPEndPoint endpoint)
        {
            _endpoint = endpoint;
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _socket.SendBufferSize = DefaultSendBufferBytes;
            _socket.SendTimeout = DefaultSendTimeoutMs;
            _socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastLoopback, true);

            if (config.LocalAddress is not null)
            {
                _socket.Bind(new IPEndPoint(config.LocalAddress, 0));
                _socket.SetSocketOption(
                    SocketOptionLevel.IP,
                    SocketOptionName.MulticastInterface,
                    config.LocalAddress.GetAddressBytes());
            }
        }

        public int Pending => 0;

        public void Send(ReadOnlyMemory<byte> payload, out int messagesFlushed, out int bytesFlushed)
        {
            int sent;
            try
            {
                sent = _socket.SendTo(payload.Span, SocketFlags.None, _endpoint);
            }
            catch (SocketException ex) when (ex.SocketErrorCode is SocketError.TimedOut
                                                 or SocketError.WouldBlock
                                                 or SocketError.NoBufferSpaceAvailable)
            {
                throw new IOException(
                    $"Multicast send stalled or buffer space was exhausted for {_endpoint}.",
                    ex);
            }
            if (sent != payload.Length)
                throw new IOException($"Partial multicast send for {_endpoint}: {sent}/{payload.Length} bytes.");
            messagesFlushed = 1;
            bytesFlushed = sent;
        }

        public void Flush(out int messagesFlushed, out int bytesFlushed)
        {
            messagesFlushed = 0;
            bytesFlushed = 0;
        }

        public void Dispose() => _socket.Dispose();
    }

    /// <summary>
    /// Linux-only batched UDP publisher backed by sendmmsg(2). Buffers up to <see cref="_batchCapacity"/>
    /// datagrams in pinned scratch storage and flushes them in a single syscall, which is the
    /// classic trick to push UDP throughput past the per-datagram syscall ceiling.
    /// All native pointers (iovec/mmsghdr/scratch buffers) live in pinned managed memory for the
    /// socket's entire lifetime, so there is zero pinning work on the hot path.
    /// </summary>
    private sealed class SendmmsgPublishSocket : IPublishSocket
    {
        // Datagrams larger than this are sent immediately bypassing the batch (avoids OOR copies).
        private const int MaxDatagramSize = 1500;

        private readonly Socket _socket;
        private readonly IPEndPoint _endpoint;
        private readonly int _batchCapacity;

        // Pinned scratch storage. One contiguous staging buffer of `_batchCapacity * MaxDatagramSize`
        // bytes — each enqueued payload is copied into its slot so the caller's memory does not
        // need to remain valid until flush.
        private readonly byte[] _scratch;
        private readonly LinuxNative.Iovec[] _iovecs;
        private readonly LinuxNative.Mmsghdr[] _mmsghdrs;
        private readonly GCHandle _scratchHandle;
        private readonly GCHandle _iovecsHandle;
        private readonly GCHandle _mmsghdrsHandle;

        private int _pending;
        private bool _disposed;

        public SendmmsgPublishSocket(MulticastPublishChannelConfig config, IPEndPoint endpoint, int batchSize)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 2);
            _endpoint = endpoint;
            _batchCapacity = batchSize;
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _socket.SendBufferSize = DefaultSendBufferBytes;
            _socket.SendTimeout = DefaultSendTimeoutMs;
            _socket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastLoopback, true);

            if (config.LocalAddress is not null)
            {
                _socket.Bind(new IPEndPoint(config.LocalAddress, 0));
                _socket.SetSocketOption(
                    SocketOptionLevel.IP,
                    SocketOptionName.MulticastInterface,
                    config.LocalAddress.GetAddressBytes());
            }

            // Connect() so the kernel skips per-call destination resolution and sendmmsg can omit
            // msg_name on every entry. Our IPublishSocket is single-destination by construction.
            _socket.Connect(endpoint);

            _scratch = GC.AllocateUninitializedArray<byte>(batchSize * MaxDatagramSize, pinned: true);
            _iovecs = new LinuxNative.Iovec[batchSize];
            _mmsghdrs = new LinuxNative.Mmsghdr[batchSize];
            _scratchHandle = GCHandle.Alloc(_scratch, GCHandleType.Pinned);
            _iovecsHandle = GCHandle.Alloc(_iovecs, GCHandleType.Pinned);
            _mmsghdrsHandle = GCHandle.Alloc(_mmsghdrs, GCHandleType.Pinned);

            // Wire each iovec to its slot in the staging buffer once; iov_len is updated per send.
            IntPtr scratchBase = _scratchHandle.AddrOfPinnedObject();
            IntPtr iovBase = _iovecsHandle.AddrOfPinnedObject();
            int iovecSize = Marshal.SizeOf<LinuxNative.Iovec>();
            for (int i = 0; i < batchSize; i++)
            {
                _iovecs[i].iov_base = scratchBase + i * MaxDatagramSize;
                _iovecs[i].iov_len = 0;
                // msg_name stays IntPtr.Zero — the socket is connected, so the kernel uses the
                // associated peer for every datagram.
                _mmsghdrs[i].msg_hdr.msg_iov = iovBase + i * iovecSize;
                _mmsghdrs[i].msg_hdr.msg_iovlen = (nuint)1;
                _mmsghdrs[i].msg_len = 0;
            }
        }

        public int Pending => _pending;

        public void Send(ReadOnlyMemory<byte> payload, out int messagesFlushed, out int bytesFlushed)
        {
            if (payload.Length > MaxDatagramSize)
            {
                // Oversized datagram — flush whatever's pending then fall back to single-shot send.
                if (_pending > 0)
                    Flush(out messagesFlushed, out bytesFlushed);
                else
                {
                    messagesFlushed = 0;
                    bytesFlushed = 0;
                }
                int sent = _socket.Send(payload.Span, SocketFlags.None);
                if (sent != payload.Length)
                    throw new IOException($"Partial multicast send for {_endpoint}: {sent}/{payload.Length} bytes.");
                messagesFlushed += 1;
                bytesFlushed += sent;
                return;
            }

            // Copy into our staging slot so the caller's buffer can be reused immediately.
            int slot = _pending;
            payload.Span.CopyTo(_scratch.AsSpan(slot * MaxDatagramSize, payload.Length));
            _iovecs[slot].iov_len = (nuint)payload.Length;
            _pending++;

            if (_pending >= _batchCapacity)
            {
                Flush(out messagesFlushed, out bytesFlushed);
            }
            else
            {
                messagesFlushed = 0;
                bytesFlushed = 0;
            }
        }

        public void Flush(out int messagesFlushed, out int bytesFlushed)
        {
            messagesFlushed = 0;
            bytesFlushed = 0;
            if (_pending == 0) return;

            int toSend = _pending;
            // Reset _pending up-front so a throw mid-flush doesn't leave stale state visible.
            _pending = 0;

            IntPtr fd = _socket.SafeHandle.DangerousGetHandle();
            int n;
            unsafe
            {
                fixed (LinuxNative.Mmsghdr* hdrPtr = _mmsghdrs)
                {
                    n = LinuxNative.sendmmsg((int)fd, (IntPtr)hdrPtr, (uint)toSend, 0);
                }
            }
            if (n < 0)
            {
                int err = Marshal.GetLastPInvokeError();
                var sockEx = new SocketException(err);
                if (sockEx.SocketErrorCode is SocketError.WouldBlock
                                              or SocketError.TryAgain
                                              or SocketError.NoBufferSpaceAvailable
                                              or SocketError.TimedOut)
                {
                    throw new IOException(
                        $"sendmmsg stalled or buffer space exhausted for {_endpoint} (batch={toSend}).",
                        sockEx);
                }
                throw sockEx;
            }
            // sendmmsg returns the number of messages successfully transmitted. The kernel may
            // accept fewer than requested if a transient error occurs after some succeeded — we
            // count the missing tail as dropped (treat them as a partial send error).
            for (int i = 0; i < n; i++)
            {
                bytesFlushed += (int)_mmsghdrs[i].msg_len;
                _iovecs[i].iov_len = 0;
            }
            messagesFlushed = n;
            if (n < toSend)
            {
                throw new IOException(
                    $"sendmmsg partially flushed batch for {_endpoint}: {n}/{toSend} messages. {toSend - n} dropped.");
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            try { _socket.Dispose(); } catch { /* ignore */ }
            if (_scratchHandle.IsAllocated) _scratchHandle.Free();
            if (_iovecsHandle.IsAllocated) _iovecsHandle.Free();
            if (_mmsghdrsHandle.IsAllocated) _mmsghdrsHandle.Free();
        }
    }
}
