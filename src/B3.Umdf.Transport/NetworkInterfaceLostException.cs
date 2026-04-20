namespace B3.Umdf.Transport;

/// <summary>
/// Thrown by <see cref="MulticastPacketPublisher"/> when the underlying network interface
/// or route appears to be permanently gone (e.g., the shared network namespace was
/// destroyed because the consumer container died). Callers should treat this as terminal
/// and let the publisher process exit so the orchestrator does not keep it as a zombie
/// flooding logs with ENETUNREACH errors.
/// </summary>
public sealed class NetworkInterfaceLostException : Exception
{
    public NetworkInterfaceLostException(string message, Exception inner)
        : base(message, inner) { }
}
