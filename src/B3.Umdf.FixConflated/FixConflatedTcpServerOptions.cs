namespace B3.Umdf.FixConflated;

public sealed class FixConflatedTcpServerOptions
{
    public int OutboundQueueCapacity { get; init; } = 4096;
    public int AcceptBacklog { get; init; } = 128;
    public FixSessionOptions SessionOptions { get; init; } = new();
}
