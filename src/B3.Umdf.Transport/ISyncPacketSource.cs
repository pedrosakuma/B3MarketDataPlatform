namespace B3.Umdf.Transport;

/// <summary>
/// Synchronous packet source for high-throughput scenarios (e.g. PCAP replay).
/// Avoids async/await overhead per packet.
/// </summary>
public interface ISyncPacketSource : IDisposable
{
    bool TryReceive(out UmdfPacket packet);
}
