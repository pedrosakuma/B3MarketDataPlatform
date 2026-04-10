namespace B3.Umdf.Transport;

public interface IPacketSource : IDisposable
{
    ValueTask<UmdfPacket> ReceiveAsync(CancellationToken ct = default);
}
