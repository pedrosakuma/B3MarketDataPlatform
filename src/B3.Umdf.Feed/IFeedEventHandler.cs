using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public interface IFeedEventHandler
{
    void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId);
    void OnGapDetected(uint expected, uint received);
    void OnSequenceReset();
    void OnSnapshotStart();
    void OnSnapshotComplete(uint lastRptSeq);
    void OnInstrumentDefinitionsComplete(int instrumentCount);

    /// <summary>
    /// Called after all SBE messages in a UMDF packet have been dispatched.
    /// Used as a batch boundary for upstream conflation.
    /// </summary>
    void OnPacketProcessed() { }
}
